"""Build wikidata records for people, groups and what they touch.

Walks the wikidata *data* cache in parallel slices, in two passes, with a
combine step between them:

    # pass 1: type every record; map and cache every Person and Group
    for n in `seq 0 23`; do python wikidata-slice-build.py pass1 $n 24 & done; wait
    # dedupe the per-slice outputs into the two sets pass 2 needs
    python wikidata-slice-build.py combine 24
    # pass 2: map and cache the rest that people point at, or that point at people
    for n in `seq 0 23`; do python wikidata-slice-build.py pass2 $n 24 & done; wait

Files, all in --out-dir (default: <data_dir>/wikidata-build):

    types_<n>.tsv     pass 1   Qnnn<TAB>Type for every record in slice n
    refs_<n>.txt      pass 1   Q-ids that slice n's people and groups refer to,
                               unique within the slice, the referrer excluded
    persons.npy       combine  every Person Q-id, sorted uint32
    refs.npy          combine  refs_*.txt deduplicated across slices, sorted uint32

Each slice file is written as .tmp and renamed when the slice finishes, so
combine can refuse to run over a slice that died part way.

Pass 2 maps a record when it is not a Person, Group or scholarly article and
either (a) it is in refs.npy, or (b) its mapped form refers to a Person. For
(b) the raw record is checked first -- only a record with a person Q-id among
its property values is mapped at all -- because mapping every record in the
cache just to look would cost far more than the records it finds.

The sets are numpy arrays rather than python sets: persons alone is ~10M
ids, which as a set of ints is most of a gigabyte in every one of the
workers; as uint32 it is 40MB.

References are taken from the *mapped* record, not the raw one: the mapper
drops most properties, and a Q-id that never makes it into the Linked Art
is not something the record needs to resolve.
"""

import argparse
import glob
import os
import re
import sys
import time
import traceback

import numpy as np
import ujson
from dotenv import load_dotenv

from pipeline.config import Config

QID = re.compile(r"Q[1-9][0-9]*$")
UINT32_MAX = np.iinfo(np.uint32).max
SCHOLARLY_ARTICLE = "Q13442814"
CACHED_TYPES = ("Person", "Group")


def setup():
    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    cfgs.cache_globals()
    cfgs.instantiate_all()
    return cfgs, cfgs.external["wikidata"]


def out_dir(args, cfgs):
    d = args.out_dir or os.path.join(cfgs.data_dir, "wikidata-build")
    os.makedirs(d, exist_ok=True)
    return d


def raw_qids(data):
    """Q-ids among a raw record's property values. The fetcher stores
    wikibase-entityid values as bare "Qnnn" strings, so anything else in a
    property list (dates, quantities, external ids) is not a reference."""
    out = set()
    for k, vals in data.items():
        if k[0] != "P" or not isinstance(vals, list):
            continue
        for v in vals:
            if isinstance(v, str) and v[0] == "Q" and QID.match(v):
                out.add(v)
    out.discard(data["id"])
    return out


def mapped_qids(node, ns, out):
    """Q-ids of every wikidata URI in a mapped record."""
    if isinstance(node, dict):
        i = node.get("id")
        if isinstance(i, str) and i.startswith(ns):
            q = i[len(ns):]
            if QID.match(q):
                out.add(q)
        for v in node.values():
            if isinstance(v, (dict, list)):
                mapped_qids(v, ns, out)
    elif isinstance(node, list):
        for v in node:
            mapped_qids(v, ns, out)
    return out


def qnums(qids):
    # malformed values (e.g. Q5806108879) can't be real Q-ids, so can't match
    nums = [n for n in (int(q[1:]) for q in qids) if n <= UINT32_MAX]
    return np.array(nums, dtype=np.uint32)


def contains(sorted_arr, vals):
    """Boolean mask: which of vals are in sorted_arr."""
    if not len(sorted_arr) or not len(vals):
        return np.zeros(len(vals), dtype=bool)
    idx = np.searchsorted(sorted_arr, vals)
    idx[idx == len(sorted_arr)] = 0
    return sorted_arr[idx] == vals


class Builder:
    def __init__(self, cfgs, src):
        self.cfgs = cfgs
        self.mapper = src["mapper"]
        self.out_db = src["recordcache"]
        self.ns = self.mapper.namespace
        self.out_db.defer_commits(every=1000)

    def map(self, rec, rectype):
        try:
            rec2 = self.mapper.transform(rec, rectype)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            where = f" [{os.path.basename(tb[-1].filename)}:{tb[-1].lineno}]" if tb else ""
            print(f"Failed to map {rec['identifier']}: {e}{where}")
            return None
        if rec2 is not None:
            rec2["identifier"] = rec["identifier"]
        return rec2

    def store(self, rec2):
        """Post-map and store, as Acquirer.acquire does, minus the
        recordcache read it starts with -- every record here is being built
        fresh, so that read would only ever cost an IOP."""
        rec3 = self.mapper.post_mapping(rec2, rec2["data"]["type"])
        if rec3 is None:
            return None
        rec3["identifier"] = self.cfgs.make_qua(rec3["identifier"], rec3["data"]["type"])
        self.out_db[rec3["identifier"]] = rec3
        self.out_db.checkpoint()
        return rec3

    def build(self, rec, rectype):
        rec2 = self.map(rec, rectype)
        return self.store(rec2) if rec2 is not None else None

    def flush(self):
        self.out_db.flush()


def iter_slice(src, my_slice, max_slice):
    for row in src["datacache"].iter_records_slice(my_slice, max_slice, raw=True):
        data = ujson.loads(row["data"])
        yield {"identifier": row["identifier"], "data": data}


def progress(label, n, built, start):
    el = time.time() - start
    print(f"  {label}: {n:,} read, {built:,} built, {n / el:,.0f}/s", flush=True)


def pass1(args):
    cfgs, src = setup()
    d = out_dir(args, cfgs)
    b = Builder(cfgs, src)
    types_path = os.path.join(d, f"types_{args.slice}.tsv")
    refs_path = os.path.join(d, f"refs_{args.slice}.txt")

    refs = set()
    n = built = 0
    start = time.time()
    with open(types_path + ".tmp", "w") as tfh:
        for rec in iter_slice(src, args.slice, args.max_slice):
            n += 1
            crmcls = b.mapper.guess_type(rec["data"])
            # None is a disambiguation page: not anything, so not typed
            if crmcls is not None:
                typ = crmcls.__name__
                tfh.write(f"{rec['data']['id']}\t{typ}\n")
                if typ in CACHED_TYPES:
                    rec3 = b.build(rec, typ)
                    if rec3 is not None:
                        built += 1
                        qs = mapped_qids(rec3["data"], b.ns, set())
                        qs.discard(rec["data"]["id"])
                        refs.update(qs)
            if args.progress and not n % args.progress:
                progress(f"pass1 {args.slice}", n, built, start)
    b.flush()

    with open(refs_path + ".tmp", "w") as rfh:
        for q in sorted(refs, key=lambda q: int(q[1:])):
            rfh.write(q + "\n")
    os.rename(types_path + ".tmp", types_path)
    os.rename(refs_path + ".tmp", refs_path)
    progress(f"pass1 {args.slice} done", n, built, start)
    print(f"  {len(refs):,} distinct references -> {refs_path}")


def combine(args):
    cfgs, _ = setup()
    d = out_dir(args, cfgs)
    missing = [i for i in range(args.max_slice)
               if not os.path.exists(os.path.join(d, f"types_{i}.tsv"))
               or not os.path.exists(os.path.join(d, f"refs_{i}.txt"))]
    if missing:
        sys.exit(f"pass1 has not finished slices {missing}")
    stray = glob.glob(os.path.join(d, "*.tmp"))
    if stray:
        print(f"warning: ignoring unfinished files {stray}")

    persons = []
    for i in range(args.max_slice):
        with open(os.path.join(d, f"types_{i}.tsv")) as fh:
            persons.extend(int(line[1:line.index("\t")]) for line in fh
                           if line.endswith("\tPerson\n"))
    persons = np.unique(np.array(persons, dtype=np.uint32))
    np.save(os.path.join(d, "persons.npy"), persons)
    print(f"{len(persons):,} persons -> persons.npy")

    parts = []
    total = 0
    for i in range(args.max_slice):
        with open(os.path.join(d, f"refs_{i}.txt")) as fh:
            a = np.array([int(line[1:]) for line in fh if line.strip()], dtype=np.uint32)
        total += len(a)
        parts.append(a)
    refs = np.unique(np.concatenate(parts)) if parts else np.array([], dtype=np.uint32)
    np.save(os.path.join(d, "refs.npy"), refs)
    print(f"{total:,} per-slice references, {len(refs):,} distinct -> refs.npy")


def load_scholarly_markers(path):
    with open(path) as fh:
        return frozenset(ujson.load(fh)["marker"])


def is_scholarly(data, markers):
    if SCHOLARLY_ARTICLE in data.get("P31", ()):
        return True
    return any(k in markers for k in data)


def pass2(args):
    cfgs, src = setup()
    d = out_dir(args, cfgs)
    persons = np.load(os.path.join(d, "persons.npy"))
    refs = np.load(os.path.join(d, "refs.npy"))
    markers = load_scholarly_markers(args.scholarly)
    print(f"{len(persons):,} persons, {len(refs):,} referenced from people, "
          f"{len(markers):,} scholarly marker properties")
    b = Builder(cfgs, src)

    n = built = by_ref = by_person = skipped = 0
    start = time.time()
    for rec in iter_slice(src, args.slice, args.max_slice):
        n += 1
        if args.progress and not n % args.progress:
            progress(f"pass2 {args.slice}", n, built, start)
        data = rec["data"]
        crmcls = b.mapper.guess_type(data)
        if crmcls is None or crmcls.__name__ in CACHED_TYPES:
            continue
        if is_scholarly(data, markers):
            skipped += 1
            continue
        typ = crmcls.__name__

        qid = data["id"]
        if QID.match(qid) and contains(refs, np.array([int(qid[1:])], dtype=np.uint32))[0]:
            if b.build(rec, typ) is not None:
                built += 1
                by_ref += 1
            continue

        cand = raw_qids(data)
        if not cand or not contains(persons, qnums(cand)).any():
            continue
        # the raw record names a person; build it, but only keep it if the
        # mapped record still does -- otherwise the mapper dropped the link
        rec2 = b.map(rec, typ)
        if rec2 is None:
            continue
        mq = mapped_qids(rec2["data"], b.ns, set())
        mq.discard(qid)
        if mq and contains(persons, qnums(mq)).any():
            if b.store(rec2) is not None:
                built += 1
                by_person += 1
    b.flush()
    progress(f"pass2 {args.slice} done", n, built, start)
    print(f"  {by_ref:,} referenced by people, {by_person:,} referring to people, "
          f"{skipped:,} scholarly skipped")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("pass1", "pass2"):
        p = sub.add_parser(name)
        p.add_argument("slice", type=int)
        p.add_argument("max_slice", type=int)
    sub.add_parser("combine").add_argument("max_slice", type=int)
    for p in sub.choices.values():
        p.add_argument("--out-dir", default="")
        p.add_argument("--progress", type=int, default=250_000,
                       help="records between progress lines; 0 to silence")
    sub.choices["pass2"].add_argument(
        "--scholarly", default="wikidata-scholarly-properties.json",
        help="properties whose presence marks a scholarly work")
    args = ap.parse_args()
    {"pass1": pass1, "combine": combine, "pass2": pass2}[args.cmd](args)


if __name__ == "__main__":
    main()
