"""Look a record up: where it is in every cache, its identity, and its JSON.

    lux show http://vocab.getty.edu/aat/300033618
    lux show aat:300033618                          # a curie
    lux show obj/12345 --source ycba                # a source's own identifier
    lux show 7f3a1c2e-...                            # a YUID: its merged record and members
    lux show aat:300033618 --cache recordcache      # and print that record
    lux show aat:300033618 --map                    # also map the stored raw record now
    lux show aat:300033618 --cache merged --validate
    lux show aat:300033618 --raw                    # everything, as JSON
    lux show --sample aat --cache recordcache --type Type -n 20

One line per stage: the source's datacache, recordcache and (external)
reconciled cache, the identity map (YUID and members), and the
rewritten, merged and MarkLogic records under the YUID; found or not,
under which key. Keys are tried in every form the pipeline writes
(lux_pipeline.records). Nothing is stored or fetched; ``--map`` runs the
source's mapper on the stored raw record (and builds every source, as a
build does, since mappers read each other). ``--validate`` checks the
``--cache`` record (default: the recordcache, or merged for a YUID)
against the schemas and exits 1 when it is invalid.

``--sample`` lists keys of one of a source's caches (datacache,
recordcache, reconciled, rewritten); ``--type`` reads every record to
filter, so it scans the cache.
"""

import json

from lux_pipeline import records, runtime

from ._handler import BaseHandler

STAGES = ("datacache", "recordcache", "reconciled", "mapped", "identity", "rewritten", "merged",
          "marklogic")
SAMPLE_CACHES = {"datacache": "datacache", "recordcache": "recordcache",
                 "reconciled": "reconciledRecordcache", "rewritten": "recordcache2"}


def _time(row, field):
    value = (row or {}).get(field)
    return value.isoformat(sep=" ", timespec="seconds") if hasattr(value, "isoformat") else value


def summary_lines(j):
    """The journey as the lines `lux show` prints."""
    ref = j["ref"]
    if ref["kind"] == "yuid":
        head = f"{ref['text']}: YUID {ref['uuid']}"
    else:
        head = (f"{ref['text']}: {ref['source']} ({ref['group']}) {ref['identifier']}  {ref['uri']}"
                + (f"  as {ref['rectype']}" if ref.get("rectype") else ""))
    lines = [head]
    for stage in STAGES:
        entry = j.get(stage)
        if stage == "mapped":
            if entry is not None:
                lines.append(f"  {'mapped':<12} {len(entry['records'])} record(s)")
            continue
        if stage == "identity":
            if entry is None:
                lines.append(f"  {'identity':<12} -")
            else:
                lines.append(f"  {'identity':<12} {entry['yuid']}  ({len(entry['members'])} member(s))")
                lines += [f"  {'':<12}   {m}" for m in entry["members"]]
            continue
        if entry is None:
            lines.append(f"  {stage:<12} -")
            continue
        when = _time(entry["row"], "record_time")
        lines.append(f"  {stage:<12} {entry['key']}" + (f"  record_time {when}" if when else ""))
    lines += [f"  note: {n}" for n in j["notes"]]
    return lines


class CommandHandler(BaseHandler):
    # read, and only what the reference needs is built
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("ref", nargs="?",
                        help="A URI, a curie (source:identifier), a YUID, or an identifier with "
                             "--source")
        ap.add_argument("--cache", choices=STAGES,
                        help="Print this stage's record (with --sample: the cache to list)")
        ap.add_argument("--map", dest="mapped", action="store_true",
                        help="Also map the stored raw record with the source's mapper (stores "
                             "nothing)")
        ap.add_argument("--validate", action="store_true",
                        help="Validate the --cache record (default recordcache; merged for a "
                             "YUID); exit 1 when invalid")
        ap.add_argument("--raw", action="store_true", help="Print everything as JSON")
        ap.add_argument("--sample", metavar="SOURCE",
                        help="List keys of SOURCE's --cache instead of looking one up")
        ap.add_argument("--type", dest="record_type", help="--sample: only records of this class")
        ap.add_argument("--since", help="--sample: only records with a record time from this on")
        ap.add_argument("-n", dest="count", type=int, default=50, help="--sample: how many keys")

    def prepare(self, cfgs, ref, mapped):
        """Build what the lookup reads: everything for --map (mappers reach
        other sources and the globals), else the source and the results."""
        if mapped:
            cfgs.get_idmap()
            cfgs.cache_globals()
            cfgs.instantiate_all()
            return
        if ref.source:
            records.instantiate_source(cfgs, ref.source)
        for name in ("merged", "marklogic"):
            if name in cfgs.results:
                records.instantiate_source(cfgs, name)

    def do_sample(self, cfgs, args):
        cache_name = args.cache or "recordcache"
        if cache_name not in SAMPLE_CACHES:
            raise ValueError(f"--sample lists one of {', '.join(SAMPLE_CACHES)}, not {cache_name}")
        cfg = records.instantiate_source(cfgs, args.sample)
        cache = cfg.get(SAMPLE_CACHES[cache_name])
        if cache is None:
            raise ValueError(f"{args.sample} has no {cache_name}")
        keys = records.sample(cache, args.count, typ=args.record_type, since=args.since)
        for k in keys:
            print(k)
        return keys

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs or runtime.get_configs(instantiate=False)
        if args.sample:
            return self.do_sample(cfgs, args)
        if not args.ref:
            raise ValueError("give a reference to look up, or --sample SOURCE")
        ref = records.resolve(cfgs, args.ref, source=getattr(args, "source", None))
        self.prepare(cfgs, ref, args.mapped)
        j = records.journey(cfgs, ref, mapped=args.mapped)
        if args.raw:
            print(json.dumps(j, indent=2, default=str, ensure_ascii=False))
        else:
            for line in summary_lines(j):
                print(line)
        stage = args.cache or (("merged" if ref.kind == "yuid" else "recordcache")
                               if args.validate else None)
        if stage:
            entry = j.get(stage)
            if not entry:
                raise ValueError(f"{args.ref}: nothing in {stage}")
            rows = entry["records"] if stage == "mapped" else \
                [{"data": entry["members"]}] if stage == "identity" else [entry["row"]]
            if args.cache and not args.raw:
                for row in rows:
                    print(json.dumps(row.get("data"), indent=2, default=str, ensure_ascii=False))
            if args.validate:
                if stage == "identity":
                    raise ValueError("--validate needs a record, not the identity")
                errors = [e for row in rows for e in records.validate(cfgs, row)]
                for e in errors:
                    print(e)
                if errors:
                    raise RuntimeError(f"{len(errors)} validation error(s) in the {stage} record")
                print(f"{stage}: valid")
        return j
