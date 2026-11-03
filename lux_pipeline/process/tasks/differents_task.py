"""`DifferentsManager`: write a source's own "different from" assertions as
a CSV of URI pairs for the merged differents index. The manager behind
`lux build-differents`; replaces the root ``make-wd-differents.py``.

A source takes part when one of its components (mapper, reconciler or
loader, the first that has it) has ``process_equivs(record)`` returning
``(sames, diffs)`` -- today wikidata, through `WdConfigManager`, which
reads ``P1889`` and pairs every member of a disambiguation page with every
other -- and it has a datacache and a ``namespace``. The diffs are bare
identifiers; they are written as ``<namespace><identifier>`` because the
merged index loader skips any row that is not two URIs.

Each worker reads its slice of each source's datacache and writes its
pairs, sorted and without duplicates, to
``<temp_dir>/differents/<source>-w<n>.csv`` (written as ``.tmp`` and
renamed, so a part that exists is complete). The driver then merges the
parts in order into ``<out_dir>/<source>.csv`` (default
``<data_dir>/differentFrom``, where ``load-csv-map2.py --all`` looks),
again through a ``.tmp`` and a rename, so a failed run leaves the previous
file in place. With ``--my-worker`` only the part is written; ``--merge-only``
merges the parts once every slice is done. ``load`` then adds each CSV to
the merged source's indexLoader (``diffs``), which adds and never clears.
"""

import heapq
import logging
import os

from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")

COMPONENTS = ("mapper", "reconciler", "loader")


def equivs_component(cfg):
    """The source's first component with ``process_equivs``, or ``None``."""
    for c in COMPONENTS:
        comp = cfg.get(c)
        if comp is not None and callable(getattr(comp, "process_equivs", None)):
            return comp
    return None


def unique(lines):
    """`lines` (sorted) without repeats."""
    last = None
    for line in lines:
        if line != last:
            yield line
            last = line


class DifferentsManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``out_dir``
    (``--out-dir``), ``load`` (``--load``), ``merge_only``
    (``--merge-only``)."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.out_dir = None
        self.load = False
        self.merge_only = False
        self.skipped = []
        self.written = {}
        self.loaded = []
        self.results = None
        if args is not None:
            self.out_dir = getattr(args, "out_dir", None)
            self.load = bool(getattr(args, "load", False))
            self.merge_only = bool(getattr(args, "merge_only", False))

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        name = cfg["name"]
        if cfg.get("datacache") is None or not cfg.get("namespace") \
                or equivs_component(cfg) is None:
            self.skipped.append(name)
            return
        if (which, name) not in self.sources:
            self.sources.append((which, name))

    def check(self):
        if not self.sources:
            extra = f" (skipped: {', '.join(self.skipped)})" if self.skipped else ""
            raise ValueError("None of the selected sources has different-from assertions to "
                             f"write: one needs a datacache, a namespace and a component with "
                             f"process_equivs{extra}")
        if self.skipped:
            logger.info(f"no different-from assertions in: {', '.join(self.skipped)}")
        if self.load and self.configs.results.get("merged", {}).get("indexLoader") is None:
            raise RuntimeError("the merged source has no indexLoader to load the CSVs into")

    # ----------------------------------------------------------- paths

    def part_dir(self, cfgs=None):
        return os.path.join((cfgs or self.configs).temp_dir, "differents")

    def part_path(self, name, n, cfgs=None):
        return os.path.join(self.part_dir(cfgs), f"{name}-w{n}.csv")

    def csv_path(self, name):
        out = self.out_dir or os.path.join(self.configs.data_dir, "differentFrom")
        return os.path.join(out, f"{name}.csv")

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        self.check()
        if not self.merge_only:
            # a run over every slice starts clean: no part of an earlier run
            # may be merged into this one
            if self.my_slice == -1:
                for _, name in self.sources:
                    for n in range(self.max_workers):
                        fn = self.part_path(name, n)
                        if os.path.exists(fn):
                            os.remove(fn)
            self.results = super().process(engine, prefix, view)
            # the null engine runs one worker: --my-worker i, or worker 0
            if self.my_slice > -1 or (engine == "null" and self.max_workers > 1):
                logger.info(f"worker {max(self.my_slice, 0)} of {self.max_workers}: part "
                            f"written; when every slice is done, run the same command with "
                            f"--merge-only")
                return self.results
        self.merge_parts()
        if self.load:
            self.load_csvs()
        return self.results

    def merge_parts(self):
        """Merge each source's parts into its CSV. Raises, writing nothing
        for that source, if a part is missing."""
        for _, name in self.sources:
            parts = [self.part_path(name, n) for n in range(self.max_workers)]
            missing = [p for p in parts if not os.path.exists(p)]
            if missing:
                raise FileNotFoundError(f"{name}: {len(missing)} of {len(parts)} parts missing "
                                        f"({', '.join(missing[:3])}); has every slice run?")
            dest = self.csv_path(name)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            tmp = dest + ".tmp"
            fhs = [open(p) for p in parts]
            n = 0
            try:
                with open(tmp, "w") as out:
                    for line in unique(heapq.merge(*fhs)):
                        out.write(line)
                        n += 1
            finally:
                for fh in fhs:
                    fh.close()
            os.replace(tmp, dest)
            for p in parts:
                os.remove(p)
            self.written[name] = (dest, n)
            logger.info(f"{name}: {n:,} different-from pairs written to {dest}")

    def load_csvs(self):
        loader = self.configs.results["merged"]["indexLoader"]
        for _, name in self.sources:
            path = self.csv_path(name)
            logger.info(f"loading {path} into the merged differents index")
            loader.load(path, "diffs")
            self.loaded.append(path)

    def summary(self):
        """``{source: {"records": n, "with": n, "pairs": n}}`` from the
        workers (``pairs`` before duplicates across workers are dropped),
        plus ``written`` / ``loaded`` from the driver."""
        per = {}
        for r in self.results or []:
            if not isinstance(r, dict):
                continue
            for name, c in r.items():
                tot = per.setdefault(name, {"records": 0, "with": 0, "pairs": 0})
                for k in tot:
                    tot[k] += c.get(k, 0)
        return {"sources": per, "written": dict(self.written), "loaded": list(self.loaded)}

    # ----------------------------------------------------------- workers

    def _distributed(self, n, cfgs, progress):
        mx = self.max_workers
        sliced = mx > 1
        out = {}
        os.makedirs(self.part_dir(cfgs), exist_ok=True)
        for which, name in self.sources:
            cfg = getattr(cfgs, which)[name]
            cache = cfg["datacache"]
            comp = equivs_component(cfg)
            ns = cfg["namespace"]
            counts = {"records": 0, "with": 0, "pairs": 0}
            out[name] = counts
            est = getattr(cache, "len_estimate", None)
            total = (est() if est else 0) // (mx if sliced else 1)
            progress(phase="build-differents", description=name, total=total, completed=0)
            pairs = set()
            records = cache.iter_records_slice(n, mx) if sliced else cache.iter_records()
            for rec in records:
                if self.max_records > -1 and counts["records"] >= self.max_records:
                    break
                counts["records"] += 1
                progress(advance=1)
                data = rec.get("data") if isinstance(rec, dict) else None
                if not isinstance(data, dict) or "id" not in data:
                    continue
                try:
                    _, diffs = comp.process_equivs({"data": data})
                except Exception as e:
                    logger.error(f"{name}/{rec.get('identifier')}: {type(e).__name__}: {e}")
                    continue
                if diffs:
                    counts["with"] += 1
                for a, b in diffs:
                    pairs.add(f"{ns}{a},{ns}{b}\n")
            counts["pairs"] = len(pairs)
            dest = self.part_path(name, n, cfgs)
            with open(dest + ".tmp", "w") as fh:
                fh.writelines(sorted(pairs))
            os.replace(dest + ".tmp", dest)
            logger.info(f"{name}: {counts['records']:,} records, {counts['with']:,} with "
                        f"different-from, {counts['pairs']:,} pairs")
        return out
