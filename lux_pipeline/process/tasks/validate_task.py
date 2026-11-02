"""`ValidateManager`: validate records against the Linked Art schemas, in
parallel. The manager behind `lux validate`; replaces ``manage-data.py
--validate``.

Each worker validates its slice of every selected ``(source, cache)``
(`Validator.validate`, which reads the schemas under ``<data_dir>/schema``),
and writes one JSON line per error to ``<report_dir>/validate-w<n>.jsonl``:
``source``, ``cache``, ``identifier``, ``class``, ``path`` (``/a/0/b``),
``rule`` (the schema keyword that failed) and ``message``. An ``anyOf`` /
``oneOf`` failure ("is not valid under any of the given schemas", followed
by the whole offending object) is reported by its most relevant sub-error
instead (`jsonschema.exceptions.best_match`): rule ``anyOf>required``, the
sub-error's path and message, which say why. A record whose
class has no schema (or no class at all) cannot be validated; it is counted
as ``unvalidatable`` and its reason written as an error with rule
``class``. Errors whose message (as reported, or as jsonschema raised it)
contains any of ``ignore`` are dropped, as manage-data.py's
``ignore_matches`` did.

A worker returns its counts per ``source/cache`` (``records``,
``invalid``, ``errors``, ``unvalidatable``) and ``kinds``: errors grouped
by class, rule and path with array positions as ``*``, each with a count
and the first message seen, so a fault every record shares is one line of
the summary rather than a million. `summary` adds the workers up.
"""

import json
import logging
import os
from collections import Counter

from jsonschema.exceptions import best_match

from ._task_manager import TaskManager

logger = logging.getLogger("lux_pipeline")

DEFAULT_CACHES = ("recordcache",)
TOP_KINDS = 15


def explain(error):
    """``(rule, error)``: an ``anyOf`` / ``oneOf`` error's best sub-error,
    whose message says what is actually wrong; any other error as it is."""
    if error.context:
        best = best_match(error.context)
        if best is not None:
            return f"{error.validator}>{best.validator}", best
    return str(error.validator), error


def error_path(error):
    return "/" + "/".join(str(x) for x in error.absolute_path)


def path_pattern(error):
    """The path with list positions as ``*``: ``/identified_by/*/content``."""
    return "/" + "/".join("*" if isinstance(x, int) else str(x) for x in error.absolute_path)


class ValidateManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``caches`` (``--cache``,
    default recordcache), ``ignore`` (``--ignore``, repeatable) and
    ``report_dir`` (``--out-dir``; default ``<prefix>-errors`` beside the
    run's logs, else ``log_dir``)."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.caches = list(DEFAULT_CACHES)
        self.ignore = []
        self.report_dir = None
        self.results = None
        if args is not None:
            if getattr(args, "cache", None):
                self.caches = [c.strip() for c in args.cache.split(",") if c.strip()]
            self.ignore = list(getattr(args, "ignore", None) or [])
            self.report_dir = getattr(args, "out_dir", None)

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        if (which, cfg["name"]) not in self.sources:
            self.sources.append((which, cfg["name"]))

    def targets(self, cfgs=None):
        """``[(group, source, cache type)]`` that exist, in order."""
        cfgs = cfgs or self.configs
        out = []
        for which, name in self.sources:
            cfg = getattr(cfgs, which)[name]
            for c in self.caches:
                if cfg.get(c) is not None:
                    out.append((which, name, c))
        return out

    def check(self):
        """In the driver, before any worker: something to validate, and the
        schemas present."""
        if not self.sources:
            raise ValueError("No source selected to validate")
        if not self.targets():
            raise ValueError(f"None of the selected sources has a {' or '.join(self.caches)}")
        v = getattr(self.configs, "validator", None)
        if v is None:
            raise RuntimeError("no validator configured (validatorClass)")
        missing = v.missing() if hasattr(v, "missing") else []
        if missing:
            from lux_pipeline.process.validator import SCHEMA_BASE_URL
            raise FileNotFoundError(f"validator schemas missing from {v.schema_dir}: "
                                    f"{', '.join(missing)} (from {SCHEMA_BASE_URL}; "
                                    f"`lux initialize` fetches them)")

    # ----------------------------------------------------------- driver

    def process(self, engine="mp", prefix=None, view=None):
        self.check()
        if self.report_dir is None:
            self.report_dir = f"{prefix}-errors" if prefix else self.configs.log_dir
        os.makedirs(self.report_dir, exist_ok=True)
        for n in range(self.max_workers):
            fn = self.report_path(n)
            if self.my_slice in (-1, n) and os.path.exists(fn):
                os.remove(fn)
        self.results = super().process(engine, prefix, view)
        return self.results

    def report_path(self, n):
        return os.path.join(self.report_dir, f"validate-w{n}.jsonl")

    def summary(self):
        """The workers' results added up: ``{"sources": {"src/cache": counts},
        "kinds": [(count, class, rule, path, sample message)], "report_dir"}``,
        kinds most common first."""
        per = {}
        kinds = Counter()
        samples = {}
        for r in self.results or []:
            if not isinstance(r, dict):
                continue
            for key, counts in r.get("sources", {}).items():
                tot = per.setdefault(key, {"records": 0, "invalid": 0, "errors": 0,
                                           "unvalidatable": 0})
                for k in tot:
                    tot[k] += counts.get(k, 0)
            for k in r.get("kinds", []):
                key = (k["class"], k["rule"], k["path"])
                kinds[key] += k["count"]
                samples.setdefault(key, k["message"])
        ordered = [(n, *key, samples[key]) for key, n in kinds.most_common()]
        return {"sources": per, "kinds": ordered, "report_dir": self.report_dir}

    # ----------------------------------------------------------- workers

    def _distributed(self, n, cfgs, progress):
        v = cfgs.validator
        mx = self.max_workers
        sliced = mx > 1
        out = {"sources": {}, "kinds": []}
        kinds = Counter()
        samples = {}
        with open(self.report_path(n), "w") as report:
            for which, name, ctype in self.targets(cfgs):
                cache = getattr(cfgs, which)[name][ctype]
                counts = {"records": 0, "invalid": 0, "errors": 0, "unvalidatable": 0}
                out["sources"][f"{name}/{ctype}"] = counts
                est = getattr(cache, "len_estimate", None)
                total = (est() if est else 0) // (mx if sliced else 1)
                progress(phase="validate", description=f"{name} {ctype}", total=total,
                         completed=0)
                records = (cache.iter_records_slice(n, mx) if sliced else cache.iter_records())
                for rec in records:
                    if self.max_records > -1 and counts["records"] >= self.max_records:
                        break
                    counts["records"] += 1
                    progress(advance=1)
                    self._one(rec, name, ctype, v, counts, kinds, samples, report)
                logger.info(f"{name} {ctype}: {counts['records']:,} records, "
                            f"{counts['invalid']:,} invalid, {counts['errors']:,} errors")
        out["kinds"] = [{"class": c, "rule": r, "path": p, "count": k,
                         "message": samples[(c, r, p)]} for (c, r, p), k in kinds.items()]
        return out

    def _one(self, rec, name, ctype, v, counts, kinds, samples, report):
        data = rec.get("data") or {}
        cls = data.get("type") if isinstance(data, dict) else None
        ident = rec.get("identifier") or rec.get("yuid")
        found = []
        try:
            errs = v.validate(rec)
        except Exception as e:
            counts["unvalidatable"] += 1
            found.append(("class", "/", "/", str(e)))
        else:
            for error in errs:
                rule, err = explain(error)
                # the message as reported, or the one jsonschema raised
                if any(im in err.message or im in error.message for im in self.ignore):
                    continue
                found.append((rule, error_path(err), path_pattern(err), err.message))
        if not found:
            return
        if found[0][0] != "class":
            counts["invalid"] += 1
        for rule, path, pattern, message in found:
            counts["errors"] += 1
            key = (str(cls), rule, pattern)
            kinds[key] += 1
            samples.setdefault(key, message)
            report.write(json.dumps({"source": name, "cache": ctype, "identifier": ident,
                                     "class": cls, "path": path, "rule": rule,
                                     "message": message}) + "\n")
