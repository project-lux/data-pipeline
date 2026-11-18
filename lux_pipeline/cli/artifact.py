"""Make artifacts again, or list and check them.

    lux artifact                             # every artifact: age, rows, stale or not
    lux artifact --type getty_replacement_uris
    lux artifact --stale                     # make every stale one again
    lux artifact --all
    lux artifact --check                     # sizes and md5s against the manifest

An artifact is a file a build reads that a generator makes from a
configuration record (``"type": "artifact"``): the Getty replaced ids, the
language type overrides, the Google Sheet's CSVs. Each is written to
``artifacts_dir`` (default ``<data_dir>/artifacts``) with an entry in its
``manifest.json`` saying when, by which code and configuration, and what
it should checksum to. An artifact is stale when it is older than its
record's ``maxAgeDays`` or its record changed since it was made
(docs/artifacts.md).

Making one refuses while a build runs (mappers read these files as they
start), and refuses to replace a file whose row count drops by more than
its ``maxShrink``; ``--force`` overrides both. Listing and checking never
refuse. Needs no database.
"""

import json
import logging

from lux_pipeline import runtime
from lux_pipeline.artifacts.base import ArtifactError

from ._handler import BaseHandler, refuse_while_building

logger = logging.getLogger("lux_pipeline")


def status_lines(rows):
    """`ArtifactCache.status` rows as a plain table."""
    if not rows:
        return ["No artifacts configured."]
    width = max(len(r["name"]) for r in rows)
    out = [f"{'ARTIFACT':<{width}}  {'ROWS':>10}  {'MADE':<20}  {'AGE':>7}  {'MAX':>5}  STATE"]
    for r in rows:
        rows_text = "" if r["rows"] is None else f"{r['rows']:,}"
        made = (r["generated_at"] or "never")[:19].replace("T", " ")
        age = "" if r["age_days"] is None else f"{r['age_days']:.0f}d"
        max_age = "" if r["max_age_days"] is None else f"{r['max_age_days']}d"
        state = "current" if r["stale"] is None else f"stale: {r['stale']}"
        out.append(f"{r['name']:<{width}}  {rows_text:>10}  {made:<20}  {age:>7}  {max_age:>5}  {state}")
    return out


class CommandHandler(BaseHandler):
    # the configuration is read, not instantiated: no source, cache or idmap
    # is needed to make or list an artifact
    needs_configs = False

    def add_args(self, ap):
        which = ap.add_mutually_exclusive_group()
        which.add_argument("--type", metavar="NAME[,NAME...]",
                           help="Make these artifacts again")
        which.add_argument("--stale", action="store_true",
                           help="Make every stale artifact again")
        which.add_argument("--all", action="store_true", help="Make every artifact again")
        ap.add_argument("--list", action="store_true",
                        help="List the artifacts (the default with nothing else to do)")
        ap.add_argument("--check", action="store_true",
                        help="Check the files' sizes and md5s against the manifest; "
                             "exit 1 on a mismatch")
        ap.add_argument("--force", action="store_true",
                        help="Make them even while a build runs, or when a file shrinks "
                             "by more than its maxShrink")
        ap.add_argument("--json", action="store_true", help="--list and --check as JSON")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs or runtime.get_configs(instantiate=False)
        cache = cfgs.artifacts_cache
        making = bool(args.type or args.stale or args.all)
        if making:
            self.make(cfgs, cache, args)
        if args.list or not (making or args.check):
            rows = cache.status()
            print(json.dumps(rows, indent=2) if args.json else "\n".join(status_lines(rows)))
        if args.check:
            problems = cache.check()
            if args.json:
                print(json.dumps(problems, indent=2))
            elif problems:
                for p in problems:
                    print(f"{p['artifact']}: {p['path'] or ''} {p['problem']}".replace("  ", " "))
            else:
                print(f"Every file matches {cache.manifest_path}.")
            if problems:
                raise SystemExit(1)

    def make(self, cfgs, cache, args):
        if args.type:
            names = [n.strip() for n in args.type.split(",") if n.strip()]
            unknown = [n for n in names if n not in cache.records]
            if unknown:
                raise ArtifactError(f"no artifact record {', '.join(unknown)}; configured: "
                                    f"{', '.join(sorted(cache.records)) or 'none'}")
        elif args.stale:
            names = [r["name"] for r in cache.status() if r["configured"] and r["stale"]]
        else:
            names = sorted(cache.records)
        if not names:
            logger.info("No artifacts to make" + (": none is stale" if args.stale else ""))
            return
        refuse_while_building(cfgs, "artifact", args.force)
        failed = []
        for name in names:
            try:
                cache.make(name, force=args.force)
            except Exception as e:
                logger.error(f"artifact {name} not made: {type(e).__name__}: {e}")
                failed.append(name)
        if failed:
            raise ArtifactError(f"not made: {', '.join(failed)}")
