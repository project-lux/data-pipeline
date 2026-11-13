"""Validate records against the Linked Art schemas, and say what is wrong.

    lux validate --source ycba                          # ycba's recordcache, every record
    lux validate --source internal --max-workers 24     # every internal source (the default)
    lux validate --source merged --max-workers 24       # the merged records
    lux validate --source ycba --max-records 1000       # a sample: 1000 per worker
    lux validate --source ypm --ignore "is not valid under any of the given schemas"

Replaces ``manage-data.py --validate --<source>``. Each worker validates
its slice of each source's cache (``--cache``, default recordcache) against
the schemas in ``<data_dir>/schema`` (``lux initialize`` fetches them; a
missing one stops the run before any worker starts). Every error goes to
``validate-w<n>.jsonl`` in ``<prefix>/errors/`` with the run's logs
(``--out-dir`` overrides): source, cache, identifier, class, path, the
schema rule that failed, and the message.

The summary gives, per source, the records checked, how many were
invalid, and how many errors they had; then the most common kinds of
error -- grouped by class, rule and path with list positions as ``*`` --
with a count and an example message, so one fault shared by every record
is one line. A record whose class has no schema is counted as
unvalidatable. ``--ignore TEXT`` (repeatable) drops errors whose message
contains it. The exit status is 0 whatever is found; ``--strict`` makes an
invalid record exit 1.
"""

import logging

from lux_pipeline.process.tasks.validate_task import TOP_KINDS, ValidateManager

from ._handler import CommandHandler as CH

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def __init__(self, cfgs):
        super().__init__(cfgs)
        self.default_source = "internal"

    def add_args(self, ap):
        ap.add_argument("--cache", default=None,
                        help="Cache types to validate, separated by commas (default recordcache)")
        ap.add_argument("--ignore", action="append", default=[], metavar="TEXT",
                        help="Drop errors whose message contains TEXT; repeatable")
        ap.add_argument("--out-dir", dest="out_dir", default=None,
                        help="Where to write the error reports (default: <prefix>/errors)")
        ap.add_argument("--strict", action="store_true",
                        help="Exit 1 if any record is invalid")
        self.extra_args = {"caches": "cache", "ignore": "ignore", "report_dir": "out_dir"}

    def make_manager(self, wks, args):
        return ValidateManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        for line in summary_lines(s):
            # the summary is the command's result
            print(line)
        invalid = sum(c["invalid"] + c["unvalidatable"] for c in s["sources"].values())
        if invalid and getattr(args, "strict", False):
            raise RuntimeError(f"{invalid:,} invalid record(s)")
        return manager


def summary_lines(s, top=TOP_KINDS):
    out = []
    width = max([len(k) for k in s["sources"]] + [12])
    out.append(f"{'source/cache':<{width}} {'records':>12} {'invalid':>10} {'errors':>10}"
               f" {'no schema':>10}")
    for key, c in s["sources"].items():
        out.append(f"{key:<{width}} {c['records']:>12,} {c['invalid']:>10,} {c['errors']:>10,}"
                   f" {c['unvalidatable']:>10,}")
    if s["kinds"]:
        out.append("")
        out.append(f"most common errors (of {len(s['kinds']):,} kinds):")
        for n, cls, rule, path, message in s["kinds"][:top]:
            msg = message if len(message) <= 100 else message[:97] + "..."
            out.append(f"  {n:>10,}  {cls} {path} [{rule}]: {msg}")
        out.append(f"every error, record by record: {s['report_dir']}/validate-w*.jsonl")
    else:
        out.append("no errors")
    return out
