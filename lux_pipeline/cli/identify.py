"""Resolve the identity map from the reconcile assertions, then vacuum it.

    lux identify               # after every reconcile slice has finished
    lux identify --no-vacuum

`python run-identify.py` as a command, and the first of the three steps
between reconcile and merge that `lux reconcile` runs itself unless told
not to (``--no-finish``, ``--my-worker``): ``lux identify``, then ``lux
metatypes``, then ``lux write-refs``. Run it from the directory reconcile
ran in; refused conflicts go to ``identity_conflicts.jsonl`` there.

Runs in this process, never sliced: it is a streaming unix ``sort``
pipeline over every slice's assertions. It refuses while a ``lux`` build is
running (``--force`` overrides): identity resolved from part of the
assertions would rewrite the map from part of the evidence.
"""

from lux_pipeline.process.phases.identify import report_lines, resolve_identity, vacuum_idmap

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--no-vacuum", "--no_vacuum", dest="no_vacuum", action="store_true",
                        help="Do not vacuum the identity map afterwards")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        refuse_while_building(cfgs, "identify", args.force)
        idmap = cfgs.get_idmap()
        stats = resolve_identity(cfgs, idmap)
        for line in report_lines(stats):
            print(line)
        if not args.no_vacuum:
            print("\nVacuuming the identity map...")
            vacuum_idmap(idmap)
        return stats
