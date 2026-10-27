"""Reconcile the sources' records and the references they lead to.

    lux reconcile --source all --max-workers 24           # every internal source; then identify, metatypes, write-refs
    lux reconcile --source all --max-workers 24 --new-token
    lux reconcile --source ycba --recid obj/123 --no-ui   # chosen records: one worker, no finishing steps
    lux reconcile --source all --no-refs                  # records only, no references pass
    lux reconcile --baseline                              # the global terms a new idmap needs
    lux reconcile --source all --my-worker 3 --max-workers 24 --no-ui --no-finish   # one slice, shell-orchestrated

Replaces `run-reconcile.py` and `reconcile_parallel.sh`, and, once every
worker has finished, `run-identify.py`, `merge-metatypes.py` and
`manage-data.py --write-refs`. `--source all` is every internal source, as
`run-reconcile.py --all`: the external records are reached through
references. See `lux_pipeline.process.tasks.reconcile_task` for when the
finishing steps are skipped.
"""

from lux_pipeline.process.tasks.reconcile_task import ReconcileManager

from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--recid", help="Comma-separated identifiers to reconcile, from one "
                                        "source; runs one worker and skips the finishing steps")
        ap.add_argument("--no-refs", "--norefs", dest="no_refs", action="store_true",
                        help="Records only: skip the references pass")
        ap.add_argument("--new-token", "--new_token", dest="new_token", action="store_true",
                        help="Mint this build's idmap update token before starting")
        ap.add_argument("--baseline", action="store_true",
                        help="Only the global terms a new idmap needs (aat, then wikidata); "
                             "ignores --source and --recid, skips the finishing steps")
        ap.add_argument("--no-finish", "--no_finish", dest="no_finish", action="store_true",
                        help="Skip identify, metatypes and write-refs after the workers")
        self.extra_args = {"recid": "recid", "no_refs": "no_refs", "new_token": "new_token",
                           "baseline": "baseline", "no_finish": "no_finish"}

    def resolve_sources(self, args):
        """``all`` is every internal source, as the script's ``--all``;
        the rest as for every command."""
        if (args.source or self.default_source) == "all":
            return list(self.configs.internal.keys())
        return super().resolve_sources(args)

    def make_manager(self, wks, args):
        return ReconcileManager(self.configs, wks, args)
