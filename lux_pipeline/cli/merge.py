"""Merge the sources' records into one record per identity, then the references.

    lux merge --source all --max-workers 24          # every internal source, then the references
    lux merge --source all --resume                  # skip what the merged cache already holds
    lux merge --source all --no-refs                 # records only, no references pass
    lux merge --only-refs --max-workers 24           # only the references pass
    lux merge --source ycba --recid obj/123 --no-ui  # chosen records: one worker, no references pass
    lux merge --source all --my-worker 3 --max-workers 24 --no-ui   # one slice, shell-orchestrated

Replaces `run-merge.py` and `merge_parallel.sh`. `--source all` is every
internal source, as `run-merge.py --all`. Run it from the directory
reconcile ran in, after `lux identify`, `lux metatypes` and `lux
write-refs` (or a `lux reconcile` that ran them): the references pass reads
``reference_uris.txt`` from the working directory. See
`lux_pipeline.process.tasks.merge_task` for what is checked before the
workers start.
"""

from lux_pipeline.process.tasks.merge_task import MergeManager

from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--recid", help="Comma-separated identifiers to merge, from one source; "
                                        "runs one worker and no references pass")
        ap.add_argument("--no-refs", "--norefs", dest="no_refs", action="store_true",
                        help="Records only: skip the references pass")
        ap.add_argument("--only-refs", "--onlyrefs", dest="only_refs", action="store_true",
                        help="Only the references pass; ignores --source")
        ap.add_argument("--resume", action="store_true",
                        help="Skip records and references the merged cache already holds")
        self.extra_args = {"recid": "recid", "no_refs": "no_refs", "only_refs": "only_refs",
                           "resume": "resume"}

    def resolve_sources(self, args):
        """``all`` is every internal source, as the script's ``--all``;
        the rest as for every command."""
        if (args.source or self.default_source) == "all":
            return list(self.configs.internal.keys())
        return super().resolve_sources(args)

    def make_manager(self, wks, args):
        return MergeManager(self.configs, wks, args)
