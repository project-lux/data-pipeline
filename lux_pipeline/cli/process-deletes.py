from ._handler import BaseHandler as BH
from ..process.update_manager import UpdateManager


class CommandHandler(BH):
    """Consume {data_dir}/pending_deletes.jsonl.

    Harvest deletes append their required follow-up there (rebuild the
    merged record without the deleted member, or remove an orphaned
    merged record entirely). This drops the stale merged/ML-cache
    records so the next merge/export cannot serve a version containing
    the deleted member, removes token-only YUID sets for emptied
    clusters, and rotates processed entries to pending_deletes.done.jsonl.
    Safe to run any time between harvest and merge.
    """

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        idmap = cfgs.get_idmap()
        cfgs.instantiate("merged", "results")
        mgr = UpdateManager(cfgs, idmap)
        stats = mgr.process_pending_deletes()
        print(f"processed={stats['processed']} "
              f"merged_removed={stats.get('merged_removed', 0)} "
              f"yuids_removed={stats.get('yuids_removed', 0)} "
              f"errors={stats.get('errors', 0)}")
        return stats.get("errors", 0) == 0
