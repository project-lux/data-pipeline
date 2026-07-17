from ._handler import BaseHandler as BH
from ..process.identity import resolve_identity_from_config


class CommandHandler(BH):
    """Re-run deterministic identity resolution from the assertion logs
    already in temp_dir.

    `lux reconcile` runs this automatically after its reference phase;
    this command exists to redo resolution without re-reconciling --
    e.g. after reviewing identity_conflicts.jsonl or rebuilding the
    differents index.
    """

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        idmap = cfgs.get_idmap()
        stats = resolve_identity_from_config(cfgs, idmap)
        print(f"nodes={stats['nodes']} pairs={stats['pairs']} "
              f"diff_pairs={stats['diff_pairs']} clusters={stats['clusters']}")
        print(f"reused={stats['reused']} minted={stats['minted']} "
              f"moved={stats['moved']} deleted_yuids={stats['deleted_yuids']}")
        print(f"conflicts={stats['conflicts']} (see identity_conflicts.jsonl in temp_dir)")
        return True
