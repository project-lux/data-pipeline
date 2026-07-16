"""Consume pending_deletes.jsonl written by the harvest delete path.

Removes stale merged/ML-cache records for deleted members (so the next
merge/export can't serve a version containing them) and deletes
token-only YUID sets for fully-emptied clusters. Safe to run any time
between harvest and merge; processed entries rotate to
pending_deletes.done.jsonl.
"""

import os

from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

mgr = UpdateManager(cfgs, idmap)
stats = mgr.process_pending_deletes()
print(f"pending deletes: {stats}")
