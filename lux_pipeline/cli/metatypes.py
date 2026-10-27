"""Merge the reconcile slices' metatypes into <data_dir>/metatypes.json.

    lux metatypes              # after lux identify, before lux write-refs

`merge-metatypes.py` plus `run-all.sh`'s ``rm`` and ``mv`` as a command
(`ReferenceManager.merge_metatypes`), and the second of the three steps
between reconcile and merge: ``lux identify``, ``lux metatypes``, ``lux
write-refs``. Reads and deletes the ``metatypes-*.json`` files the
reconcile slices wrote into the working directory, so run it from the
directory reconcile ran in.

With no slice files there is nothing to merge, and writing would replace
an existing ``metatypes.json`` with an empty one -- the second run of this
command, or a run from the wrong directory -- so that is refused
(``--force`` writes it anyway). Refuses while a ``lux`` build is running
too.
"""

import glob
import os

from lux_pipeline.process.reference_manager import ReferenceManager

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running, or with no "
                             "slice files to merge")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        refuse_while_building(cfgs, "metatypes", args.force)
        target = os.path.join(cfgs.data_dir, "metatypes.json")
        if not glob.glob("metatypes-*.json") and os.path.exists(target) and not args.force:
            raise RuntimeError(f"no metatypes-*.json in {os.getcwd()} to merge, and merging "
                               f"none would empty {target}. Run from the directory reconcile "
                               f"ran in, or pass --force")
        # logs "metatypes.json: <n> keys from <m> slice file(s)"
        return ReferenceManager(cfgs, cfgs.get_idmap()).merge_metatypes()
