"""Write the referenced URIs to reference_uris.txt and clear the queue.

    lux write-refs             # after lux identify and lux metatypes, before merge

`manage-data.py --write-refs` as a command, and the last of the three
steps between reconcile and merge: ``lux identify``, ``lux metatypes``,
``lux write-refs``. Each line carries its URI's YUID, so identity must be
resolved first. The file goes to the **working directory** and merge reads
it from there, so every phase of a build runs from the same directory.

The done-refs queue is cleared once written. With the queue already empty
and a ``reference_uris.txt`` here, writing would replace the file merge
needs with an empty one -- the second run of this command -- so that is
refused (``--force`` writes it anyway). Refuses while a ``lux`` build is
running too: the reconcile workers fill the queue this clears.
"""

import os

from lux_pipeline.process.phases.reconcile import write_refs
from lux_pipeline.process.reference_manager import ReferenceManager

from ._handler import BaseHandler, refuse_while_building


def _is_empty(store):
    """Whether a reference queue holds nothing: counted no further than one
    row where the store can (`queue_length`), exactly otherwise."""
    queue_length = getattr(store, "queue_length", None)
    if queue_length is not None:
        return queue_length(1) == 0
    return len(store) == 0


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running, or with an "
                             "empty queue")

    def process(self, args, rest):
        super().process(args, rest)
        cfgs = self.configs
        refuse_while_building(cfgs, "write-refs", args.force)
        idmap = cfgs.get_idmap()
        ref_mgr = ReferenceManager(cfgs, idmap)
        out = os.path.abspath("reference_uris.txt")
        if os.path.exists(out) and _is_empty(ref_mgr.done_refs) and not args.force:
            raise RuntimeError(f"the done-refs queue is empty, and writing it would empty "
                               f"{out}, which merge reads. Already written? Pass --force "
                               f"to write it anyway")
        path = write_refs(cfgs, idmap, ref_mgr)
        print(f"wrote {path}")
        return path
