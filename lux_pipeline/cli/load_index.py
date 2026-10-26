"""Build each source's reconciliation indexes from its datacache.

    lux load-index --source lcnaf,lcsh,aat,ulan,wikidata,viaf
    lux load-index --source all            # every source with an indexLoader
    lux load-index --source aat --no-overwrite   # merge over the existing entries

Replaces `manage-data.py --load-index`. One worker per source (an LMDB
index has a single writer), so no more workers run than there are
sources. A source that fails is logged and the rest still build; the
command exits non-zero at the end if any failed.
"""

import logging

from lux_pipeline.process.tasks.index_load_task import IndexLoadManager

from ._handler import CommandHandler as CH

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--no-overwrite", dest="overwrite", action="store_false",
                        help="Keep the existing index entries and merge the new ones over them")
        self.extra_args = {"overwrite": "overwrite"}

    def make_manager(self, wks, args):
        return IndexLoadManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        logger.info(f"indexed {s['sources']} source(s): {s['names']} names, {s['uris']} uris, "
                    f"{s['failed']} failed")
        if s["failed"]:
            raise RuntimeError(f"{s['failed']} index build(s) failed; see the worker logs")
        return manager
