from lux_pipeline.process.tasks.download_task import DownloadManager
from ._handler import CommandHandler as CH
import logging

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--type", type=str, default="all", help="all|records|export for which type to download")
        ap.add_argument("--no-overwrite", action="store_false", help="Do not overwrite existing files/records")
        self.extra_args = {"download_type": "type"}

    def make_manager(self, wks, args):
        mgr = DownloadManager(self.configs, wks)
        # The manager needs the type before processing for prepare_single()
        if hasattr(args, "type") and args.type:
            mgr.download_type = args.type
            if args.type not in ["records", "export"]:
                logger.warning(f"Unknown download type: {args.type}, continuing anyway...")
        return mgr
