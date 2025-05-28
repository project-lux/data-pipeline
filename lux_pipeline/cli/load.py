from lux_pipeline.process.tasks.load_task import LoadManager
from ._handler import CommandHandler as CH
import logging

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--type", type=str, default="records")
        ap.add_argument("--no-overwrite", action="store_false", help="Do not overwrite existing files/records")
        self.extra_args = {"overwrite": "no_overwrite", "load_type": "type"}

    def make_manager(self, wks, args):
        mgr = LoadManager(self.configs, wks)
        if hasattr(args, "type") and args.type:
            mgr.load_type = args.type
            if args.type not in ["records", "export", "all"]:
                logger.warning(f"Unknown download type: {args.type}, continuing anyway...")
        return mgr
