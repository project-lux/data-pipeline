from lux_pipeline.process.tasks.download_task import DownloadManager
from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--type", type=str, default="all", help="all|records|export for which type to download")
        ap.add_argument("--no-overwrite", action="store_false", help="Do not overwrite existing files/records")
        self.extra_args = {"download_type": "type"}

    def make_manager(self, wks, args):
        return DownloadManager(self.configs, wks)
