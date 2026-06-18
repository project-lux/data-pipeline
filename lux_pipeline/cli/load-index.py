from lux_pipeline.process.tasks.index_load_task import IndexLoadManager
from ._handler import CommandHandler as CH


class CommandHandler(CH):

    def add_args(self, ap):
        ap.add_argument("--type", type=str, default="records")
        ap.add_argument(
            "--no-overwrite",
            action="store_false",
            help="Do not overwrite existing files/records",
        )
        self.extra_args = {"overwrite": "no_overwrite", "load_type": "type"}

    def make_manager(self, wks, args):
        return IndexLoadManager(self.configs, wks, args)
