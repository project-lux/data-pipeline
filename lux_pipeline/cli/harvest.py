from lux_pipeline.process.tasks.harvest_task import HarvestManager
from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--no-overwrite", action="store_false", help="Do not overwrite existing files/records")
        ap.add_argument("--two-phase", action="store_true", help="Process in two phases")
        ap.add_argument("--until", type=str, help="ISO8601 date to harvest backwards to")

    def make_manager(self, wks, args):
        return HarvestManager(self.configs, wks)
