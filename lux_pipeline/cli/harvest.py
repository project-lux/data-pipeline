from lux_pipeline.process.harvest_task import HarvestManager
from ._handler import CommandHandler as CH

class CommandHandler(CH):

    def add_args(self, ap):
        ap.add_argument("--no-overwrite", action='store_false', help="Do not overwrite existing files/records")

    def make_manager(self, wks, args):
        return HarvestManager(self.configs, wks)
