from lux_pipeline.process.tasks.index_load_task import IndexLoadManager
from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def make_manager(self, wks, args):
        return IndexLoadManager(self.configs, wks)
