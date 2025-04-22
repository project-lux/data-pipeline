
from ..process.export_task import ExportManager
from ._handler import CommandHandler as CH

class CommandHandler(CH):

    def add_args(self, ap):
        ap.add_argument("--cache", type=str, default="recordcache", help="Type of cache to export")
        ap.add_argument('--type', type=str, default="marklogic", help="Which type of export to generate")
        self.extra_args = {"export_type": "type", "cache": "cache"}
        self.default_source = "merged"

    def make_manager(self, wks, args):
        if args.source == "all":
            args.source = "merged"
        return ExportManager(self.configs, wks)
