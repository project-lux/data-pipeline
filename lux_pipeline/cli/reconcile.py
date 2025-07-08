from lux_pipeline.process.tasks.reconcile_task import ReconcileManager
from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--recid", type=str, help="Comma separated list of recids")
        ap.add_argument("--no-refs", action="store_true", help="Should not process references")
        ap.add_argument("--new-token", action="store_true", help="Should we create a new reconciliation token")
        self.extra_args = {"no_refs": "no_refs", "my_slice": "my_worker", "new_token": "new_token"}

    def make_manager(self, wks, args):
        return ReconcileManager(self.configs, wks, args)
