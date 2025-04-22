from ..process.reconcile_task import ReconcileManager
from ._handler import CommandHandler as CH

class CommandHandler(CH):

    def add_args(self, ap):
        ap.add_argument('--recid', type=str, help="Comma separated list of recids")
        ap.add_argument('--no-refs', action="store_true", help="Should not process references")
        self.extra_args = {"no_refs": "no_refs", "my_slice": "my_worker"}

    def make_manager(self, wks, args):
        return ReconcileManager(self.configs, wks)
