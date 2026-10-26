"""Load source dumps into their datacaches.

    lux load --source ror                          # one process
    lux load --source wikidata --max-workers 24    # 24 workers, one slice each
    lux load --source all --type export            # LUX exports into datacaches
    lux load --source ycba --no-overwrite --clear

Replaces `manage-data.py --load` and `import_parallel.sh`. See
`lux_pipeline.process.tasks.load_task` for how each loader is dispatched.
"""

from lux_pipeline.process.tasks.load_task import LOAD_TYPES, LoadManager

from ._handler import CommandHandler as CH


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--type", choices=LOAD_TYPES, default="records",
                        help="What to load: the source's records dump, a LUX "
                             "export, or an LMDB distribution (default records)")
        ap.add_argument("--no-overwrite", dest="overwrite", action="store_false",
                        help="Keep records already in the datacache")
        ap.add_argument("--clear", action="store_true",
                        help="Empty each source's datacache before loading "
                             "(done once, before the workers start)")
        self.extra_args = {"load_type": "type", "overwrite": "overwrite",
                           "clear": "clear"}

    def make_manager(self, wks, args):
        return LoadManager(self.configs, wks, args)
