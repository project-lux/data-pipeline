"""Load source dumps into their datacaches.

    lux load --source ror                          # one process
    lux load --source wikidata --max-workers 24    # 24 workers, one slice each
    lux load --source all --type export            # LUX exports into datacaches
    lux load --source ycba --no-overwrite --clear
    lux load --source wikidata --max-workers 24 --file /data/dumps/latest-all.json.bz2
    lux load --source ycba --file extra.jsonl.gz --file-type lines.gz/json

Replaces `manage-data.py --load` and `import_parallel.sh`. See
`lux_pipeline.process.tasks.load_task` for how each loader is dispatched.

`--file` loads the given file (or directory) instead of the one selected
source's configured one; `--file-type` gives its loader-DSL type when it
differs from the configured file's (otherwise the configured type, or one
guessed from the content). A loader with its own `load()` reads the file
its own way and takes no `--file-type`.
"""

import os

from lux_pipeline.process.tasks.load_task import LOAD_TYPES, LoadManager

from ._handler import CommandHandler as CH


def existing_path(value):
    path = os.path.abspath(os.path.expanduser(value))
    if not os.path.exists(path):
        raise ValueError(f"{value} does not exist")
    return path


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
        ap.add_argument("--file", type=existing_path,
                        help="Load this file or directory instead of the configured one; "
                             "one --source only")
        ap.add_argument("--file-type", "--file_type", dest="file_type",
                        help="The --file's loader-DSL type, e.g. lines.gz/json (default: the "
                             "configured file's type, else guessed from the content)")
        self.extra_args = {"load_type": "type", "overwrite": "overwrite",
                           "clear": "clear", "file": "file", "file_type": "file_type"}

    def make_manager(self, wks, args):
        return LoadManager(self.configs, wks, args)
