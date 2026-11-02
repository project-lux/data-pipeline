"""Write sources' own different-from assertions as CSVs, and optionally load them.

    lux differents --source wikidata --max-workers 24          # <data_dir>/differentFrom/wikidata.csv
    lux differents --source wikidata --max-workers 24 --load   # ... and add it to the merged index
    lux differents --source wikidata --max-workers 24 --my-worker 3 --engine null
    lux differents --source wikidata --max-workers 24 --merge-only --load

Replaces the root ``make-wd-differents.py``. Each worker reads its slice of
the source's datacache through the source's ``process_equivs`` (wikidata:
``P1889``, plus every pair of items on a disambiguation page), and the
driver merges the slices into ``<data_dir>/differentFrom/<source>.csv``
(``--out-dir`` overrides) as full URI pairs, sorted and without
duplicates; a failed run leaves the previous file in place. With
``--my-worker`` only that slice's part is written; ``--merge-only`` merges
the parts once every slice has run. ``--load`` adds the CSV to the merged
differents index (what ``load-csv-map2.py --different`` did), refusing
while a build runs unless ``--force``. The default ``--source all`` covers
every source that has such assertions.
"""

import logging

from lux_pipeline.process.tasks.differents_task import DifferentsManager

from ._handler import CommandHandler as CH, refuse_while_building

logger = logging.getLogger("lux_pipeline")


class CommandHandler(CH):
    def add_args(self, ap):
        ap.add_argument("--out-dir", dest="out_dir", default=None,
                        help="Where to write <source>.csv (default <data_dir>/differentFrom)")
        ap.add_argument("--load", action="store_true",
                        help="Add the CSVs to the merged differents index afterwards")
        ap.add_argument("--merge-only", dest="merge_only", action="store_true",
                        help="Only merge the parts every --my-worker slice wrote")
        ap.add_argument("--force", action="store_true",
                        help="With --load, load even while a build is running")
        self.extra_args = {"out_dir": "out_dir", "load": "load", "merge_only": "merge_only"}

    def make_manager(self, wks, args):
        # called once the options are parsed, before any worker starts; a
        # --my-worker slice loads nothing
        if args.load and getattr(args, "my_worker", -1) == -1:
            refuse_while_building(self.configs, "differents --load", args.force)
        return DifferentsManager(self.configs, wks, args)

    def process(self, args, rest):
        manager = super().process(args, rest)
        if manager is None:
            return None
        s = manager.summary()
        # the summary is the command's result
        for name, c in s["sources"].items():
            print(f"{name}: {c['records']:,} records read, {c['with']:,} with different-from")
        for name, (path, n) in s["written"].items():
            print(f"{name}: {n:,} pairs in {path}")
        for path in s["loaded"]:
            print(f"loaded {path} into the merged differents index")
        return manager
