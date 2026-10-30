"""Fetch the curators' Google Sheet and load its same-as / different-from pairs.

    lux google                 # fetch, then load into the merged indexes
    lux google --fetch-only    # write the CSVs and xpath_fixes.json, load nothing
    lux google --load-only     # load the CSVs already in data_dir

Replaces `google-sames-diffs.py` and the two `load-csv-map2.py --same /
--different .../google.csv` calls of `run-all.sh`; `lux build` runs it as
its `google` phase. Writes ``<data_dir>/sameAs/google.csv``,
``<data_dir>/differentFrom/google.csv`` and ``<data_dir>/xpath_fixes.json``;
see `lux_pipeline.process.google_sheet`. Needs the optional Google client
libraries (``pip install -e '.[google]'``) unless ``--load-only``.

Refuses while a ``lux`` build is running (``--force`` overrides): reconcile
and merge read the indexes it loads, and every mapper the fixes file.
"""

from lux_pipeline.process import google_sheet

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        only = ap.add_mutually_exclusive_group()
        only.add_argument("--fetch-only", "--fetch_only", dest="fetch_only", action="store_true",
                          help="Read the sheet and write the files; load nothing")
        only.add_argument("--load-only", "--load_only", dest="load_only", action="store_true",
                          help="Load the CSVs already in data_dir; do not read the sheet")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        refuse_while_building(self.configs, "google", args.force)
        counts = loaded = None
        if not args.load_only:
            counts = google_sheet.fetch(self.configs)
            print(f"Sheet read: {counts['equivs']} same-as, {counts['diffs']} different-from, "
                  f"{counts['fixes']} fixes")
        if not args.fetch_only:
            loaded = google_sheet.load(self.configs)
            print(f"Loaded {len(loaded)} file(s) into the merged indexes")
        return counts, loaded
