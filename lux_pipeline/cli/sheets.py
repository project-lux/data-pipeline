"""Read the curators' spreadsheet and load its same-as / different-from pairs.

    lux sheets                          # the Google Sheet: fetch, then load into the merged indexes
    lux sheets --fetch-only             # write the CSVs and xpath_fixes.json, load nothing
    lux sheets --load-only              # load the CSVs already in data_dir
    lux sheets --file curation.xlsx     # the same tabs from an Excel workbook
    lux sheets --file exported/         # or from a directory of <tab name>.csv files

Replaces `google-sames-diffs.py` and the two `load-csv-map2.py --same /
--different .../google.csv` calls of `run-all.sh`; `lux build` runs it as
its `sheets` phase. Writes ``<data_dir>/sameAs/google.csv``,
``<data_dir>/differentFrom/google.csv`` and ``<data_dir>/xpath_fixes.json``;
see `lux_pipeline.process.sheets` for the tabs and the formats. The Google
Sheet needs ``pip install -e '.[google]'``, a workbook ``.[xlsx]``.

Refuses while a ``lux`` build is running (``--force`` overrides): reconcile
and merge read the indexes it loads, and every mapper the fixes file.
"""

from lux_pipeline.process import sheets

from ._handler import BaseHandler, refuse_while_building


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--file", help="Read an .xlsx workbook or a directory of <tab name>.csv "
                                       "files instead of the Google Sheet")
        only = ap.add_mutually_exclusive_group()
        only.add_argument("--fetch-only", "--fetch_only", dest="fetch_only", action="store_true",
                          help="Read the spreadsheet and write the files; load nothing")
        only.add_argument("--load-only", "--load_only", dest="load_only", action="store_true",
                          help="Load the CSVs already in data_dir; do not read the spreadsheet")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def process(self, args, rest):
        super().process(args, rest)
        importer = sheets.sheet_import(self.configs, args.file)
        refuse_while_building(self.configs, "sheets", args.force)
        counts = loaded = None
        if not args.load_only:
            counts = importer.fetch()
            shown = {k: "no tab" if v is None else v for k, v in counts.items()}
            print(f"{importer.describe()}: {shown['equivs']} same-as, "
                  f"{shown['diffs']} different-from, {shown['fixes']} fixes")
        if not args.fetch_only:
            loaded = importer.load()
            print(f"Loaded {len(loaded)} file(s) into the merged indexes")
        return counts, loaded
