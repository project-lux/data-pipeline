"""The curators' Google Sheet: same-as / different-from pairs and record fixes.

`fetch` reads the sheet named by ``<data_dir>/google_sheet_id.txt`` and
writes ``<data_dir>/sameAs/google.csv``, ``<data_dir>/differentFrom/google.csv``
and ``<data_dir>/xpath_fixes.json`` (read by every mapper). `load` adds the
two CSVs to the merged source's equivalent / different indexes. Together
they are `google-sames-diffs.py` followed by the two `load-csv-map2.py`
calls of `run-all.sh`.

The Google client libraries are optional (``pip install -e '.[google]'``)
and imported only when the sheet is read. OAuth credentials are
``<data_dir>/credentials.json``; the token minted from them is kept in
``<data_dir>/token.json``.
"""

import csv
import json
import logging
import os

logger = logging.getLogger("lux_pipeline")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SAME_SHEET = "Same As"
DIFFERENT_SHEET = "Different From"
FIXES_SHEET = "Fixes"
FIX_FIELDS = ("source", "identifier", "class", "equivalent", "path", "operation", "argument")
FIRST_ROW = 2  # row 1 is the header
PAGE = 500


def csv_paths(data_dir):
    """``{"equivs": same-as CSV, "diffs": different-from CSV}``"""
    return {"equivs": os.path.join(data_dir, "sameAs", "google.csv"),
            "diffs": os.path.join(data_dir, "differentFrom", "google.csv")}


def sheet_client(data_dir):
    """The Sheets API ``spreadsheets()`` resource, authorised from the
    token in `data_dir`, refreshing it or running the browser flow on
    ``credentials.json`` when it is missing or expired."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as e:
        raise RuntimeError(f"the Google client libraries are not installed ({e}); "
                           f"pip install -e '.[google]'") from e

    tokfn = os.path.join(data_dir, "token.json")
    credfn = os.path.join(data_dir, "credentials.json")
    creds = Credentials.from_authorized_user_file(tokfn, SCOPES) if os.path.exists(tokfn) else None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(credfn):
                raise RuntimeError(f"no valid token in {tokfn} and no {credfn} to authorise with")
            creds = InstalledAppFlow.from_client_secrets_file(credfn, SCOPES).run_local_server(port=0)
        with open(tokfn, "w") as fh:
            fh.write(creds.to_json())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def read_rows(sheet, spreadsheet_id, name, last_col):
    """Every row of sheet `name` from row 2, columns A to `last_col`, a
    page at a time until a page comes back empty."""
    start = FIRST_ROW
    while True:
        rng = f"{name}!A{start}:{last_col}{start + PAGE - 1}"
        values = sheet.values().get(spreadsheetId=spreadsheet_id, range=rng).execute().get("values", [])
        if not values:
            return
        yield from values
        start += PAGE


def canonical_pairs(rows, canonicalize, name):
    """``[[a, b]]`` canonicalized; rows that are not two URIs, or that do
    not canonicalize, are logged and dropped."""
    out = []
    for n, row in enumerate(rows, FIRST_ROW):
        if len(row) < 2 or not row[0].strip() or not row[1].strip():
            if any(c.strip() for c in row):
                logger.warning(f"{name} row {n}: not two URIs: {row}")
            continue
        a, b = row[0].strip(), row[1].strip()
        af, bf = canonicalize(a), canonicalize(b)
        if not af:
            logger.warning(f"{name} row {n}: failed to canonicalize {a}")
        elif not bf:
            logger.warning(f"{name} row {n}: failed to canonicalize {b}")
        else:
            out.append([af, bf])
    return out


def parse_fixes(rows):
    """The Fixes sheet as dicts of `FIX_FIELDS`; rows with fewer than six
    cells are logged and dropped, the seventh (argument) may be empty."""
    out = []
    for n, row in enumerate(rows, FIRST_ROW):
        if len(row) < 6:
            if any(c.strip() for c in row):
                logger.warning(f"{FIXES_SHEET} row {n}: needs at least six cells: {row}")
            continue
        cells = [c.strip() for c in row[:7]] + [""] * (7 - len(row[:7]))
        out.append(dict(zip(FIX_FIELDS, cells)))
    return out


def write_pairs(path, pairs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(pairs)


def fetch(cfgs, sheet=None):
    """Read the sheet and write the two CSVs and ``xpath_fixes.json``.
    Returns ``{"equivs": n, "diffs": n, "fixes": n}``. An empty same-as or
    different-from sheet leaves the previous CSV in place, as the script
    did. Any API error propagates, so a half-read sheet never overwrites a
    file."""
    data_dir = cfgs.data_dir
    with open(os.path.join(data_dir, "google_sheet_id.txt")) as fh:
        sheet_id = fh.read().strip()
    if sheet is None:
        sheet = sheet_client(data_dir)

    paths = csv_paths(data_dir)
    counts = {}
    for which, name in (("diffs", DIFFERENT_SHEET), ("equivs", SAME_SHEET)):
        pairs = canonical_pairs(read_rows(sheet, sheet_id, name, "B"), cfgs.canonicalize, name)
        counts[which] = len(pairs)
        if pairs:
            write_pairs(paths[which], pairs)
            logger.info(f"{name}: {len(pairs)} pairs written to {paths[which]}")
        else:
            logger.warning(f"{name}: no rows; {paths[which]} left as it was")

    fixes = parse_fixes(read_rows(sheet, sheet_id, FIXES_SHEET, "G"))
    fixfn = os.path.join(data_dir, "xpath_fixes.json")
    with open(fixfn, "w") as fh:
        fh.write(json.dumps(fixes))
    counts["fixes"] = len(fixes)
    logger.info(f"{FIXES_SHEET}: {len(fixes)} fixes written to {fixfn}")
    return counts


def load(cfgs):
    """Add the two CSVs to the merged source's indexes (adds, never
    clears: other CSVs are loaded into the same indexes). Returns the
    paths loaded."""
    loader = cfgs.results["merged"].get("indexLoader")
    if loader is None:
        raise RuntimeError("the merged source has no indexLoader to load the sheet into")
    loaded = []
    for which, path in csv_paths(cfgs.data_dir).items():
        if not os.path.exists(path):
            logger.warning(f"{path} does not exist; nothing to load into {which}")
            continue
        logger.info(f"loading {path} into {which}")
        loader.load(path, which)
        loaded.append(path)
    return loaded
