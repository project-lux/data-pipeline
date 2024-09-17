import os
import sys
import csv
import json
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

gidfn = os.path.join(cfgs.data_dir, "google_sheet_id.txt")
fh = open(gidfn)
SPREADSHEET_ID = fh.read().strip()
fh.close()

diff_map = {}
same_map = {}
SHEET_NAMES = [["Different From", diff_map], ["Same As", same_map]]
RANGE_START = 2

creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.

### FIXME: These should use config.data_dir
tokfn = os.path.join(cfgs.data_dir, "token.json")
credfn = os.path.join(cfgs.data_dir, "credentials.json")
if os.path.exists(tokfn):
    creds = Credentials.from_authorized_user_file(tokfn, SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(credfn, SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(tokfn, "w") as token:
        token.write(creds.to_json())

try:
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    for sn, my_map in SHEET_NAMES:
        page = RANGE_START
        rng = f"A{page}:B{page+500}"
        RANGE = f"{sn}!{rng}"
        cont = True
        while cont:
            result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
            values = result.get("values", [])

            if not values:
                cont = False
            else:
                for row in values:
                    uria, urib = row
                    # Canonicalize URIs
                    uriaf = cfgs.canonicalize(uria)
                    uribf = cfgs.canonicalize(urib)
                    if not uriaf:
                        print(f"Failed to canonicalize {uria}")
                    elif not uribf:
                        print(f"Failed to canonicalize {urib}")
                    else:
                        my_map[uriaf] = uribf
                page += 500
                rng = f"A{page}:B{page+500}"
                RANGE = f"{sn}!{rng}"

except HttpError as err:
    print(f"Trapped error: {err}")

# Now write the dicts to CSVs
dfn = os.path.join(cfgs.data_dir, "differentFrom/google.csv")
with open(dfn, "w") as fh:
    writer = csv.writer(fh)
    for r in diff_map.items():
        writer.writerow(r)
sfn = os.path.join(cfgs.data_dir, "sameAs/google.csv")
with open(sfn, "w") as fh:
    writer = csv.writer(fh)
    for r in same_map.items():
        writer.writerow(r)

# Later we'll call load-csv-map2.py on them

### Now fetch the Fixes sheet

fixes = []

page = RANGE_START
rng = f"A{page}:G{page+500}"
RANGE = f"Fixes!{rng}"
cont = True
while cont:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    values = result.get("values", [])
    if not values:
        cont = False
    else:
        for row in values:
            try:
                src, ident, clss, equiv, path, op = row[:6]
                if len(row) == 7:
                    arg = row[6]
                else:
                    arg = ""
            except:
                print(row)
                cont = False
            fixes.append(
                {
                    "source": src.strip(),
                    "identifier": ident.strip(),
                    "class": clss.strip(),
                    "equivalent": equiv.strip(),
                    "path": path.strip(),
                    "operation": op.strip(),
                    "argument": arg.strip(),
                }
            )
        page += 500
        rng = f"A{page}:G{page+500}"
        RANGE = f"Fixes!{rng}"

dfn = os.path.join(cfgs.data_dir, "xpath_fixes.json")
ofh = open(dfn, "w")
ofh.write(json.dumps(fixes))
ofh.close()
