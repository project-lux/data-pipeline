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
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
same_map = cfgs.instantiate_map('equivalents')['store']
diff_map = cfgs.instantiate_map('distinct')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '--- ID GOES HERE ---'

# FIXME: Provide sample CSV to upload for these

SHEET_NAMES = [['Different From', diff_map], ['Same As', same_map]]
RANGE_START = 2

creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.

### FIXME: These should use config.data_dir

if os.path.exists('../data/files/token.json'):
    creds = Credentials.from_authorized_user_file('../data/files/token.json', SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            '../data/files/credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('../data/files/token.json', 'w') as token:
        token.write(creds.to_json())

try:
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    for sn, my_map in SHEET_NAMES:
        page = RANGE_START
        rng = f"A{page}:B{page+500}"
        RANGE= f"{sn}!{rng}"
        cont = True
        while cont:
            result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                range=RANGE).execute()
            values = result.get('values', [])

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
                RANGE= f"{sn}!{rng}"                

except HttpError as err:
    print(err)
