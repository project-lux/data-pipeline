from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os
import requests
import random
from datetime import datetime
from pipeline.config import Config
from dotenv import load_dotenv

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.instantiate_all()

directory = cfgs.data_dir
creds = None
gidfn = os.path.join(directory, 'populate-timestamps.txt')
with open(gidfn) as fh:
    SPREADSHEET_ID = fh.read().strip()

scope = ['https://www.googleapis.com/auth/spreadsheets']

tokfn = os.path.join(directory, 'token-timestamps.json')
credfn = os.path.join(directory, 'credentials.json')
if os.path.exists(tokfn):
    creds = Credentials.from_authorized_user_file(tokfn, scope)

# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(credfn, scope)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(tokfn, 'w') as token:
        token.write(creds.to_json())

def populate_google_sheet(data):    
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        
        now = datetime.now()
        now_str = now.strftime("%B %d, %Y")

        # Create a new sheet and get the sheet ID
        add_sheet_body = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': now_str
                        }
                    }
                }
            ]
        }
        response = sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=add_sheet_body
        ).execute()

        sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

        header_row = [
            {'userEnteredValue': {'stringValue': 'Source'}, 'userEnteredFormat': {'textFormat': {'bold': True}}},
            {'userEnteredValue': {'stringValue': 'Timestamp'}, 'userEnteredFormat': {'textFormat': {'bold': True}}},
            {'userEnteredValue': {'stringValue': 'Internal or External?'}, 'userEnteredFormat': {'textFormat': {'bold': True}}}
        ]

        data_rows = [
            [
                {'userEnteredValue': {'stringValue': cache}},
                {'userEnteredValue': {'stringValue': data[cache]['timestamp']}},
                {'userEnteredValue': {'stringValue': data[cache]['type']}}
            ] for cache in data
        ]
        
        # Use batchUpdate to set cell values with formatting
        requests = []
        # Append header row
        requests.append({
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': 3
                },
                'rows': [{'values': header_row}],
                'fields': 'userEnteredValue,userEnteredFormat'
            }
        })

        # Append data rows
        for i, row in enumerate(data_rows, start=1):
            requests.append({
                'updateCells': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': i,
                        'endRowIndex': i + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 3
                    },
                    'rows': [{'values': row}],
                    'fields': 'userEnteredValue'
                }
            })

        body = {
            'requests': requests
        }

        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()
        
        print(f"Sheet '{now_str}' updated successfully.")

    except HttpError as err:
        print(err)

def check_datacache_times(cache):
    print(f"Checking {cache} timestamp")

    internal = False
    if cache in ['ils','ipch','ycba','yuag','ypm','pmc']:
        datacache = cfgs.internal[cache]['datacache']
        internal = True
    else:
        datacache = cfgs.external[cache]['datacache']
    cachets = datacache.latest()
    if cachets.startswith("0000"):
        print(f"{cache} failed because latest time begins with 0000")
        return None
    
    cachedt = datetime.fromisoformat(cachets)
    datetime_str = cachedt.strftime("%Y-%m-%d %H:%M:%S")

    cachetimes[cache] = {
        'timestamp': datetime_str,
        'type': "Internal" if internal else "External"
    }

def check_file_timestamps():
    filepath = f"/data-io2-2/output/lux/latest/"
    print(f"Checking Marklogic timestamp")
    for filename in os.listdir(filepath):
        file = os.path.join(filepath, filename)
        if os.path.isfile(file):         
            try:
                timestamp = os.path.getmtime(file)
                datestamp = datetime.fromtimestamp(timestamp)
                datetime_str = datestamp.strftime("%Y-%m-%d %H:%M:%S")
                cachetimes["Marklogic Export"] = {
                    'timestamp': datetime_str,
                    'type': "Internal"
                }
                break
            except OSError as e:
                print(f"***Failed getting timestamp from Marklogic export")
                return None

    return cachetimes

cachetimes = {}
check_file_timestamps()

for cache in cfgs.internal:
    check_datacache_times(cache)

for cache in cfgs.external:
    check_datacache_times(cache)


print("Writing to Google Sheet")
populate_google_sheet(cachetimes)
