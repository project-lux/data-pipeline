from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from pipeline.config import Config
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("populate-timestamps.log","w"),
        logging.StreamHandler()
    ]
)

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.instantiate_all()

directory = cfgs.data_dir

with open(os.path.join(directory, 'populate-timestamps.txt')) as fh:
    SPREADSHEET_ID = fh.read().strip()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

tokfn = os.path.join(directory, 'token-timestamps.json')
credfn = os.path.join(directory, 'credentials.json')
creds = None
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
    with open(tokfn, 'w') as token:
        token.write(creds.to_json())

def format_rows(data):
    return [
        [
            {'userEnteredValue': {'stringValue': cache}},
            {'userEnteredValue': {'stringValue': data[cache]['timestamp']}},
            {'userEnteredValue': {'stringValue': data[cache]['type']}}
        ] for cache in data
    ]

def populate_google_sheet(data):    
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        
        now_str = datetime.now().strftime("%Y-%m-%d")

        # Create a new sheet and get the sheet ID
        add_sheet_body = {'requests': [{'addSheet': {'properties': {'title': now_str}}}]}
        response = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=add_sheet_body).execute()
        sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

        move_sheet_body = {
            'requests': [{
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'index': 0
                    },
                    'fields': 'index'
                }
            }]
        }

        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=move_sheet_body).execute()

        header_row = [
            {'userEnteredValue': {'stringValue': col}, 'userEnteredFormat': {'textFormat': {'bold': True}}}
            for col in ['Source', 'Timestamp', 'Internal or External?']
        ]

        data_rows = format_rows(data)
        
        # Use batchUpdate to set cell values with formatting
        # Append header row
        requests = [{
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
        }]

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

        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'requests': requests}
        ).execute()
        
        logging.info(f"Sheet '{now_str}' updated successfully.")

    except HttpError as err:
        logging.error(f"Error: {err}")

def check_datacache_times(cache, cachetimes):
    internal = cache in cfgs.internal
    datacache = (cfgs.internal if internal else cfgs.external)[cache]['datacache']
    cachets = datacache.latest()
    if cachets.startswith("0000"):
        logging.warning(f"{cache} failed: invalid timestamp")
        return None
    
    cachedt = datetime.fromisoformat(cachets)
    cachetimes[cache] = {
        'timestamp': cachedt.strftime("%Y-%m-%d %H:%M:%S"),
        'type': "Internal" if internal else "External"
    }

def check_file_timestamps():
    filepath = cfgs.exports_dir
    logging.info(f"Checking Marklogic export directory: {filepath}")

    try:
        for file in Path(filepath).iterdir():
            if file.is_file() and file.name.startswith("export"):
                timestamp = file.stat().st_mtime
                datestamp = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

                logging.info(f"Using timestamp from: {file.name}, Timestamp: {datestamp}")
                return {'Marklogic Export': {'timestamp': datestamp, 'type': 'Internal'}}

        logging.warning("No valid files found in the export directory.")
        return {}

    except Exception as e:
        logging.warning(f"Unexpected error with Marklogic export: {e}")
        return {}

def delete_old_tabs():
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', [])
    
        three_months_ago = datetime.now() - timedelta(days=90)
        to_delete = []

        for sheet in sheets:
            title = sheet['properties']['title']
            try:
                sheet_date = datetime.strptime(title, '%Y-%m-%d')
                if sheet_date < three_months_ago:
                    to_delete.append(sheet['properties']['sheetId'])
            except ValueError:
                logging.info(f"Skipped sheet with non-date title: {title}")

        if not to_delete:
            logging.info("No tabs older than 90 days to delete.")
            return

        for sheet_id in to_delete:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'requests': [{'deleteSheet': {'sheetId': sheet_id}}]}
            ).execute()
            logging.info(f"Deleted tab with Sheet ID: {sheet_id}")
    except HttpError as err:
        logging.error(f"Error while deleting old tabs: {err}")

cachetimes = check_file_timestamps()

for cache in cfgs.internal:
    check_datacache_times(cache, cachetimes)

for cache in cfgs.external:
    check_datacache_times(cache, cachetimes)

populate_google_sheet(cachetimes)
delete_old_tabs()
