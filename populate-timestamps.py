import os
import sys
import csv
import json
import time

from google.oauth2.service_account.credentials import Credentials
from googleapiclient.discovery import build


from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

directory = "/home/kd736/data-pipeline"

gidfn = os.path.join(directory, 'populate-timestamps.txt')
fh = open(gidfn)
SPREADSHEET_ID = fh.read().strip()
fh.close()

scope = ['https://www.googleapis.com/auth/spreadsheets']


def populate_google_sheet(data):
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credfn = os.path.join(cfgs.data_dir, 'credentials.json')
    creds = Credentials.from_service_account_file(credfn, scopes=scope)
    
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    
    body = {
        'values': data
    }
    
    result = sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range='Sheet1!A1',
        valueInputOption='RAW',
        body=body
    ).execute()

    print(f"{result.get('updatedCells')} cells updated.")

# Example usage
data = [
    ['Name', 'Age', 'City'],
    ['Alice', '24', 'New York'],
    ['Bob', '30', 'Los Angeles']
]

populate_google_sheet(data)
