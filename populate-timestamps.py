from oauth2client.service_account import ServiceAccountCredentials
import gspread

import os
import sys
import csv
import json
import time


from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)

def populate_google_sheet(sheet_name, data):
    # Step 2: Define the scope and load credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets']

	credfn = os.path.join(cfgs.data_dir, 'credentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(cdfn, scope)
    client = gspread.authorize(creds)

    # Step 3: Open the sheet and select the first worksheet
    sheet = client.open("LUX Dataset Timestamps").sheet1

    # Step 4: Populate the sheet
    # Assuming 'data' is a list of lists, where each sublist is a row
    for i, row in enumerate(data, start=1):  # Adjust 'start' based on your sheet's requirements
        for j, value in enumerate(row, start=1):  # Adjust 'start' based on your sheet's requirements
            sheet.update_cell(i, j, value)

# Example usage
data = [
    ['Name', 'Age', 'City'],
    ['Alice', '24', 'New York'],
    ['Bob', '30', 'Los Angeles']
]
populate_google_sheet('Your Sheet Name', data)