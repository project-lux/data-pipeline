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
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

directory = "/home/kd736/data-pipeline"

gidfn = os.path.join(directory, 'populate-timestamps.txt')
fh = open(gidfn)
SPREADSHEET_ID = fh.read().strip()
fh.close()


RANGE_START = 2


# Example usage
data = [
	['Name', 'Age', 'City'],
	['Alice', '24', 'New York'],
	['Bob', '30', 'Los Angeles']
]

creds = None

scope = ['https://www.googleapis.com/auth/spreadsheets']

tokfn = os.path.join(cfgs.data_dir, 'timestamps-token.json')
credfn = os.path.join(cfgs.data_dir, 'credentials.json')
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

try:
	service = build('sheets', 'v4', credentials=creds)
	sheet = service.spreadsheets()

	for i, row in enumerate(data, start=1):  
		for j, value in enumerate(row, start=1):  
			sheet.update_cell(i, j, value)


except HttpError as err:
	print(err)