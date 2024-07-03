from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os


directory = "/home/kd736/data-pipeline"
creds = None
gidfn = os.path.join(directory, 'populate-timestamps.txt')
fh = open(gidfn)
SPREADSHEET_ID = fh.read().strip()
fh.close()

directory = "/Users/kd736/Desktop/Development/data-pipeline/"
scope = ['https://www.googleapis.com/auth/spreadsheets']

tokfn = os.path.join(directory, 'token.json')
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
