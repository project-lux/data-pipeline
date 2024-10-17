from flask import Flask, render_template, request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from debugger import process_uri

from dotenv import load_dotenv
from pipeline.config import Config
import os

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

directory = cfgs.data_dir
creds = None
gidfn = os.path.join(cfgs.data_dir, 'google_sheet_id.txt')
fh = open(gidfn)
SPREADSHEET_ID = fh.read().strip()
fh.close()
RANGE_NAME = 'Sheet1!A:B'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

tokfn = os.path.join(cfgs.data_dir, 'token-timestamps.json')
credfn = os.path.join(cfgs.data_dir, 'credentials.json')
if os.path.exists(tokfn):
    creds = Credentials.from_authorized_user_file(tokfn, SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(credfn, SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(tokfn, 'w') as token:
        token.write(creds.to_json())

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

app = Flask(__name__)
@app.route('/', methods=['GET','POST'])

def index():
    if request.method == "POST":
        #get uri from form
        uri = request.form['uri']
        if "view" in uri:
            uri = uri.replace("view","data")

        # Check the status of the checkboxes
        option1 = 'option1' in request.form
        option2 = 'option2' in request.form


        #call function to process URI
        result = process_uri(uri, option1, option2)

        #pass result to template
        return render_template('result.html', result=result)
    return render_template("index.html")

@app.route('/add_to_sheet', methods=['POST'])
def add_to_sheet():
    record_equivalent_pairs = request.form.getlist('record_equivalent_pairs')  # Get selected pairs
    
    # Prepare the data to append
    values = []
    for pair in record_equivalent_pairs:
        record_uri, equivalent_uri = pair.split(",")  # Split the pair into Record and Equivalent
        values.append([record_uri, equivalent_uri])  # Append as a list of lists

    # Create the request body
    body = {
        'values': values
    }

    # Call the Sheets API to append the data
    result = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption="RAW",
        body=body
    ).execute()

    return "Selected pairs added to Google Sheet!"

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=8080)
