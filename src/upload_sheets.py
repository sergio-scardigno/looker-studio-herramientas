import os
import gspread
from google.oauth2.service_account import Credentials
import logging
import pandas as pd

def get_gspread_client():
    """Authenticates with Google Sheets API using Service Account."""
    creds_path = os.getenv('GOOGLE_CREDS_PATH', 'credentials.json')
    
    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"Credentials file not found at {creds_path}")
        
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    logging.info("Authenticated with Google Sheets successfully.")
    return client

def upload_to_sheet(csv_path, sheet_id, worksheet_name='MetaAdsData'):
    """
    Uploads CSV data to a specific Google Sheet worksheet.
    Clears the worksheet before uploading.
    """
    if not os.path.exists(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        return

    client = get_gspread_client()
    
    try:
        sheet = client.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound:
        logging.error(f"Spreadsheet with ID {sheet_id} not found.")
        return

    try:
        worksheet = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        logging.info(f"Worksheet {worksheet_name} not found. Creating it.")
        worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
        
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Handle NaN values (Google Sheets doesn't like them)
    df = df.fillna('')
    
    # Clear existing content
    worksheet.clear()
    
    # Update with new data
    # set_with_dataframe is deprecated in newer gspread-dataframe versions or requires extra lib
    # We will use basic gspread update
    
    data = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.update(range_name='A1', values=data)
    
    logging.info(f"Successfully uploaded {len(df)} rows to Google Sheet.")
