import os
import gspread
from google.oauth2.service_account import Credentials
import logging
import pandas as pd
from typing import Dict

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
    
    Args:
        csv_path: Path to CSV file
        sheet_id: Google Sheet ID
        worksheet_name: Name of the worksheet
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
    data = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.update(range_name='A1', values=data)
    
    logging.info(f"Successfully uploaded {len(df)} rows to worksheet '{worksheet_name}'.")

def upload_multiple_tables(tables: Dict[str, pd.DataFrame], sheet_id: str):
    """
    Uploads multiple DataFrames to separate worksheets in a Google Sheet.
    Creates worksheets if they don't exist, clears them before uploading.
    
    Args:
        tables: Dictionary with worksheet names as keys and DataFrames as values
        sheet_id: Google Sheet ID
    """
    if not tables:
        logging.warning("No tables to upload.")
        return
    
    client = get_gspread_client()
    
    try:
        sheet = client.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound:
        logging.error(f"Spreadsheet with ID {sheet_id} not found.")
        return
    
    for worksheet_name, df in tables.items():
        if df.empty:
            logging.warning(f"DataFrame for '{worksheet_name}' is empty, skipping.")
            continue
        
        try:
            worksheet = sheet.worksheet(worksheet_name)
            logging.info(f"Worksheet '{worksheet_name}' exists, clearing it.")
            worksheet.clear()
        except gspread.WorksheetNotFound:
            logging.info(f"Worksheet '{worksheet_name}' not found. Creating it.")
            # Estimar tamaño necesario
            rows = max(1000, len(df) + 10)
            cols = max(20, len(df.columns) + 5)
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)
        
        # Handle NaN values (Google Sheets doesn't like them)
        df_clean = df.fillna('')
        
        # Convertir fechas a string para evitar problemas
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
        
        # Update with new data
        data = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
        
        try:
            worksheet.update(range_name='A1', values=data)
            logging.info(f"Successfully uploaded {len(df_clean)} rows to worksheet '{worksheet_name}'.")
        except Exception as e:
            logging.error(f"Error uploading to '{worksheet_name}': {str(e)}")
            continue
    
    logging.info(f"Completed upload of {len(tables)} tables to Google Sheet.")
