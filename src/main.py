import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.meta_client import init_api, get_first_campaign_start_date
from src.extract import fetch_campaign_insights, save_to_csv
from src.transform import normalize_raw_data, create_reporting_tables
from src.upload_sheets import upload_multiple_tables

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    # Load environment variables
    load_dotenv()
    
    try:
        # Initialize Meta API
        init_api()
        
        # Determine date range
        # Check if user wants to fetch from beginning
        fetch_from_beginning = os.getenv('FETCH_FROM_BEGINNING', '').lower() in ('true', '1', 'yes')
        
        if fetch_from_beginning or not os.getenv('START_DATE'):
            # Obtener fecha de la primera campaña
            logging.info("Fetching first campaign start date...")
            first_campaign_date = get_first_campaign_start_date()
            start_date = os.getenv('START_DATE') or first_campaign_date
            logging.info(f"Using first campaign date as start: {start_date}")
        else:
            start_date = os.getenv('START_DATE')
        
        # Default end_date to yesterday if not specified
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = os.getenv('END_DATE') or yesterday
        
        logging.info(f"Starting pipeline for period: {start_date} to {end_date}")
        
        # Extract
        df_raw = fetch_campaign_insights(start_date, end_date)
        
        if df_raw.empty:
            logging.info("No data to upload. Exiting.")
            return

        # Normalize and transform data
        logging.info("Normalizing and transforming data for reporting...")
        df_normalized = normalize_raw_data(df_raw)
        
        # Create reporting tables
        tables = create_reporting_tables(df_normalized)
        
        if not tables:
            logging.warning("No reporting tables created. Exiting.")
            return
        
        # Save raw CSV for backup/reference
        csv_filename = 'meta_ads.csv'
        save_to_csv(df_normalized, csv_filename)
        logging.info(f"Raw data saved to {csv_filename}")
        
        # Upload all tables to Google Sheets
        sheet_id = os.getenv('GOOGLE_SHEET_ID')
        if not sheet_id:
            logging.error("GOOGLE_SHEET_ID not set in environment variables.")
            return
        
        logging.info(f"Uploading {len(tables)} tables to Google Sheet...")
        upload_multiple_tables(tables, sheet_id)
        
        logging.info("Pipeline completed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
