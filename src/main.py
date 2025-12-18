import os
import sys
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.meta_client import init_api
from src.extract import fetch_campaign_insights, save_to_csv
from src.upload_sheets import upload_to_sheet

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
        # Default to yesterday if not specified
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        start_date = os.getenv('START_DATE') or yesterday
        end_date = os.getenv('END_DATE') or yesterday
        
        logging.info(f"Starting pipeline for period: {start_date} to {end_date}")
        
        # Extract
        df = fetch_campaign_insights(start_date, end_date)
        
        if df.empty:
            logging.info("No data to upload. Exiting.")
            return

        # Save to CSV
        csv_filename = 'meta_ads.csv'
        save_to_csv(df, csv_filename)
        
        # Upload to Sheets
        sheet_id = os.getenv('GOOGLE_SHEET_ID')
        if not sheet_id:
            logging.error("GOOGLE_SHEET_ID not set in environment variables.")
            return
            
        upload_to_sheet(csv_filename, sheet_id)
        
        logging.info("Pipeline completed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
