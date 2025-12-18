import logging
from src.meta_client import get_ad_account
from facebook_business.adobjects.adsinsights import AdsInsights
import pandas as pd

def fetch_campaign_insights(start_date, end_date):
    """
    Fetches insights from Meta Ads API for the given date range.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        
    Returns:
        pd.DataFrame: DataFrame containing the insights data.
    """
    account = get_ad_account()
    
    fields = [
        'campaign_name',
        'adset_name',
        'ad_name',
        'impressions',
        'clicks',
        'spend',
        'ctr',
        'cpc',
        'reach',
        'date_start',
        'date_stop'
    ]
    
    params = {
        'level': 'ad',
        'time_range': {'since': start_date, 'until': end_date},
        'time_increment': 1, # Daily breakdown
    }
    
    logging.info(f"Fetching insights from {start_date} to {end_date}...")
    
    insights = account.get_insights(fields=fields, params=params)
    
    data = []
    for item in insights:
        data.append(dict(item))
        
    # Handle pagination
    while insights.load_next_page():
        for item in insights:
            data.append(dict(item))
            
    if not data:
        logging.warning("No data found for the specified date range.")
        return pd.DataFrame(columns=fields)
        
    df = pd.DataFrame(data)
    logging.info(f"Fetched {len(df)} records.")
    return df

def save_to_csv(df, filename='meta_ads.csv'):
    """Saves the DataFrame to a CSV file."""
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")
