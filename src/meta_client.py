import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import logging

def init_api():
    """Initializes the Meta Ads API with credentials from environment variables."""
    access_token = os.getenv('META_ACCESS_TOKEN')
    app_id = os.getenv('META_APP_ID') # Optional, but good practice if available
    app_secret = os.getenv('META_APP_SECRET') # Optional

    if not access_token:
        raise ValueError("Missing META_ACCESS_TOKEN environment variable.")

    FacebookAdsApi.init(access_token=access_token)
    logging.info("Meta Ads API initialized successfully.")

def get_ad_account():
    """Returns the AdAccount object based on the ID in environment variables."""
    account_id = os.getenv('META_AD_ACCOUNT_ID')
    if not account_id:
        raise ValueError("Missing META_AD_ACCOUNT_ID environment variable.")
    
    # Ensure account_id starts with 'act_'
    if not account_id.startswith('act_'):
        account_id = f'act_{account_id}'
        
    return AdAccount(account_id)
