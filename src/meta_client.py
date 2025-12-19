import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from datetime import datetime, timedelta
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

def get_first_campaign_start_date():
    """
    Obtiene la fecha de inicio de la primera campaña en la cuenta publicitaria.
    Respeta el límite de 37 meses de la API de Meta.
    
    Returns:
        str: Fecha en formato YYYY-MM-DD (máximo 37 meses atrás desde hoy)
    """
    # Límite de la API de Meta: 37 meses (aproximadamente 1127 días)
    max_months_back = 37
    max_days_back = max_months_back * 30  # Aproximadamente 1110 días
    api_limit_date = (datetime.now() - timedelta(days=max_days_back))
    
    try:
        account = get_ad_account()
        
        # Obtener todas las campañas
        campaigns = account.get_campaigns(fields=[
            'id',
            'name',
            'start_time',
            'created_time'
        ])
        
        earliest_date = None
        
        for campaign in campaigns:
            campaign_data = dict(campaign)
            
            # Intentar obtener start_time primero, luego created_time
            start_time = campaign_data.get('start_time')
            if not start_time:
                start_time = campaign_data.get('created_time')
            
            if start_time:
                # Convertir a datetime
                if isinstance(start_time, str):
                    try:
                        # Formato: 2023-01-01T00:00:00+0000
                        dt = datetime.strptime(start_time.split('+')[0].split('.')[0], '%Y-%m-%dT%H:%M:%S')
                    except:
                        try:
                            # Formato alternativo
                            dt = datetime.strptime(start_time.split('T')[0], '%Y-%m-%d')
                        except:
                            continue
                else:
                    dt = start_time
                
                # Comparar y guardar la fecha más antigua
                if earliest_date is None or dt < earliest_date:
                    earliest_date = dt
        
        # Si encontramos una fecha, verificar que no exceda el límite de la API
        if earliest_date:
            # Si la fecha es más antigua que el límite, usar el límite
            if earliest_date < api_limit_date:
                logging.info(f"First campaign date ({earliest_date.strftime('%Y-%m-%d')}) is older than API limit (37 months). Using limit date: {api_limit_date.strftime('%Y-%m-%d')}")
                return api_limit_date.strftime('%Y-%m-%d')
            else:
                return earliest_date.strftime('%Y-%m-%d')
        else:
            # Si no se puede obtener, usar el límite de la API (37 meses atrás)
            logging.warning(f"Could not determine first campaign date, using API limit date (37 months ago): {api_limit_date.strftime('%Y-%m-%d')}")
            return api_limit_date.strftime('%Y-%m-%d')
            
    except Exception as e:
        logging.warning(f"Error getting first campaign date: {str(e)}. Using API limit date (37 months ago).")
        # Usar el límite de la API como fallback
        return api_limit_date.strftime('%Y-%m-%d')
