import logging
import re
from src.meta_client import get_ad_account
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
import pandas as pd
import json

def fetch_campaign_insights(start_date, end_date, chunk_days=15):
    """
    Fetches insights from Meta Ads API for the given date range.
    Si el rango es muy grande, lo divide en chunks más pequeños para evitar errores de la API.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        chunk_days (int): Número de días por chunk (default: 15 días para evitar errores de la API)
        
    Returns:
        pd.DataFrame: DataFrame containing the insights data.
    """
    from datetime import datetime, timedelta
    import time
    
    # Convertir fechas a datetime
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # No permitir fechas futuras - limitar a hoy
    today = datetime.now().date()
    if end_dt.date() > today:
        end_dt = datetime.combine(today, datetime.min.time())
        logging.warning(f"End date was in the future. Adjusted to today: {end_dt.strftime('%Y-%m-%d')}")
    
    total_days = (end_dt - start_dt).days
    
    # Si el rango es menor o igual a chunk_days, hacer una sola consulta
    if total_days <= chunk_days:
        df = _fetch_insights_chunk_with_retry(start_date, end_dt.strftime('%Y-%m-%d'))
        if not df.empty:
            # Enriquecer con estado de campañas
            if 'campaign_id' in df.columns:
                logging.info("Fetching campaign status information...")
                df = _enrich_with_campaign_status(df)
            # Enriquecer con información de creativos
            if 'ad_id' in df.columns:
                logging.info("Fetching creative information for ads...")
                df = _enrich_with_creative_info(df)
        return df
    
    # Si es más grande, dividir en chunks
    logging.info(f"Date range is {total_days} days. Splitting into chunks of {chunk_days} days...")
    
    all_dataframes = []
    current_start = start_dt
    chunk_number = 0
    
    while current_start < end_dt:
        # Calcular el final del chunk
        current_end = min(current_start + timedelta(days=chunk_days - 1), end_dt)
        
        # Asegurar que no sea fecha futura
        if current_end.date() > today:
            current_end = datetime.combine(today, datetime.min.time())
        
        if current_start >= current_end:
            break
        
        chunk_start_str = current_start.strftime('%Y-%m-%d')
        chunk_end_str = current_end.strftime('%Y-%m-%d')
        chunk_number += 1
        
        logging.info(f"Fetching chunk {chunk_number}: {chunk_start_str} to {chunk_end_str}...")
        
        try:
            chunk_df = _fetch_insights_chunk_with_retry(chunk_start_str, chunk_end_str)
            if not chunk_df.empty:
                all_dataframes.append(chunk_df)
                logging.info(f"✓ Successfully fetched {len(chunk_df)} records from chunk {chunk_number}")
            else:
                logging.warning(f"⚠ Chunk {chunk_number} returned no data")
        except Exception as e:
            error_msg = str(e)
            # Si es un error 500 con subcode 99, intentar con chunk más pequeño
            if "error_subcode" in error_msg and "99" in error_msg:
                logging.warning(f"⚠ Chunk {chunk_number} too large, trying smaller sub-chunks...")
                # Dividir este chunk en mitades
                mid_date = current_start + timedelta(days=(chunk_days // 2))
                if mid_date < current_end:
                    try:
                        df1 = _fetch_insights_chunk_with_retry(chunk_start_str, mid_date.strftime('%Y-%m-%d'))
                        if not df1.empty:
                            all_dataframes.append(df1)
                    except:
                        pass
                    time.sleep(2)  # Delay entre sub-chunks
                    try:
                        df2 = _fetch_insights_chunk_with_retry((mid_date + timedelta(days=1)).strftime('%Y-%m-%d'), chunk_end_str)
                        if not df2.empty:
                            all_dataframes.append(df2)
                    except:
                        pass
                else:
                    logging.error(f"✗ Failed to fetch chunk {chunk_number}: {error_msg[:200]}")
            else:
                logging.error(f"✗ Failed to fetch chunk {chunk_number}: {error_msg[:200]}")
        
        # Delay entre chunks para evitar rate limiting (3 segundos)
        if chunk_number < (total_days // chunk_days + 1):
            time.sleep(3)
        
        # Mover al siguiente chunk
        current_start = current_end + timedelta(days=1)
    
    # Combinar todos los DataFrames
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        # Eliminar duplicados por si acaso
        combined_df = combined_df.drop_duplicates(subset=['ad_id', 'date_start'], keep='first')
        logging.info(f"Combined {len(all_dataframes)} chunks into {len(combined_df)} total records.")
        
        # Obtener información del estado de las campañas
        if not combined_df.empty and 'campaign_id' in combined_df.columns:
            logging.info("Fetching campaign status information...")
            combined_df = _enrich_with_campaign_status(combined_df)
        
        # Obtener información del creativo después de combinar (para evitar duplicados)
        if not combined_df.empty and 'ad_id' in combined_df.columns:
            logging.info("Fetching creative information for ads...")
            combined_df = _enrich_with_creative_info(combined_df)
        
        return combined_df
    else:
        logging.warning("No data fetched from any chunk.")
        return pd.DataFrame()

def _fetch_insights_chunk_with_retry(start_date, end_date, max_retries=3):
    """
    Fetches insights with retry logic for handling transient API errors.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        pd.DataFrame: DataFrame containing the insights data.
    """
    import time
    
    for attempt in range(max_retries):
        try:
            return _fetch_insights_chunk(start_date, end_date)
        except Exception as e:
            error_str = str(e)
            # Si es un error 500 con subcode 99, esperar y reintentar
            if "error_subcode" in error_str and "99" in error_str and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Backoff exponencial: 5s, 10s, 15s
                logging.warning(f"API error (subcode 99), retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                # Si no es un error recuperable o se agotaron los intentos, lanzar el error
                raise

def _fetch_insights_chunk(start_date, end_date):
    """
    Fetches insights from Meta Ads API for a specific date range chunk.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
        
    Returns:
        pd.DataFrame: DataFrame containing the insights data.
    """
    account = get_ad_account()
    
    fields = [
        # Identificadores únicos
        'campaign_id',
        'campaign_name',
        'adset_id',
        'adset_name',
        'ad_id',
        'ad_name',
        
        # Métricas básicas de alcance
        'impressions',
        'reach',
        'frequency',
        
        # Métricas de interacción
        'clicks',
        'unique_clicks',
        'ctr',  # Click-through rate
        'unique_ctr',
        'outbound_clicks',
        
        # Métricas de costo
        'spend',
        'cpc',  # Cost per click
        'cpm',  # Cost per 1000 impressions
        'cpp',  # Cost per purchase
        
        # Conversiones y acciones
        'conversions',
        'cost_per_conversion',
        'actions',  # Incluye link_clicks, post_engagement, etc.
        'action_values',
        'cost_per_action_type',
        
        # Métricas de video (si aplica)
        'video_30_sec_watched_actions',
        'video_avg_time_watched_actions',
        'video_p100_watched_actions',
        'video_play_actions',
        
        # Métricas sociales
        'social_spend',
        
        # Métricas de calidad
        'quality_ranking',
        'engagement_rate_ranking',
        'conversion_rate_ranking',
        
        # Fechas
        'date_start',
        'date_stop'
    ]
    
    params = {
        'level': 'ad',
        'time_range': {'since': start_date, 'until': end_date},
        'time_increment': 1, # Daily breakdown
        'limit': 500  # Limitar resultados por página
    }
    
    logging.info(f"Fetching insights from {start_date} to {end_date}...")
    
    import time
    
    try:
        insights = account.get_insights(fields=fields, params=params)
        
        data = []
        page_count = 0
        max_pages = 50  # Límite de seguridad para evitar loops infinitos
        
        # Primera página
        try:
            for item in insights:
                data.append(dict(item))
        except Exception as e:
            logging.warning(f"Error reading first page: {str(e)[:200]}")
            if not data:
                return pd.DataFrame()
        
        # Handle pagination con límite de seguridad
        try:
            while page_count < max_pages:
                if not insights.load_next_page():
                    break
                page_count += 1
                
                for item in insights:
                    data.append(dict(item))
                
                # Pequeño delay cada 5 páginas para evitar rate limiting
                if page_count % 5 == 0:
                    time.sleep(1)
                    logging.debug(f"Fetched {page_count} pages, {len(data)} records so far...")
        except Exception as e:
            logging.warning(f"Error during pagination (page {page_count}): {str(e)[:200]}. Continuing with data fetched so far...")
        
        if page_count >= max_pages:
            logging.warning(f"Reached maximum page limit ({max_pages}) for chunk {start_date} to {end_date}")
            
    except Exception as e:
        error_str = str(e)
        # Si el error menciona "reduce the amount of data", el chunk es demasiado grande
        if "reduce the amount" in error_str.lower() or "too much data" in error_str.lower() or "error_subcode" in error_str:
            raise Exception(f"Chunk too large or API error. Try reducing chunk_days. Error: {error_str[:300]}")
        else:
            raise
    
    if not data:
        logging.warning(f"No data found for the date range {start_date} to {end_date}.")
        return pd.DataFrame()
        
    df = pd.DataFrame(data)
    
    # Procesar campos complejos que vienen como listas/diccionarios
    df = _process_complex_fields(df)
    
    # NO procesar creativos aquí - se hará después de combinar todos los chunks
    # para evitar consultas duplicadas
    
    logging.info(f"Fetched {len(df)} records with {len(df.columns)} columns from chunk {start_date} to {end_date}.")
    return df

def _process_complex_fields(df):
    """
    Procesa campos complejos como 'actions', 'action_values', etc.
    que vienen como listas de diccionarios y los convierte en columnas separadas.
    """
    # Procesar 'actions' - extraer cada tipo de acción como columna separada
    if 'actions' in df.columns:
        action_types = set()
        for actions_list in df['actions'].dropna():
            if isinstance(actions_list, list):
                for action in actions_list:
                    if isinstance(action, dict) and 'action_type' in action:
                        action_types.add(action['action_type'])
        
        # Crear columnas para cada tipo de acción
        for action_type in action_types:
            col_name = f"actions_{action_type}"
            df[col_name] = df['actions'].apply(
                lambda x: next(
                    (float(a.get('value', 0)) for a in x 
                     if isinstance(x, list) and isinstance(a, dict) 
                     and a.get('action_type') == action_type), 
                    0
                ) if isinstance(x, list) and x else 0
            )
    
    # Procesar 'action_values' - valores monetarios de acciones
    if 'action_values' in df.columns:
        action_value_types = set()
        for action_values_list in df['action_values'].dropna():
            if isinstance(action_values_list, list):
                for action_value in action_values_list:
                    if isinstance(action_value, dict) and 'action_type' in action_value:
                        action_value_types.add(action_value['action_type'])
        
        for action_type in action_value_types:
            col_name = f"action_value_{action_type}"
            df[col_name] = df['action_values'].apply(
                lambda x: next(
                    (float(a.get('value', 0)) for a in x 
                     if isinstance(x, list) and isinstance(a, dict) 
                     and a.get('action_type') == action_type), 
                    0
                ) if isinstance(x, list) and x else 0
            )
    
    # Procesar 'cost_per_action_type' - costo por tipo de acción
    if 'cost_per_action_type' in df.columns:
        cost_action_types = set()
        for cost_list in df['cost_per_action_type'].dropna():
            if isinstance(cost_list, list):
                for cost_item in cost_list:
                    if isinstance(cost_item, dict) and 'action_type' in cost_item:
                        cost_action_types.add(cost_item['action_type'])
        
        for action_type in cost_action_types:
            col_name = f"cost_per_{action_type}"
            df[col_name] = df['cost_per_action_type'].apply(
                lambda x: next(
                    (float(a.get('value', 0)) for a in x 
                     if isinstance(x, list) and isinstance(a, dict) 
                     and a.get('action_type') == action_type), 
                    0
                ) if isinstance(x, list) and x else 0
            )
    
    # Convertir campos numéricos que pueden venir como strings
    numeric_columns = [
        'impressions', 'clicks', 'spend', 'reach', 'frequency',
        'ctr', 'cpc', 'cpm', 'cpp', 'conversions',
        'cost_per_conversion',
        'unique_clicks', 'unique_ctr', 'outbound_clicks',
        'social_spend'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calcular métricas derivadas útiles
    if 'spend' in df.columns and 'clicks' in df.columns:
        # Calcular CPC si no viene o está vacío
        if 'cpc' not in df.columns or df['cpc'].isna().any():
            if 'cpc_calculated' not in df.columns:
                df['cpc_calculated'] = df.apply(
                    lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
                    axis=1
                )
    
    if 'spend' in df.columns and 'impressions' in df.columns:
        # Calcular CPM si no viene o está vacío
        if 'cpm' not in df.columns or df['cpm'].isna().any():
            if 'cpm_calculated' not in df.columns:
                df['cpm_calculated'] = df.apply(
                    lambda row: (row['spend'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0,
                    axis=1
                )
    
    # Calcular conversion_rate si tenemos conversions y clicks
    if 'conversions' in df.columns and 'clicks' in df.columns:
        df['conversion_rate_calculated'] = df.apply(
            lambda row: (row['conversions'] / row['clicks'] * 100) if row['clicks'] > 0 else 0,
            axis=1
        )
    
    # Calcular valor total si tenemos action_values
    if 'action_values' in df.columns:
        df['total_value'] = df['action_values'].apply(
            lambda x: sum(
                float(a.get('value', 0)) for a in x 
                if isinstance(x, list) and isinstance(a, dict)
            ) if isinstance(x, list) and x else 0
        )
    
    # Ordenar columnas: IDs primero, luego nombres, luego métricas
    id_cols = [c for c in df.columns if c.endswith('_id')]
    name_cols = [c for c in df.columns if c.endswith('_name')]
    date_cols = [c for c in df.columns if 'date' in c]
    metric_cols = [c for c in df.columns if c not in id_cols + name_cols + date_cols]
    
    df = df[id_cols + name_cols + date_cols + sorted(metric_cols)]
    
    return df

def _enrich_with_campaign_status(df):
    """
    Enriquece el DataFrame con el estado de las campañas (activa, pausada, etc.).
    
    Args:
        df: DataFrame con datos de insights que incluye campaign_id
        
    Returns:
        DataFrame enriquecido con campaign_status y campaign_effective_status
    """
    if df.empty or 'campaign_id' not in df.columns:
        return df
    
    # Obtener campaign_ids únicos
    unique_campaign_ids = df['campaign_id'].dropna().unique().tolist()
    
    if not unique_campaign_ids:
        return df
    
    # Diccionario para cachear información del estado de las campañas
    campaign_status_cache = {}
    
    logging.info(f"Fetching campaign status for {len(unique_campaign_ids)} unique campaigns...")
    
    from facebook_business.adobjects.campaign import Campaign
    
    for campaign_id in unique_campaign_ids:
        try:
            campaign = Campaign(campaign_id)
            campaign_data = campaign.api_get(fields=[
                'status',           # Estado actual: ACTIVE, PAUSED, ARCHIVED, DELETED, etc.
                'effective_status', # Estado efectivo considerando presupuesto, fechas, etc.
                'configured_status' # Estado configurado manualmente
            ])
            
            campaign_status_cache[campaign_id] = {
                'campaign_status': campaign_data.get('status', 'UNKNOWN'),
                'campaign_effective_status': campaign_data.get('effective_status', 'UNKNOWN'),
                'campaign_configured_status': campaign_data.get('configured_status', 'UNKNOWN')
            }
            
        except Exception as e:
            logging.warning(f"Could not fetch campaign status for {campaign_id}: {str(e)}")
            campaign_status_cache[campaign_id] = {
                'campaign_status': 'UNKNOWN',
                'campaign_effective_status': 'UNKNOWN',
                'campaign_configured_status': 'UNKNOWN'
            }
    
    # Agregar columnas al DataFrame
    df['campaign_status'] = df['campaign_id'].map(
        lambda x: campaign_status_cache.get(x, {}).get('campaign_status', 'UNKNOWN')
    )
    df['campaign_effective_status'] = df['campaign_id'].map(
        lambda x: campaign_status_cache.get(x, {}).get('campaign_effective_status', 'UNKNOWN')
    )
    df['campaign_configured_status'] = df['campaign_id'].map(
        lambda x: campaign_status_cache.get(x, {}).get('campaign_configured_status', 'UNKNOWN')
    )
    
    # Crear columna booleana para saber si está activa
    # Una campaña está activa si effective_status es ACTIVE
    df['campaign_is_active'] = df['campaign_effective_status'].apply(
        lambda x: x == 'ACTIVE' if pd.notna(x) else False
    )
    
    logging.info(f"Enriched {len(df)} records with campaign status information.")
    return df

def _enrich_with_creative_info(df):
    """
    Enriquece el DataFrame con información del creativo de cada anuncio.
    Obtiene URLs de video, imágenes y otra información del creativo.
    
    Args:
        df: DataFrame con datos de insights que incluye ad_id
        
    Returns:
        DataFrame enriquecido con información del creativo
    """
    if df.empty or 'ad_id' not in df.columns:
        return df
    
    # Obtener ad_ids únicos
    unique_ad_ids = df['ad_id'].dropna().unique().tolist()
    
    if not unique_ad_ids:
        return df
    
    # Diccionario para cachear información del creativo
    creative_info_cache = {}
    
    logging.info(f"Fetching creative info for {len(unique_ad_ids)} unique ads...")
    
    for ad_id in unique_ad_ids:
        try:
            ad = Ad(ad_id)
            ad_data = ad.api_get(fields=[
                'creative',
                'name',
                'status'
            ])
            
            # El creative puede venir como objeto AdCreative, dict, o string
            creative_obj = ad_data.get('creative')
            creative_id = None
            
            if creative_obj:
                # Si es un dict, obtener el id
                if isinstance(creative_obj, dict):
                    creative_id = creative_obj.get('id')
                # Si es un string (ID directo)
                elif isinstance(creative_obj, str):
                    # Verificar si es solo el ID o contiene el objeto serializado
                    if creative_obj.isdigit():
                        creative_id = creative_obj
                    else:
                        # Intentar extraer ID del string serializado
                        import re
                        id_match = re.search(r'"id":\s*"(\d+)"', creative_obj)
                        if id_match:
                            creative_id = id_match.group(1)
                # Si es un objeto AdCreative o similar
                else:
                    # Intentar obtener el id de diferentes formas
                    if hasattr(creative_obj, 'get') and callable(getattr(creative_obj, 'get', None)):
                        creative_id = creative_obj.get('id')
                    elif hasattr(creative_obj, 'id'):
                        creative_id = str(creative_obj.id)
                    else:
                        # Convertir a string y extraer ID con regex
                        creative_str = str(creative_obj)
                        id_match = re.search(r'"id":\s*"(\d+)"', creative_str)
                        if id_match:
                            creative_id = id_match.group(1)
                        else:
                            # Último intento: buscar cualquier número largo que parezca un ID
                            id_match = re.search(r'(\d{15,})', creative_str)
                            if id_match:
                                creative_id = id_match.group(1)
            
            # Log para debug si no se pudo extraer el creative_id
            if not creative_id:
                logging.warning(f"Could not extract creative_id from ad {ad_id}. Creative object: {type(creative_obj)} - {str(creative_obj)[:100]}")
            
            if creative_id:
                try:
                    creative = AdCreative(creative_id)
                    creative_data = creative.api_get(fields=[
                        'object_story_spec',
                        'thumbnail_url',
                        'video_id',
                        'image_url',
                        'image_hash',
                        'name',
                        'body',
                        'title',
                        'link_url',
                        'object_story_id'  # ID del post si es un post de página
                    ])
                    
                    # Extraer información relevante
                    video_url = None
                    image_url = None
                    thumbnail_url = None
                    creative_name = creative_data.get('name', '')
                    creative_body = creative_data.get('body', '')
                    creative_title = creative_data.get('title', '')
                    link_url = creative_data.get('link_url', '')
                    object_story_id = creative_data.get('object_story_id', '')
                    
                    # Intentar obtener video_id y construir URL
                    video_id = creative_data.get('video_id')
                    
                    # Obtener thumbnail_url
                    thumbnail_url = creative_data.get('thumbnail_url')
                    
                    # Obtener image_url
                    image_url = creative_data.get('image_url')
                    
                    # Procesar object_story_spec para obtener más información
                    object_story = creative_data.get('object_story_spec', {})
                    if object_story:
                        # Si object_story es string, intentar parsearlo
                        if isinstance(object_story, str):
                            try:
                                import json
                                object_story = json.loads(object_story)
                            except:
                                pass
                        
                        if isinstance(object_story, dict):
                            # Video data
                            video_data = object_story.get('video_data', {})
                            if isinstance(video_data, dict):
                                if not image_url:
                                    image_url = video_data.get('image_url')
                                if not video_id:
                                    video_id = video_data.get('video_id')
                                if not thumbnail_url:
                                    thumbnail_url = video_data.get('image_url')  # Thumbnail puede estar aquí
                            
                            # Link data
                            link_data = object_story.get('link_data', {})
                            if isinstance(link_data, dict):
                                if not image_url:
                                    image_url = link_data.get('image_url')
                                if not link_url:
                                    link_url = link_data.get('link')
                            
                            # Page ID para construir URL del post
                            page_id = object_story.get('page_id')
                            if page_id and object_story_id:
                                # Construir URL del post de Facebook
                                if not video_url and object_story_id:
                                    video_url = f"https://www.facebook.com/{page_id}/posts/{object_story_id.split('_')[1] if '_' in object_story_id else object_story_id}"
                    
                    # Construir video URL si tenemos video_id
                    if video_id and not video_url:
                        video_url = f"https://www.facebook.com/video.php?v={video_id}"
                    
                    # Si tenemos object_story_id pero no video_url, intentar construir URL del post
                    if object_story_id and not video_url:
                        # El object_story_id suele ser "page_id_post_id"
                        if '_' in object_story_id:
                            parts = object_story_id.split('_')
                            if len(parts) >= 2:
                                page_id = parts[0]
                                post_id = parts[1]
                                video_url = f"https://www.facebook.com/{page_id}/posts/{post_id}"
                    
                    creative_info_cache[ad_id] = {
                        'creative_id': str(creative_id),
                        'video_url': video_url or '',
                        'image_url': image_url or '',
                        'thumbnail_url': thumbnail_url or '',
                        'creative_name': creative_name,
                        'creative_body': creative_body,
                        'creative_title': creative_title,
                        'link_url': link_url
                    }
                    
                except Exception as e:
                    logging.warning(f"Could not fetch creative {creative_id} for ad {ad_id}: {str(e)}")
                    creative_info_cache[ad_id] = {
                        'creative_id': str(creative_id) if creative_id else '',
                        'video_url': '',
                        'image_url': '',
                        'thumbnail_url': '',
                        'creative_name': '',
                        'creative_body': '',
                        'creative_title': '',
                        'link_url': ''
                    }
            else:
                creative_info_cache[ad_id] = {
                    'creative_id': '',
                    'video_url': '',
                    'image_url': '',
                    'thumbnail_url': '',
                    'creative_name': '',
                    'creative_body': '',
                    'creative_title': '',
                    'link_url': ''
                }
                
        except Exception as e:
            logging.warning(f"Could not fetch ad {ad_id}: {str(e)}")
            creative_info_cache[ad_id] = {
                'creative_id': '',
                'video_url': '',
                'image_url': '',
                'thumbnail_url': '',
                'creative_name': '',
                'creative_body': '',
                'creative_title': '',
                'link_url': ''
            }
    
    # Agregar columnas al DataFrame
    df['creative_id'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('creative_id', ''))
    df['video_url'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('video_url', ''))
    df['image_url'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('image_url', ''))
    df['thumbnail_url'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('thumbnail_url', ''))
    df['creative_name'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('creative_name', ''))
    df['creative_body'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('creative_body', ''))
    df['creative_title'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('creative_title', ''))
    df['link_url'] = df['ad_id'].map(lambda x: creative_info_cache.get(x, {}).get('link_url', ''))
    
    logging.info(f"Enriched {len(df)} records with creative information.")
    return df

def save_to_csv(df, filename='meta_ads.csv'):
    """Saves the DataFrame to a CSV file."""
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")
