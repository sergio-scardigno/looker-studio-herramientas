"""
Transformación y agregación de datos de Meta Ads para reporting en Looker Studio.
Genera múltiples tablas optimizadas para análisis.
"""
import logging
import pandas as pd
from typing import Dict
import ast

def _extract_video_value(value):
    """
    Extrae el valor numérico de métricas de video que vienen como listas de diccionarios.
    
    Args:
        value: Puede ser una lista de diccionarios, string que representa una lista, o ya un número
        
    Returns:
        Valor numérico extraído, o 0 si no se puede extraer
    """
    if pd.isna(value) or value == '' or value is None:
        return 0
    
    # Si ya es un número, retornarlo
    if isinstance(value, (int, float)):
        return float(value)
    
    # Si es string, intentar parsearlo
    if isinstance(value, str):
        try:
            # Intentar parsear como lista de Python
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list) and len(parsed) > 0:
                # Tomar el primer elemento si es un diccionario
                if isinstance(parsed[0], dict):
                    return float(parsed[0].get('value', 0))
                else:
                    return float(parsed[0])
            elif isinstance(parsed, dict):
                return float(parsed.get('value', 0))
            else:
                return float(parsed)
        except (ValueError, SyntaxError, TypeError):
            # Si falla el parsing, intentar convertir directamente
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0
    
    # Si es una lista directamente
    if isinstance(value, list):
        if len(value) > 0:
            if isinstance(value[0], dict):
                return float(value[0].get('value', 0))
            else:
                return float(value[0])
        return 0
    
    # Si es un diccionario directamente
    if isinstance(value, dict):
        return float(value.get('value', 0))
    
    return 0

def normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza el DataFrame raw extraído de la API.
    Extrae métricas clave de campos complejos y estandariza tipos.
    
    Args:
        df: DataFrame raw con datos de la API
        
    Returns:
        DataFrame normalizado con columnas estables
    """
    if df.empty:
        return df
    
    # Crear copia para no modificar el original
    df_norm = df.copy()
    
    # Extraer métricas clave de actions
    # Mensajes totales (KPI principal) - total_messaging_connection
    messages_col = 'actions_onsite_conversion.total_messaging_connection'
    if messages_col in df_norm.columns:
        df_norm['messages_total'] = pd.to_numeric(df_norm[messages_col], errors='coerce').fillna(0)
    else:
        df_norm['messages_total'] = 0
    
    # Conversaciones iniciadas (métrica que usa Meta Ads Manager)
    conv_started_col = 'actions_onsite_conversion.messaging_conversation_started_7d'
    if conv_started_col in df_norm.columns:
        df_norm['conversations_started'] = pd.to_numeric(df_norm[conv_started_col], errors='coerce').fillna(0)
    else:
        df_norm['conversations_started'] = 0
    
    # Primer reply (respuestas)
    first_reply_col = 'actions_onsite_conversion.messaging_first_reply'
    if first_reply_col in df_norm.columns:
        df_norm['first_replies'] = pd.to_numeric(df_norm[first_reply_col], errors='coerce').fillna(0)
    else:
        df_norm['first_replies'] = 0
    
    # Link clicks
    link_clicks_col = 'actions_link_click'
    if link_clicks_col in df_norm.columns:
        df_norm['link_clicks'] = pd.to_numeric(df_norm[link_clicks_col], errors='coerce').fillna(0)
    else:
        df_norm['link_clicks'] = 0
    
    # Leads (combinar diferentes tipos de leads)
    lead_cols = [
        'actions_lead',
        'actions_onsite_web_lead',
        'actions_onsite_conversion.lead',
        'actions_onsite_conversion.lead_grouped'
    ]
    df_norm['leads'] = 0
    for col in lead_cols:
        if col in df_norm.columns:
            df_norm['leads'] += pd.to_numeric(df_norm[col], errors='coerce').fillna(0)
    
    # Procesar métricas de video que vienen como listas de diccionarios
    video_columns = [
        'video_30_sec_watched_actions',
        'video_avg_time_watched_actions',
        'video_p100_watched_actions',
        'video_play_actions'
    ]
    
    for video_col in video_columns:
        if video_col in df_norm.columns:
            df_norm[video_col] = df_norm[video_col].apply(_extract_video_value)
    
    # Estandarizar columnas numéricas clave
    numeric_cols = {
        'spend': 0,
        'impressions': 0,
        'clicks': 0,
        'reach': 0,
        'frequency': 0,
        'ctr': 0,
        'cpc': 0,
        'cpm': 0,
        'outbound_clicks': 0
    }
    
    for col, default in numeric_cols.items():
        if col in df_norm.columns:
            df_norm[col] = pd.to_numeric(df_norm[col], errors='coerce').fillna(default)
        else:
            df_norm[col] = default
    
    # Calcular métricas derivadas si no existen
    if 'cpc' not in df_norm.columns or df_norm['cpc'].isna().all():
        df_norm['cpc'] = df_norm.apply(
            lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
            axis=1
        )
    
    if 'cpm' not in df_norm.columns or df_norm['cpm'].isna().all():
        df_norm['cpm'] = df_norm.apply(
            lambda row: (row['spend'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # Costo por mensaje
    df_norm['cost_per_message'] = df_norm.apply(
        lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
        axis=1
    )
    
    # Costo por conversación iniciada (métrica de Meta Ads Manager)
    df_norm['cost_per_conversation'] = df_norm.apply(
        lambda row: row['spend'] / row['conversations_started'] if row['conversations_started'] > 0 else 0,
        axis=1
    )
    
    # Asegurar que date_start sea datetime
    if 'date_start' in df_norm.columns:
        df_norm['date_start'] = pd.to_datetime(df_norm['date_start'], errors='coerce')
    
    return df_norm


def create_reporting_tables(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Crea múltiples tablas agregadas para reporting en Looker Studio.
    
    Args:
        df: DataFrame normalizado
        
    Returns:
        Diccionario con nombres de tablas como keys y DataFrames como values
    """
    if df.empty:
        logging.warning("DataFrame vacío, no se pueden crear tablas.")
        return {}
    
    tables = {}
    
    # Tabla 1: ad_daily - Granularidad: fecha + anuncio (MÁXIMO DETALLE)
    # Incluir TODAS las columnas disponibles para análisis detallado
    # Ordenar columnas de manera lógica: IDs, nombres, fechas, métricas básicas, métricas de mensajería, costos, engagement, video, creativo
    
    # Columnas base (siempre incluir si existen)
    base_cols = [
        'date_start', 'date_stop',
        'campaign_id', 'campaign_name',
        'campaign_status', 'campaign_effective_status', 'campaign_is_active',  # Estado de campaña
        'adset_id', 'adset_name',
        'ad_id', 'ad_name'
    ]
    
    # Métricas básicas
    basic_metrics = [
        'spend', 'impressions', 'clicks', 'reach', 'frequency',
        'ctr', 'cpc', 'cpm', 'cpp',
        'unique_clicks', 'unique_ctr', 'outbound_clicks'
    ]
    
    # Métricas de mensajería (todas las disponibles)
    messaging_metrics = [
        'messages_total',  # total_messaging_connection
        'conversations_started',  # messaging_conversation_started_7d
        'first_replies',  # messaging_first_reply
        'link_clicks', 'leads'
    ]
    
    # Métricas de engagement
    engagement_metrics = [
        'actions_comment', 'actions_like', 'actions_post_engagement',
        'actions_post_reaction', 'actions_page_engagement'
    ]
    
    # Métricas de video
    video_metrics = [
        'video_30_sec_watched_actions', 'video_avg_time_watched_actions',
        'video_p100_watched_actions', 'video_play_actions',
        'actions_video_view'
    ]
    
    # Métricas de conversión
    conversion_metrics = [
        'conversions', 'cost_per_conversion'
    ]
    
    # Información del creativo
    creative_cols = [
        'creative_id', 'video_url', 'image_url', 'thumbnail_url',
        'creative_name', 'creative_body', 'creative_title', 'link_url'
    ]
    
    # Rankings
    ranking_cols = [
        'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking'
    ]
    
    # Costos calculados
    cost_metrics = [
        'cost_per_message', 'cost_per_conversation'
    ]
    
    # Combinar todas las columnas en orden lógico
    all_cols = (base_cols + basic_metrics + messaging_metrics + engagement_metrics + 
                video_metrics + conversion_metrics + cost_metrics + ranking_cols + creative_cols)
    
    # Filtrar solo las que existen en el DataFrame
    available_cols = [c for c in all_cols if c in df.columns]
    
    # Agregar cualquier columna adicional que no esté en la lista pero que sea relevante
    additional_cols = [c for c in df.columns if c not in available_cols and 
                       not c.startswith('actions_onsite_conversion.messaging_user_depth') and
                       not c.startswith('cost_per_onsite_conversion.messaging_user_depth') and
                       not c.startswith('actions_offsite') and
                       not c.startswith('cost_per_offsite') and
                       c not in ['actions', 'action_values', 'cost_per_action_type']]
    
    final_cols = available_cols + sorted(additional_cols)
    
    tables['ad_daily'] = df[final_cols].copy()
    
    # Asegurar que las métricas calculadas estén incluidas
    if 'cost_per_message' in df.columns and 'cost_per_message' not in tables['ad_daily'].columns:
        tables['ad_daily']['cost_per_message'] = df['cost_per_message']
    if 'cost_per_conversation' in df.columns and 'cost_per_conversation' not in tables['ad_daily'].columns:
        tables['ad_daily']['cost_per_conversation'] = df['cost_per_conversation']
    
    # Tabla 2: messages_daily - Agregado por día
    agg_dict = {
        'messages_total': 'sum',
        'conversations_started': 'sum',  # Métrica de Meta Ads Manager
        'first_replies': 'sum',
        'spend': 'sum',
        'clicks': 'sum',
        'impressions': 'sum',
        'reach': 'sum'
    }
    available_agg = {k: v for k, v in agg_dict.items() if k in df.columns}
    
    messages_daily = df.groupby('date_start').agg(available_agg).reset_index()
    
    # Calcular métricas derivadas
    if 'spend' in messages_daily.columns and 'messages_total' in messages_daily.columns:
        messages_daily['cost_per_message'] = messages_daily.apply(
            lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
            axis=1
        )
    
    if 'spend' in messages_daily.columns and 'conversations_started' in messages_daily.columns:
        messages_daily['cost_per_conversation'] = messages_daily.apply(
            lambda row: row['spend'] / row['conversations_started'] if row['conversations_started'] > 0 else 0,
            axis=1
        )
    
    if 'spend' in messages_daily.columns and 'clicks' in messages_daily.columns:
        messages_daily['cpc'] = messages_daily.apply(
            lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
            axis=1
        )
    
    if 'clicks' in messages_daily.columns and 'impressions' in messages_daily.columns:
        messages_daily['ctr'] = messages_daily.apply(
            lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    tables['messages_daily'] = messages_daily
    
    # Tabla 3: campaign_daily - Agregado por campaña y día
    # NO incluir CTR en la agregación - se calculará después de sumar clicks e impressions
    campaign_agg = {
        'messages_total': 'sum',
        'conversations_started': 'sum',  # Métrica principal de Meta Ads Manager
        'first_replies': 'sum',
        'spend': 'sum',
        'clicks': 'sum',
        'impressions': 'sum',
        'reach': 'sum',
        'frequency': 'mean'  # Frecuencia promedio
    }
    available_campaign_agg = {k: v for k, v in campaign_agg.items() if k in df.columns}
    
    campaign_daily = df.groupby(['date_start', 'campaign_id', 'campaign_name']).agg(
        available_campaign_agg
    ).reset_index()
    
    # Recalcular métricas derivadas a nivel campaña (CORRECTO - después de sumar)
    if 'spend' in campaign_daily.columns and 'messages_total' in campaign_daily.columns:
        campaign_daily['cost_per_message'] = campaign_daily.apply(
            lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
            axis=1
        )
    
    if 'spend' in campaign_daily.columns and 'clicks' in campaign_daily.columns:
        campaign_daily['cpc'] = campaign_daily.apply(
            lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
            axis=1
        )
    
    # Calcular CTR correctamente: (suma de clicks / suma de impressions) * 100
    if 'clicks' in campaign_daily.columns and 'impressions' in campaign_daily.columns:
        campaign_daily['ctr'] = campaign_daily.apply(
            lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # Calcular CPM si no está
    if 'spend' in campaign_daily.columns and 'impressions' in campaign_daily.columns:
        campaign_daily['cpm'] = campaign_daily.apply(
            lambda row: (row['spend'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # Costo por conversación iniciada (métrica de Meta Ads Manager)
    if 'spend' in campaign_daily.columns and 'conversations_started' in campaign_daily.columns:
        campaign_daily['cost_per_conversation'] = campaign_daily.apply(
            lambda row: row['spend'] / row['conversations_started'] if row['conversations_started'] > 0 else 0,
            axis=1
        )
    
    tables['campaign_daily'] = campaign_daily
    
    # Tabla adicional: campaign_period - Resumen total por campaña (sin fecha)
    # MÁXIMO DETALLE para comparar qué campaña fue más exitosa
    campaign_period_agg = {
        # Métricas de mensajería (sumar)
        'messages_total': 'sum',
        'conversations_started': 'sum',  # Métrica principal de Meta Ads Manager
        'first_replies': 'sum',
        'link_clicks': 'sum',
        'leads': 'sum',
        
        # Métricas básicas (sumar)
        'spend': 'sum',
        'clicks': 'sum',
        'impressions': 'sum',
        'reach': 'sum',
        'unique_clicks': 'sum',
        'outbound_clicks': 'sum',
        
        # Métricas de engagement (sumar)
        'actions_comment': 'sum',
        'actions_like': 'sum',
        'actions_post_engagement': 'sum',
        'actions_post_reaction': 'sum',
        'actions_page_engagement': 'sum',
        
        # Métricas de video (sumar)
        'video_30_sec_watched_actions': 'sum',
        'video_play_actions': 'sum',
        'actions_video_view': 'sum',
        
        # Métricas de conversión (sumar)
        'conversions': 'sum',
        
        # Promedios
        'frequency': 'mean',
        'ctr': 'mean',  # Se recalculará después
        'cpc': 'mean',  # Se recalculará después
        'cpm': 'mean',  # Se recalculará después
    }
    
    # Solo incluir columnas que existen
    available_campaign_period_agg = {k: v for k, v in campaign_period_agg.items() if k in df.columns}
    
    # Agregar estado de campaña si existe (usar 'first' porque es el mismo para toda la campaña)
    if 'campaign_status' in df.columns:
        available_campaign_period_agg['campaign_status'] = 'first'
    if 'campaign_effective_status' in df.columns:
        available_campaign_period_agg['campaign_effective_status'] = 'first'
    if 'campaign_configured_status' in df.columns:
        available_campaign_period_agg['campaign_configured_status'] = 'first'
    if 'campaign_is_active' in df.columns:
        available_campaign_period_agg['campaign_is_active'] = 'first'
    
    campaign_period = df.groupby(['campaign_id', 'campaign_name']).agg(
        available_campaign_period_agg
    ).reset_index()
    
    # Calcular métricas derivadas CORRECTAMENTE (después de sumar)
    # CTR: (suma de clicks / suma de impressions) * 100
    if 'clicks' in campaign_period.columns and 'impressions' in campaign_period.columns:
        campaign_period['ctr'] = campaign_period.apply(
            lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # CPC: suma de spend / suma de clicks
    if 'spend' in campaign_period.columns and 'clicks' in campaign_period.columns:
        campaign_period['cpc'] = campaign_period.apply(
            lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
            axis=1
        )
    
    # CPM: (suma de spend / suma de impressions) * 1000
    if 'spend' in campaign_period.columns and 'impressions' in campaign_period.columns:
        campaign_period['cpm'] = campaign_period.apply(
            lambda row: (row['spend'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # Costo por mensaje total
    if 'spend' in campaign_period.columns and 'messages_total' in campaign_period.columns:
        campaign_period['cost_per_message'] = campaign_period.apply(
            lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
            axis=1
        )
    
    # Costo por conversación iniciada (métrica principal de Meta Ads Manager)
    if 'spend' in campaign_period.columns and 'conversations_started' in campaign_period.columns:
        campaign_period['cost_per_conversation'] = campaign_period.apply(
            lambda row: row['spend'] / row['conversations_started'] if row['conversations_started'] > 0 else 0,
            axis=1
        )
    
    # Costo por lead
    if 'spend' in campaign_period.columns and 'leads' in campaign_period.columns:
        campaign_period['cost_per_lead'] = campaign_period.apply(
            lambda row: row['spend'] / row['leads'] if row['leads'] > 0 else 0,
            axis=1
        )
    
    # Costo por conversión
    if 'spend' in campaign_period.columns and 'conversions' in campaign_period.columns:
        campaign_period['cost_per_conversion'] = campaign_period.apply(
            lambda row: row['spend'] / row['conversions'] if row['conversions'] > 0 else 0,
            axis=1
        )
    
    # Calcular ratios de eficiencia
    if 'conversations_started' in campaign_period.columns and 'impressions' in campaign_period.columns:
        campaign_period['conversation_rate'] = campaign_period.apply(
            lambda row: (row['conversations_started'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    if 'messages_total' in campaign_period.columns and 'impressions' in campaign_period.columns:
        campaign_period['message_rate'] = campaign_period.apply(
            lambda row: (row['messages_total'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # Calcular días activos (rango de fechas)
    if 'date_start' in df.columns:
        date_range = df.groupby(['campaign_id', 'campaign_name'])['date_start'].agg(['min', 'max']).reset_index()
        date_range['days_active'] = (pd.to_datetime(date_range['max']) - pd.to_datetime(date_range['min'])).dt.days + 1
        date_range['first_date'] = date_range['min']
        date_range['last_date'] = date_range['max']
        campaign_period = campaign_period.merge(
            date_range[['campaign_id', 'campaign_name', 'days_active', 'first_date', 'last_date']],
            on=['campaign_id', 'campaign_name'],
            how='left'
        )
    
    # Calcular promedio diario
    if 'spend' in campaign_period.columns and 'days_active' in campaign_period.columns:
        campaign_period['spend_per_day'] = campaign_period.apply(
            lambda row: row['spend'] / row['days_active'] if row['days_active'] > 0 else 0,
            axis=1
        )
    
    if 'conversations_started' in campaign_period.columns and 'days_active' in campaign_period.columns:
        campaign_period['conversations_per_day'] = campaign_period.apply(
            lambda row: row['conversations_started'] / row['days_active'] if row['days_active'] > 0 else 0,
            axis=1
        )
    
    # Ordenar columnas de manera lógica
    priority_cols = [
        'campaign_id', 'campaign_name', 
        'campaign_status', 'campaign_effective_status', 'campaign_is_active',  # Estado de campaña
        'days_active', 'first_date', 'last_date',
        'spend', 'spend_per_day', 'impressions', 'clicks', 'reach', 'frequency',
        'ctr', 'cpc', 'cpm',
        'messages_total', 'conversations_started', 'conversations_per_day', 'first_replies',
        'cost_per_message', 'cost_per_conversation', 'cost_per_lead',
        'conversation_rate', 'message_rate',
        'link_clicks', 'leads', 'conversions', 'cost_per_conversion',
        'unique_clicks', 'outbound_clicks',
        'actions_comment', 'actions_like', 'actions_post_engagement', 'actions_post_reaction',
        'actions_page_engagement',
        'video_30_sec_watched_actions', 'video_play_actions', 'actions_video_view'
    ]
    
    # Reordenar columnas: primero las prioritarias, luego el resto
    existing_priority = [c for c in priority_cols if c in campaign_period.columns]
    other_cols = [c for c in campaign_period.columns if c not in existing_priority]
    campaign_period = campaign_period[existing_priority + sorted(other_cols)]
    
    # Ordenar por conversaciones iniciadas (métrica principal) descendente
    if 'conversations_started' in campaign_period.columns:
        campaign_period = campaign_period.sort_values('conversations_started', ascending=False).reset_index(drop=True)
    elif 'spend' in campaign_period.columns:
        campaign_period = campaign_period.sort_values('spend', ascending=False).reset_index(drop=True)
    
    tables['campaign_period'] = campaign_period
    
    # Tabla 4: adset_daily - Agregado por conjunto de anuncios y día
    # NO incluir CTR en la agregación - se calculará después
    adset_agg = {
        'messages_total': 'sum',
        'spend': 'sum',
        'clicks': 'sum',
        'impressions': 'sum',
        'reach': 'sum',
        'frequency': 'mean'
    }
    available_adset_agg = {k: v for k, v in adset_agg.items() if k in df.columns}
    
    adset_daily = df.groupby(['date_start', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name']).agg(
        available_adset_agg
    ).reset_index()
    
    # Recalcular métricas derivadas CORRECTAMENTE (después de sumar)
    if 'spend' in adset_daily.columns and 'messages_total' in adset_daily.columns:
        adset_daily['cost_per_message'] = adset_daily.apply(
            lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
            axis=1
        )
    
    if 'spend' in adset_daily.columns and 'clicks' in adset_daily.columns:
        adset_daily['cpc'] = adset_daily.apply(
            lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
            axis=1
        )
    
    # CTR correcto: (suma de clicks / suma de impressions) * 100
    if 'clicks' in adset_daily.columns and 'impressions' in adset_daily.columns:
        adset_daily['ctr'] = adset_daily.apply(
            lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    # CPM
    if 'spend' in adset_daily.columns and 'impressions' in adset_daily.columns:
        adset_daily['cpm'] = adset_daily.apply(
            lambda row: (row['spend'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0,
            axis=1
        )
    
    tables['adset_daily'] = adset_daily
    
    # Tabla 5: top_ads_period - Ranking de anuncios (sin fecha, todo el período)
    group_cols = ['campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name']
    agg_dict = {
        'messages_total': 'sum',
        'spend': 'sum',
        'clicks': 'sum',
        'impressions': 'sum'
    }
    
    # Agregar columnas de creativo si existen (tomar el primer valor ya que son iguales para el mismo ad_id)
    creative_cols = ['creative_id', 'video_url', 'image_url', 'thumbnail_url', 'creative_name']
    for col in creative_cols:
        if col in df.columns:
            agg_dict[col] = 'first'
    
    top_ads = df.groupby(group_cols).agg(agg_dict).reset_index()
    
    # Calcular métricas agregadas
    top_ads['cost_per_message'] = top_ads.apply(
        lambda row: row['spend'] / row['messages_total'] if row['messages_total'] > 0 else 0,
        axis=1
    )
    
    top_ads['cpc'] = top_ads.apply(
        lambda row: row['spend'] / row['clicks'] if row['clicks'] > 0 else 0,
        axis=1
    )
    
    top_ads['ctr'] = top_ads.apply(
        lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0,
        axis=1
    )
    
    # Ordenar por mensajes totales (descendente)
    top_ads = top_ads.sort_values('messages_total', ascending=False).reset_index(drop=True)
    
    tables['top_ads_period'] = top_ads
    
    # Limpiar valores NaN y reemplazar por 0 o string vacío según tipo
    for table_name, table_df in tables.items():
        # Para columnas numéricas, reemplazar NaN con 0
        numeric_cols = table_df.select_dtypes(include=['number']).columns
        table_df[numeric_cols] = table_df[numeric_cols].fillna(0)
        
        # Para columnas de texto, reemplazar NaN con string vacío
        text_cols = table_df.select_dtypes(include=['object']).columns
        table_df[text_cols] = table_df[text_cols].fillna('')
        
        # Para fechas, mantener como están (pueden ser NaT)
        date_cols = table_df.select_dtypes(include=['datetime64']).columns
        # No hacer nada con fechas
    
    logging.info(f"Created {len(tables)} reporting tables: {', '.join(tables.keys())}")
    
    return tables

