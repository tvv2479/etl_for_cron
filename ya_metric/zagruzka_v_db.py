
#%%
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime 
import logging
import configparser
#%%
config = configparser.ConfigParser()
config.read("E:\Projects/tb\data_collection/config.ini")

BdAnalisHost = config['KeyBd']['host']
BdAnalisUser = config['KeyBd']['bd_user']
BdAnalisName = config['KeyBd']['bd_name']
BdAnalisPass = config['KeyBd']['password']
#%%

# Настройка логирования
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"E:\Projects/tb\data_collection\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")

def ya_hits_to_bd(df_hits):
       logging.info("Start zagruzka v bazu logs api HITS.")
       # HITS загрузка в базу
       
       # Подключаемся к базе данных PGSQL
       engine = create_engine(f'postgresql+psycopg2://{BdAnalisUser}:{BdAnalisPass}@{BdAnalisHost}/{BdAnalisName}')
       # Выдаём генеральные права
       with engine.connect() as conn:
              conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))

       try:
              # ym_hits_stranitsi_new
              logging.info("Start zagruzka dannih - ym_hits_stranitsi_new.")
              col = df_hits[['counter_user_id_hash', 'client_id', 'watch_id', 'date_event', 
                             'datetime_event', 'not_bounce', 'link', 'title', 'url']]
              # Пишем в PGSQL
              col.to_sql('ym_hits_stranitsi_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_hits_stranitsi_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_stranitsi_new - {err}", exc_info=True)

       try:
              # ym_hits_obshee_new
              logging.info("Start zagruzka dannih - ym_hits_obshee_new.")
              col = df_hits[['counter_user_id_hash', 'client_id', 'watch_id', 'date_event', 'datetime_event',
                             'counter_id', 'device_category', 'region_city', 'region_city_id', 
                             'region_country', 'region_country_id', 'last_traffic_source', 'referer', 
                             'ip_address', 'is_page_view', 'artificial']]
              # Пишем в PGSQL
              col.to_sql('ym_hits_obshee_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_hits_obshee_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_obshee_new - {err}", exc_info=True)

       try:
              # ym_hits_deystviya_new
              logging.info("Start zagruzka dannih - ym_hits_deystviya_new.")
              col = df_hits[['counter_user_id_hash', 'client_id', 'watch_id', 'date_event', 'datetime_event', 
                             'download', 'goals_id', 'last_social_network', 'last_search_engine', 
                             'last_search_engine_root', 'share_service', 'share_url', 'share_title', 
                             'last_social_network_profile']]

              col.to_sql('ym_hits_deystviya_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_hits_deystviya_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_deystviya_new - {err}", exc_info=True)
              
       try:
              # ym_hits_ad_new
              logging.info("Start zagruzka dannih - ym_hits_ad_new.")
              col = df_hits[['counter_user_id_hash', 'client_id', 'watch_id', 'date_event', 'datetime_event',
                             'utm_campaign', 'utm_content', 'utm_medium', 'utm_source', 'utm_term', 
                             'openstat_ad', 'openstat_campaign', 'openstat_service', 'openstat_source', 
                             'last_adv_engine']]

              col.to_sql('ym_hits_ad_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_hits_ad_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_ad_new - {err}", exc_info=True)

       del df_hits
#%%

def ya_visits_to_bd(df_visits):
       logging.info("Start zagruzka v bazu logs api VISITS.")
       # VISITS загрузка в базу

       # Подключение к базе данных PGSQL
       engine = create_engine(f'postgresql+psycopg2://{BdAnalisUser}:{BdAnalisPass}@{BdAnalisHost}/{BdAnalisName}')
       # Выдача генеральных прав
       with engine.connect() as conn:
              conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))

       try:
              # ym_visits_obshee_new
              logging.info("Start zagruzka dannih - ym_visits_obshee_new.")
              col = df_visits[['counter_user_id_hash', 'client_id', 'visit_id', 'date_event', 
                               'datetime_event', 'counter_id', 'is_new_user', 'start_url', 'end_url', 
                               'page_views', 'visit_duration', 'device_category', 'region_country', 
                               'region_country_id', 'region_city', 'region_city_id', 'bounce', 'ip_address']]

              col.to_sql('ym_visits_obshee_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_visits_obshee_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_visits_obshee_new - {err}", exc_info=True)

       try:
              # ym_visits_detali_new
              logging.info("Start zagruzka dannih - # ym_visits_detali_new.")
              col = df_visits[['counter_user_id_hash', 'client_id', 'visit_id', 'date_event', 
                               'datetime_event', 'last_traffic_source', 'last_adv_engine', 
                               'last_referal_source', 'last_search_engine_root', 'last_search_engine', 
                               'last_social_network', 'last_social_network_profile', 'referer',
                               'impressions_product_coupon']]

              col.to_sql('ym_visits_detali_new', engine, schema='public', if_exists='append', index=False)
              logging.info("End zagruzka dannih - ym_visits_detali_new.")
       except Exception as err:
              logging.error(f"Error: oshibka zagruzki v tablitsu - ym_visits_detali_new - {err}", exc_info=True)

       del df_visits

