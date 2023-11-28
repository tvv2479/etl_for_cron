#%%
from ya_metric import load_data_ym, clearing_data_ym, zagruzka_v_db
from support import clean_logs, bot

from glob import glob
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
from datetime import datetime, date, timedelta

#%%
# данные для запросов
TOKEN = 'y0_AgAEA7qiSjdCAArJCwAAAADxWZyxBecwQ4LuQ_S8XoBPdw6D-8DNi7Y'
COUNTER = '31318467'

DATE1 = '2023-11-26'
DATE2 = '2023-11-26'
#%%
log_load = load_data_ym.Logsapi(TOKEN, COUNTER, DATE1, DATE2)
data_hit = load_data_ym.Logsapi.download_hits(log_load)
clean_hit = clearing_data_ym.clean_logs_hits(data_hit)
zagruzka_v_db.ya_hits_to_bd(clean_hit)
#%%
data_visit = load_data_ym.Logsapi.download_visits(log_load)
clean_visit = clearing_data_ym.clean_logs_visits(data_visit)
zagruzka_v_db.ya_visits_to_bd(clean_visit)
# %%
