#%%
from ya_metric import load_data_ym, clearing_data_ym, zagruzka_v_db
from support import clean_logs, bot
#from site_dat import load_data_site

from glob import glob
from sqlalchemy import create_engine
from sqlalchemy import text
import psycopg2
from datetime import datetime, date, timedelta
import datetime
import configparser
#%%
config = configparser.ConfigParser()
config.read("config.ini")
#%%
# данные для запросов
TOKEN = config['Keys']['TOKEN']
COUNTER = config['Keys']['COUNTER']

# https://habr.com/ru/companies/domclick/articles/581304/
# https://docs.sqlalchemy.org/en/20/core/connections.html
# Подключение к базе
engine = create_engine('postgresql+psycopg2://postgres:listopad@localhost/test_char')
# Дата на вчера
end_date = date.today() - timedelta(days=1)
#%%
# Проверка максимальной даты Hits в базе
with engine.connect() as conn:
    result = conn.execute(text('select max(date_event) from ym_hits_obshee_new'))
    for row in result:
        row
        
start_date = row[0]

DATE1 = (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
DATE2 = end_date.strftime('%Y-%m-%d')

dateDelta = int((end_date - start_date).total_seconds()/86400)

# Загрузка данных HITS в базу
if dateDelta > 0:
    log_load = load_data_ym.Logsapi(TOKEN, COUNTER, DATE1, DATE2)
    data_hit = load_data_ym.Logsapi.download_hits(log_load)
    clean_hit = clearing_data_ym.clean_logs_hits(data_hit)
    zagruzka_v_db.ya_hits_to_bd(clean_hit)
    
#%%
# Проверка максимальной даты Visits в базе
with engine.connect() as conn:
    result = conn.execute(text('select max(date_event) from ym_visits_obshee_new'))
    for row in result:
        row
        
start_date = row[0]
DATE1 = (start_date + timedelta(days=1)).strftime('%Y-%m-%d')

dateDelta = int((end_date - start_date).total_seconds()/86400)

# Загрузка данных VISITS в базу
if dateDelta > 0:
    log_load = load_data_ym.Logsapi(TOKEN, COUNTER, DATE1, DATE2)
    data_visit = load_data_ym.Logsapi.download_visits(log_load)
    clean_visit = clearing_data_ym.clean_logs_visits(data_visit)
    zagruzka_v_db.ya_visits_to_bd(clean_visit)
#%%
# Удаление файлов логов
logs = glob(fr"E:\Projects\tb\data_collection\logs/*.log")
clean_logs.logs_clean(logs, 4) 

# Запрос к базе 
with engine.connect() as conn:
    result1 = conn.execute(text('select max(date_event) from ym_hits_obshee_new'))
    result2 = conn.execute(text('select max(date_event) from ym_visits_obshee_new'))
    for row1 in result1:
        row1
        
    for row2 in result2:
        row2    
        
if row1 == row2:
    MSG = f'''Загрузка данных из яндексметрики в базу прошла успешно!\n
    Крайняя дата:\n 
    Таблица ym_hits_obshee_new - {row1[0]}\n
    Таблица ym_visits_obshee_new - {row2[0]}
       '''
else:
    MSG = f'''Внимание!\n 
    Данные из яндекс метрики загружены не полностью:\n
    Таблица ym_hits_obshee_new - {row1[0]}\n
    Таблица ym_visits_obshee_new - {row2[0]}
    '''
       
# Отправка сообщения в Телеграм
bot.bot_maccege(MSG)
#%%

del log_load, data_hit, clean_hit, data_visit, clean_visit
