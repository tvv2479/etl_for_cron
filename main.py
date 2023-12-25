#%%
from ya_metric import load_data_ym, clearing_data_ym, zagruzka_v_db
from support import clean_logs, bot
from site_dat import load_data_site as lds

from glob import glob
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime, date, timedelta
import datetime
import configparser
import pandas as pd
#%%
config = configparser.ConfigParser()
config.read("E:\Projects/tb\data_collection/config.ini")
#%%
# данные для запросов
TOKEN = config['KeysYm']['token']
COUNTER = config['KeysYm']['counter']

# https://habr.com/ru/companies/domclick/articles/581304/
# https://docs.sqlalchemy.org/en/20/core/connections.html
# Подключение к базе

BdAnalisHost = config['KeyBd']['host']
BdAnalisUser = config['KeyBd']['bd_user']
BdAnalisName = config['KeyBd']['bd_name']
BdAnalisPass = config['KeyBd']['password']

engine = create_engine(f'postgresql+psycopg2://{BdAnalisUser}:{BdAnalisPass}@{BdAnalisHost}/{BdAnalisName}')
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

# Секунд в сутках 86400
dateDelta = int((end_date - start_date).total_seconds()/86400)

# Загрузка данных HITS в базу
# Если дельта между крайней датой в базе и текущей датой минус 1 день больше 0, то делаем загрузку в базу.
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
# Если дельта между крайней датой в базе и текущей датой минус 1 день больше 0, то делаем загрузку в базу.
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
bot.bot_message(MSG)
#%%

del log_load, data_hit, clean_hit, data_visit, clean_visit
#%%

# ЗАГРУЗКА ДАННЫХ SITE

date1 = "'2023-12-13'"
date2 = "'2023-12-13'"

# %%
site_date2 = f"'{end_date.strftime('%Y-%m-%d')}'"


#%%
# Загружаем таблицы-справочники
# https://ru.stackoverflow.com/questions/717808/%D0%9F%D0%BE%D0%BB%D1%83%D1%87%D0%B8%D1%82%D1%8C-%D1%81%D1%82%D1%80%D0%BE%D0%BA%D0%B8-%D0%B4%D0%B0%D1%82%D0%B0%D1%84%D1%80%D0%B5%D0%B9%D0%BC%D0%B0-%D0%BA%D0%BE%D1%82%D0%BE%D1%80%D1%8B%D0%B5-%D0%BD%D0%B5-%D0%B2%D1%85%D0%BE%D0%B4%D1%8F%D1%82-%D0%B2-%D0%B4%D1%80%D1%83%D0%B3%D0%BE%D0%B9-%D0%B4%D0%B0%D1%82%D0%B0%D1%84%D1%80%D0%B5%D0%B9%D0%BC

# Получаем данные с сайта

# site_location_country_new таблица-справочник
df1 = lds.siteLocationCountry()
# Получаем данные с сайта аналитики
sql = 'select * from site_location_country_new'
df2 = pd.read_sql_query(text(sql), engine)

# Сравниваем два датафрейма и оставляем то, что есть на сайте (df1) и нет в аналитике (df2)
df = df1.merge(df2, how='left', indicator=True) \
               .query("_merge == 'left_only'") \
               .drop('_merge', axis=1)[df1.columns]

if df['country_id'].count() > 0:
    df.to_sql('site_location_country_new', engine, schema='public', if_exists='append', index=False)

# site_location_city_new таблица-справочник
df1 = lds.siteLocationCity()
# Получаем данные с сайта аналитики
sql = 'select * from site_location_city_new'
df2 = pd.read_sql_query(text(sql), engine)

# Сравниваем два датафрейма и оставляем то, что есть на сайте (df1) и нет в аналитике (df2)
df = df1.merge(df2, how='left', indicator=True) \
               .query("_merge == 'left_only'") \
               .drop('_merge', axis=1)[df1.columns]

if df['city_id'].count() > 0:
    df.to_sql('site_location_city_new', engine, schema='public', if_exists='append', index=False)

# site_sale_status_new таблица-справочник
df1 = lds.siteStatus()
# Получаем данные с сайта аналитики
sql = 'select * from site_sale_status_new'
df2 = pd.read_sql_query(text(sql), engine)

# Сравниваем два датафрейма и оставляем то, что есть на сайте (df1) и нет в аналитике (df2)
df = df1.merge(df2, how='left', indicator=True) \
               .query("_merge == 'left_only'") \
               .drop('_merge', axis=1)[df1.columns]
               
if df['status_id'].count() > 0:
    df.to_sql('site_sale_status_new', engine, schema='public', if_exists='append', index=False)
    
del df1, df2, df

# site_insert_order_new
# получаем крайнюю дату в таблице бызы аналитики
sql = 'select max(date(date_insert)) from site_location_country_new'
date = pd.read_sql_query(text(sql), engine)

df = lds.siteInsertOrders(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_insert_order_new')

# site_update_order_new
df = lds.siteUpdateOrders(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_update_order_new')

# site_user_new
df = lds.siteUsers(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_user_new')

# site_insert_basket_new
df = lds.siteInsertBasket(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_insert_basket_new')

# site_update_basket_new
df = lds.siteUpdateBasket(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_update_basket_new')

# site_insert_fuser_new
df = lds.siteInsertFuser(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_insert_fuser_new')

# site_update_fuser_new
df = lds.siteUpdateFuser(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_update_fuser_new')

# site_guest_new
df = lds.siteGuest(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_guest_new')

# site_session_new
df = lds.siteSession(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_session_new')

# site_order_props_value_new
df = lds.siteOrderPropsValue(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_order_props_value_new')

# site_user_transact_new
df = lds.siteTransact(date1, date2)
df = lds.cleanDataSite(df)
lds.dataSiteToBd(df, 'site_user_transact_new')


