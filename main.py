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


config = configparser.ConfigParser()
config.read("G:\py.projects/tb\data_collection/config.ini")

# данные для запросов
TOKEN = config['KeysYm']['token']
COUNTER = config['KeysYm']['counter']

# Подключение к базе

BdAnalisHost = config['KeyBd']['host']
BdAnalisUser = config['KeyBd']['bd_user']
BdAnalisName = config['KeyBd']['bd_name']
BdAnalisPass = config['KeyBd']['password']

engine = create_engine(f'postgresql+psycopg2://{BdAnalisUser}:{BdAnalisPass}@{BdAnalisHost}/{BdAnalisName}')
# Дата на вчера
end_date = datetime.date.today() - timedelta(days=1)

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



# ЗАГРУЗКА ДАННЫХ SITE

# Загружаем таблицы-справочники

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
               
df.rename(columns = {'id':'country_id'}, inplace = True )

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
               
df.rename(columns = {'id':'city_id'}, inplace = True )

if df['city_id'].count() > 0:
    df.to_sql('site_location_city_new', engine, schema='public', if_exists='append', index=False)
    


# site_sale_status_new таблица-справочник
df1 = lds.siteStatus()
df1 = df1.replace({'': None})
# Получаем данные с сайта аналитики
sql = 'select * from site_sale_status_new'
df2 = pd.read_sql_query(text(sql), engine)
df2 = df2[['status_id', 'sort', 'type', 'notify', 'color']]
# Сравниваем два датафрейма и оставляем то, что есть на сайте (df1) и нет в аналитике (df2)
df = df1.merge(df2, how='left', indicator=True) \
               .query("_merge == 'left_only'") \
               .drop('_merge', axis=1)[df1.columns]
               
df.rename(columns = {'id':'status_id'}, inplace = True )
               
if df['status_id'].count() > 0:
    df.to_sql('site_sale_status_new', engine, schema='public', if_exists='append', index=False)
    

# Основные таблицы

# Общая конечная дата для site
dateEnd = end_date.strftime('%Y-%m-%d')
date2 = f"'{dateEnd}'"

# site_user_new
# получаем крайнюю дату в таблице бызы аналитики
sql = 'select max(date_register::date) from site_user_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteUsers(date1, date2)
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_user_new')

# site_insert_order_new
# получаем крайнюю дату в таблице бызы аналитики
sql = 'select max(date_insert::date) from site_insert_order_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteInsertOrders(date1, date2)
    df.rename(columns = {'id':'order_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_insert_order_new')

# site_update_order_new
sql = 'select max(date_update::date) from site_update_order_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteUpdateOrders(date1, date2)
    df.rename(columns = {'id':'order_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_update_order_new')


# site_insert_fuser_new
sql = 'select max(date_insert::date) from site_insert_fuser_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteInsertFuser(date1, date2)
    df.rename(columns = {'id':'fuser_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_insert_fuser_new')
    

# site_update_fuser_new
sql = 'select max(date_update::date) from site_update_fuser_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteUpdateFuser(date1, date2)
    df.rename(columns = {'id':'fuser_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_update_fuser_new')
    

# site_insert_basket_new
sql = 'select max(date_insert::date) from site_insert_basket_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteInsertBasket(date1, date2)
    df.rename(columns = {'id':'basket_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_insert_basket_new')

# site_update_basket_new
sql = 'select max(date_update::date) from site_update_basket_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteUpdateBasket(date1, date2)
    df.rename(columns = {'id':'basket_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_update_basket_new')

# site_guest_new
sql = 'select max(first_date::date) from site_guest_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteGuest(date1, date2)
    df.rename(columns = {'id':'guest_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_guest_new')

# site_session_new
sql = 'select max(date_stat) from site_session_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteSession(date1, date2)
    df.rename(columns = {'id':'session_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_session_new')

# site_order_props_value_new
sql = 'select max(date_insert::date) from site_order_props_value_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteOrderPropsValue(date1, date2)
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_order_props_value_new')

# site_user_transact_new
sql = 'select max(transact_date::date) from site_user_transact_new'
date = pd.read_sql_query(text(sql), engine)
# Начальная дата для диапазона site
d = list(date['max'])
dateStart = d[0] + timedelta(days=1)
date11 = dateStart.strftime('%Y-%m-%d')
date1 = f"'{date11}'"

dateDelta = int((end_date - d[0]).total_seconds()/86400)

if dateDelta > 0:
    df = lds.siteTransact(date1, date2)
    df.rename(columns = {'id':'transact_id'}, inplace = True )
    df = lds.cleanDataSite(df)
    lds.dataSiteToBd(df, 'site_user_transact_new')
    

# МОНИТОРИНГ

# Удаление файлов логов
logs = glob(fr"G:\py.projects/tb\data_collection\logs/*.log")
clean_logs.logs_clean(logs, 4) 

# Проверка наличия ошибок в логах
for i in logs:
    st = i.split('\\')[-1].split('_')
    st1 = st[2]+'-'+st[3]+'-'+st[4].split('.')[0]
    dt = datetime.datetime.strptime(st1, "%d-%m-%Y").date()
    if dt == datetime.date.today():
        res = i

log_err = []

with open(res, 'r') as file:
    lines = file.readlines()
    
for row in lines:
    s = row.split(' ')
    if s[0] == 'root':
        if s[3] == 'ERROR':
            log_err.append(s[3])

DatesTable = []

# Запрос к базе 
with engine.connect() as conn:
    result1 = conn.execute(text('select max(date_event) from ym_hits_obshee_new'))
    result2 = conn.execute(text('select max(date_event) from ym_visits_obshee_new'))
    result3 = conn.execute(text('select max(date_register::date) from site_user_new'))
    result4 = conn.execute(text('select max(date_insert::date) from site_insert_order_new'))
    result5 = conn.execute(text('select max(date_update::date) from site_update_order_new'))
    result6 = conn.execute(text('select max(date_insert::date) from site_insert_fuser_new'))
    result7 = conn.execute(text('select max(date_update::date) from site_update_fuser_new'))
    result8 = conn.execute(text('select max(date_insert::date) from site_insert_basket_new'))
    result9 = conn.execute(text('select max(date_update::date) from site_update_basket_new'))
    result10 = conn.execute(text('select max(first_date::date) from site_guest_new'))
    result11 = conn.execute(text('select max(date_stat) from site_session_new'))
    result12 = conn.execute(text('select max(date_insert::date) from site_order_props_value_new'))
    result13 = conn.execute(text('select max(transact_date::date) from site_user_transact_new'))
    
    res = [result1, result2, result3, result4, result5, result6, result7, result8, result9, result10, 
           result11, result12, result13]
    
for sub in res:
    for row in sub:
        DatesTable.append(row[0].strftime('%Y-%m-%d'))
    
        

MSG = f'''Загрузка данных из яндексметрики в базу.\n
    **Количество ошибок** - {len(log_err)}\n
    Крайняя дата в таблицах:\n 
    Таблица ym_hits_obshee_new - {DatesTable[0]}\n
    Таблица ym_visits_obshee_new - {DatesTable[1]}\n
    Таблица site_user_new - {DatesTable[2]}\n
    Таблица site_insert_order_new - {DatesTable[3]}\n
    Таблица site_update_order_new - {DatesTable[4]}\n
    Таблица site_insert_fuser_new - {DatesTable[5]}\n
    Таблица site_update_fuser_new - {DatesTable[6]}\n
    Таблица site_insert_basket_new - {DatesTable[7]}\n
    Таблица site_update_basket_new - {DatesTable[8]}\n
    Таблица site_guest_new - {DatesTable[9]}\n
    Таблица site_session_new - {DatesTable[10]}\n
    Таблица site_order_props_value_new - {DatesTable[11]}\n
    Таблица site_user_transact_new - {DatesTable[12]}
       '''

       
# Отправка сообщения в Телеграм
bot.bot_message(MSG)


# %%
