#%%
from ya_metric import load_data_ym, clearing_data_ym, zagruzka_v_db
from site_dat import load_data_site as lds

from datetime import date, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
import configparser
import pandas as pd

#%%
config = configparser.ConfigParser()
config.read("G:\py.projects/tb\data_collection/config.ini")

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

sql = 'select max(date(date_insert)) from site_location_country_new'
date = pd.read_sql_query(text(sql), engine)

date
# %%
# Генератор дат 
def daterange(start, stop, step=timedelta(days=1), inclusive=True):
    # inclusive=False вести себя как диапазон по умолчанию
    if step.days > 0:
        while start < stop:
            yield start
            start = start + step
    elif step.days < 0:
        while start > stop:
            yield start
            start = start + step
    if inclusive and start == stop:
        yield start
        
#%%
# Генерирует список дат
end_date = date.fromisoformat('2019-12-21')
# start_date = date.today()-timedelta(days=365)
start_date = date.fromisoformat('2019-10-01')
dates = [x for x in daterange(start_date,
                              end_date,
                              step=timedelta(weeks=2),
                              inclusive=True)]

#%%
# Спазу в строку
date.today().isoformat()
#%%

date(2023, 10, 1)

# %%
date.today()

# %%
date.fromisoformat('2019-10-01')
# %%
dates
#%%
date1 = "'2023-12-25'"
date2 = "'2023-12-25'"
# %%
df = lds.siteLocationCountry()
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_location_country_new')

df = lds.siteLocationCity()
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_location_city_new')

df = lds.siteStatus()
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_sale_status_new')

#%%
# site_insert_order_new
df = lds.siteInsertOrders(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'order_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_insert_order_new')

# site_update_order_new
df = lds.siteUpdateOrders(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'order_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_update_order_new')
#%%

# site_user_new
df = lds.siteUsers(date1, date2)
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_user_new')
#%%

# site_insert_basket_new
df = lds.siteInsertBasket(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'basket_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_insert_basket_new')

# site_update_basket_new
df = lds.siteUpdateBasket(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'basket_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_update_basket_new')
#%%

# site_insert_fuser_new
df = lds.siteInsertFuser(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'fuser_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_insert_fuser_new')

# site_update_fuser_new
df = lds.siteUpdateFuser(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'fuser_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_update_fuser_new')
#%%

# site_guest_new
df = lds.siteGuest(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'guest_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_guest_new')
#%%

# site_session_new
df = lds.siteSession(date1, date2)
df = df.replace({'': None})
df.rename(columns = {'id':'session_id'}, inplace = True )
lds.dataSiteToBd(df, 'site_session_new')

# site_order_props_value_new
df = lds.siteOrderPropsValue(date1, date2)
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_order_props_value_new')
#%%

# site_user_transact_new
df = lds.siteTransact(date1, date2)
df.rename(columns = {'id':'transact_id'}, inplace = True )
df = df.replace({'': None})
lds.dataSiteToBd(df, 'site_user_transact_new')


# %%
date1 = '2023-12-25'
date2 = '2023-12-25'
#%%
log_load = load_data_ym.Logsapi(TOKEN, COUNTER, date1, date2)
data_hit = load_data_ym.Logsapi.download_hits(log_load)
clean_hit = clearing_data_ym.clean_logs_hits(data_hit)
zagruzka_v_db.ya_hits_to_bd(clean_hit)
#%%
log_load = load_data_ym.Logsapi(TOKEN, COUNTER, date1, date2)
data_visit = load_data_ym.Logsapi.download_visits(log_load)
clean_visit = clearing_data_ym.clean_logs_visits(data_visit)
zagruzka_v_db.ya_visits_to_bd(clean_visit)
# %%
