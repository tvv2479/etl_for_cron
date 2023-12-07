#%%
from sqlalchemy import text
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import logging
import pymysql
import pandas as pd
# %%
# Подключение к базе сайта

SSH_HOST = '194.38.9.81'
SSH_USER = 'poweruser'
SSH_PASSWORD = 'mTpTnRX}uK0rD?t'
DB_USERNAME = 'seconduser'
DB_PASSWORD = 'Av4l#7qa6x4C'
DB_NAME = 'sitemanager'
HOST = '127.0.0.1'
#%%
# Настраиваем логирование
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"G:\py.projects/tb\data_collection\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")


# %%
def dataSite(sql):
    '''
    sql - запрос к базе для получения данных для выгрузки.
    '''
    
    try:
        logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
        tunnel = SSHTunnelForwarder((SSH_HOST, 22),
                                    ssh_username = SSH_USER,
                                    ssh_password = SSH_PASSWORD,
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host='127.0.0.1',
                                     user=DB_USERNAME,
                                     passwd=DB_PASSWORD,
                                     db=DB_NAME,
                                     port=tunnel.local_bind_port)

        query = sql
        df = pd.read_sql_query(query, connection)
        connection.close()
        tunnel.close()
        logging.info(f"site.dataSite(). Dannie polucheni yspeshno.")
        
    except Exception as err:
            logging.error(f"site.dataSite(). Oshibka polycheniya dannih - {err}", exc_info=True)
    
    return df
#%%

def dataSiteToBd(df, tableName):
    '''
    df - фрейм с данными для звгрузки в базу.\n
    tableName - наименование таблицы в кторую нужно загрузить фрейм (str).
    '''
    try:
        logging.info(f"site.dataSiteToBd(). Zapis dataframe v tablitsu {tableName}.")
        # Подключаемся к базе данных PGSQL на сервере
        engine_pg = create_engine('postgresql+psycopg2://postgres:listopad@localhost/test_char')
        # Выдаём генеральные права
        with engine_pg.connect() as conn:
            conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))
        
        df.to_sql(tableName, engine_pg, schema='public', if_exists='append', index=False)
        logging.info(f"site.dataSiteToBd(). Dataframe uspeshno zapisan v tablitsu - {tableName}.")
    except Exception as err:
            logging.error(f"site.dataSiteToBd(). Oshibka zapisi dannih v tab {tableName} - {err}", exc_info=True)
   

    
#%%

date1 = (row1[0] + timedelta(days=1)).strftime('%Y-%m-%d')

#%%

query = f"""select bso.id,
                      bso.account_number,
                      bso.lid,
                      bso.person_type_id,
                      bso.user_id,
                      bso.price,
                      bso.currency,
                      bso.date_insert,
                      bsopv.value as promocode
                 from b_sale_order bso
                 join (select order_id,
                              value
                         from b_sale_order_props_value  
                        where name = 'Промокод') as bsopv
                      on bso.id = bsopv.order_id
                where date(bso.date_insert) = date_add('2023-11-25', INTERVAL 6 day)"""
                

# %%
df = dataSite(query)
df


# %%