#%%
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime, timedelta
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import logging
import pymysql
import pandas as pd
import configparser
# %%
config = configparser.ConfigParser()
config.read("E:\Projects/tb\data_collection/config.ini")

#%%
# Подключение к базе аналитики

BdAnalisHost = config['KeyBd']['host']
BdAnalisUser = config['KeyBd']['bd_user']
BdAnalisName = config['KeyBd']['bd_name']
BdAnalisPass = config['KeyBd']['password']

#%%
# Настраиваем логирование
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"E:\Projects/tb\data_collection\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")


#%%
# Получение данных с сайта

def dataSite(sql):
    '''
    Функция получает данные из источника site.\n
    dataSite(sql)\n
    sql - запрос к базе для получения данных для выгрузки.
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                    user=config['KeyBdSite']['userName'],
                                    passwd=config['KeyBdSite']['password'],
                                    db=config['KeyBdSite']['bdName'],
                                    port=tunnel.local_bind_port)

        query = sql
        #df = pd.read_sql_query(query, connection)
        df = pd.read_sql(query, connection)
        connection.close()
        tunnel.close()
        logging.info(f"site.dataSite(). Dannie polucheni yspeshno.")
        
    except Exception as err:
            logging.error(f"site.dataSite(). Oshibka polycheniya dannih - {err}", exc_info=True)
    
    return df

# Загрузка данных в базу аналитики
    
def dataSiteToBd(df, tableName):
    '''
    Функция загружает датафрейм в базу данных.\n
    dataSiteToBd(df, tableName)\n
    df - фрейм с данными для звгрузки в базу.\n
    tableName - наименование таблицы в кторую нужно загрузить фрейм (str).
    '''
    try:
        logging.info(f"site.dataSiteToBd(). Zapis dataframe v tablitsu {tableName}.")
        # Подключаемся к базе данных PGSQL на сервере
        engine_pg = create_engine(f'postgresql+psycopg2://{BdAnalisUser}:{BdAnalisPass}@{BdAnalisHost}/{BdAnalisName}')
        # Выдаём генеральные права
        with engine_pg.connect() as conn:
            conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))
        
        df.to_sql(tableName, engine_pg, schema='public', if_exists='append', index=False)
        logging.info(f"site.dataSiteToBd(). Dataframe uspeshno zapisan v tablitsu - {tableName}.")
    except Exception as err:
            logging.error(f"site.dataSiteToBd(). Oshibka zapisi dannih v tab {tableName} - {err}", exc_info=True)

# Очистка датафрейма
    
def cleanDataSite(df):
    '''
    Функция очищает данные датафрейма.\n
    cleanDataSite(df)\n
    df - фрейм с данными для звгрузки в базу.
    '''
    df= df.replace({'': None})
    return df

def siteInsertOrders(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_order по дате создания\n
    siteInsertOrders(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteInsertOrders(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select bso.id, user_id, bso.status_id, bso.account_number, bso.additional_info, 
                        bso.affiliate_id, bso.allow_delivery, bso.bx_user_id, bso.canceled, bso.comments, 
                        bso.company_id, bso.created_by, bso.currency, bso.date_allow_delivery, bso.date_bill, 
                        bso.date_canceled, bso.date_deducted, bso.date_insert, bso.date_lock, bso.date_marked, 
                        bso.date_pay_before, bso.date_payed, bso.date_status, bso.date_update, bso.deducted, 
                        bso.delivery_doc_date, bso.delivery_doc_num, bso.delivery_id, bso.discount_value, 
                        bso.emp_allow_delivery_id, bso.emp_canceled_id, bso.emp_deducted_id, bso.emp_marked_id, 
                        bso.emp_payed_id, bso.emp_status_id, bso.external_order, bso.id_1c, bso.is_recurring, 
                        bso.is_sync_b24, bso.lid, bso.locked_by, bso.marked, bso.order_topic, bso.pay_system_id,
                        bso.pay_voucher_date, bso.pay_voucher_num, bso.payed, bso.person_type_id, bso.price, 
                        bso.price_delivery, bso.price_payment, bso.ps_currency, bso.ps_response_date, 
                        bso.ps_status, bso.ps_status_code, bso.ps_status_description, bso.ps_status_message, 
                        bso.ps_sum, bso.reason_canceled, bso.reason_marked, bso.reason_undo_deducted, 
                        bso.recount_flag, bso.recurring_id, bso.reserved, bso.responsible_id, bso.running, 
                        bso.search_content, bso.stat_gid, bso.store_id, bso.sum_paid, bso.tax_value, 
                        bso.tracking_number, bso.updated_1c, bso.user_description, bso.version, bso.version_1c, 
                        bsopv.value as promocode
                    from b_sale_order bso
                        join (select order_id,
                                        value
                                from b_sale_order_props_value  
                                where name = 'Промокод') as bsopv
                                on bso.id = bsopv.order_id
                    where date(bso.date_insert) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'user_id', 'status_id', 'account_number', 'additional_info', 'affiliate_id', 
               'allow_delivery', 'bx_user_id', 'canceled', 'comments', 'company_id', 
               'created_by', 'currency', 'date_allow_delivery', 'date_bill', 'date_canceled', 
               'date_deducted', 'date_insert', 'date_lock', 'date_marked', 'date_pay_before', 
               'date_payed', 'date_status', 'date_update', 'deducted', 'delivery_doc_date', 
               'delivery_doc_num', 'delivery_id', 'discount_value', 'emp_allow_delivery_id', 
               'emp_canceled_id', 'emp_deducted_id', 'emp_marked_id', 'emp_payed_id', 
               'emp_status_id', 'external_order', 'id_1c', 'is_recurring', 'is_sync_b24', 'lid', 
               'locked_by', 'marked', 'order_topic', 'pay_system_id', 'pay_voucher_date', 
               'pay_voucher_num', 'payed', 'person_type_id', 'price', 'price_delivery', 'price_payment', 
               'ps_currency', 'ps_response_date', 'ps_status', 'ps_status_code', 'ps_status_description', 
               'ps_status_message', 'ps_sum', 'reason_canceled', 'reason_marked', 'reason_undo_deducted', 
               'recount_flag', 'recurring_id', 'reserved', 'responsible_id', 'running', 'search_content', 
               'stat_gid', 'store_id', 'sum_paid', 'tax_value', 'tracking_number', 'updated_1c', 
               'user_description', 'version', 'version_1c', 'promocode']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteInsertOrders(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteInsertOrders(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err
            
    return df

def siteUpdateOrders(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_order по дате обновления\n
    siteUpdateOrders(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteUpdateOrders(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select bso.id, user_id, bso.status_id, bso.account_number, bso.additional_info, 
                        bso.affiliate_id, bso.allow_delivery, bso.bx_user_id, bso.canceled, bso.comments, 
                        bso.company_id, bso.created_by, bso.currency, bso.date_allow_delivery, bso.date_bill, 
                        bso.date_canceled, bso.date_deducted, bso.date_insert, bso.date_lock, bso.date_marked, 
                        bso.date_pay_before, bso.date_payed, bso.date_status, bso.date_update, bso.deducted, 
                        bso.delivery_doc_date, bso.delivery_doc_num, bso.delivery_id, bso.discount_value, 
                        bso.emp_allow_delivery_id, bso.emp_canceled_id, bso.emp_deducted_id, bso.emp_marked_id, 
                        bso.emp_payed_id, bso.emp_status_id, bso.external_order, bso.id_1c, bso.is_recurring, 
                        bso.is_sync_b24, bso.lid, bso.locked_by, bso.marked, bso.order_topic, bso.pay_system_id,
                        bso.pay_voucher_date, bso.pay_voucher_num, bso.payed, bso.person_type_id, bso.price, 
                        bso.price_delivery, bso.price_payment, bso.ps_currency, bso.ps_response_date, 
                        bso.ps_status, bso.ps_status_code, bso.ps_status_description, bso.ps_status_message, 
                        bso.ps_sum, bso.reason_canceled, bso.reason_marked, bso.reason_undo_deducted, 
                        bso.recount_flag, bso.recurring_id, bso.reserved, bso.responsible_id, bso.running, 
                        bso.search_content, bso.stat_gid, bso.store_id, bso.sum_paid, bso.tax_value, 
                        bso.tracking_number, bso.updated_1c, bso.user_description, bso.version, bso.version_1c, 
                        bsopv.value as promocode
                    from b_sale_order bso
                        join (select order_id,
                                        value
                                from b_sale_order_props_value  
                                where name = 'Промокод') as bsopv
                                on bso.id = bsopv.order_id
                    where date(bso.date_update) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'user_id', 'status_id', 'account_number', 'additional_info', 'affiliate_id', 
               'allow_delivery', 'bx_user_id', 'canceled', 'comments', 'company_id', 
               'created_by', 'currency', 'date_allow_delivery', 'date_bill', 'date_canceled', 
               'date_deducted', 'date_insert', 'date_lock', 'date_marked', 'date_pay_before', 
               'date_payed', 'date_status', 'date_update', 'deducted', 'delivery_doc_date', 
               'delivery_doc_num', 'delivery_id', 'discount_value', 'emp_allow_delivery_id', 
               'emp_canceled_id', 'emp_deducted_id', 'emp_marked_id', 'emp_payed_id', 
               'emp_status_id', 'external_order', 'id_1c', 'is_recurring', 'is_sync_b24', 'lid', 
               'locked_by', 'marked', 'order_topic', 'pay_system_id', 'pay_voucher_date', 
               'pay_voucher_num', 'payed', 'person_type_id', 'price', 'price_delivery', 'price_payment', 
               'ps_currency', 'ps_response_date', 'ps_status', 'ps_status_code', 'ps_status_description', 
               'ps_status_message', 'ps_sum', 'reason_canceled', 'reason_marked', 'reason_undo_deducted', 
               'recount_flag', 'recurring_id', 'reserved', 'responsible_id', 'running', 'search_content', 
               'stat_gid', 'store_id', 'sum_paid', 'tax_value', 'tracking_number', 'updated_1c', 
               'user_description', 'version', 'version_1c', 'promocode']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteUpdateOrders(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteUpdateOrders(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteUsers(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_user по дате регистрации\n
    siteUsers(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteUsers(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, date_register, timestamp_x, active, admin_notes, auto_time_zone, blocked, 
                           bx_user_id, checkword, checkword_time, confirm_code, email, external_auth_id, 
                           language_id, last_activity_date, last_login, name, last_name, second_name, lid, 
                           login, login_attempts, password, password_expired, personal_birthdate, 
                           personal_birthday, personal_city, personal_country, personal_fax, personal_gender, 
                           personal_icq, personal_mailbox, personal_mobile, personal_notes, personal_pager, 
                           personal_phone, personal_photo, personal_profession, personal_state, personal_street,
                           personal_www, personal_zip, stored_hash, time_zone, time_zone_offset, title, 
                           work_city, work_company, work_country, work_department, work_fax, work_logo, 
                           work_mailbox, work_notes, work_pager, work_phone, work_position, work_profile, 
                           work_state, work_street, work_www, work_zip
                    FROM b_user
                    where date(date_register) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'date_register', 'timestamp_x', 'active', 'admin_notes', 'auto_time_zone', 'blocked', 
               'bx_user_id', 'checkword', 'checkword_time', 'confirm_code', 'email', 'external_auth_id', 
               'language_id', 'last_activity_date', 'last_login', 'name', 'last_name', 'second_name', 'lid', 
               'login', 'login_attempts', 'password', 'password_expired', 'personal_birthdate', 
               'personal_birthday', 'personal_city', 'personal_country', 'personal_fax', 'personal_gender', 
               'personal_icq', 'personal_mailbox', 'personal_mobile', 'personal_notes', 'personal_pager', 
               'personal_phone', 'personal_photo', 'personal_profession', 'personal_state', 'personal_street',
               'personal_www', 'personal_zip', 'stored_hash', 'time_zone', 'time_zone_offset', 'title', 
               'work_city', 'work_company', 'work_country', 'work_department', 'work_fax', 'work_logo', 
               'work_mailbox', 'work_notes', 'work_pager', 'work_phone', 'work_position', 'work_profile', 
               'work_state', 'work_street', 'work_www', 'work_zip']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteUsers(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteUsers(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df


def siteInsertBasket(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_basket по дате создания\n
    siteInsertBasket(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteInsertBasket(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, order_id, fuser_id, barcode_multi, base_price, callback_func, can_buy, 
                           cancel_callback_func, catalog_xml_id, currency, custom_price, date_insert, 
                           date_refresh, date_update, deducted, delay, detail_page_url, dimensions, 
                           discount_coupon, discount_name, discount_price, discount_value, lid, 
                           marking_code_group, measure_code, measure_name, module, name, notes, 
                           order_callback_func, pay_callback_func, price, price_type_id, product_id, 
                           product_price_id, product_provider_class, product_xml_id, quantity, recommendation, 
                           reserve_quantity, reserved, set_parent_id, sort, subscribe, type, vat_included, 
                           vat_rate, weight
                      FROM b_sale_basket
                     where date(date_insert) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'order_id', 'fuser_id', 'barcode_multi', 'base_price', 'callback_func', 'can_buy', 
               'cancel_callback_func', 'catalog_xml_id', 'currency', 'custom_price', 'date_insert', 
               'date_refresh', 'date_update', 'deducted', 'delay', 'detail_page_url', 'dimensions', 
               'discount_coupon', 'discount_name', 'discount_price', 'discount_value', 'lid', 
               'marking_code_group', 'measure_code', 'measure_name', 'module', 'name', 'notes', 
               'order_callback_func', 'pay_callback_func', 'price', 'price_type_id', 'product_id', 
               'product_price_id', 'product_provider_class', 'product_xml_id', 'quantity', 'recommendation', 
               'reserve_quantity', 'reserved', 'set_parent_id', 'sort', 'subscribe', 'type', 'vat_included', 
               'vat_rate', 'weight']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteInsertBasket(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteInsertBasket(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteUpdateBasket(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_basket по дате обновления\n
    siteUpdateBasket(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteUpdateBasket(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, order_id, fuser_id, barcode_multi, base_price, callback_func, can_buy, 
                           cancel_callback_func, catalog_xml_id, currency, custom_price, date_insert, 
                           date_refresh, date_update, deducted, delay, detail_page_url, dimensions, 
                           discount_coupon, discount_name, discount_price, discount_value, lid, 
                           marking_code_group, measure_code, measure_name, module, name, notes, 
                           order_callback_func, pay_callback_func, price, price_type_id, product_id, 
                           product_price_id, product_provider_class, product_xml_id, quantity, recommendation, 
                           reserve_quantity, reserved, set_parent_id, sort, subscribe, type, vat_included, 
                           vat_rate, weight
                      FROM b_sale_basket
                     where date(date_update) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'order_id', 'fuser_id', 'barcode_multi', 'base_price', 'callback_func', 'can_buy', 
               'cancel_callback_func', 'catalog_xml_id', 'currency', 'custom_price', 'date_insert', 
               'date_refresh', 'date_update', 'deducted', 'delay', 'detail_page_url', 'dimensions', 
               'discount_coupon', 'discount_name', 'discount_price', 'discount_value', 'lid', 
               'marking_code_group', 'measure_code', 'measure_name', 'module', 'name', 'notes', 
               'order_callback_func', 'pay_callback_func', 'price', 'price_type_id', 'product_id', 
               'product_price_id', 'product_provider_class', 'product_xml_id', 'quantity', 'recommendation', 
               'reserve_quantity', 'reserved', 'set_parent_id', 'sort', 'subscribe', 'type', 'vat_included', 
               'vat_rate', 'weight']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteUpdateBasket(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteUpdateBasket(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteInsertFuser(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_fuser по дате создания\n
    siteInsertFuser(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteInsertFuser(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, date_insert, date_update, user_id, code
                      FROM b_sale_fuser
                     where date(date_insert) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'date_insert', 'date_update', 'user_id', 'code']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteInsertFuser(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteInsertFuser(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteUpdateFuser(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_fuser по дате обновления\n
    siteUpdateFuser(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteUpdateFuser(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, date_insert, date_update, user_id, code
                      FROM b_sale_fuser
                     where date(date_update) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'date_insert', 'date_update', 'user_id', 'code']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteUpdateFuser(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteUpdateFuser(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteGuest(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_stat_guest.\n
    siteGuest(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteGuest(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, timestamp_x, favorites, c_events, sessions, hits, repair, first_session_id, 
                           first_date, first_url_from, first_url_to, first_url_to_404, first_site_id, 
                           first_adv_id, first_referer1, first_referer2, first_referer3, last_session_id, 
                           last_date, last_user_id, last_user_auth, last_url_last, last_url_last_404, 
                           last_user_agent, last_ip, last_cookie, last_language, last_adv_id, last_adv_back, 
                           last_referer1, last_referer2, last_referer3, last_site_id, last_country_id, 
                           LAST_city_id, last_city_info
                      FROM b_stat_guest
                     where date(first_date) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'timestamp_x', 'favorites', 'c_events', 'sessions', 'hits', 'repair', 'first_session_id', 
               'first_date', 'first_url_from', 'first_url_to', 'first_url_to_404', 'first_site_id', 
               'first_adv_id', 'first_referer1', 'first_referer2', 'first_referer3', 'last_session_id', 
               'last_date', 'last_user_id', 'last_user_auth', 'last_url_last', 'last_url_last_404', 
               'last_user_agent', 'last_ip', 'last_cookie', 'last_language', 'last_adv_id', 'last_adv_back', 
               'last_referer1', 'last_referer2', 'last_referer3', 'last_site_id', 'last_country_id', 
               'LAST_city_id', 'last_city_info']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteGuest(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteGuest(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteSession(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_stat_session.\n
    siteSession(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteSession(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, guest_id, new_guest, user_id, user_auth, c_events, hits, favorites, url_from, 
                           url_to, url_to_404, url_last, url_last_404, user_agent, date_stat, date_first, 
                           date_last, ip_first, ip_first_number, ip_last, ip_last_number, first_hit_id, 
                           last_hit_id, adv_id, adv_back, referer1, referer2, referer3, stop_list_id, 
                           country_id, city_id, first_site_id, last_site_id
                      FROM b_stat_session
                     where date(date_stat) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'guest_id', 'new_guest', 'user_id', 'user_auth', 'c_events', 'hits', 'favorites', 
               'url_from', 'url_to', 'url_to_404', 'url_last', 'url_last_404', 'user_agent', 'date_stat', 
               'date_first', 'date_last', 'ip_first', 'ip_first_number', 'ip_last', 'ip_last_number', 
               'first_hit_id', 'last_hit_id', 'adv_id', 'adv_back', 'referer1', 'referer2', 'referer3', 
               'stop_list_id', 'country_id', 'city_id', 'first_site_id', 'last_site_id']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteSession(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteSession(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteOrderPropsValue(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_order_props_value.\n
    siteOrderPropsValue(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteOrderPropsValue(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select bsopv.order_id, bso.date_insert, bso.user_id,
                           max(case when order_props_id = 2 then value end) as city,
                           max(case when order_props_id = 6 then value end) as email,
                           max(case when order_props_id = 17 then value end) as company,
                           max(case when order_props_id = 19 then value end) as first_name,
                           max(case when order_props_id = 18 then value end) as last_name,
                           max(case when order_props_id = 20 then value end) as phone,
                           max(case when order_props_id = 23 then value end) as promocode,
                           max(case when order_props_id = 24 then value end) as customer_type_shop,
                           max(case when order_props_id = 25 then value end) as customer_type_org,
                           max(case when order_props_id = 26 then value end) as customer_type_company,
                           max(case when order_props_id = 27 then value end) as customer_type_personal,
                           min(entity_type)  as entity_type 
                      from b_sale_order_props_value as bsopv
                           join b_sale_order bso
                                on bsopv.order_id = bso.id
                     where date(bso.date_insert) BETWEEN {date1} AND {date2}
                     group by order_id, user_id
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['order_id', 'date_insert', 'user_id', 'city', 'email', 'company', 'first_name', 'last_name',
               'phone', 'promocode', 'customer_type_shop', 'customer_type_org', 'customer_type_compsny',
               'customer_type_personal', 'entity_type' ]

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteOrderPropsValue(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteOrderPropsValue(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteTransact(date1, date2):
    '''
    Функция получает данные из источника site для таблицы b_sale_user_transact.\n
    siteTransact(date1, date2)\n
    date1 - начало диапазона дат\n
    date2 - конец диапазона дат
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteTransact(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select id, timestamp_x, transact_date, order_id, user_id, amount, currency, 
                           current_budget, debit, description, employee_id, notes, payment_id
                      from b_sale_user_transact
                     where date(transact_date) BETWEEN {date1} AND {date2}
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'timestamp_x', 'transact_date', 'order_id', 'user_id', 'amount', 'currency', 
               'current_budget', 'debit', 'description', 'employee_id', 'notes', 'payment_id']

        df = pd.DataFrame(res, columns= kol)
        del res, rows
        logging.info(f"siteTransact(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteTransact(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteStatus():
    '''
    Функция получает данные из источника site для таблицы b_sale_status\n
    siteStatus()
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteStatus(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    SELECT id, sort, type, notify, color
                      FROM b_sale_status
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'sort', 'type', 'notify', 'color']

        df = pd.DataFrame(res, columns= kol)
        df.rename(columns = {'id':'status_id'}, inplace = True )
        del res, rows
        logging.info(f"siteStatus(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteStatus(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteLocationCity():
    '''
    Функция получает данные из источника site для таблицы b_sale_location_city\n
    siteLocationCity()
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteLocationCity(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select id, name, region_id, short_name
                      from b_sale_location_city
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'name', 'region_id', 'short_name']

        df = pd.DataFrame(res, columns= kol)
        df.rename(columns = {'id':'city_id'}, inplace = True )
        del res, rows
        logging.info(f"siteLocationCity(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteLocationCity(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df

def siteLocationCountry():
    '''
    Функция получает данные из источника site для таблицы b_sale_location_country\n
    siteLocationCountry()
    '''
    
    try:
        #logging.info(f"site.dataSite(). Polychenie dannih s saita.")
        # Открываем SSH туннель
        #sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
        
        logging.info(f"siteLocationCountry(). Podkluchenie k base site i otptavka zaprosa.")
    
        tunnel = SSHTunnelForwarder((config['KeySsh']['ssh_host'], 22),
                                    ssh_username = config['KeySsh']['ssh_user'],
                                    ssh_password = config['KeySsh']['ssh_password'],
                                    remote_bind_address = ('127.0.0.1', 3306)
                                    )

        tunnel.start()

        # Подключаемся к базе сайта
        connection = pymysql.connect(host=config['KeyBdSite']['bdHost'],
                                     user=config['KeyBdSite']['userName'],
                                     passwd=config['KeyBdSite']['password'],
                                     db=config['KeyBdSite']['bdName'],
                                     port=tunnel.local_bind_port)
        
        res = []

        with connection.cursor() as cursor:
            query = f"""
                    select id, name, short_name
                      from b_sale_location_country
                    """
            
            cursor.execute(query)
            rows = cursor.fetchall()
    
            for row in rows:
                res.append(list(row))
    
        tunnel.close()
        
        kol = ['id', 'name', 'short_name']

        df = pd.DataFrame(res, columns= kol)
        df.rename(columns = {'id':'country_id'}, inplace = True )
        del res, rows
        logging.info(f"siteLocationCountry(). Uspeshnoe poluchenie dannih.")
        
    except Exception as err:
        logging.error(f"siteLocationCountry(). Oshibka polycheniya dannih - {err}", exc_info=True)
        df = err   
    
    return df
    
