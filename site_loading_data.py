from sqlalchemy import text
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import logging
import pymysql

# Подключение к базе сайта

ssh_host = '194.38.9.81'
ssh_username = 'poweruser'
ssh_password = 'mTpTnRX}uK0rD?t'
database_username = 'seconduser'
database_password = 'Av4l#7qa6x4C'
database_name = 'sitemanager'
localhost = '127.0.0.1'


def open_ssh_tunnel(verbose=False):
    """Откройте SSH-туннель и подключитесь, используя имя пользователя и пароль.
    :подробный параметр: установите значение True, чтобы показывать ведение журнала.
    :обратный туннель: глобальное подключение по SSH-туннелю.
    """
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=('127.0.0.1', 3306)
    )

    tunnel.start()


def mysql_connect():
    """Подключитесь к серверу MySQL, используя туннельное соединение SSH.
    :обратное соединение: подключение к глобальной базе данных MySQL.
    """
    global connection

    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )


def run_query(sql):
    """Запускает заданный SQL-запрос через подключение к глобальной базе данных.
    :параметр sql: запрос MySQL
    :возврат: Фрейм данных Pandas, содержащий результаты
    """
    return pd.read_sql_query(sql, connection)


def mysql_disconnect():
    """Закрывает соединение с базой данных MySQL.
    """
    connection.close()


def close_ssh_tunnel():
    """Закрывает туннельное соединение SSH.
    """
    tunnel.close


# ПОДКЛЮЧЕНИЕ К БАЗЕ CHAR

# Подключаемся к базе данных PGSQL на сервере
engine_pg = create_engine('postgresql+psycopg2://postgres:listopad2479@localhost/char')
# Выдаём генеральные права
with engine_pg.connect() as conn:
    conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))


# ФУНКЦИИ ТАБЛИЦ

def site_insert_order(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_insert)::date 
                 from site_insert_order'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запросы
    sql_1 = """select bso.id,
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
                where date(bso.date_insert) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""select bso.id,
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
                 where date(bso.date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['order_id', 'account_number', 'lid', 'person_type_id', 'user_id', 'price', 'currency',
                  'date_insert', 'promocode']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод DF больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['order_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_insert_order', engine_pg, schema='public', if_exists='append', index=False)


def site_location_country():
    # Запрос в базу сайта
    sql = """SELECT id,
                    name,
                    short_name
               FROM b_sale_location_country"""

    # Получаем данные по запросу
    df = run_query(sql)

    # Делаем замену в названиях колонок т.к. иногда в датафрейм приходят названия из заглавных символов.
    df.columns = ['id', 'name_country', 'short_name']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Запрос данных из БД аналитики и получаем вывод по запросу
    sql = """select * from site_location_country"""
    df2 = pd.read_sql_query(text(sql), engine_pg)

    # Если таблица с сайта больще аналитики, то дубли и оставляем уникальные
    # Оставшиеся строки вносим в таблицу
    # Если таблица с сайта равна таблице аналитики, ничего не делаем
    if df['id'].max() > df2['id'].max():
        df3 = df.loc[~df['id'].isin(df2['id'])]
        df3.to_sql('site_location_country', engine_pg, schema='public', if_exists='append', index=False)


def site_location_city():
    # Пишем запрос
    sql = """SELECT id,
                    name,
                    short_name
               FROM b_sale_location_city
              order by id desc
              limit 10"""

    # Получаем данные по запросу
    df = run_query(sql)

    # Делаем замену в названиях колонок.
    df.columns = ['id', 'name_city', 'short_name']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Запрос данных из БД аналитики и получаем вывод по запросу
    sql = """select * 
               from site_location_city
              order by id desc
              limit 10"""
    df2 = pd.read_sql_query(text(sql), engine_pg)

    # Если таблица с сайта больще аналитики, то дубли и оставляем уникальные
    # Оставшиеся строки вносим в таблицу
    # Если таблица с сайта равна таблице аналитики, ничего не делаем
    if df['id'].max() > df2['id'].max():
        df3 = df.loc[~df['id'].isin(df2['id'])]
        df3.to_sql('site_location_city', engine_pg, schema='public', if_exists='append', index=False)


def site_location():
    # Пишем запрос
    sql = """SELECT id,
                    city_id,
                    country_id,
                    loc_default
               FROM b_sale_location
              order by id desc
              limit 10"""

    # Получаем данные по запросу
    df = run_query(sql)

    # Делаем замену в названиях колонок.
    df.columns = ['id', 'city_id', 'country_id', 'loc_default']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    sql = """select * 
               from site_location
              order by id desc
              limit 10"""
    df2 = pd.read_sql_query(text(sql), engine_pg)

    # Если таблица с сайта больще аналитики, то дубли и оставляем уникальные
    # Оставшиеся строки вносим в таблицу
    # Если таблица с сайта равна таблице аналитики, ничего не делаем
    if df['id'].max() > df2['id'].max():
        df3 = df.loc[~df['id'].isin(df2['id'])]
        df3.to_sql('site_location', engine_pg, schema='public', if_exists='append', index=False)


def site_person_type():
    # Пишем запрос
    sql = """SELECT id,
                    lid,
                    name,
                    sort,
                    active,
                    entity_registry_type
               FROM b_sale_person_type"""

    # Получаем данные по запросу
    df = run_query(sql)

    # Делаем замену в названиях колонок.
    df.columns = ['id', 'lid', 'name_type', 'sort', 'active', 'entity_registry_type']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    sql = """select * from site_person_type"""
    df2 = pd.read_sql_query(text(sql), engine_pg)

    # Если таблица с сайта больще аналитики, то дубли и оставляем уникальные
    # Оставшиеся строки вносим в таблицу
    # Если таблица с сайта равна таблице аналитики, ничего не делаем
    if df['id'].max() > df2['id'].max():
        df3 = df.loc[~df['id'].isin(df2['id'])]
        df3.to_sql('site_person_type', engine_pg, schema='public', if_exists='append', index=False)


def site_user(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_register)::date 
                 from site_user'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    login,
                    active,
                    name,
                    second_name,
                    last_name,
                    email,
                    date_register,
                    lid,
                    personal_gender,
                    personal_birthday,
                    personal_phone,
                    personal_mobile,
                    personal_city,
                    personal_state,
                    personal_country,
                    work_phone
               FROM b_user
              where date(date_register) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    login,
                    active,
                    name,
                    second_name,
                    last_name,
                    email,
                    date_register,
                    lid,
                    personal_gender,
                    personal_birthday,
                    personal_phone,
                    personal_mobile,
                    personal_city,
                    personal_state,
                    personal_country,
                    work_phone
               FROM b_user
              where date(date_register) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['id', 'login', 'active', 'first_name', 'second_name', 'last_name', 'email', 'date_register', 'lid',
                  'personal_gender', 'personal_birthday', 'personal_phone', 'personal_mobile', 'personal_city',
                  'personal_state', 'personal_country', 'work_phone']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_user', engine_pg, schema='public', if_exists='append', index=False)


def site_user_activity(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(last_login)::date 
                 from site_user_activity'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    last_login,
                    login_attempts
               FROM b_user
              where date(last_login) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    last_login,
                    login_attempts
               FROM b_user
              where date(last_login) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['user_id', 'last_login', 'login_attempts']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['user_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_user_activity', engine_pg, schema='public', if_exists='append', index=False)


def site_insert_fuser(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_insert)::date 
                 from site_insert_fuser'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    date_insert,
                    user_id
               FROM b_sale_fuser
              where date(date_insert) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    date_insert,
                    user_id
               FROM b_sale_fuser
              where date(date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['fuser_id', 'date_insert', 'user_id']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['fuser_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_insert_fuser', engine_pg, schema='public', if_exists='append', index=False)


def site_update_fuser(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_update)::date 
                 from site_update_fuser'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    date_update,
                    user_id
               FROM b_sale_fuser
              where date(date_update) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    date_update,
                    user_id
               FROM b_sale_fuser
              where date(date_update) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['fuser_id', 'date_update', 'user_id']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['fuser_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_update_fuser', engine_pg, schema='public', if_exists='append', index=False)


def site_update_order(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_update)::date 
                 from site_update_order'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запросы
    sql_1 = """select bso.id,
                      bso.account_number,
                      bso.lid,
                      bso.person_type_id,
                      bso.user_id,
                      bso.price,
                      bso.currency,
                      bso.date_update,
                      bsopv.value as promocode
                 from b_sale_order bso
                 join (select order_id,
                              value
                         from b_sale_order_props_value  
                        where name = 'Промокод') as bsopv
                      on bso.id = bsopv.order_id
                where date(bso.date_update) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""select bso.id,
                       bso.account_number,
                       bso.lid,
                       bso.person_type_id,
                       bso.user_id,
                       bso.price,
                       bso.currency,
                       bso.date_update,
                       bsopv.value as promocode
                  from b_sale_order bso
                  join (select order_id,
                               value
                          from b_sale_order_props_value  
                         where name = 'Промокод') as bsopv
                       on bso.id = bsopv.order_id
                 where date(bso.date_update) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['order_id', 'account_number', 'lid', 'person_type_id', 'user_id', 'price', 'currency',
                  'date_update', 'promocode']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['order_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_update_order', engine_pg, schema='public', if_exists='append', index=False)


def site_insert_basket(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_insert)::date 
                 from site_insert_basket'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    fuser_id,
                    order_id,
                    product_id,
                    price,
                    currency,
                    date_insert,
                    quantity,
                    lid,
                    delay,
                    name,
                    can_buy,
                    notes,
                    detail_page_url,
                    discount_price,
                    discount_name,
                    discount_value,
                    discount_coupon
               FROM b_sale_basket
              where date(date_insert) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    fuser_id,
                    order_id,
                    product_id,
                    price,
                    currency,
                    date_insert,
                    quantity,
                    lid,
                    delay,
                    name,
                    can_buy,
                    notes,
                    detail_page_url,
                    discount_price,
                    discount_name,
                    discount_value,
                    discount_coupon
               FROM b_sale_basket
              where date(date_insert) BETWEEN '{date_start}' AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['basket_id', 'fuser_id', 'order_id', 'product_id', 'price', 'currency', 'date_insert', 'quantity',
                  'lid', 'delay', 'name_product', 'can_buy', 'notes', 'detail_page_url', 'discount_price',
                  'discount_name',
                  'discount_value', 'discount_coupon']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['basket_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_insert_basket', engine_pg, schema='public', if_exists='append', index=False)


def site_update_basket(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_update)::date 
                 from site_update_basket'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    # Пишем запрос
    sql_1 = """SELECT id,
                    fuser_id,
                    order_id,
                    product_id,
                    price,
                    currency,
                    date_update,
                    quantity,
                    lid,
                    delay,
                    name,
                    can_buy,
                    notes,
                    detail_page_url,
                    discount_price,
                    discount_name,
                    discount_value,
                    discount_coupon
               FROM b_sale_basket
              where date(date_update) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                    fuser_id,
                    order_id,
                    product_id,
                    price,
                    currency,
                    date_update,
                    quantity,
                    lid,
                    delay,
                    name,
                    can_buy,
                    notes,
                    detail_page_url,
                    discount_price,
                    discount_name,
                    discount_value,
                    discount_coupon
               FROM b_sale_basket
              where date(date_update) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['basket_id', 'fuser_id', 'order_id', 'product_id', 'price', 'currency', 'date_update', 'quantity',
                  'lid', 'delay', 'name_product', 'can_buy', 'notes', 'detail_page_url', 'discount_price',
                  'discount_name',
                  'discount_value', 'discount_coupon']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['basket_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_update_basket', engine_pg, schema='public', if_exists='append', index=False)


def site_guest(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(first_date)::date 
                 from site_guest'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    sql_1 = """SELECT id,
                      favorites,
                      c_events,
                      sessions,
                      hits,
                      repair,
                      first_session_id,
                      first_date,
                      first_url_from,
                      first_url_to,
                      first_adv_id,
                      first_referer1,
                      first_referer2,
                      first_referer3,
                      last_session_id,
                      last_date,
                      last_user_id,
                      last_user_auth,
                      last_url_last,
                      last_user_agent,
                      last_ip,
                      last_adv_id,
                      last_adv_back,
                      last_referer1,
                      last_referer2,
                      last_referer3,
                      last_country_id,
                      last_city_id
                 FROM b_stat_guest
                where date(first_date) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                      favorites,
                      c_events,
                      sessions,
                      hits,
                      repair,
                      first_session_id,
                      first_date,
                      first_url_from,
                      first_url_to,
                      first_adv_id,
                      first_referer1,
                      first_referer2,
                      first_referer3,
                      last_session_id,
                      last_date,
                      last_user_id,
                      last_user_auth,
                      last_url_last,
                      last_user_agent,
                      last_ip,
                      last_adv_id,
                      last_adv_back,
                      last_referer1,
                      last_referer2,
                      last_referer3,
                      last_country_id,
                      last_city_id
                 FROM b_stat_guest
                where date(first_date) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['guest_id', 'favorites', 'c_events', 'sessions', 'hits', 'repair', 'first_session_id', 'first_date',
                  'first_url_from', 'first_url_to', 'first_adv_id', 'first_referer1', 'first_referer2',
                  'first_referer3',
                  'last_session_id', 'last_date', 'last_user_id', 'last_user_auth', 'last_url_last', 'last_user_agent',
                  'last_ip', 'last_adv_id', 'last_adv_back', 'last_referer1', 'last_referer2', 'last_referer3',
                  'last_country_id', 'last_city_id']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['guest_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_guest', engine_pg, schema='public', if_exists='append', index=False)


def site_session(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_stat)::date 
                 from site_session'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    sql_1 = """SELECT id,
                      guest_id,
                      new_guest,
                      user_id,
                      user_auth,
                      c_events,
                      hits,
                      favorites,
                      url_from,
                      url_to,
                      url_last,
                      user_agent,
                      date_stat,
                      date_first,
                      date_last,
                      ip_first,
                      ip_last,
                      adv_id,
                      adv_back,
                      referer1,
                      referer2,
                      referer3,
                      stop_list_id,
                      country_id,
                      city_id
                 FROM b_stat_session
                where date(date_stat) = date_add(CURDATE(), INTERVAL -1 day)"""

    sql_2 = f"""SELECT id,
                       guest_id,
                       new_guest,
                       user_id,
                       user_auth,
                       c_events,
                       hits,
                       favorites,
                       url_from,
                       url_to,
                       url_last,
                       user_agent,
                       date_stat,
                       date_first,
                       date_last,
                       ip_first,
                       ip_last,
                       adv_id,
                       adv_back,
                       referer1,
                       referer2,
                       referer3,
                       stop_list_id,
                       country_id,
                       city_id
                  FROM b_stat_session
                 where date(date_stat) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой втаблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['session_id', 'guest_id', 'new_guest', 'user_id', 'user_auth', 'c_events', 'hits', 'favorites',
                  'url_from', 'url_to', 'url_last', 'user_agent', 'date_stat', 'date_first',
                  'date_last', 'ip_first', 'ip_last', 'adv_id', 'adv_back', 'referer1', 'referer2', 'referer3',
                  'stop_list_id', 'country_id', 'city_id']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['session_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_session', engine_pg, schema='public', if_exists='append', index=False)


def site_order_props_value(date_now):
    # запрос на получение крайней даты в таблице
    query = '''select max(date_insert)::date 
                 from site_order_props_value'''

    # Отправляем запрос в базу аналитики и получаем крайнюю дату
    with engine_pg.connect() as conn:
        result = conn.execute(text(query))
        for row in result:
            dat = max(row)

    # Получаем дату с которой нужна загрузка данных
    date_pg = dat + timedelta(days=1)
    date_start = date_pg.strftime('%Y-%m-%d')

    sql_1 = """select bsopv.order_id,
                      bso.date_insert,
                      bso.user_id,
                      max(case when order_props_id = 2 then value end) as city,
                      max(case when order_props_id = 6 then value end) as email,
                      max(case when order_props_id = 17 then value end) as company,
                      max(case when order_props_id = 19 then value end) as first_name,
                      max(case when order_props_id = 18 then value end) as last_name,
                      max(case when order_props_id = 20 then value end) as phone,
                      max(case when order_props_id = 23 then value end) as promocode,
                      max(case when order_props_id = 24 then value end) as customer_type_shop,
                      max(case when order_props_id = 25 then value end) as customer_type_org,
                      max(case when order_props_id = 26 then value end) as customer_type_compsny,
                      max(case when order_props_id = 27 then value end) as customer_type_personal,
                      min(entity_type)  as entity_type 
                 from b_sale_order_props_value as bsopv
                      join b_sale_order bso
                           on bsopv.order_id = bso.id
                where date(bso.date_insert) = date_add(CURDATE(), INTERVAL -1 day)
                group by order_id, user_id"""

    sql_2 = f"""select bsopv.order_id,
                      bso.date_insert,
                      bso.user_id,
                      max(case when order_props_id = 2 then value end) as city,
                      max(case when order_props_id = 6 then value end) as email,
                      max(case when order_props_id = 17 then value end) as company,
                      max(case when order_props_id = 19 then value end) as first_name,
                      max(case when order_props_id = 18 then value end) as last_name,
                      max(case when order_props_id = 20 then value end) as phone,
                      max(case when order_props_id = 23 then value end) as promocode,
                      max(case when order_props_id = 24 then value end) as customer_type_shop,
                      max(case when order_props_id = 25 then value end) as customer_type_org,
                      max(case when order_props_id = 26 then value end) as customer_type_compsny,
                      max(case when order_props_id = 27 then value end) as customer_type_personal,
                      min(entity_type)  as entity_type 
                 from b_sale_order_props_value as bsopv
                      join b_sale_order bso
                           on bsopv.order_id = bso.id
                where date(bso.date_insert) BETWEEN date('{date_start}') AND date_add(CURDATE(), INTERVAL -1 day)
                group by order_id, user_id"""

    # смотрим разницу между крайней датой в таблице и текущей датой
    d = date_now - date_pg

    # если разница между текущей датой (минус 1 день) и крайней датой в таблице равна 0 дней, используем запрос sql_1.
    # в других случаях sql_2
    if d.days == 1:
        df = run_query(sql_1)
    elif d.days > 1:
        df = run_query(sql_2)

    # Делаем замену в названиях колонок.
    df.columns = ['order_id', 'date_insert', 'user_id', 'city', 'email', 'company', 'first_name', 'last_name', 'phone',
                  'promocode', 'customer_type_shop', 'customer_type_org', 'customer_type_company',
                  'customer_type_personal', 'entity_type']

    # Меняем все пустые ячейки на null
    df = df.replace({'': None})

    # Если вывод больще либо равно 1, то добавляем в базу аналитики.
    # Если вывод равен 0, ничего не делаем.
    if df['order_id'].count() >= 1:
        # Пишем в PGSQL
        df.to_sql('site_order_props_value', engine_pg, schema='public', if_exists='append', index=False)


date_now = datetime.now().date()


# Получаем данные по запросу
open_ssh_tunnel()
mysql_connect()

# Исполняем загрузку данных
site_insert_order(date_now)
site_location_country()
site_location_city()
site_location()
site_person_type()
site_user(date_now)
site_user_activity(date_now)
site_insert_fuser(date_now)
site_update_fuser(date_now)
site_update_order(date_now)
site_insert_basket(date_now)
site_update_basket(date_now)
site_guest(date_now)
site_session(date_now)
site_order_props_value(date_now)

# Закрываем соединение с mysql
mysql_disconnect()
close_ssh_tunnel()

