from datetime import date, timedelta
from tapi_yandex_metrika import YandexMetrikaLogsapi
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
# import psycopg2

# год, месяц, число
now = date.today()

#dat1 = '2023-10-14'
#dat2 = '2023-10-15'

dat1 = now - timedelta(days=1)
dat2 = now - timedelta(days=1)

# Запрос данных с ЯМ.
# Запрашиваем сразу все требуемые столбцы из hits.

ACCESS_TOKEN = 'y0_AgAEA7qiSjdCAArBiQAAAADwur_MQQA8I5fUSNGRogeV4Nz3JxI2xos'
COUNTER_ID = '24237841'

# hits
client = YandexMetrikaLogsapi(
    access_token=ACCESS_TOKEN,
    default_url_params={'counterId': COUNTER_ID},
    # Загрузите отчет, когда он будет создан
    wait_report=True,
)
params={
    "fields": "ym:pv:clientID,ym:pv:watchID,ym:pv:UTMCampaign,ym:pv:UTMContent,ym:pv:UTMMedium,ym:pv:UTMSource,"
              "ym:pv:UTMTerm,ym:pv:openstatAd,ym:pv:openstatCampaign,ym:pv:openstatService,ym:pv:openstatSource,"
              "ym:pv:lastAdvEngine,ym:pv:download,ym:pv:goalsID,ym:pv:lastSocialNetwork,ym:pv:lastSearchEngine,"
              "ym:pv:lastSearchEngineRoot,ym:pv:shareService,ym:pv:shareURL,ym:pv:shareTitle,"
              "ym:pv:lastSocialNetworkProfile,ym:pv:notBounce,ym:pv:link,ym:pv:title,ym:pv:URL,ym:pv:counterID,"
              "ym:pv:date,ym:pv:dateTime,ym:pv:deviceCategory,ym:pv:regionCity,ym:pv:regionCityID,ym:pv:regionCountry,"
              "ym:pv:regionCountryID,ym:pv:lastTrafficSource,ym:pv:referer,ym:pv:ipAddress,ym:pv:isPageView,"
              "ym:pv:artificial",
    "source": "hits",
    "date1": dat1,
    "date2": dat2
}
info = client.create().post(params=params)
request_id = info["log_request"]["request_id"]

report = client.download(requestId=request_id).get()
dicts = report().to_dicts()
df = pd.DataFrame(dicts)

# Делаем замену в названиях колонок.
df=df.rename(columns = {'ym:pv:clientID':'client_id',
                        'ym:pv:watchID':'watch_id',
                        'ym:pv:UTMCampaign':'utm_campaign',
                        'ym:pv:UTMContent':'utm_content',
                        'ym:pv:UTMMedium':'utm_medium',
                        'ym:pv:UTMSource':'utm_source',
                        'ym:pv:UTMTerm':'utm_term',
                        'ym:pv:openstatAd':'openstat_ad',
                        'ym:pv:openstatCampaign':'openstat_campaign',
                        'ym:pv:openstatService':'openstat_service',
                        'ym:pv:openstatSource':'openstat_source',
                        'ym:pv:lastAdvEngine':'last_adv_engine',
                        'ym:pv:download':'download',
                        'ym:pv:goalsID':'goals_id',
                        'ym:pv:lastSocialNetwork':'last_social_network',
                        'ym:pv:lastSearchEngine':'last_search_engine',
                        'ym:pv:lastSearchEngineRoot':'last_search_engine_root',
                        'ym:pv:shareService':'share_service',
                        'ym:pv:shareURL':'share_url',
                        'ym:pv:shareTitle':'share_title',
                        'ym:pv:lastSocialNetworkProfile':'last_social_network_profile',
                        'ym:pv:notBounce':'not_bounce',
                        'ym:pv:link':'link',
                        'ym:pv:title':'title',
                        'ym:pv:URL':'url',
                        'ym:pv:counterID':'counter_id',
                        'ym:pv:date':'date_event',
                        'ym:pv:dateTime':'datetime_event',
                        'ym:pv:deviceCategory':'device_category',
                        'ym:pv:regionCity':'region_city',
                        'ym:pv:regionCityID':'region_city_id',
                        'ym:pv:regionCountry':'region_country',
                        'ym:pv:regionCountryID':'region_country_id',
                        'ym:pv:lastTrafficSource':'last_traffic_source',
                        'ym:pv:referer':'referer',
                        'ym:pv:ipAddress':'ip_address',
                        'ym:pv:isPageView':'is_page_view',
                        'ym:pv:artificial':'artificial'})

# Делаем замену в столбце.
df['goals_id']=df['goals_id'].str.replace('\[|\]', '', regex=True)

# Меняем все пустые ячейки на null
df=df.replace({'': None})

# Таблица ym_hits_ad
# Выбираем нужные колонки.
ad = df[["client_id",
         "watch_id",
         "utm_campaign",
         "utm_content",
         "utm_medium",
         "utm_source",
         "utm_term",
         "openstat_ad",
         "openstat_campaign",
         "openstat_service",
         "openstat_source",
         "last_adv_engine"]]


# Таблица ym_hits_ad
# предполагаем, что у нас есть DataFrame с названием 'date'
# Подключаемся к базе данных PGSQL
engine = create_engine('postgresql+psycopg2://postgres:listopad2479@localhost/char')
# Выдаём генеральные права
with engine.connect() as conn:
    conn.execute(text('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres'))
# Пишем в PGSQL
ad.to_sql('ym_hits_ad', engine, schema='public', if_exists='append', index=False)
ad=0

# Таблица ym_hits_deystviya
# Выбираем нужные колонки.
deystviya=df[['client_id',
              'watch_id',
              'download',
              'goals_id',
              'last_social_network',
              'last_search_engine',
              'last_search_engine_root',
              'share_service',
              'share_url',
              'share_title',
              'last_social_network_profile']]

# Таблица ym_hits_deystviya
# Пишем в PGSQL
deystviya.to_sql('ym_hits_deystviya', engine, schema='public', if_exists='append', index=False)
deystviya=0

# Таблица ym_hits_stranitsi
# Выбираем нужные колонки.
stranitsi=df[['client_id',
              'watch_id',
              'not_bounce',
              'link',
              'title',
              'url']]

# Таблица ym_hits_stranitsi
# Пишем в PGSQL
stranitsi.to_sql('ym_hits_stranitsi', engine, schema='public', if_exists='append', index=False)
stranitsi=0

# Таблица ym_hits_obshee
# Выбираем нужные колонки.
obshee=df[['client_id',
              'watch_id',
              'counter_id',
              'date_event',
              'datetime_event',
              'device_category',
              'region_city',
              'region_city_id',
              'region_country',
              'region_country_id',
              'last_traffic_source',
              'referer',
              'ip_address',
              'is_page_view',
              'artificial']]

# Таблица ym_hits_obshee
# Пишем в PGSQL
obshee.to_sql('ym_hits_obshee', engine, schema='public', if_exists='append', index=False)
obshee=0

with engine.connect() as conn:
    conn.execute(text('delete from ym_hits_ad where client_id + watch_id in (select client_id + watch_id from ym_hits_obshee where date_event::date < (current_date - 1100))'))
    conn.execute(text('delete from ym_hits_deystviya where client_id + watch_id in (select client_id + watch_id from ym_hits_obshee where date_event::date < (current_date - 1100))'))
    conn.execute(text('delete from ym_hits_stranitsi where client_id + watch_id in (select client_id + watch_id from ym_hits_obshee where date_event::date < (current_date - 1100))'))
    conn.execute(text('delete from ym_hits_obshee where date_event::date < (current_date - 1100)'))

# visits
client = YandexMetrikaLogsapi(
    access_token=ACCESS_TOKEN,
    default_url_params={'counterId': COUNTER_ID},
    # Загрузите отчет, когда он будет создан
    wait_report=True,
)
params={
    "fields": "ym:s:clientID,ym:s:visitID,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,"
              "ym:s:lastSearchEngineRoot,ym:s:lastSearchEngine,ym:s:lastSocialNetwork,ym:s:lastSocialNetworkProfile,"
              "ym:s:referer,ym:s:impressionsProductCoupon,ym:s:counterID,ym:s:dateTimeUTC,"
              "ym:s:dateTime,ym:s:isNewUser,ym:s:startURL,ym:s:endURL,ym:s:pageViews,ym:s:visitDuration,"
              "ym:s:deviceCategory,ym:s:regionCountry,ym:s:regionCountryID,ym:s:regionCity,ym:s:regionCityID,"
              "ym:s:bounce,ym:s:ipAddress",
    "source": "visits",
    "date1": dat1,
    "date2": dat2
}
info = client.create().post(params=params)
request_id = info["log_request"]["request_id"]

report = client.download(requestId=request_id).get()
dicts = report().to_dicts()
df = pd.DataFrame(dicts)

# Делаем замену в названиях колонок.
df=df.rename(columns = {'ym:s:clientID':'client_id','ym:s:visitID':'visit_id',
                        'ym:s:lastTrafficSource':'last_traffic_source','ym:s:lastAdvEngine':'last_adv_engine',
                        'ym:s:lastReferalSource':'last_referal_source',
                        'ym:s:lastSearchEngineRoot':'last_search_engine_root',
                        'ym:s:lastSearchEngine':'last_search_engine',
                        'ym:s:lastSocialNetwork':'last_social_network',
                        'ym:s:lastSocialNetworkProfile':'last_social_network_profile','ym:s:referer':'referer',
                        'ym:s:impressionsProductCoupon':'impressions_product_coupon','ym:s:counterID':'counter_id',
                        'ym:s:dateTimeUTC':'date_time_utc','ym:s:dateTime':'date_time','ym:s:isNewUser':'is_new_user',
                        'ym:s:startURL':'start_url','ym:s:endURL':'end_url','ym:s:pageViews':'page_views',
                        'ym:s:visitDuration':'visit_duration','ym:s:deviceCategory':'device_category',
                        'ym:s:regionCountry':'region_country','ym:s:regionCountryID':'region_country_id',
                        'ym:s:regionCity':'region_city','ym:s:regionCityID':'region_city_id','ym:s:bounce':'bounce',
                        'ym:s:ipAddress':'ip_address'})

# Меняем все пустые ячейки на null
df=df.replace({'': None})

# Таблица ym_visits_detali
# Выбираем нужные колонки.
vis_detali=df[['client_id',
               'visit_id',
               'last_traffic_source',
               'last_adv_engine',
               'last_referal_source',
               'last_search_engine_root',
               'last_search_engine',
               'last_social_network',
               'last_social_network_profile',
               'referer',
               'impressions_product_coupon']]

# Пишем в PGSQL. Таблица ym_visits_detali
vis_detali.to_sql('ym_visits_detali', engine, schema='public', if_exists='append', index=False)
vis_detali=0

# Таблица ym_visits_obshee
# Выбираем нужные колонки.
vis_obshee=df[['client_id',
               'visit_id',
               'counter_id',
               'date_time_utc',
               'date_time',
               'is_new_user',
               'start_url',
               'end_url',
               'page_views',
               'visit_duration',
               'device_category',
               'region_country',
               'region_country_id',
               'region_city',
               'region_city_id',
               'bounce',
               'ip_address']]

# Пишем в PGSQL. Таблица ym_visits_obshee.
vis_obshee.to_sql('ym_visits_obshee', engine, schema='public', if_exists='append', index=False)
vis_obshee=0
df=0

with engine.connect() as conn:
    conn.execute(text('delete from ym_visits_detali where client_id + visit_id in (select client_id + visit_id from ym_visits_obshee where date_time::date < (current_date - 1100))'))
    conn.execute(text('delete from ym_visits_obshee where date_time::date < (current_date - 1100)'))