# преобразовать text в датафрейм

from io import StringIO
import pandas as pd
pd.set_option('display.max_columns', 90)

import logging
from datetime import datetime

# Настройка логирования
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"G:\py.projects/tb\data_collection\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")



# Очистка посещений
def clean_logs_hits(data):
    logging.info("Start clearing data HITS.")
    # Чтение полученных дынных и запись в датафрейм
    try:
        logging.info("Reading data HITS and writing to a dataframe.")
        df_hit = pd.read_csv(StringIO(data), sep='\t', low_memory=False)
    except Exception as err:
        logging.error(f"Error in reading HITS and writing to dataframe - {err}", exc_info=True)
    
    # Переименовывание столбцов
    logging.info("Renaming columns HITS.")
    df_hit=df_hit.rename(columns = {'ym:pv:counterUserIDHash': 'counter_user_id_hash',
                                    'ym:pv:watchID': 'watch_id',
                                    'ym:pv:clientID': 'client_id',
                                    'ym:pv:date': 'date_event',
                                    'ym:pv:dateTime': 'datetime_event',
                                    'ym:pv:counterID': 'counter_id',
                                    'ym:pv:title': 'title',
                                    'ym:pv:URL': 'url',
                                    'ym:pv:referer': 'referer',
                                    'ym:pv:UTMCampaign': 'utm_campaign',
                                    'ym:pv:UTMContent': 'utm_content',
                                    'ym:pv:UTMMedium': 'utm_medium',
                                    'ym:pv:UTMSource': 'utm_source',
                                    'ym:pv:UTMTerm': 'utm_term',
                                    'ym:pv:browser': 'browser',
                                    'ym:pv:browserMajorVersion': 'browser_major_version',
                                    'ym:pv:browserMinorVersion': 'browser_minor_version',
                                    'ym:pv:browserCountry': 'browser_country',
                                    'ym:pv:browserEngine': 'browser_engine',
                                    'ym:pv:browserEngineVersion1': 'browser_engine_version_1',
                                    'ym:pv:browserEngineVersion2': 'browser_engine_version_2',
                                    'ym:pv:browserEngineVersion3': 'browser_engine_version_3',
                                    'ym:pv:browserEngineVersion4': 'browser_engine_version_4',
                                    'ym:pv:browserLanguage': 'browser_language',
                                    'ym:pv:clientTimeZone': 'client_time_zone',
                                    'ym:pv:cookieEnabled': 'cookie_enabled',
                                    'ym:pv:deviceCategory': 'device_category',
                                    'ym:pv:from': 'from',
                                    'ym:pv:hasGCLID': 'has_gcid',
                                    'ym:pv:GCLID': 'gcid',
                                    'ym:pv:ipAddress': 'ip_address',
                                    'ym:pv:javascriptEnabled': 'javascript_enabled',
                                    'ym:pv:mobilePhone': 'mobile_phone',
                                    'ym:pv:mobilePhoneModel': 'mobile_phone_model',
                                    'ym:pv:openstatAd': 'openstat_ad',
                                    'ym:pv:openstatCampaign': 'openstat_campaign',
                                    'ym:pv:openstatService': 'openstat_service',
                                    'ym:pv:openstatSource': 'openstat_source',
                                    'ym:pv:operatingSystem': 'operating_system',
                                    'ym:pv:operatingSystemRoot': 'operating_system_root',
                                    'ym:pv:physicalScreenHeight': 'physical_screen_height',
                                    'ym:pv:physicalScreenWidth': 'physical_screen_width',
                                    'ym:pv:regionCity': 'region_city',
                                    'ym:pv:regionCountry': 'region_country',
                                    'ym:pv:regionCityID': 'region_city_id',
                                    'ym:pv:regionCountryID': 'region_country_id',
                                    'ym:pv:screenColors': 'screen_colors',
                                    'ym:pv:screenFormat': 'screen_format',
                                    'ym:pv:screenHeight': 'screen_height',
                                    'ym:pv:screenOrientation': 'screen_orientation',
                                    'ym:pv:screenWidth': 'screen_width',
                                    'ym:pv:windowClientHeight': 'window_client_height',
                                    'ym:pv:windowClientWidth': 'window_client_width',
                                    'ym:pv:lastTrafficSource': 'last_traffic_source',
                                    'ym:pv:lastSearchEngine': 'last_search_engine',
                                    'ym:pv:lastSearchEngineRoot': 'last_search_engine_root',
                                    'ym:pv:lastAdvEngine': 'last_adv_engine',
                                    'ym:pv:artificial': 'artificial',
                                    'ym:pv:pageCharset': 'page_charset',
                                    'ym:pv:isPageView': 'is_page_view',
                                    'ym:pv:isTurboPage': 'is_turbo_page',
                                    'ym:pv:link': 'link',
                                    'ym:pv:download': 'download',
                                    'ym:pv:notBounce': 'not_bounce',
                                    'ym:pv:lastSocialNetwork': 'last_social_network',
                                    'ym:pv:httpError': 'http_error',
                                    'ym:pv:networkType': 'network_type',
                                    'ym:pv:lastSocialNetworkProfile': 'last_social_network_profile',
                                    'ym:pv:goalsID': 'goals_id',
                                    'ym:pv:shareService': 'share_service',
                                    'ym:pv:shareURL': 'share_url',
                                    'ym:pv:shareTitle': 'share_title',
                                    'ym:pv:iFrame': 'i_frame',
                                    'ym:pv:recommendationSystem': 'recommendation_system',
                                    'ym:pv:messenger': 'messenger',
                                    'ym:pv:parsedParamsKey1': 'parsed_params_key_1',
                                    'ym:pv:parsedParamsKey2': 'parsed_params_key_2',
                                    'ym:pv:parsedParamsKey3': 'parsed_params_key_3',
                                    'ym:pv:parsedParamsKey4': 'parsed_params_key_4',
                                    'ym:pv:parsedParamsKey5': 'parsed_params_key_5',
                                    'ym:pv:parsedParamsKey6': 'parsed_params_key_6',
                                    'ym:pv:parsedParamsKey7': 'parsed_params_key_7',
                                    'ym:pv:parsedParamsKey8': 'parsed_params_key_8',
                                    'ym:pv:parsedParamsKey9': 'parsed_params_key_9',
                                    'ym:pv:parsedParamsKey10': 'parsed_params_key_10'}
                         )
    
    # Делаем замену в столбцах.
    try:
        logging.info("Clearing data HITS.")
        df_hit['goals_id']=df_hit['goals_id'].replace('\[|\]', '', regex=True)
        # Меняем все пустые ячейки на null
        df_hit = df_hit.replace({'': None})
        # Изменить тип столбца
        df_hit['counter_user_id_hash'] = df_hit['counter_user_id_hash'].astype(str)
        df_hit['client_id'] = df_hit['client_id'].astype(str)
        df_hit['watch_id'] = df_hit['watch_id'].astype(str)
        df_hit['ip_address'] = df_hit['ip_address'].astype(str)
    except Exception as err:
        logging.error(f"Data Clearing HITS error - {err}", exc_info=True)
    
    return df_hit


# Очистка визитов
def clean_logs_visits(data):
    logging.info("Start clearing data VISITS.")
    # Получение данных
    try:
        logging.info("Reading data VISITS and writing to a dataframe.")
        df_visit = pd.read_csv(StringIO(data), sep='\t', low_memory=False)
    except Exception as err:
        logging.error(f"Error in reading VISITS and writing to dataframe - {err}", exc_info=True)
       
    # Переименование столбцов
    logging.info("Renaming columns VISITS.")
    df_visit= df_visit.rename(columns = {'ym:s:counterUserIDHash': 'counter_user_id_hash',
                                             'ym:s:visitID': 'visit_id',
                                             'ym:s:counterID': 'counter_id',
                                             'ym:s:watchIDs': 'watch_id_s',
                                             'ym:s:date': 'date_event',
                                             'ym:s:dateTime': 'datetime_event',
                                             'ym:s:dateTimeUTC': 'datetime_uts',
                                             'ym:s:isNewUser': 'is_new_user',
                                             'ym:s:startURL': 'start_url',
                                             'ym:s:endURL': 'end_url',
                                             'ym:s:pageViews': 'page_views',
                                             'ym:s:visitDuration': 'visit_duration',
                                             'ym:s:bounce': 'bounce',
                                             'ym:s:ipAddress': 'ip_address',
                                             'ym:s:regionCountry': 'region_country',
                                             'ym:s:regionCity': 'region_city',
                                             'ym:s:regionCountryID': 'region_country_id',
                                             'ym:s:regionCityID': 'region_city_id',
                                             'ym:s:clientID': 'client_id',
                                             'ym:s:networkType': 'network_type',
                                             'ym:s:goalsID': 'goals_id',
                                             'ym:s:goalsSerialNumber': 'goals_serial_number',
                                             'ym:s:goalsDateTime': 'goals_date_time',
                                             'ym:s:goalsPrice': 'goals_price',
                                             'ym:s:goalsOrder': 'goals_order',
                                             'ym:s:goalsCurrency': 'goals_currency',
                                             'ym:s:lastTrafficSource': 'last_traffic_source',
                                             'ym:s:lastAdvEngine': 'last_adv_engine',
                                             'ym:s:lastReferalSource': 'last_referal_source',
                                             'ym:s:lastSearchEngineRoot': 'last_search_engine_root',
                                             'ym:s:lastSearchEngine': 'last_search_engine',
                                             'ym:s:lastSocialNetwork': 'last_social_network',
                                             'ym:s:lastSocialNetworkProfile': 'last_social_network_profile',
                                             'ym:s:referer': 'referer',
                                             'ym:s:lastDirectClickOrder': 'last_direct_click_order',
                                             'ym:s:lastDirectBannerGroup': 'last_direct_banner_group',
                                             'ym:s:lastDirectClickBanner': 'last_direct_click_banner',
                                             'ym:s:lastDirectClickOrderName': 'last_direct_click_order_name',
                                             'ym:s:lastClickBannerGroupName': 'last_click_banner_group_name',
                                             'ym:s:lastDirectClickBannerName': 'last_direct_click_banner_name',
                                             'ym:s:lastDirectPhraseOrCond': 'last_direct_phrase_or_cond',
                                             'ym:s:lastDirectPlatformType': 'last_direct_platform_type',
                                             'ym:s:lastDirectPlatform': 'last_direct_platform',
                                             'ym:s:lastDirectConditionType': 'last_direct_condition_type',
                                             'ym:s:lastCurrencyID': 'last_currency_id',
                                             'ym:s:from': 'from',
                                             'ym:s:UTMCampaign': 'utm_campaign',
                                             'ym:s:UTMContent': 'utm_content',
                                             'ym:s:UTMMedium': 'utm_medium',
                                             'ym:s:UTMSource': 'utm_source',
                                             'ym:s:UTMTerm': 'utm_term',
                                             'ym:s:openstatAd': 'openstat_ad',
                                             'ym:s:openstatCampaign': 'openstat_campaign',
                                             'ym:s:openstatService': 'openstat_service',
                                             'ym:s:openstatSource': 'openstat_source',
                                             'ym:s:hasGCLID': 'has_gclid',
                                             'ym:s:lastGCLID': 'last_gclid',
                                             'ym:s:firstGCLID': 'first_gclid',
                                             'ym:s:lastSignificantGCLID': 'last_significant_gclid',
                                             'ym:s:browserLanguage': 'browser_language',
                                             'ym:s:browserCountry': 'browser_country',
                                             'ym:s:clientTimeZone': 'client_time_zone',
                                             'ym:s:deviceCategory': 'device_category',
                                             'ym:s:mobilePhone': 'mobile_phone',
                                             'ym:s:mobilePhoneModel': 'mobile_phone_model',
                                             'ym:s:operatingSystemRoot': 'operating_system_root',
                                             'ym:s:operatingSystem': 'operating_system',
                                             'ym:s:browser': 'browser',
                                             'ym:s:browserMajorVersion': 'browser_major_version',
                                             'ym:s:browserMinorVersion': 'browser_minor_version',
                                             'ym:s:browserEngine': 'browser_engine',
                                             'ym:s:browserEngineVersion1': 'browser_engine_version_1',
                                             'ym:s:browserEngineVersion2': 'browser_engine_version_2',
                                             'ym:s:browserEngineVersion3': 'browser_engine_version_3',
                                             'ym:s:browserEngineVersion4': 'browser_engine_version_4',
                                             'ym:s:cookieEnabled': 'cookie_enabled',
                                             'ym:s:javascriptEnabled': 'javascript_enabled',
                                             'ym:s:screenFormat': 'screen_format',
                                             'ym:s:screenColors': 'screen_colors',
                                             'ym:s:screenOrientation': 'screen_orientation',
                                             'ym:s:screenWidth': 'screen_width',
                                             'ym:s:screenHeight': 'screen_height',
                                             'ym:s:physicalScreenWidth': 'physical_screen_width',
                                             'ym:s:physicalScreenHeight': 'physical_screen_height',
                                             'ym:s:windowClientWidth': 'window_client_width',
                                             'ym:s:windowClientHeight': 'window_client_height',
                                             'ym:s:purchaseID': 'purchase_id',
                                             'ym:s:purchaseDateTime': 'purchase_date_time',
                                             'ym:s:purchaseAffiliation': 'purchase_affiliation',
                                             'ym:s:purchaseRevenue': 'purchase_revenue',
                                             'ym:s:purchaseTax': 'purchase_tax',
                                             'ym:s:purchaseShipping': 'purchase_shipping',
                                             'ym:s:purchaseCoupon': 'purchase_coupon',
                                             'ym:s:purchaseCurrency': 'purchase_currency',
                                             'ym:s:purchaseProductQuantity': 'purchase_product_quantity',
                                             'ym:s:productsPurchaseID': 'products_purchase_id',
                                             'ym:s:productsID': 'products_id',
                                             'ym:s:productsName': 'products_name',
                                             'ym:s:productsBrand': 'products_brand',
                                             'ym:s:productsCategory': 'products_category',
                                             'ym:s:productsCategory1': 'products_category_1',
                                             'ym:s:productsCategory2': 'products_category_2',
                                             'ym:s:productsCategory3': 'products_category_3',
                                             'ym:s:productsCategory4': 'products_category_4',
                                             'ym:s:productsCategory5': 'products_category_5',
                                             'ym:s:productsVariant': 'products_variant',
                                             'ym:s:productsPosition': 'products_position',
                                             'ym:s:productsPrice': 'products_price',
                                             'ym:s:productsCurrency': 'products_currency',
                                             'ym:s:productsCoupon': 'products_coupon',
                                             'ym:s:productsQuantity': 'products_quantity',
                                             'ym:s:impressionsURL': 'impressions_url',
                                             'ym:s:impressionsDateTime': 'impressions_date_time',
                                             'ym:s:impressionsProductID': 'impressions_product_id',
                                             'ym:s:impressionsProductName': 'impressions_product_name',
                                             'ym:s:impressionsProductBrand': 'impressions_product_brand',
                                             'ym:s:impressionsProductCategory': 'impressions_product_category',
                                             'ym:s:impressionsProductCategory1': 'impressions_product_category_1',
                                             'ym:s:impressionsProductCategory2': 'impressions_product_category_2',
                                             'ym:s:impressionsProductCategory3': 'impressions_product_category_3',
                                             'ym:s:impressionsProductCategory4': 'impressions_product_category_4',
                                             'ym:s:impressionsProductCategory5': 'impressions_product_category_5',
                                             'ym:s:impressionsProductVariant': 'impressions_product_variant',
                                             'ym:s:impressionsProductPrice': 'impressions_product_price',
                                             'ym:s:impressionsProductCurrency': 'impressions_product_currency',
                                             'ym:s:impressionsProductCoupon': 'impressions_product_coupon',
                                             'ym:s:offlineCallTalkDuration': 'offline_call_talk_duration',
                                             'ym:s:offlineCallHoldDuration': 'offline_call_hold_duration',
                                             'ym:s:offlineCallMissed': 'offline_call_missed',
                                             'ym:s:offlineCallTag': 'offline_callTag',
                                             'ym:s:offlineCallFirstTimeCaller': 'offline_call_first_time_caller',
                                             'ym:s:offlineCallURL,': 'offline_call_url',
                                             'ym:s:parsedParamsKey1': 'parsed_params_key_1',
                                             'ym:s:parsedParamsKey2': 'parsed_params_key_2',
                                             'ym:s:parsedParamsKey3': 'parsed_params_key_3',
                                             'ym:s:parsedParamsKey4': 'parsed_params_key_4',
                                             'ym:s:parsedParamsKey5': 'parsed_params_key_5',
                                             'ym:s:parsedParamsKey6': 'parsed_params_key_6'})

    # Изменить тип столбца
    df_visit['counter_user_id_hash'] = df_visit['counter_user_id_hash'].astype(str)
    df_visit['client_id'] = df_visit['client_id'].astype(str)
    df_visit['visit_id'] = df_visit['visit_id'].astype(str)
    df_visit['ip_address'] = df_visit['ip_address'].astype(str)
    
    # Делаем замену в столбцах.
    try:
        logging.info("Clearing data VISITS.")
        df_visit=df_visit.replace(['\[|\]'], '', regex=True)
        # Меняем все пустые ячейки на null
        df_visit= df_visit.replace({'': None})
    except Exception as err:
        logging.error(f"Data Clearing VISITS error - {err}", exc_info=True)
    
    return df_visit



