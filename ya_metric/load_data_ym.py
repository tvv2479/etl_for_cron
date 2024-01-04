import requests
import json
from time import sleep
import logging
from datetime import datetime
from io import StringIO

# Настраиваем логирование
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"D:/vit\py.projects\char\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")



class Logsapi:
    """
    В класс Logsapi требуется добавить обязательные переменные:\n
    token - Токен, специальный код, разрешающий доступ к данным конкретного пользователя.\n
    counter - Номер счётчика у которого запрашиваются данные.\n
    date1 - От какой даты данные? Формат даты "ГГГГ-ММ-ДД".\n
    date2 - До какой даты данные? Формат даты "ГГГГ-ММ-ДД".\n
    """
    
    def __init__(self, token, counter, date1, date2):
        self.token = token
        self.counter = counter
        self.date1 = date1
        self.date2 = date2
    
       
    def download_hits(self):
        source_hits = 'hits'
        
        fields_hits = ('ym:pv:counterUserIDHash,ym:pv:watchID,ym:pv:counterID,'
                       'ym:pv:date,ym:pv:dateTime,ym:pv:title,ym:pv:URL,ym:pv:referer,'
                       'ym:pv:UTMCampaign,ym:pv:UTMContent,ym:pv:UTMMedium,ym:pv:UTMSource,'
                       'ym:pv:UTMTerm,ym:pv:browser,ym:pv:browserMajorVersion,'
                       'ym:pv:browserMinorVersion,ym:pv:browserCountry,ym:pv:browserEngine,'
                       'ym:pv:browserEngineVersion1,ym:pv:browserEngineVersion2,'
                       'ym:pv:browserEngineVersion3,ym:pv:browserEngineVersion4,'
                       'ym:pv:browserLanguage,ym:pv:clientTimeZone,ym:pv:cookieEnabled,'
                       'ym:pv:deviceCategory,ym:pv:from,ym:pv:hasGCLID,ym:pv:GCLID,'
                       'ym:pv:ipAddress,ym:pv:javascriptEnabled,ym:pv:mobilePhone,'
                       'ym:pv:mobilePhoneModel,ym:pv:openstatAd,ym:pv:openstatCampaign,'
                       'ym:pv:openstatService,ym:pv:openstatSource,ym:pv:operatingSystem,'
                       'ym:pv:operatingSystemRoot,ym:pv:physicalScreenHeight,' 
                       'ym:pv:physicalScreenWidth,ym:pv:regionCity,ym:pv:regionCountry,'
                       'ym:pv:regionCityID,ym:pv:regionCountryID,ym:pv:screenColors,'
                       'ym:pv:screenFormat,ym:pv:screenHeight,ym:pv:screenOrientation,'
                       'ym:pv:screenWidth,ym:pv:windowClientHeight,ym:pv:windowClientWidth,'
                       'ym:pv:lastTrafficSource,ym:pv:lastSearchEngine,ym:pv:lastSearchEngineRoot,'
                       'ym:pv:lastAdvEngine,ym:pv:artificial,ym:pv:pageCharset,ym:pv:isPageView,'
                       'ym:pv:isTurboPage,ym:pv:link,ym:pv:download,ym:pv:notBounce,'
                       'ym:pv:lastSocialNetwork,ym:pv:httpError,ym:pv:clientID,' 
                       'ym:pv:networkType,ym:pv:lastSocialNetworkProfile,'
                       'ym:pv:goalsID,ym:pv:shareService,ym:pv:shareURL,ym:pv:shareTitle,'
                       'ym:pv:iFrame,ym:pv:recommendationSystem,ym:pv:messenger,' 
                       'ym:pv:parsedParamsKey1,ym:pv:parsedParamsKey2,ym:pv:parsedParamsKey3,'
                       'ym:pv:parsedParamsKey4,ym:pv:parsedParamsKey5,ym:pv:parsedParamsKey6,'
                       'ym:pv:parsedParamsKey7,ym:pv:parsedParamsKey8,ym:pv:parsedParamsKey9,'
                       'ym:pv:parsedParamsKey10')

        headers_post = {'Authorization': 'OAuth ' + self.token,
                        'Content-Type': 'application/json; charset=utf-8'}

        headers_get = {'Authorization': 'OAuth ' + self.token,
                       'Accept': 'application/json'}

        params_hits = {'date1': self.date1,
                       'date2': self.date2,
                       'source': source_hits,
                       'fields': fields_hits}
        
        # ШАГ 1. Создать лог
        logging.info("Step_1_hits. Creating a log.")
        url1 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequests.json'

        # СОЗДАЁМ ЛОГ ЗАПРОСА
        try:
            r = requests.post(url1, headers=headers_post, params=params_hits)
            #while r.status_code != 200:
                #r = requests.post(url1, headers=headers_post, params=params_hits)
                #sleep(5)
                
            logging.info(f"Code status {r.status_code}", exc_info=True)
            data1 = r.json()    
            logging.info(f"Step_1_hits. Successful log request.")
        except Exception as err:
            logging.error(f"Code status {r.status_code}", exc_info=True)
            logging.error(f"Step_1_hits. An error has occurred {err}", exc_info=True)
    
        requestId = data1['log_request']['request_id']
        status = data1['log_request']['status']

        logging.info(f"Step_1_hits. Request number {requestId}.")
        logging.info(f"Step_1_hits. Request status {status}.")
        
        # ШАГ 2. Узнать статус обработки лога
        logging.info("Step_2_hits. Log processing status.")
        url2 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequest/{requestId}'

        try:
            r = requests.get(url2, headers=headers_get)
            while r.status_code != 200:
                r = requests.get(url2, headers=headers_get)
                sleep(5)

            data2 = r.json()
            logging.info(f"Step_2_hits. Loading data. Code {r.status_code}.")
        except Exception as err:
            logging.error(f"Step_2_hits. Loading statuses. An error has occurred {err}", exc_info=True)

        # Делаем запросы до тех пор, пока появится статус 'processed'
        # Задержка перед каждым повтором 5 сек.
        while data2['log_request']['status'] != 'processed':
            r = requests.get(url2, headers=headers_get)
            data2 = r.json()
            sleep(5)

        parts = data2['log_request']['parts']
        status = data2['log_request']['status']

        logging.info(f"Step_2_hits. Number of parts - {len(parts)}.")
        logging.info(f"Step_2_hits. Request status {status}.")
        
        # Шаг 3. Выгрузка запроса
        logging.info("Step_3_hits. Loading the log.")

        res = []

        try:
            if len(parts) == 1:
                # Если всего одна часть выгрузки
                url3 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}\
                        /logrequest/{requestId}/part/0/download'
                r = requests.get(url3, headers=headers_get)
                while r.status_code != 200:
                    r = requests.get(url3, headers=headers_get)
                    sleep(5)
    
                res = r.text
                logging.info("Step_3_hits. Loading a log from one part.")
            # Если частей выгрузки болльше чем одна
            else:
                a = list(range(0, len(parts)))
                d = []
                for i in a:
                    url3 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}\
                            /logrequest/{requestId}/part/{i}/download'
                    r = requests.get(url3, headers=headers_get)
                    while r.status_code != 200:
                        r = requests.get(url3, headers=headers_get)
                        sleep(5)
            
                    d.append(r.text)
    
                res.append(d)
                logging.info(f"Step_3_hits. Loading {len(parts)} parts of the log.")
        
        except Exception as err:
            logging.error(f"Step_3_hits. Error loading the log: {err}", exc_info=True)

        # ШАГ 4. Очистка логов и переменных
        logging.info("Step_4_hits. Deleting a log.")
        url4 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequest/{requestId}/clean?'

        try:
            r = requests.post(url4, headers=headers_get)
            while r.status_code != 200:
                r = requests.post(url4, headers=headers_get)
                sleep(5)
            logging.info(f"Step_4_hits. Successful log deletion # {requestId}.")
        except Exception as err:
            logging.error(f"Step_4_hits. Log deletion error: {err}", exc_info=True)
    

        variables = [source_hits, fields_hits, headers_post, headers_get, params_hits, url1, r, data1,
                     requestId, status, url2, data2, parts, url3, url4]
        for var in variables:
            del var
            
        logging.info("Step_4_hits. Deleting variables.")

        return res

    
    def download_visits(self):
        source_visits = 'visits'

        fields_visits = ('ym:s:counterUserIDHash,'
                         'ym:s:visitID,'
                         'ym:s:counterID,'
                         'ym:s:watchIDs,'
                         'ym:s:date,'
                         'ym:s:dateTime,'
                         'ym:s:dateTimeUTC,'
                         'ym:s:isNewUser,'
                         'ym:s:startURL,'
                         'ym:s:endURL,'
                         'ym:s:pageViews,'
                         'ym:s:visitDuration,'
                         'ym:s:bounce,'
                         'ym:s:ipAddress,'
                         'ym:s:regionCountry,'
                         'ym:s:regionCity,'
                         'ym:s:regionCountryID,'
                         'ym:s:regionCityID,'
                         'ym:s:clientID,'
                         'ym:s:networkType,'
                         'ym:s:goalsID,'
                         'ym:s:goalsSerialNumber,'
                         'ym:s:goalsDateTime,'
                         'ym:s:goalsPrice,'
                         'ym:s:goalsOrder,'
                         'ym:s:goalsCurrency,'
                         'ym:s:lastTrafficSource,'
                         'ym:s:lastAdvEngine,'
                         'ym:s:lastReferalSource,'
                         'ym:s:lastSearchEngineRoot,'
                         'ym:s:lastSearchEngine,'
                         'ym:s:lastSocialNetwork,'
                         'ym:s:lastSocialNetworkProfile,'
                         'ym:s:referer,'
                         'ym:s:lastDirectClickOrder,'
                         'ym:s:lastDirectBannerGroup,'
                         'ym:s:lastDirectClickBanner,'
                         'ym:s:lastDirectClickOrderName,'
                         'ym:s:lastClickBannerGroupName,'
                         'ym:s:lastDirectClickBannerName,'
                         'ym:s:lastDirectPhraseOrCond,'
                         'ym:s:lastDirectPlatformType,'
                         'ym:s:lastDirectPlatform,'
                         'ym:s:lastDirectConditionType,'
                         'ym:s:lastCurrencyID,'
                         'ym:s:from,'
                         'ym:s:UTMCampaign,'
                         'ym:s:UTMContent,'
                         'ym:s:UTMMedium,'
                         'ym:s:UTMSource,'
                         'ym:s:UTMTerm,'
                         'ym:s:openstatAd,'
                         'ym:s:openstatCampaign,'
                         'ym:s:openstatService,'
                         'ym:s:openstatSource,'
                         'ym:s:hasGCLID,'
                         'ym:s:lastGCLID,'
                         'ym:s:firstGCLID,'
                         'ym:s:lastSignificantGCLID,'
                         'ym:s:browserLanguage,'
                         'ym:s:browserCountry,'
                         'ym:s:clientTimeZone,'
                         'ym:s:deviceCategory,'
                         'ym:s:mobilePhone,'
                         'ym:s:mobilePhoneModel,'
                         'ym:s:operatingSystemRoot,'
                         'ym:s:operatingSystem,'
                         'ym:s:browser,'
                         'ym:s:browserMajorVersion,'
                         'ym:s:browserMinorVersion,'
                         'ym:s:browserEngine,'
                         'ym:s:browserEngineVersion1,'
                         'ym:s:browserEngineVersion2,'
                         'ym:s:browserEngineVersion3,'
                         'ym:s:browserEngineVersion4,'
                         'ym:s:cookieEnabled,'
                         'ym:s:javascriptEnabled,'
                         'ym:s:screenFormat,'
                         'ym:s:screenColors,'
                         'ym:s:screenOrientation,'
                         'ym:s:screenWidth,'
                         'ym:s:screenHeight,'
                         'ym:s:physicalScreenWidth,'
                         'ym:s:physicalScreenHeight,'
                         'ym:s:windowClientWidth,'
                         'ym:s:windowClientHeight,'
                         'ym:s:purchaseID,'
                         'ym:s:purchaseDateTime,'
                         'ym:s:purchaseAffiliation,'
                         'ym:s:purchaseRevenue,'
                         'ym:s:purchaseTax,'
                         'ym:s:purchaseShipping,'
                         'ym:s:purchaseCoupon,'
                         'ym:s:purchaseCurrency,'
                         'ym:s:purchaseProductQuantity,'
                         'ym:s:productsPurchaseID,'
                         'ym:s:productsID,'
                         'ym:s:productsName,'
                         'ym:s:productsBrand,'
                         'ym:s:productsCategory,'
                         'ym:s:productsCategory1,'
                         'ym:s:productsCategory2,'
                         'ym:s:productsCategory3,'
                         'ym:s:productsCategory4,'
                         'ym:s:productsCategory5,'
                         'ym:s:productsVariant,'
                         'ym:s:productsPosition,'
                         'ym:s:productsPrice,'
                         'ym:s:productsCurrency,'
                         'ym:s:productsCoupon,'
                         'ym:s:productsQuantity,'
                         'ym:s:impressionsURL,'
                         'ym:s:impressionsDateTime,'
                         'ym:s:impressionsProductID,'
                         'ym:s:impressionsProductName,'
                         'ym:s:impressionsProductBrand,'
                         'ym:s:impressionsProductCategory,'
                         'ym:s:impressionsProductCategory1,'
                         'ym:s:impressionsProductCategory2,'
                         'ym:s:impressionsProductCategory3,'
                         'ym:s:impressionsProductCategory4,'
                         'ym:s:impressionsProductCategory5,'
                         'ym:s:impressionsProductVariant,'
                         'ym:s:impressionsProductPrice,'
                         'ym:s:impressionsProductCurrency,'
                         'ym:s:impressionsProductCoupon,'
                         'ym:s:offlineCallTalkDuration,'
                         'ym:s:offlineCallHoldDuration,'
                         'ym:s:offlineCallMissed,'
                         'ym:s:offlineCallTag,'
                         'ym:s:offlineCallFirstTimeCaller,'
                         'ym:s:offlineCallURL,'
                         'ym:s:parsedParamsKey1,'
                         'ym:s:parsedParamsKey2,'
                         'ym:s:parsedParamsKey3,'
                         'ym:s:parsedParamsKey4,'
                         'ym:s:parsedParamsKey5,'
                         'ym:s:parsedParamsKey6,'
                         #'ym:s:parsedParamsKey7,'
                         #'ym:s:parsedParamsKey8,'
                         #'ym:s:parsedParamsKey9,'
                         #'ym:s:parsedParamsKey10,'
                         #'ym:s:recommendationSystem,'
                         #'ym:s:messenger'
                         )

        params_visits = {'date1':self.date1,
                         'date2':self.date2,
                         'source':source_visits,
                         'fields':fields_visits}

        headers_post = {'Authorization': 'OAuth ' + self.token,
                        'Content-Type': 'application/json; charset=utf-8'}

        headers_get = {'Authorization': 'OAuth ' + self.token,
                       'Accept': 'application/json'}
        
        # ШАГ 1. Создать лог
        logging.info("Step_1_visit. Creating a log.")
        url1 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequests.json'

        try:
            r = requests.post(url1, headers=headers_post, params=params_visits)
            #while r.status_code != 200:
                #r = requests.post(url1, headers=headers_post, params=params_visits)
                #sleep(5)
                
            logging.info(f"Code status {r.status_code}", exc_info=True)
            data1 = r.json()
            logging.info(f"Step_1_visit. Successful log request.")
        except Exception as err:
            logging.error(f"Code status {r.status_code}", exc_info=True)
            logging.error(f"Step_1_visit. An error has occurred {err}", exc_info=True)
    
        requestId = data1['log_request']['request_id']
        status = data1['log_request']['status']

        logging.info(f"Step_1_visit. Request number {requestId}.")
        logging.info(f"Step_1_visit. Request status {status}.")
    
        
        # ШАГ 2. Узнать статус обработки лога
        logging.info("Step_2_visit. Log processing status.")
        url2 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequest/{requestId}'

        try:
            r = requests.get(url2, headers=headers_get)
            while r.status_code != 200:
                r = requests.get(url2, headers=headers_get)
                sleep(5)

            data2 = r.json()
            logging.info(f"Step_2_visit. Loading data. Code {r.status_code}.")
        except Exception as err:
            logging.error(f"Step_2_visit. Loading statuses. An error has occurred {err}", exc_info=True)

 
        # Делаем запросы до тех пор, пока появится статус 'processed'
        # Задержка перед каждым повтором 5 сек.
        while data2['log_request']['status'] != 'processed':
            r = requests.get(url2, headers=headers_get)
            data2 = r.json()
            sleep(5)

        parts = data2['log_request']['parts'][0]['part_number']
        status = data2['log_request']['status']

        logging.info(f"Step_2_visit. Number of parts - {parts}.")
        logging.info(f"Step_2_visit. Request status {status}.")
        
        # Шаг 3. Выгрузка запроса
        logging.info("Step_3_visit. Loading the log.")

        res = []

        try:
            if parts == 0:
                # Если всего одна часть выгрузки
                url3 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}\
                         /logrequest/{requestId}/part/0/download'
                r = requests.get(url3, headers=headers_get)
                while r.status_code != 200:
                    r = requests.get(url3, headers=headers_get)
                    sleep(5)
    
                res = r.text
                logging.info("Step_3_visit. Loading a log from one part.")
        
            else:
                # Если частей выгрузки болльше чем одна
                a = list(range(0, len(parts)))
                d = []
                for i in a:
                    url3 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}\
                             /logrequest/{requestId}/part/{i}/download'
                    r = requests.get(url3, headers=headers_get)
                    while r.status_code != 200:
                        r = requests.get(url3, headers=headers_get)
                        sleep(5)
            
                    d.append(r.text)
    
                res.append(d)
            logging.info(f"Step_3_visit. Loading {parts} parts of the log.")
        
        except Exception as err:
            logging.error(f"Step_3_visit. Error loading the log: {err}", exc_info=True)

        # ШАГ 4. Очистка логов и переменных
        logging.info("Step_4_visit. Deleting a log.")
        url4 = f'https://api-metrika.yandex.net/management/v1/counter/{self.counter}/logrequest/{requestId}/clean?'

        try:
            r = requests.post(url4, headers=headers_get)
            while r.status_code != 200:
                r = requests.post(url4, headers=headers_get)
                sleep(5)
            logging.info(f"Step_4_visit. Successful log deletion # {requestId}.")
        except Exception as err:
            logging.error(f"Step_4_visit. Log deletion error: {err}", exc_info=True)
    

        variables = [source_visits, fields_visits, headers_post, headers_get, params_visits, url1, r, data1,
                     requestId, status, url2, data2, parts, url3, url4]
        for var in variables:
            del var
            
        logging.info("Step_4_visit. Deleting variables.")

        return res
    