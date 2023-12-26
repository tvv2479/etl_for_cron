
# https://tproger.ru/articles/kak-napisat-telegram-bota-na-python-delaem-remajnder
# https://pythonist.ru/otpravka-soobshhenij-v-telegram-pri-pomoshhi-python/
#%%
import requests
import time
from datetime import datetime
import logging
import configparser
# %%
config = configparser.ConfigParser()
config.read("G:\py.projects/tb\data_collection/config.ini")

TOKEN = config['BotTel']['token']
chat_id = config['BotTel']['chat']
#%%
# Настройка логирования
current_date = datetime.now().date()
cd = current_date.strftime('%d_%m_%Y')

log_file = f"G:\py.projects/tb\data_collection\logs\ym_load_{cd}.log"

logging.basicConfig(level=logging.INFO, filename=log_file, filemode="a",
                    format="%(name)s %(asctime)s %(levelname)s %(message)s")
#%%

# Отправляем сообщение

def bot_message(msg):
    '''
    msg - Текст сообщения в фомате str
    '''
    logging.info("Otpravka soobsheniya v telegram.")
    
    try:
        message = msg
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
        # print(requests.get(url).json()) # Эта строка отсылает сообщение
        requests.get(url).json()
        logging.info("Soobshenie v telegram otpravleno.")
    except Exception as err:
        logging.error(f"Оshibka otpravki soobsheniya v telegram - {err}", exc_info=True)
        
# %%
