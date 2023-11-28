#%%
from glob import glob
import os
from datetime import datetime, date, timedelta

def logs_clean(logs, day=4):
    '''
    logs - Список файлов логов\n
    day - Количество дней от текущей даты. За какой период оставляем файлы логов.
    '''
    # Удаление файла
    for i in logs:
        st = i.split('\\')[1].split('_')
        st1 = st[2]+'-'+st[3]+'-'+st[4].split('.')[0]
        dt = datetime.strptime(st1, "%d-%m-%Y")
        now = datetime.now() - timedelta(days=day)
    
        if dt.date() < now.date():
            path = i
            os.remove(path)

# %%
