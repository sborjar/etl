from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extract import loaddata, log

def delivery(action,date1,date2):
    print(action, date1, date2)
    
    if (action=="d"):
        date_range = pd.date_range(start=date1, end=date1)
    else:
        date_range = pd.date_range(start=date1, end=date2)
        
    print(date_range)
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()
    print(lista_fechas)
    
    for date in lista_fechas:
        log(f'------------------------------------------------------------------------')
        log(f'EXTRACTION PHASE')
        log(f'------------------------------------------------------------------------')
        result = loaddata(date)
        time.sleep(2)
        
        #thread = threading.Thread(target=extract, args=(date))
        #thread.start()
        
    print("END")
