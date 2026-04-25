from concurrent.futures import thread
import threading
import pandas as pd

from src.extract import extract

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
        thread = threading.Thread(target=extract, args=(date))
        thread.start()
        
    print("se fini")