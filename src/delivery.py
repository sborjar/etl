from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extract import loaddata
from src.funcs import log
from src.transform import transformDay, transform
from src.load import loadDB
from src.join import join|

def delivery(action,date1,date2):
    
    log(f'----------------------------- BEGIN -------------------------------------------')
    log(f' ACTION: {"Especific date" if action == "d" else "Range date"}')
    
    start_time = time.perf_counter()
    
    if (action=="d"):
        log(f' ACTION: By date')
        log(f' DATE: {date1}')
        date2 = date1
    else:
        log(f' ACTION: By date range')
        log(f' DATE: {date1} to {date2}')
    
    date_range = pd.date_range(start=date1, end=date2)
        
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()
    log(lista_fechas)
    
    df_general = pd.DataFrame() #DataFrame Empty
    
    log(f' ------------------------ EXTRACTION PHASE')
    start_time_ex = time.perf_counter()
    
    for date in lista_fechas:
        loaddata(date)
    
    end_time_ex = time.perf_counter()
    ex_total = end_time_ex - start_time_ex
    log(f" Elapsed Extraction {ex_total} second ")
    
    if (action=="d"):
        log(f' ------------------------ TRANSFORM PHASE')
        start_time_tra = time.perf_counter()
        transformDay(date1)
        end_time_tra = time.perf_counter()
        tra_total = end_time_tra - start_time_tra
        log(f" Elapsed Transformation {tra_total } second ")
        
        log(f' ------------------------ LOAD PHASE' )
        start_time_load = time.perf_counter()
        loadDBDay(date1)
        end_time_load = time.perf_counter()
        load_total = end_time_load - start_time_load
        log(f" Elapsed Load {load_total } second ")
    else:
        log(f' ------------------------ JOIN PHASE')
        start_time_join = time.perf_counter()
        join(date1,date2)
        end_time_join = time.perf_counter()
        join_total = end_time_join - start_time_join
        log(f" Elapsed Join {join_total} second ")
    
        log(f' ------------------------ TRANSFORM PHASE')
        start_time_tra = time.perf_counter()
        transform(date1,date2)
        end_time_tra = time.perf_counter()
        tra_total = end_time_tra - start_time_tra
        log(f" Elapsed Transformation {tra_total } second ")
        
        log(f' ------------------------ LOAD PHASE' )
        start_time_load = time.perf_counter()
        loadDB(date1)
        end_time_load = time.perf_counter()
        load_total = end_time_load - start_time_load
        log(f" Elapsed Load {load_total } second ")
        
    end_time = time.perf_counter()
    elapsed_total = end_time - start_time
    
    
    log(f'----------------------------- TOTALS -------------------------------------------')
    log(f" Date                                 {date1} {date2}")
    log(f" Elapsed Extraction                   {ex_total} seconds ")
    log(f" Elapsed Join                         {join_total} seconds ")
    log(f" Elapsed Transformation               {tra_total} seconds ")
    log(f" Elapsed Load                         {load_total } seconds ")
    log(f"                                      -------------------------------- ")
    log(f" Elapsed General                      {elapsed_total} seconds")
    log(f'----------------------------- END ----------------------------------------------')
