from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extract import loaddata
from src.funcs import log
# from src.transform import transform
from src.load import loadDB
from src.join import join, transform

def delivery(action,date1,date2):
    # print(action, date1, date2)
    
    log(f'----------------------------- BEGIN -------------------------------------------')
    log(f' ACTION: {"Especific date" if action == "d" else "Range date"}')
    log(f' DATE: {date1} to {date2}')
    log(f'')
    
    start_time = time.perf_counter()
    
    if (action=="d"):
        date_range = pd.date_range(start=date1, end=date1)
    else:
        date_range = pd.date_range(start=date1, end=date2)
        
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()
    print(lista_fechas)
    
    df_general = pd.DataFrame() #DataFrame Empty
    
    for date in lista_fechas:
        log(f' ------------------------ EXTRACTION PHASE')
        result = loaddata(date)

    log(f' ------------------------ JOIN PHASE')
    result = join(date1,date2)

        # log(f' ------------------------ TRANSFORM PHASE')
        # log(f' ------------ TRANSFORM PHASE / CLASIFICATION, CLEANING AND GROUPING.')
        # df = transform(date)
        
        # if (df is not None):
        #     log(f' ------------ TRANSFORM PHASE / MERGE FILE RESULTS')
            
        #     if df_general.empty:
        #         df_general = df.copy()
        #     else:
        #         df_general = pd.concat([df_general, df], ignore_index=True)
            
    # if (action == "r"):
        # df_general.to_csv(f'data/summary_{date1[:7]}_by_day.csv', index=False)   

        # log(f' ------------ TRANSFORM PHASE - SUMARY')
        
        # dt_resultado = df_general.groupby(['tenantid', 'camp_id', 'year', 'month' ]).agg(
        #     agents=('agentid', 'nunique'),
        #     totalcalls=('totalcalls', 'sum'),
        #     totalagentcalls=('totalagentcalls', 'sum'),
        #     totaldrops=('totaldrops', 'sum'),
        #     billsec=('billsec', 'sum'),
        #     units=('units', 'sum'),
        #     waiting=('waiting', 'sum'),
        #     talked=('talked', 'sum'),
        #     wrapped=('wrapped', 'sum'),
        #     sla=('sla', 'sum'),
        #     dispositioned=('dispositioned', 'sum')
        # ).reset_index()
        
        # dt_resultado.to_csv(f'data/summary_{date1[:7]}_by_camp.csv', index=False)   
        
        # dt_resultado = df_general.groupby(['tenantid', 'year', 'month' ]).agg(
        #     agents=('agentid', 'nunique'),
        #     totalcalls=('totalcalls', 'sum'),
        #     totalagentcalls=('totalagentcalls', 'sum'),
        #     totaldrops=('totaldrops', 'sum'),
        #     billsec=('billsec', 'sum'),
        #     units=('units', 'sum'),
        #     waiting=('waiting', 'sum'),
        #     talked=('talked', 'sum'),
        #     wrapped=('wrapped', 'sum'),
        #     sla=('sla', 'sum'),
        #     dispositioned=('dispositioned', 'sum')
        # ).reset_index()
        
        # dt_resultado.to_csv(f'data/summary_{date1[:7]}_by_month.csv', index=False)   
    
    log(f' ------------------------ TRANSFORM PHASE')
    start_time_t = time.perf_counter()
    transform(date1,date2)
    end_time_t = time.perf_counter()
    ttotal = end_time_t - start_time_t
    print(f" Elapsed Transformation {ttotal } second ")
    
    log(f' ------------------------ LOAD')
    loadDB(date1)
        
    end_time = time.perf_counter()
    elapsed_total = end_time - start_time
        
    log(f"Elapsed General = {elapsed_total}")
    log(f'----------------------------- END -------------------------------------------')
