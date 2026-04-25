from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# from src.extract import loaddata
from src.funcs import log
from src.transform import transform

def delivery(action,date1,date2):
    # print(action, date1, date2)
    
    start_time = time.perf_counter()
    
    if (action=="d"):
        date_range = pd.date_range(start=date1, end=date1)
    else:
        date_range = pd.date_range(start=date1, end=date2)
        
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()
    print(lista_fechas)
    
    elapsed_total_extraction = 0
    elapsed_total_transform = 0
    elapser_merge = 0
    df_general = pd.DataFrame() #DataFrame Empty
    
    for date in lista_fechas:
        # log(f'------------------------------------------------------------------------')
        # log(f'EXTRACTION PHASE')
        # log(f'------------------------------------------------------------------------')
        # result, elapsed_total_extraction = loaddata(date)
        # time.sleep(2)

        log(f'------------------------------------------------------------------------')
        log(f'TRANSFORM PHASE')
        log(f'CLASIFICATION, CLEANING, GROUPING.')
        log(f'------------------------------------------------------------------------')
        df = transform(date)
        
        if (df is not None):
            log(f'------------------------------------------------------------------------')
            log(f'TRANSFORM PHASE')
            log(f'MERGE FILE RESULTS')
            log(f'------------------------------------------------------------------------')
            
            if df_general.empty:
                df_general = df.copy()
            else:
                df_general = pd.concat([df_general, df], ignore_index=True)
            
            df_general.to_csv(f'data/summary_{date[:7]}_by_day.csv', index=False)   
    
    log(f'------------------------------------------------------------------------')
    log(f'TRANSFORM PHASE')
    log(f'SUMARY')
    log(f'------------------------------------------------------------------------')
    
    dt_resultado = df_general.groupby(['tenantid', 'camp_id', 'year', 'month' ]).agg(
        totalcalls=('totalcalls', 'sum'),
        totalagentcalls=('totalagentcalls', 'sum'),
        totaldrops=('totaldrops', 'sum'),
        billsec=('billsec', 'sum'),
        units=('units', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        dispositioned=('dispositioned', 'sum')
    ).reset_index()
    
    dt_resultado.to_csv(f'data/summary_{date[:7]}_by_camp.csv', index=False)   
    
    dt_resultado = df_general.groupby(['tenantid', 'year', 'month' ]).agg(
        totalcalls=('totalcalls', 'sum'),
        totalagentcalls=('totalagentcalls', 'sum'),
        totaldrops=('totaldrops', 'sum'),
        billsec=('billsec', 'sum'),
        units=('units', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        dispositioned=('dispositioned', 'sum')
    ).reset_index()
    
    dt_resultado.to_csv(f'data/summary_{date[:7]}_by_month.csv', index=False)   
    
    
    end_time = time.perf_counter()
    elapsed_total = end_time - start_time
    
        
    log(f"Elapsed General = {elapsed_total}")
    print("END")
