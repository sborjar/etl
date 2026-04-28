import numpy as np
import pandas as pd
import os
import time
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.funcs import log

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_data(date, prefix): 
    file = f"{prefix}_{date}.csv"
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1')
        return df
    except FileNotFoundError:
        return None


def transform(d1,d2):
    """ 
    Function that transforms the extracted date for processing into billing information.
    """
    start_time = time.perf_counter()
    df = load_data(f"{d1}_{d2}", "general")
    end_time = time.perf_counter()
    elapsed_time_load = end_time - start_time
    log(f" Elapsed file load = {elapsed_time_load} seconds")
    
    if df is None or df.empty:
        log(f" The file is empty or does not exist")
        return None
    
    # start time to elapsed 
    start_time = time.perf_counter()

    # Convert calldate to datetime format
    df["calldate"] = pd.to_datetime(df["calldate"])

    # -----------------
    # Preparation
    # ----------------
    
    # Add columns year, month, day
    df["year"] =  df['calldate'].dt.year
    df["month"] =  df['calldate'].dt.month
    df["day"] =  df['calldate'].dt.day
    
    # Make sure there are no null values
    df['billsec'] = pd.to_numeric(df['billsec'], errors='coerce').fillna(0)
    
    # Cost calculation
    df['units_calc'] = np.where(
        df['billsec'] == 0,
        0,
        np.maximum(3, np.ceil(df['billsec'] / 6 * 1.3))
    )
    # Condition for callresult 1 and 5 
    df['is_agent_call'] = (df['callresult'] == 1).astype(int)
    df['is_drop']       = (df['callresult'] == 5).astype(int)

    # -----------------
    # Preparation
    # ----------------
    
    df_day = df.groupby(['tenantid', 'camp_id', 'year', 'month', 'day' ]).agg(
        agents=('agentid', 'nunique'),
        totalcalls=('callid', 'count'),
        totalagentcalls=('is_agent_call', 'sum'),
        totaldrops=('is_drop', 'sum'),
        billsec=('billsec', 'sum'),
        units=('units_calc', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        dispositioned=('dispositioned', 'sum')
    ).reset_index()
    
    df_day['billsec'] *= 1.3

    df_summary_cmp = df.groupby(['tenantid', 'camp_id', 'year', 'month' ]).agg(
        agents=('agentid', 'nunique'),
        totalcalls=('callid', 'count'),
        totalagentcalls=('is_agent_call', 'sum'),
        totaldrops=('is_drop', 'sum'),
        billsec=('billsec', 'sum'),
        units=('units_calc', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        dispositioned=('dispositioned', 'sum')
    ).reset_index()
    
    df_summary_cmp['billsec'] *= 1.3
    
    df_summary_month = df.groupby(['tenantid', 'year', 'month' ]).agg(
        agents=('agentid', 'nunique'),
        totalcalls=('callid', 'count'),
        totalagentcalls=('is_agent_call', 'sum'),
        totaldrops=('is_drop', 'sum'),
        billsec=('billsec', 'sum'),
        units=('units_calc', 'sum'),
        waiting=('waiting', 'sum'),
        talked=('talked', 'sum'),
        wrapped=('wrapped', 'sum'),
        sla=('sla', 'sum'),
        dispositioned=('dispositioned', 'sum')
    ).reset_index()
    
    df_summary_month['billsec'] *= 1.3

    end_time = time.perf_counter()
    elapsed_time_transform = end_time - start_time
    log(f" Elapsed operations {elapsed_time_transform} second ")
    
    log(f"Date={d1}")
    log("Saving files ...")
    fname = d1[:7]
    log(f"fname {fname}")
    df_day.to_csv(f'data/summary_{fname}_by_day.csv', index=False)
    df_summary_cmp.to_csv(f'data/summary_{fname}_by_camp.csv', index=False)
    df_summary_month.to_csv(f'data/summary_{fname}_by_month.csv', index=False)  
    
    elapsed_total = elapsed_time_load + elapsed_time_transform
    
    log(f" Elapsed total transform = {elapsed_total} second ")
    


def join(d1, d2):
    date_range = pd.date_range(start=d1, end=d2)
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()

    df_general = pd.DataFrame()
    
    if df_general is not None:
        for date in lista_fechas:
            log(f"Processing calls_{date}.csv ...")
            df = load_data(date, "calls")
            
            df_general = pd.concat([df_general, df ], ignore_index=True)
    
    log("Saving file ...")
    df_general.to_csv(f'data/general_{d1}_{d2}.csv', index=False)
    
