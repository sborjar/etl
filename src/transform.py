import numpy as np
import pandas as pd
import os
import time

from src.funcs import log

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_data(date, prefix): 
    file = f"{prefix}_{date}.csv"
    log(f" Importing file {file}")
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1')
        return df
    except FileNotFoundError:
        log(f"Error: No se encontró el archivo {PROCESSED_FILE}")
        return None
    
def delete_file(date, prefix): 
    file = f"{prefix}_{date}.csv"
    log(f" Deleting file {file}")
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        os.remove(PROCESSED_FILE)
    except FileNotFoundError:
        log(f"Error: No se encontró el archivo {PROCESSED_FILE}")
        return None
    
def transform(date1,date2):
    """ 
    Function that transforms the extracted date for processing into billing information.
    """
    start_time = time.perf_counter()
    
    date_range = pd.date_range(start=date1, end=date2)
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()
    
    df = pd.DataFrame()
    for date in lista_fechas:
        log(f" Loading calls_{date}.csv ...")
        df_file = load_data(date, "calls")
        df = pd.concat([df, df_file ], ignore_index=True)
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
    
    # Add columns year, month
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
    
    end_time = time.perf_counter()
    elapsed_time_transform = end_time - start_time
    
    log(f" Elapsed operations {elapsed_time_transform} second ")
    
    log("Saving files ...")
    fname = date1[:7]
    log(f"fname {fname}")
    df_day.to_csv(f'data/summary_{fname}_by_day.csv', index=False)
    df_summary_cmp.to_csv(f'data/summary_{fname}_by_camp.csv', index=False)
    # df_summary_month.to_csv(f'data/summary_{fname}_by_month.csv', index=False)  
    
    elapsed_total = elapsed_time_load + elapsed_time_transform
    log(f" Elapsed total transform = {elapsed_time_transform} second ")
    

    
# def transform(d1,d2):
#     """ 
#     Function that transforms the extracted date for processing into billing information.
#     """
#     start_time = time.perf_counter()
#     df = load_data(date, "calls")
#     end_time = time.perf_counter()
#     elapsed_time_load = end_time - start_time
#     log(f" Elapsed file load = {elapsed_time_load} seconds")
    
#     if df is None or df.empty:
#         log(f" The file is empty or does not exist")
#         return None
    
#     # start time to elapsed 
#     start_time = time.perf_counter()

#     # Convert calldate to datetime format
#     df["calldate"] = pd.to_datetime(df["calldate"])

#     # -----------------
#     # Preparation
#     # ----------------
    
#     # Add columns year, month
#     df["year"] =  df['calldate'].dt.year
#     df["month"] =  df['calldate'].dt.month
#     df["day"] =  df['calldate'].dt.day
    
#     # Make sure there are no null values
#     df['billsec'] = pd.to_numeric(df['billsec'], errors='coerce').fillna(0)
    
#     # Cost calculation
#     df['units_calc'] = np.where(
#         df['billsec'] == 0,
#         0,
#         np.maximum(3, np.ceil(df['billsec'] / 6 * 1.3))
#     )
#     # Condition for callresult 1 and 5 
#     df['is_agent_call'] = (df['callresult'] == 1).astype(int)
#     df['is_drop']       = (df['callresult'] == 5).astype(int)

#     # -----------------
#     # Preparation
#     # ----------------
    
#     dt_resultado = df.groupby(['tenantid', 'camp_id', 'year', 'month', 'day' ]).agg(
#         agents=('agentid', 'nunique'),
#         totalcalls=('callid', 'count'),
#         totalagentcalls=('is_agent_call', 'sum'),
#         totaldrops=('is_drop', 'sum'),
#         billsec=('billsec', 'sum'),
#         units=('units_calc', 'sum'),
#         waiting=('waiting', 'sum'),
#         talked=('talked', 'sum'),
#         wrapped=('wrapped', 'sum'),
#         sla=('sla', 'sum'),
#         dispositioned=('dispositioned', 'sum')
#     ).reset_index()
    
#     dt_resultado['billsec'] *= 1.3
    
#     end_time = time.perf_counter()
#     elapsed_time_transform = end_time - start_time
    
#     log(f" Elapsed operations {elapsed_time_transform} second ")
    
#     elapsed_total = elapsed_time_load + elapsed_time_transform
#     log(f" Elapsed total transform = {elapsed_time_transform} second ")
    
#     return dt_resultado