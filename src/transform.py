import numpy as np
import pandas as pd
import os
import time
import logging

from src.extract import log

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_data(file):
    log(f"Importing file {file}")
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1')
        return df
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {PROCESSED_FILE}")
        return None
    
def transform(df):
    """ 
    Function that transforms the extracted date for processing into billing information.
    """
    
    # start time to elapsed 
    start_time = time.perf_counter()
    
    # Load data in memory
    # file = "data/temp.csv"
    # df = load_data(file)
    
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
    
    dt_resultado = df.groupby(['tenantid', 'year', 'month', 'day']).agg(
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
    
    dt_resultado['billsec'] *= 1.3

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    
   
    print(dt_resultado.head(5))
    
    log(f"Transform for {file} elapsed {elapsed_time} second ")
    
    dt_resultado.to_csv(f'data/summary_{file}', index=False)

# if __name__ == "__main__":
#     print("casa")    
#     transform("calls_2026-04-23.csv")
    
    
    