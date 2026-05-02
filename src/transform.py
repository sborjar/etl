import numpy as np
import pandas as pd
import os
import time

from src.funcs import log
from src.load import loadDB

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def transform(df, deep=0):
    """ 
    Function that transforms the extracted date for processing into billing information.
    """
    
    log(f' >>> TRANSFORM ')
    
    if df is None or df.empty:
        log(f" The Data frame comes empty")
        return None
    
    # start time to elapsed 
    start_time = time.perf_counter()

    # Convert calldate to datetime format
    log(f" Convert calldate to datetime format")
    df["calldate"] = pd.to_datetime(df["calldate"])

    # -----------------
    # Preparation
    # ----------------
    log(f" Add columns year, month, day")
    # Add columns year, month, day
    df["year"] =  df['calldate'].dt.year
    df["month"] =  df['calldate'].dt.month
    df["day"] =  df['calldate'].dt.day
    
    log(f" Make sure there are no null values")
    # Make sure there are no null values
    df['billsec'] = pd.to_numeric(df['billsec'], errors='coerce').fillna(0)
    
    # log(f" Cost calculation")
    # Cost calculation
    df['units_calc'] = np.where(
        df['billsec'] == 0,
        0,
        np.maximum(3, np.ceil(df['billsec'] / 6 * 1.3))
    )
    log(f" Condition for callresult 1 and 5 ")
    # Condition for callresult 1 and 5 
    df['is_agent_call'] = (df['callresult'] == 1).astype(int)
    df['is_drop']       = (df['callresult'] == 5).astype(int)

    log(f" Grouping")
    
    if (deep == 0):
        log(f" Grouping by Day")
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
    elif (deep == 1):
        log(f" Grouping by Month")
        df_day = df.groupby(['tenantid', 'camp_id', 'year', 'month' ]).agg(
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

    end_time = time.perf_counter()
    elapsed_time_transform = end_time - start_time
    if deep == 0:
        log(f" Elapsed TRANSFORM {elapsed_time_transform} second ")
    elif deep == 1:
        log(f" Elapsed SUMMARY TRANSFORM {elapsed_time_transform} second ")
    
    loadDB(df_day, deep)
    
    