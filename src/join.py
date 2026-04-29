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
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1', low_memory=False)
        return df
    except FileNotFoundError:
        return None



def join(d1, d2):
    date_range = pd.date_range(start=d1, end=d2)
    lista_fechas = date_range.strftime('%Y-%m-%d').tolist()

    df_general = pd.DataFrame()
    
    if df_general is not None:
        for date in lista_fechas:
            log(f" Processing calls_{date}.csv ...")
            df = load_data(date, "calls")
            
            df_general = pd.concat([df_general, df ], ignore_index=True)
    
    log(" Saving file ...")
    df_general.to_csv(f'data/general_{d1}_{d2}.csv', index=False)
    
