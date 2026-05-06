import pandas as pd
import os
import time

from src.funcs import log, logT
from src.transform import transform

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_data(file): 
    log(f" Importing file {file}")
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1', low_memory=False)
        return df
    except FileNotFoundError:
        log(f"Error: No se encontró el archivo {PROCESSED_FILE}")
        return None
    

def collect(date1, date2):
    """ 
    Function that transforms the extracted date for processing into billing information.
    """
    date_range = pd.date_range(start=date1, end=date2)
    date_list = date_range.strftime('%Y-%m').tolist()
    # unique_dates = list(set(date_list))
    unique_dates = list(dict.fromkeys(date_list))
    
    log(f" List of date ranges to summary calculation {unique_dates}")
    
    for date in unique_dates:
        log(f' SUMMARY ',"",1)
        log(f' Date: {date}')
        
        """ Load general file """
        start_time = time.perf_counter()
        file = f"general_{date}.csv"
        df = load_data(file)
        end_time = time.perf_counter()
        elapsed_time_load = end_time - start_time
        log(f" Elapsed file {file} = {elapsed_time_load} seconds")
        logT(f'File {file}',df.shape[0],elapsed_time_load)

        if df is None or df.empty:
            log(f" The file is empty or does not exist")
            return None
        
        transform(df, 1)