import numpy as np
import pandas as pd
import os
import sys
import time
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.funcs import log

load_dotenv()
mode = os.getenv("MODE", "SNBX")
user = os.getenv(f"DB_USER_{mode}", "SNBX")
password = os.getenv(f"DB_PASS_{mode}", "SNBX")
host = os.getenv(f"DB_HOST_{mode}", "SNBX")
database = os.getenv("DB_NAME", "SNBX")

    
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_data(name): 
    file = f"{name}.csv"
    BASE_DIR = os.getcwd() 
    DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1')
        return df
    except FileNotFoundError:
        # log(f" File {file} not found")
        return None
    
def loadDB(d1):
    """ 
    Function that retrieves information from the summary files and uploads it to the database
    """
    start_time = time.perf_counter()
    
    file_day = f'summary_{d1[:7]}_by_day'
    df_day = load_data(file_day)
    
    file_camp = f'summary_{d1[:7]}_by_camp'
    df_camp = load_data(file_camp)
    
    # file_month = f'summary_{d1[:7]}_by_month'
    # df_month = load_data(file_month)
    
    end_time = time.perf_counter()
    elapsed_time_load = end_time - start_time
    log(f" Elapsed files load = {elapsed_time_load} seconds")

    
    log(" CREATE ENGINE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

    
    if df_day is None or df_day.empty:
        log(f" File {file_day} is empty or not found")
    else:
        log(f" SAVING {file_day}")
        try:
            df_day.to_sql(name='billing_detail', con=engine, if_exists='append', index=False)
        except Exception as err:
            log(f"Error: {err}")
    
    
    if df_camp is None or df_camp.empty:
        log(f" File {file_camp} is empty or not found")
    else:
        log(f" SAVING {file_camp}")
        try:
            df_camp.to_sql(name='billing_summary_campaign', con=engine, if_exists='append', index=False)
        except Exception as err:
            log(f"Error: {err}")
    
    # if df_month is None or df_month.empty:
    #     log(f" File {file_month} is empty or not found")
    # else:
    #     log(f" SAVING {file_month}")
    #     try:
    #         df_month.to_sql(name='billing_summary_month', con=engine, if_exists='append', index=False)
    #     except Exception as err:
    #         log(f"Error: {err}")
    
    
def loadDBDay(d1):
    """ 
    Function that retrieves information from the summary files and uploads it to the database
    """
    start_time = time.perf_counter()
    
    file_day = f'summary_{d1}_by_day'
    df_day = load_data(file_day)
   
    end_time = time.perf_counter()
    elapsed_time_load = end_time - start_time
    log(f" Elapsed files load = {elapsed_time_load} seconds")
    
    log(" CREATE ENGINE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    
    if df_day is None or df_day.empty:
        log(f" File {file_day} is empty or not found")
    else:
        log(f" SAVING {file_day}")
        try:
            df_day.to_sql(name='billing_detail', con=engine, if_exists='append',index=False)
        except Exception as err:
            log(f"Error: {err}")
    
