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

def loadDB(df, deep):
    """ 
    Function that retrieves information from the summary files and uploads it to the database
    """
    log(f' >>> LOAD ')
    start_time = time.perf_counter()
    
    
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    
    if df is None or df.empty:
        log(f" Dataframe is empty")
    else:
        if deep == 0:
            log(f" Saving transformed dataframe to billing_detail")
            try:
                df.to_sql(name='billing_detail', con=engine, if_exists='append', index=False, method='multi')
            except Exception as err:
                log(f"Error: {err}")
        elif deep == 1:
            log(f" Saving transformed dataframe to billing_summary_campaign")
            try:
                df.to_sql(name='billing_summary_campaign', con=engine, if_exists='append', index=False, method='multi')
            except Exception as err:
                log(f"Error: {err}")
    
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    if deep == 0:
        log(f" Elapsed LOAD {elapsed_time} second ")
    elif deep == 1:
        log(f" Elapsed SUMMARY LOAD {elapsed_time} second ")