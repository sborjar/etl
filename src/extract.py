from datetime import datetime
import os
import time
import csv
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
# from src.db.connection import db
from src.funcs import log
from src.transform import transform

load_dotenv()
mode = os.getenv("MODE", "SNBX")
user = os.getenv(f"DB_USER_{mode}", "SNBX")
password = os.getenv(f"DB_PASS_{mode}", "SNBX")
host = os.getenv(f"DB_HOST_{mode}", "SNBX")
database = os.getenv("DB_NAME", "SNBX")
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

def loaddata(date_obj):
    """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
    log(f' >>> EXTRACTION')
    log(f' Date: {date_obj}')
    start_time_total = time.perf_counter()
    
    
    """ QUERY """
    query =  text("SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, waiting, talked, wrapped, sla, dispositioned FROM calls WHERE DATE(calldate) = :date")
    params = {"date": date_obj}
    log(f' Query the database')
    
    start_time = time.perf_counter()
    df = pd.read_sql(
        query, 
        engine, 
        params=params
    )
    end_time = time.perf_counter()
    elapsed_time_query = end_time - start_time
    rows = df.shape[0]
    log(f" Rows retrieved = {rows} rows")
    log(f' Elapsed query = {elapsed_time_query} seconds')

    start_time = time.perf_counter()
    month = date_obj[:7]
    file_path = f"data/general_{month}.csv"  
    df.to_csv(
        file_path,  
        mode='a', 
        index=False,
        header=not os.path.exists(file_path)
    )
    end_time = time.perf_counter()
    elapsed_time_save = end_time - start_time
    log(f' Elapsed save {file_path} = {elapsed_time_save} seconds')

    end_time_total = time.perf_counter()
    elapsed_time_total = end_time_total - start_time_total
    log(f' Elapsed EXTRACTION = {elapsed_time_total} seconds')
        
    if rows == 0:
        log(f" There are no records for {date_obj}", "error")
    else:
        transform(df)
        
