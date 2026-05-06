from datetime import datetime
import os
import time
import csv
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.db.connection import db
from src.funcs import log, logT
# from src.config import Config

def getDisposition():
    """ It retrieves the raw data from the users"""
    
    cursor = db.cursor()

    log(f' DISPOSITION',"",1)

    """ QUERY """
    query = "SELECT dispid as agentdisp, success, contact FROM disposition order by dispid"
    start_time = time.perf_counter()
    cursor.execute(query)
    rows = cursor.fetchall()
    end_time = time.perf_counter()
    elapsed_time1 = end_time - start_time
    log(f' Elapsed query = {elapsed_time1} seconds')
    log(f" Row exported = {len(rows)} rows")
    logT("Disposition",len(rows),elapsed_time1)
    
    if len(rows)>0:
        start_time = time.perf_counter()
        # file_path = Config.DATA_DIR / "agents.csv"
        column_headers = [i[0] for i in cursor.description]
            
        df = pd.DataFrame(rows, columns=column_headers)
        # df.to_csv(file_path, mode='w',  lineterminator='\n', encoding='latin1', index=False)
                
        end_time = time.perf_counter()
        elapsed_time2 = end_time - start_time
        
        return df
    else:
        log(f" There are no records for {date_obj}", "error")
        return None

