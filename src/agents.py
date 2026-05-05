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
from src.config import Config

def getAgents():
    """ It retrieves the raw data from the users"""
    
    cursor = db.cursor()

    log(f' AGENTS',"",1)

    """ QUERY """
    query = """SELECT 
            a.agentid,
            u.usertype,
            t.activate_tenant
        FROM
            agents a
                INNER JOIN users u ON u.userid = a.userid 
                INNER JOIN tenant t ON t.tenantid = a.tenantid
        AND 
            u.usertype = 27
            AND t.activate_tenant = 'y'
    """
    start_time = time.perf_counter()
    cursor.execute(query)
    rows = cursor.fetchall()
    end_time = time.perf_counter()
    elapsed_time1 = end_time - start_time
    log(f' Elapsed query = {elapsed_time1} seconds')
    log(f" Row exported = {len(rows)} rows")
    logT("Agents",len(rows),elapsed_time1)
    
    if len(rows)>0:
        start_time = time.perf_counter()
        file_path = Config.DATA_DIR / "agents.csv"
        with open(file_path, 'w', newline='', encoding='latin1') as f:
            writer = csv.writer(f)
            column_headers = [i[0] for i in cursor.description]
            writer.writerow(column_headers)
            if rows:
                writer.writerows(rows)
            
            df = pd.DataFrame(rows, columns=column_headers)
                
        end_time = time.perf_counter()
        elapsed_time2 = end_time - start_time
        
        return df
    else:
        log(f" There are no records for {date_obj}", "error")
        return None

