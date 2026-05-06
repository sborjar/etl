import numpy as np
import pandas as pd
import os
import sys
import time
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.funcs import log, logT
from src.db.connection import db

load_dotenv()
mode = os.getenv("MODE", "SNBX")
user = os.getenv(f"DB_USER_{mode}", "SNBX")
password = os.getenv(f"DB_PASS_{mode}", "SNBX")
host = os.getenv(f"DB_HOST_{mode}", "SNBX")
database = os.getenv("DB_NAME", "SNBX")
method = "python"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def loadDB(df, deep):
    """ 
    Function that retrieves information from the summary files and uploads it to the database
    """
    log(f' LOAD', "", 1)
    start_time = time.perf_counter()
    
    if df is None or df.empty:
        log(f" Dataframe is empty", "error")
        exit(1)
        
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    
    if deep == 0:
        log(f" Saving transformed dataframe to billing_detail")
        try:
            
            df.to_sql(name='billing_detail', con=engine, if_exists='append', index=False, method='multi')
        except Exception as err:
            log(f"Error: {err}")
    elif deep == 1:
        log(f" Saving transformed dataframe to billing_summary_campaign, METHOD {method}")
        try:
            if method == "python":
                
                collection = []
                there_insert = False
                
                for index, row in df.iterrows():
                    tenantid = row["tenantid"]
                    camp_id = row["camp_id"]
                    year = row["year"]
                    month = row["month"]
                    agents = row["agents"]
                    totalcalls = row["totalcalls"]
                    totalagentcalls = row["totalagentcalls"]
                    totaldrops = row["totaldrops"]
                    billsec = row["billsec"]
                    units = row["units"]
                    waiting = row["waiting"]
                    talked = row["talked"]
                    wrapped = row["wrapped"]
                    sla = row["sla"]
                    dispositioned = row["dispositioned"]
                    
                    cursor = db.cursor()
                    query = "SELECT * FROM billing_summary_campaign WHERE tenantid=%s AND camp_id=%s AND year=%s AND month=%s"
                    param = (tenantid, camp_id, year, month, )
                    cursor.execute(query, param)
                    rows = cursor.fetchall()
                    # log(f"Records: {len(rows)}")
                    
                    if len(rows) > 0:
                        # log(f"--> UPDATE")
                        ## UPDATE
                        query = "UPDATE billing_summary_campaign SET agents=%s, totalcalls=%s, totalagentcalls=%s, totaldrops=%s, billsec=%s, units=%s, waiting=%s, talked=%s, wrapped=%s, sla=%s, dispositioned=%s WHERE tenantid=%s AND camp_id=%s AND year=%s AND month=%s"
                        param = (tenantid, camp_id, year, month, agents, totalcalls, totalagentcalls, totaldrops, billsec, units, waiting, talked, wrapped, sla, dispositioned, )
                        cursor = db.cursor()
                        cursor.execute(query, param)
                    else:
                        there_insert = True
                        # log(f"--> INSERT")
                        ## INSERT
                        collection.append((tenantid, camp_id, year, month, agents, totalcalls, totalagentcalls, totaldrops, billsec, units, waiting, talked, wrapped, sla, dispositioned))
                        query = "INSERT INTO billing_summary_campaign (tenantid, camp_id, year, month, agents, totalcalls, totalagentcalls, totaldrops, billsec, units, waiting, talked, wrapped, sla, dispositioned) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        # param = (tenantid, camp_id, year, month, agents, totalcalls, totalagentcalls, totaldrops, billsec, units, waiting, talked, wrapped, sla, dispositioned, )
                
                if there_insert:
                    cursor.executemany(query, collection)
                
                db.commit()

            elif method == "panda":
                df.to_sql(name='billing_summary_campaign', con=engine, if_exists='fail', index=False, method='multi')
        except Exception as err:
            log(f"Error: {err}")

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logT('LOAD',df.shape[0],elapsed_time)
    if deep == 0:
        log(f" Elapsed LOAD {elapsed_time} second ")
    elif deep == 1:
        log(f" Elapsed SUMMARY LOAD {elapsed_time} second ")
    
    log(f' END LOAD', "", 1)