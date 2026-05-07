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
from src.config import Config

method = "python"

def loadDB(df, deep):
    """ 
    Function that retrieves information from the summary files and uploads it to the database
    """
    log(f' LOAD', "", 1)
    start_time = time.perf_counter()
    
    if df is None or df.empty:
        log(f" Dataframe is empty", "error")
        exit(1)
        
    # engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    engine = create_engine(Config.DB_engine)
    
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
                    agenthandled = row["agenthandled"]
                    noanswers = row["noanswers"]
                    busy = row["busy"]
                    oi = row["oi"]
                    drops = row["drops"]
                    amd = row["amd"]
                    others = row["others"]
                    contacts = row["contacts"]
                    success = row["success"]
                    waiting = row["waiting"]
                    talked = row["talked"]
                    wrapped = row["wrapped"]
                    sla = row["sla"]
                    billsec = row["billsec"]
                    units = row["units"]
                    
                    # tenantid, camp_id, year, month, agents, agenthandled, noanswers, busy, oi, drops, amd, others, contacts, success, waiting, talked, wrapped, sla, billsec, units

                    cursor = db.cursor()
                    query = "SELECT * FROM billing_summary_campaign WHERE tenantid=%s AND camp_id=%s AND year=%s AND month=%s"
                    param = (tenantid, camp_id, year, month, )
                    cursor.execute(query, param)
                    rows = cursor.fetchall()
                    # log(f"Records: {len(rows)}")
                    
                    if len(rows) > 0:
                        # log(f"--> UPDATE")
                        ## UPDATE
                        query = """UPDATE billing_summary_campaign 
                            SET 
                                agents = %s, 
                                agenthandled = %s, 
                                noanswers = %s, 
                                busy = %s, 
                                oi = %s, 
                                drops = %s, 
                                amd = %s, 
                                others = %s, 
                                contacts = %s, 
                                success = %s, 
                                waiting = %s, 
                                talked = %s, 
                                wrapped = %s, 
                                sla = %s, 
                                billsec = %s, 
                                units = %s
                            WHERE 
                                tenantid=%s AND camp_id=%s AND year=%s AND month=%s"""
                        param = (agents, agenthandled, noanswers, busy, oi, 
                            drops, amd, others, contacts, success, waiting,
                            talked, wrapped, sla,billsec, units, tenantid, camp_id, year, month
                        ,)
                        cursor = db.cursor()
                        cursor.execute(query, param)
                    else:
                        there_insert = True
                        # log(f"--> INSERT")
                        ## INSERT
                        collection.append((tenantid, camp_id, year, month, agents, agenthandled, noanswers, busy, oi, drops, amd, others, contacts, success, waiting, talked, wrapped, sla, billsec, units))
                        query = """INSERT INTO billing_summary_campaign (
                            tenantid, camp_id, year, month, agents, agenthandled, noanswers, busy, oi, drops, amd, 
                            others, contacts, success, waiting, talked, wrapped, sla, billsec, units
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                if there_insert:
                    cursor.executemany(query, collection)
                
                db.commit()

            elif method == "panda":
                df.to_sql(name='billing_summary_campaign', con=engine, if_exists='fail', index=False, method='multi')
        except Exception as err:
            log(f"Error: {err}")

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    if deep == 0:
        # logT('LOAD',df.shape[0],elapsed_time)
        log(f" Elapsed LOAD {elapsed_time} second ")
    elif deep == 1:
        log(f" Elapsed SUMMARY LOAD {elapsed_time} second ")
    
    log(f' END LOAD', "", 1)