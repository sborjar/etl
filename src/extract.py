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
from src.transform import transform
from src.config import Config

def downloadData(date_obj, df_agents):
    """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
    cursor = db.cursor()

    log(f' BEGIN EXTRACT', "", 1)
    log(f' DATE: {date_obj}', "", 2)

    """ QUERY """
    query = "SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, waiting, talked, wrapped, sla, dispositioned FROM calls WHERE DATE(calldate) = %s"
    param = (date_obj, )
    
    start_time = time.perf_counter()

    cursor.execute(query, param)
    rows = cursor.fetchall()
    end_time = time.perf_counter()
    elapsed_time1 = end_time - start_time
    log(f' Elapsed query = {elapsed_time1} seconds', "", 2)
        
    logT(f"Query date {date_obj}",len(rows),elapsed_time1)
    
    if len(rows)>0:
        column_headers = [i[0] for i in cursor.description]
        df_day = pd.DataFrame(rows, columns=column_headers)
        df = df_day.merge(df_agents, on="agentid")
        
        month = date_obj[:7]
        file_path = Config.DATA_DIR / f"general_{month}.csv"
        
        df.to_csv(file_path, mode='a', index=False)
        
        # start_time = time.perf_counter()
        
        # month = date_obj[:7]
        # file_path = f"data/general_{month}.csv"  
        # file_exists = os.path.isfile(file_path)
        # log(f" file_exists {file_exists}", "", 2)
        
        # with open(file_path, 'a', newline='', encoding='latin1') as f:
        #     writer = csv.writer(f)
            
        #     """ Get column names """
        #     column_headers = [i[0] for i in cursor.description]
            
        #     if not file_exists:
        #         writer.writerow(column_headers)
            
        #     if rows:
        #         writer.writerows(rows)
                
        #     df_day = pd.DataFrame(rows, columns=column_headers)
        #     df = df_day.merge(df_agents, on="agentid")
    
        # end_time = time.perf_counter()
        # elapsed_time2 = end_time - start_time
    
        # log(f" Row exported = {len(rows)} rows", "", 2)
        # log(f" Elapsed exported = {elapsed_time2} seconds", "", 2)
        # log(f" END EXTRACT", "", 1)
    
        # transform(df)
    else:
        log(f" There are no records for {date_obj}", "error", 2)




# def downloadData(date_obj):
#     """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
#     log(f' >>> EXTRACTION')
#     log(f' Date: {date_obj}')
#     start_time_total = time.perf_counter()
    
    
#     """ QUERY """
#     query =  text("SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, waiting, talked, wrapped, sla, dispositioned FROM calls WHERE DATE(calldate) = :date")
#     params = {"date": date_obj}
#     log(f' Query the database')
    
#     start_time = time.perf_counter()
#     df = pd.read_sql(
#         query, 
#         engine, 
#         params=params
#     )
#     end_time = time.perf_counter()
#     elapsed_time_query = end_time - start_time
#     rows = df.shape[0]
#     log(f" Rows retrieved = {rows} rows")
#     log(f' Elapsed query = {elapsed_time_query} seconds')

#     start_time = time.perf_counter()
#     month = date_obj[:7]
#     file_path = f"data/general_{month}.csv"  
#     df.to_csv(
#         file_path,  
#         mode='a', 
#         index=False,
#         header=not os.path.exists(file_path)
#     )
#     end_time = time.perf_counter()
#     elapsed_time_save = end_time - start_time
#     log(f' Elapsed save {file_path} = {elapsed_time_save} seconds')

#     end_time_total = time.perf_counter()
#     elapsed_time_total = end_time_total - start_time_total
#     log(f' Elapsed EXTRACTION = {elapsed_time_total} seconds')
        
#     if rows == 0:
#         log(f" There are no records for {date_obj}", "error")
#     else:
#         transform(df)
        
