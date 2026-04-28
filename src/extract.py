from datetime import datetime
import os
import time
import csv
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
# from src.db.connection import db
from src.funcs import log

load_dotenv()
mode = os.getenv("MODE", "SNBX")
user = os.getenv(f"DB_USER_{mode}", "SNBX")
password = os.getenv(f"DB_PASS_{mode}", "SNBX")
host = os.getenv(f"DB_HOST_{mode}", "SNBX")
database = os.getenv("DB_NAME", "SNBX")


def loaddata(date_obj):
    """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
    log(f'--------------------------------------')
    log(f' DATE: {date_obj}')
    
    log(" CREATE ENGINE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    
    """ QUERY """
    query = "SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, waiting, talked, wrapped, sla, dispositioned FROM calls WHERE DATE(calldate) = %s"
    param = (date_obj, )
    
    start_time = time.perf_counter()
    df = pd.read_sql(query, engine)
    end_time = time.perf_counter()
    elapsed_time_query = end_time - start_time
    log(f' Elapsed query = {elapsed_time_query} seconds')
    
    start_time = time.perf_counter()
    df_general.to_csv(f"data/calls_{date_obj}.csv", index=False)
    end_time = time.perf_counter()
    elapsed_time_save = end_time - start_time
    log(f' Elapsed save file = {elapsed_time_save} seconds')
    


# from src.db.connection import db
# from src.funcs import log

# def loaddata(date_obj):
#     """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
#     cursor = db.cursor()

#     log(f'--------------------------------------')
#     log(f' DATE: {date_obj}')

#     """ QUERY """
#     query = "SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, waiting, talked, wrapped, sla, dispositioned FROM calls WHERE DATE(calldate) = %s"
#     param = (date_obj, )
    
#     start_time = time.perf_counter()

#     cursor.execute(query, param)
#     rows = cursor.fetchall()

#     if len(rows)>0:
#         end_time = time.perf_counter()
    
#         elapsed_time1 = end_time - start_time
    
#         log(f' Elapsed query = {elapsed_time1} seconds')
    
#         start_time = time.perf_counter()
    
#         file = f"data/calls_{date_obj}.csv"
#         with open(file, 'w', newline='', encoding='utf-8') as f:
#            writer = csv.writer(f)
                          
#            # Write header (column names)
#            column_headers = [i[0] for i in cursor.description]
#            writer.writerow(column_headers)
                                                                                   
#            # Write data rows
#            writer.writerows(rows)
    
#         end_time = time.perf_counter()
#         elapsed_time2 = end_time - start_time
    
#         log(f" Row exported = {len(rows)} rows")
#         log(f" Elapsed exported = {elapsed_time2} seconds")
    
#     else:
#         log(f" Query result is empty")
        
    




