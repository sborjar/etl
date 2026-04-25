from datetime import datetime
import os
import time
import csv
import logging
from dotenv import load_dotenv

from src.db.connection import db

load_dotenv()

mode = os.getenv("MODE", "SNBX")

logger = logging.getLogger("callevo")
logging.basicConfig(filename='logs/etl.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def log(msg):
    print(msg)
    logger.info(msg)

def loaddata(date1):
    """ It retrieves the raw data from the calls, tenant, dialer_campaigns, and users tables.  """
    
    cursor = db.cursor()

    log(f'------------------------------------------------------------------------')
    log(f'MODE: {mode}')
    log(f'DATE: {date1}')

    """ QUERY """
    query = "SELECT callid, tenantid, camp_id, calldate, callresult, agentdisp, agentid, calltype, callduration, billsec, hangupcause, waiting, talked, wrapped, pushresult, sla, dispositioned FROM calls WHERE DATE(calldate) = %s"
    param = (date1,)

    start_time = time.perf_counter()

    cursor.execute(query, param)
    rows = cursor.fetchall()

    end_time = time.perf_counter()

    elapsed_time1 = end_time - start_time

    log(f'Elapsed query = {elapsed_time1} seconds')

    start_time = time.perf_counter()

    file = f"data/calls_{date1}.csv"
    with open(file, 'w', newline='', encoding='utf-8') as f:
       writer = csv.writer(f)
                      
       # Write header (column names)
       column_headers = [i[0] for i in cursor.description]
       writer.writerow(column_headers)
                                                                               
       # Write data rows
       writer.writerows(rows)

    end_time = time.perf_counter()
    elapsed_time2 = end_time - start_time

    log(f"Row exported = {len(rows)} rows")
    log(f"Elapsed exported = {elapsed_time2} seconds")
    
    cursor.close()
    db.close()
    




