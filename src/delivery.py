from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extract import downloadData
from src.funcs import log, logT
from src.agents import getAgents
from src.summary import collect


def delivery(action,date1,date2):
    """ Execute ETL process
    
    Args:
        action: Action type ('d', 'r', 's')
        date1: Start date (yyyy-mm-dd)
        date2: End date (yyyy-mm-dd)
    
    Expect:
        Create summary tables billing_Detail and billing_summary_campaign
    """
    
    log(f' BEGIN', "", 0)
    log(f' Action: {action}', "", 1)
    log(f' Date1: {date1}', "", 1)
    log(f' Date2: {date2}', "", 1)
    
    start_time_total = time.perf_counter()
    
    ex_total = 0
    join_total = 0
    tra_total = 0
    load_total = 0
    elapsed_total = 0
    
    """ Collect the information of the agents, usertype and tenant active """     
    
    if action in ('d', 'r'):
        df_agents = getAgents()
        date_range = pd.date_range(start=date1, end=date2)
        date_list = date_range.strftime('%Y-%m-%d').tolist()
        log(f" List of date ranges {date_list}")
        for date in date_list:
            downloadData(date, df_agents)
    elif action=="s":
        if date2 == "":
            date2 = date1
        
    """ Summary """
    # collect(date1,date2)
        
    end_time_total = time.perf_counter()
    elapsed_total = end_time_total - start_time_total
    
    log(f' >>> TOTALS')
    log(f" Date                                 {date1} {date2}")
    log(f" Elapsed General                      {elapsed_total} seconds")
    log(f' END',"", 0)
    logT(f' Elapsed General',"",elapsed_total)
    logT(f' END',date1,date2)
