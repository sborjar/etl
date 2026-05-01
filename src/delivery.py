from concurrent.futures import thread
import threading
import pandas as pd
import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.extract import loaddata
from src.funcs import log
# from src.transform import transform
# from src.load import loadDB
# from src.join import join
from src.summary import collect

def delivery(action,date1,date2):
    
    log(f'ooooooooooooooooooooooooooooooooooo BEGIN ooooooooooooooooooooooooooooooooooo')
    
    start_time_total = time.perf_counter()
    
    if (action=="d"):
        log(f' ACTION: By date')
        log(f' DATE: {date1}')
        date2 = date1
        date_range = pd.date_range(start=date1, end=date2)
    elif (action=="r"):
        log(f' ACTION: By date range')
        log(f' DATE: {date1} to {date2}')
        date_range = pd.date_range(start=date1, end=date2)
    elif (action=="s"):
        log(f' ACTION: Summary')
        if date2 == "":
            date2 = date1
       
    
    ex_total = 0
    join_total = 0
    tra_total = 0
    load_total = 0
    elapsed_total = 0
    
    if action=="d" or action=="r":
        date_list = date_range.strftime('%Y-%m-%d').tolist()
        log(f" List of date ranges {date_list}")
        
        for date in date_list:
            loaddata(date)
        
    """ Summary """
    collect(date1,date2)
        
    end_time_total = time.perf_counter()
    elapsed_total = end_time_total - start_time_total
    
    log(f' >>> TOTALS')
    log(f" Date                                 {date1} {date2}")
    log(f" Elapsed General                      {elapsed_total} seconds")
    log(f'oooooooooooooooooooooooooooooooooooo END oooooooooooooooooooooooooooooooooooo')
