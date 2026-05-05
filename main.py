#!/usr/bin/python

import sys
import os
from datetime import date, datetime, timedelta
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.delivery import delivery
from src.menu import showMenu
from src.presentation import showPresentation
from src.funcs import log

def main():
    
    showPresentation()
    
    if len(sys.argv) > 1:
        """ 
            Callevo ETL
            Extract, Transform and Load Process 
            
            Args:
                action: Action type ('d', 'r', 's')
                date1: Start date (yyyy-mm-dd)
                date2: End date (yyyy-mm-dd)
        """

        try:
            action = sys.argv[1]
            d1 = sys.argv[2] 
            if action in ('r','s'):
                d2 = sys.argv[3] 
            if action in ('d'):
                d2 = d1    
        except Exception as err:
            showMenu()
            log(f"Error: Arguments validation, {err}","error",0)
            exit()
    
        delivery(action,d1,d2)
                
    else:
        showMenu()
        log(f"Error: It is running without arguments","error",0)

if __name__ == "__main__":
    main()
