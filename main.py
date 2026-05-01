#!/usr/bin/python

import sys
import os
from datetime import date, datetime, timedelta
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.delivery import delivery

def main():
    
    print("")
    print("")
    print("█████  █████  ██     ██     █████  ██ ██  █████ ")
    print("██░░░░ ██░██░ ██░    ██░    ██░░░░ ██░██░ ██░██░")
    print("██░    █████░ ██░    ██░    █████  ██░██░ ██░██░")
    print("██░    ██░██░ ██░    ██░    ██░░░░ ██░██░ ██░██░")
    print("█████  ██░██░ █████  █████  █████  █████░ █████░")
    print(" ░░░░░  ░░ ░░  ░░░░░  ░░░░░  ░░░░░  ░░░░░  ░░░░░")
    print("")
    print(" CALLEVO INC.")
    print(" ETL - EXTRACT, TRANSFORM AND LOAD")
    print(" Callevo Development Team")
    print(" Copyright (c) Callevo Inc, 2026")
    print(" Version 1.0")
    
    if len(sys.argv) > 1:
        # print(f"Argumentos recibidos: {sys.argv[1:]}")

        action = sys.argv[1]
        d1 = ""
        d2 = ""

        if action == 'r':
            """ Date range """
            d1 = sys.argv[2]
            d2 = sys.argv[3]

        elif action == 'd':
            """ Search by a single date """
            d1 = sys.argv[2]
            
        elif action == 's':
            """ Calculate the summary """
            d1 = sys.argv[2]
            d2 = sys.argv[3]
        
        elif action == 'h':
            print("")
            print("")
            print(" The Callevo ETL application accepts up to three arguments separated by spaces.")
            print(" The first argument is the action:")
            print("")
            print(" r       Date range; must include two arguments: the start date and the end date.")
            print(" d       Process will run for a single day; a second argument is expected.")
            print(" s       Process the entire month to generate the summary; must include two arguments: the start date and the end date.")
            print(" h       Displays help")
            print("")
            print(" The app will process data on a daily basis and generate statistics.")
            print("")
            print(" Examples:")
            print(" python main.py r 2026-04-01 2026-04-26")
            print(" python main.py d 2026-04-14")
            print(" python main.py s 2026-01-01 2026-04-30")
            print(" python main.py h")
            print("")
            print("")
            
        else:
            print("Wrong parameters.")
            exit(1)
        
        if action != 'h':
            delivery(action,d1,d2)
                
    else:
        log("You need to set the execution parameters. Check the help by typing << python main.py h >>")

if __name__ == "__main__":
    main()
