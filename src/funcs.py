import logging
from datetime import datetime
import os
import csv


if not os.path.exists("logs"):
    os.makedirs("logs")

logger = logging.getLogger("callevo")
current_date = datetime.now().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=f'logs/etl_{current_date}.log', 
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def log(msg, type="", level = 2):
    spaces = ""
    for s in range(level):
        spaces += "  "
        
    msg = spaces + msg 
    print(msg)
    if type=="":
        logger.info(msg)
    elif type =="error":
        logger.error(msg)

def logT(description,elapsed=""):
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_path = f"logs/statistics.md"
    with open(file_path, 'a', newline='', encoding='utf-8') as f:
        f.write(f'| {current_date} | {description} | {elapsed} |\n')