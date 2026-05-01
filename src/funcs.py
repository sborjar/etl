
import logging
from datetime import datetime
import os

if not os.path.exists("logs"):
    os.makedirs("logs")

logger = logging.getLogger("callevo")
current_date = datetime.now().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=f'logs/etl_{current_date}.log', 
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def log(msg, type=""):
    print(msg)
    if type=="":
        logger.info(msg)
    elif type =="error":
        logger.error(msg)