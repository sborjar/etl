
import logging

logger = logging.getLogger("callevo")
logging.basicConfig(filename='logs/etl.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def log(msg):
    print(msg)
    logger.info(msg)