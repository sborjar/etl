import os
import sys
import pandas as pd
import glob
from sqlalchemy import create_engine
import pymysql
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))



load_dotenv()
mode = os.getenv("MODE", "SNBX")
user = os.getenv(f"DB_USER_{mode}", "SNBX")
password = os.getenv(f"DB_PASS_{mode}", "SNBX")
host = os.getenv(f"DB_HOST_{mode}", "SNBX")
database = os.getenv("DB_NAME", "SNBX")

def log(msg):
    print(msg)

def load_data(nfile): 
    file = nfile
    log(f" Importing file {file}")
    BASE_DIR = os.getcwd() 
    # DATA_PROCESS_DIR = os.path.join(BASE_DIR, 'data')
    DATA_PROCESS_DIR = os.path.join(BASE_DIR)
    PROCESSED_FILE = os.path.join(DATA_PROCESS_DIR, file)
    try:
        df = pd.read_csv(PROCESSED_FILE, encoding='latin1', low_memory=False)
        return df
    except FileNotFoundError:
        log(f"Error: No se encontro el archivo {PROCESSED_FILE}")
        return None


def init():
    folder = 'data/'
    filter = os.path.join(folder, 'calls_*')
    files = glob.glob(filter)
    
    log(" CREATE ENGINE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

    
    for file in files:
        df = load_data(file)
        df.to_sql(
            name='calls', 
            con=engine, 
            if_exists='append',
            index=False, 
            chunksize=10000
        )
    
if __name__ == "__main__":
    init()