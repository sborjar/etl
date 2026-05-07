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
    """ 
    Este es un programa independiente de todo para reconstruir (restaurar) datos desde archivos csv, 
    esto quiere decir,  que se lee cada archivo general_*.* que son los fuentes (data extraida desde 
    la base y guardada como respaldo) y se subre a la tabla calls. 
    """
    folder = 'data/'
    filter = os.path.join(folder, 'general_2026-05*')
    files = glob.glob(filter)
    
    log(" CREATE ENGINE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    
    for file in files:
        """ Se carga el archivo """
        df = load_data(file)
        """ Se eliminan columnas que no son de calls """
        df.drop(columns=["usertype","activate_tenant","success","contact"], inplace=True)
        """ Se sube el contenido resultante a la tabla calls """
        df.to_sql(
            name='calls', 
            con=engine, 
            if_exists='append',
            index=False, 
            chunksize=10000
        )
    
if __name__ == "__main__":
    init()