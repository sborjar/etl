import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

prefix = os.getenv("MODE", "SNBX")

db = mysql.connector.connect(
   host=os.getenv(f"DB_HOST_{prefix}", "localhost"),
   user=os.getenv(f"DB_USER_{prefix}", "root"),
   password=os.getenv(f"DB_PASS_{prefix}", ""),
   database=os.getenv(f"DB_NAME", "asterisk_realtime"),
)


