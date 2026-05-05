 
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / '.env')

class Config:
    """Configuration module for environment variables and database settings."""
    """Configuration class for ETL application."""
    
    MODE = os.getenv('MODE', 'SNBX').upper()
    PROCESSOR = os.getenv('PROCESSOR', 'pandas').lower()

    if MODE == 'PROD':
        DB_HOST = os.getenv('DB_HOST_PROD')
        DB_USER = os.getenv('DB_USER_PROD')
        DB_PASS = os.getenv('DB_PASS_PROD')
    else:
        DB_HOST = os.getenv('DB_HOST_SNBX')
        DB_USER = os.getenv('DB_USER_SNBX')
        DB_PASS = os.getenv('DB_PASS_SNBX')

    DB_NAME = os.getenv('DB_NAME', 'asterisk_realtime')
    DB_PORT = int(os.getenv('DB_PORT', 3306))

    DATA_DIR = BASE_DIR / 'data'
    LOG_DIR = BASE_DIR / 'logs'

    @classmethod
    def get_db_url(cls):
        """Get database URL for SQLAlchemy."""
        return f"mysql+mysqlconnector://{cls.DB_USER}:{cls.DB_PASS}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"