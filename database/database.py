import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import HTTPException


# Load environment variables
load_dotenv()

DB_USER = urllib.parse.quote_plus(os.getenv("DB_USER"))
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")

# Dictionary of database connections
DATABASES = {
    "zing": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/zing",
    "prathiksham": f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/prathiksham"
}

# Maintain separate session makers for each DB
engines = {name: create_engine(url) for name, url in DATABASES.items()}
session_makers = {name: sessionmaker(bind=eng, autocommit=False, autoflush=False) for name, eng in engines.items()}

Base = declarative_base()

# Function to get the database session dynamically
def get_db(business: str):
    if business not in session_makers:
        raise HTTPException(status_code=400, detail="Invalid business name")
    
    db = session_makers[business]()
    try:
        yield db
    finally:
        db.close()