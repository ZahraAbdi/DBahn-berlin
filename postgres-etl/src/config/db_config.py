import psycopg2
import os

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'DBahn-berlin'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'admin4321'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433')
}

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to database")
        return conn
    except Exception as e:  
        print(f"Connection error: {e}")
        return None
    