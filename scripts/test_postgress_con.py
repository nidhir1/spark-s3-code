import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
from psycopg2 import OperationalError
from myconfig import settings

try:
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD
    )
    print(f"✅ Successfully connected to Postgres at {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}")
    conn.close()
except OperationalError as e:
    print(f"❌ Connection failed: {e}")
