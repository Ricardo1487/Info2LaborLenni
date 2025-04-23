import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()  # lädt DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

try:
    # Verbindungsaufbau (mit SSL)
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require"
    )
    print("✅ Verbindung erfolgreich:", conn.get_dsn_parameters())
    conn.close()
except Exception as e:
    print("❌ Verbindung fehlgeschlagen:", e)