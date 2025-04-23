import os
from dotenv import load_dotenv
import psycopg   # statt psycopg2
from pathlib import Path

load_dotenv(Path.cwd() / ".env")

print("Verbindungsparameter:")
print("DB_HOST:", os.getenv("DB_HOST"))
print("DB_PORT:", os.getenv("DB_PORT"))
print("DB_NAME:", os.getenv("DB_NAME"))
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))

conn = psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require",
)
print("✅ Verbindung erfolgreich! DSN:", conn.info.dsn)
conn.close()