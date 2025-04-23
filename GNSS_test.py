import os
from dotenv import load_dotenv
import psycopg
from datetime import datetime
import time
from pathlib import Path

# Load environment variables from .env
load_dotenv(Path.cwd() / ".env")

# Configuration from .env
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = int(os.getenv("DB_PORT", 5432))
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Fake mode for testing without GNSS hardware
print("FAKE-GNSS-Modus aktiv – verwende Testdaten.")

def connect_db():
    """Connect to PostgreSQL using environment variables (psycopg v3)."""
    try:
        return psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode="require"  # SSL-Verbindung erzwingen
        )
    except psycopg.Error as err:
        print(f"Fehler bei Datenbankverbindung: {err}")
        return None

def parse_gpgga(line):
    """Parse a GPGGA NMEA sentence into (latitude, longitude, altitude)."""
    parts = line.split(',')
    if len(parts) < 10:
        return None
    try:
        lat_raw, lat_dir = parts[2], parts[3]
        lon_raw, lon_dir = parts[4], parts[5]
        alt = float(parts[9])
        lat = convert_to_decimal(lat_raw, lat_dir)
        lon = convert_to_decimal(lon_raw, lon_dir)
        return lat, lon, alt
    except Exception as e:
        print(f"Parse-Error: {e}")
        return None

def convert_to_decimal(raw, direction):
    """Convert NMEA raw coordinate to decimal degrees."""
    if not raw or not direction:
        return None
    deg = int(float(raw) / 100)
    minutes = float(raw) - deg * 100
    decimal = deg + minutes / 60
    if direction in ('S', 'W'):
        decimal = -decimal
    return decimal

def main():
    db = connect_db()
    if db is None:
        print("Verbindung zur Datenbank fehlgeschlagen.")
        return
    cursor = db.cursor()
    try:
        while True:
            # Simulated NMEA GPGGA sentence for testing
            line = "$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47"
            if line.startswith('$GPGGA'):
                data = parse_gpgga(line)
                if data:
                    lat, lon, alt = data
                    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    cursor.execute(
                        "INSERT INTO gps_data (timestamp, latitude, longitude, altitude) VALUES (%s, %s, %s, %s)",
                        (timestamp, lat, lon, alt)
                    )
                    db.commit()
                    print(f"Gespeichert: {timestamp} → {lat}, {lon}, {alt} m")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Beendet per Tastatur")
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    main()