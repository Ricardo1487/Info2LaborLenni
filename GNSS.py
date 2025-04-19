import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from datetime import datetime
import serial
import time

# -------------------------
# Konfiguration
# -------------------------
SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
BAUD_RATE = int(os.getenv("BAUD_RATE", 9600))

# -------------------------
# GNSS-Verbindung aufbauen
# -------------------------
try:
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    print("GNSS-Sensor verbunden")
except Exception as e:
    print(f"Fehler beim Verbinden zum GNSS-Modul: {e}")
    exit(1)

# -------------------------
# NMEA-Zeile parsen ($GPGGA)
# -------------------------
def parse_gpgga(line):
    try:
        parts = line.split(',')
        if len(parts) < 10:
            return None

        # Umrechnen in Dezimalgrad
        lat_raw = parts[2]
        lat_dir = parts[3]
        lon_raw = parts[4]
        lon_dir = parts[5]
        alt = float(parts[9])

        lat = convert_to_decimal(lat_raw, lat_dir)
        lon = convert_to_decimal(lon_raw, lon_dir)

        return (lat, lon, alt)
    except:
        return None

# -------------------------
# Umrechnen NMEA -> Dezimalgrad
# -------------------------
def convert_to_decimal(raw, direction):
    if raw == '' or direction == '':
        return None
    deg = int(float(raw) / 100)
    min = float(raw) - deg * 100
    decimal = deg + min / 60
    if direction in ['S', 'W']:
        decimal = -decimal
    return decimal

# -------------------------
# Hauptschleife
# -------------------------
db = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cursor = db.cursor()

while True:
    try:
        # FAKE-MODUS für Mac-Test ohne Hardware
        line = ser.readline().decode('utf-8', errors='ignore').strip()
        if line.startswith('$GPGGA'):
            data = parse_gpgga(line)
            if data:
                lat, lon, alt = data
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("""
                    INSERT INTO gps_data (timestamp, latitude, longitude, altitude)
                    VALUES (%s, %s, %s, %s)
                """, (timestamp, lat, lon, alt))
                db.commit()
                print(f"Gespeichert: {timestamp} → {lat}, {lon}, {alt} m")
        time.sleep(1)

    except KeyboardInterrupt:
        print("Beendet per Tastatur")
        break
    except Exception as e:
        print(f"Fehler: {e}")
        time.sleep(2)

cursor.close()
db.close()
ser.close()
