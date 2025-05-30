#!/usr/bin/env python3
import os, time, serial, csv, tempfile, psycopg2, logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------
# Logging konfigurieren
# ---------------------------------------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s  %(message)s",
    level=logging.INFO,            # DEBUG für noch mehr Details
    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ---------------------------------------------------
# Umgebungsvariablen laden
# ---------------------------------------------------
load_dotenv(Path.cwd() / ".env")

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
BAUD_RATE   = int(os.getenv("BAUD_RATE", "9600"))
BUFFER_FILE = "buffer.csv"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# ---------------------------------------------------
# Globale Handles
# ---------------------------------------------------
ser = db = cursor = None

# ---------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------
def connect_db():
    """(Re)connect und sofortigen Buffer-Flush ausführen."""
    global db, cursor
    try:
        if db:
            db.close()
    except Exception:
        pass

    db = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, sslmode="require")
    cursor = db.cursor()
    log.info("🔄 (Re)connected to DB")
    flush_buffer_to_db()

def convert_to_decimal(raw, d):
    if not raw or not d:
        return None
    deg = int(float(raw) // 100)
    dec = deg + (float(raw) - deg * 100) / 60
    return -dec if d in ("S", "W") else dec

def parse_gpgga(line):
    p = line.split(',')
    if len(p) < 10:
        return None
    return (
        convert_to_decimal(p[2], p[3]),
        convert_to_decimal(p[4], p[5]),
        float(p[9]) if p[9] else None
    )

def parse_gprmc(line):
    p = line.split(',')
    return float(p[7]) * 1.852 if len(p) > 7 and p[7] else None

def save_to_buffer(ts, lat, lon, alt, spd):
    if lat is None or lon is None:
        log.warning("⚠️  GNSS-Fix fehlt – Zeile verworfen")
        return

    fd, tmp = tempfile.mkstemp(dir=".", prefix=BUFFER_FILE, text=True)
    with os.fdopen(fd, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([ts.isoformat(), lat, lon, alt or "", spd or ""])
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE, "r", newline="") as old:
                for row in old:
                    f.write(row)
    os.replace(tmp, BUFFER_FILE)
    log.info("💾 Buffer-Write %s  lat=%.6f lon=%.6f", ts.isoformat(), lat, lon)

def flush_buffer_to_db():
    if not os.path.exists(BUFFER_FILE):
        return
    rows = list(csv.reader(open(BUFFER_FILE, "r", newline="")))
    if not rows:
        return

    success, remaining = 0, []
    for r in rows:
        try:
            ts  = r[0]
            lat = float(r[1]) if r[1] else None
            lon = float(r[2]) if r[2] else None
            alt = float(r[3]) if r[3] else None
            spd = float(r[4]) if r[4] else None

            if lat is None or lon is None:
                log.warning("⚠️  Überspringe unvollst. Row %s", r)
                continue

            cursor.execute(
                "INSERT INTO gnss_data (timestamp,latitude,longitude,altitude,speed) "
                "VALUES (%s,%s,%s,%s,%s)",
                (ts, lat, lon, alt, spd))
            success += 1

        except Exception as e:
            log.error("❌ Flush-Fehler %s  %s", r, e)
            db.rollback()
            remaining.append(r)

    if success:
        db.commit()
        log.info("✅ Flush OK – %d rows", success)

    with open(BUFFER_FILE, "w", newline="") as f:
        csv.writer(f).writerows(remaining)
    log.info("📦 Buffer verbleibend: %d Zeilen", len(remaining))

# ---------------------------------------------------
# Hauptloop
# ---------------------------------------------------
if __name__ == "__main__":
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    log.info("✅ GNSS-Sensor verbunden!")
    connect_db()

    last_speed = None
    last_flush = time.time()

    try:
        while True:
            line = ser.readline().decode('utf-8', 'ignore').strip()
            if not line:
                continue

            hdr = line.split(',')[0]

            # --- Geschwindigkeit -------------------------------------------------
            if hdr in ("$GPRMC", "$GNRMC"):
                sp = parse_gprmc(line)
                if sp is not None:
                    last_speed = sp
                    log.info("🚀 Speed %.2f km/h", sp)
                continue

            # --- Position --------------------------------------------------------
            if hdr.endswith("GGA"):
                data = parse_gpgga(line)
                if not data:
                    continue
                lat, lon, alt = data
                ts = datetime.utcnow()
                sp = last_speed or 0.0

                try:
                    if not cursor or cursor.closed or db.closed:
                        connect_db()

                    cursor.execute(
                        "INSERT INTO gnss_data (timestamp,latitude,longitude,altitude,speed) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (ts.isoformat(), lat, lon, alt, sp))
                    db.commit()

                    log.info("✅ Live-Insert %s", ts.isoformat())
                    flush_buffer_to_db()

                except Exception as e:
                    log.error("❌ Insert-Fehler: %s", e)
                    db.rollback()
                    save_to_buffer(ts, lat, lon, alt, sp)
                    connect_db()

            # --- Periodischer Flush ---------------------------------------------
            if time.time() - last_flush >= 30:
                log.debug("⏳ Periodischer Flush-Tick")
                try:
                    cursor.execute("SELECT 1")
                    flush_buffer_to_db()
                except Exception:
                    connect_db()
                last_flush = time.time()

    except KeyboardInterrupt:
        log.info("🛑 Logger beendet per Tastatur")

    finally:
        if ser:    ser.close()
        if cursor: cursor.close()
        if db:     db.close()