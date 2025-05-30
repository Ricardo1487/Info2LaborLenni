#!/usr/bin/env python3
import os, time, serial, csv, tempfile, psycopg2, logging
from dotenv import load_dotenv
from pathlib import Path

from datetime import datetime

# ---------------------------------------------------
# Online‑Check (leichter UDP‑Ping)
# ---------------------------------------------------
PING_HOST   = os.getenv("PING_HOST", "8.8.8.8")   # öffentlicher DNS‑Server (Google)
PING_PORT   = int(os.getenv("PING_PORT", "53"))   # beliebiger UDP‑Port
PING_TIMEOUT = int(os.getenv("PING_TIMEOUT", "3"))

def is_online(host: str = PING_HOST, port: int = PING_PORT, timeout: int = PING_TIMEOUT) -> bool:
    """Prüft per leichtgewichtigem UDP‑Ping, ob ein Uplink ins Internet besteht.
    Verhindert unnötige (und langsame) Verbindungsversuche zur Datenbank,
    wenn überhaupt keine Konnektivität vorhanden ist."""
    import socket
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM).connect((host, port))
        return True
    except OSError:
        return False

# ---------------------------------------------------
# Logging
# ---------------------------------------------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s  %(message)s",
    level=logging.INFO,     # DEBUG für mehr Details
    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ---------------------------------------------------
# .env einlesen
# ---------------------------------------------------
load_dotenv(Path.cwd() / ".env")

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
BAUD_RATE   = int(os.getenv("BAUD_RATE", "9600"))
BUFFER_FILE = "buffer.csv"

DB_HOST, DB_PORT = os.getenv("DB_HOST"), os.getenv("DB_PORT", "5432")
DB_NAME, DB_USER = os.getenv("DB_NAME"), os.getenv("DB_USER")
DB_PASSWORD      = os.getenv("DB_PASSWORD")

ser = db = cursor = None      # globale Handles

# ---------------------------------------------------
# DB-Reconnect
# ---------------------------------------------------
# ---------------------------------------------------
# Helfer: sicherer Rollback, der keine Exceptions propagiert
# ---------------------------------------------------
def safe_rollback():
    """Versucht einen DB‑Rollback, schluckt aber alle Fehler,
    damit der eigentliche Fehlerpfad (Buffer schreiben) nicht unterbrochen wird."""
    try:
        if db and not db.closed:
            db.rollback()
            log.debug("🔄 DB rollback ausgeführt")
    except Exception as e:
        log.debug("⏭️  Rollback nicht möglich: %s", e)

def connect_db():
    global db, cursor
    # Erst prüfen, ob überhaupt ein Uplink existiert
    if not is_online():
        log.info("🌐 Offline – DB‑Connect übersprungen")
        return
    try:
        if db: db.close()
    except Exception:
        pass

    db = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, sslmode="require")
    cursor = db.cursor()
    log.info("🔄 (Re)connected to DB")
    flush_buffer_to_db()

# ---------------------------------------------------
# GPS-Parser
# ---------------------------------------------------
def convert_to_decimal(raw, d):
    if not raw or not d: return None
    deg = int(float(raw)//100)
    return (-1 if d in ("S","W") else 1) * (deg + (float(raw)-deg*100)/60)

def parse_gpgga(line):
    p=line.split(',')
    if len(p)<10: return None
    return (
        convert_to_decimal(p[2],p[3]),
        convert_to_decimal(p[4],p[5]),
        float(p[9]) if p[9] else None
    )

def parse_gprmc(line):
    p=line.split(',')
    return float(p[7])*1.852 if len(p)>7 and p[7] else None

# ---------------------------------------------------
# Buffer-Funktionen
# ---------------------------------------------------
def save_to_buffer(ts, lat, lon, alt, spd):
    log.debug("✏️  save_to_buffer called (lat=%s lon=%s alt=%s spd=%s)", lat, lon, alt, spd)
    if lat is None or lon is None:
        log.warning("⚠️  GNSS-Fix fehlt – Datensatz verworfen")
        return
    fd,tmp=tempfile.mkstemp(dir=".",prefix=BUFFER_FILE,text=True)
    with os.fdopen(fd,"w",newline="") as f:
        csv.writer(f).writerow(
            [ts.isoformat(), lat, lon, alt or "", spd or ""])
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE,"r",newline="") as old:
                for row in old: f.write(row)
    os.replace(tmp,BUFFER_FILE)
    log.info("💾 Buffer-Write %s lat=%.6f lon=%.6f", ts.isoformat(), lat, lon)

def flush_buffer_to_db():
    if not os.path.exists(BUFFER_FILE): return
    rows=list(csv.reader(open(BUFFER_FILE,"r",newline="")))
    if not rows: return

    success, remaining = 0, []
    for r in rows:
        try:
            ts  = r[0]
            lat = float(r[1]) if r[1] else None
            lon = float(r[2]) if r[2] else None
            if lat is None or lon is None:
                log.warning("⚠️  Überspringe unvollst. Row %s", r); continue
            alt = float(r[3]) if r[3] else None
            spd = float(r[4]) if r[4] else None
            cursor.execute(
                """
                INSERT INTO gnss_data (timestamp, latitude, longitude, altitude, speed)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
                """,
                (ts, lat, lon, alt, spd))
            # success nur erhöhen, wenn tatsächlich eingefügt wurde
            if cursor.rowcount:
                success += 1
            else:
                log.debug("⏭️  Duplikat übersprungen: %s", ts)
        except Exception as e:
            log.error("❌ Flush-Fehler %s  %s", r, e)
            safe_rollback()
            remaining.append(r)

    if success:
        try:
            db.commit()
            log.info("✅ Flush OK – %d rows", success)
        except Exception as e:
            log.error("❌ Commit‑Fehler: %s", e)
            safe_rollback()
            remaining = rows  # nichts löschen, alles bleibt gepuffert

    with open(BUFFER_FILE,"w",newline="") as f:
        csv.writer(f).writerows(remaining)
    log.info("📦 Buffer verbleibend: %d", len(remaining))

# ---------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------
if __name__=="__main__":
    ser = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
    log.info("✅ GNSS-Sensor verbunden!")
    connect_db()

    last_speed, last_flush = None, time.time()

    try:
        while True:
            line = ser.readline().decode('utf-8','ignore').strip()
            if not line: continue
            hdr = line.split(',')[0]

            # Geschwindigkeit
            if hdr in ("$GPRMC","$GNRMC"):
                sp = parse_gprmc(line)
                if sp is not None:
                    last_speed = sp
                    log.info("🚀 Speed %.2f km/h", sp)
                continue

            # Position
            if hdr.endswith("GGA"):
                data = parse_gpgga(line)
                if not data: continue
                lat, lon, alt = data
                ts  = datetime.utcnow()
                spd = last_speed or 0.0
                try:
                    if not cursor or cursor.closed or db.closed:
                        connect_db()
                    cursor.execute(
                        "INSERT INTO gnss_data "
                        "(timestamp,latitude,longitude,altitude,speed)"
                        "VALUES (%s,%s,%s,%s,%s)",
                        (ts.isoformat(), lat, lon, alt, spd))
                    db.commit()
                    log.info("✅ Live-Insert %s", ts.isoformat())
                    flush_buffer_to_db()
                except Exception as e:
                    log.error("❌ Insert-Fehler: %s", e)
                    safe_rollback()
                    save_to_buffer(ts, lat, lon, alt, spd)
                    connect_db()

            # periodischer Flush
            if time.time()-last_flush >= 30:
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