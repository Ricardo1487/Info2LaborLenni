import serial

# Öffne die serielle Schnittstelle /dev/serial0
ser = serial.Serial('/dev/serial0', baudrate=9600, timeout=1)

print("🚀 GNSS Reader gestartet! Warte auf GPS-Daten...\n")

try:
    while True:
        line = ser.readline()
        if line:
            decoded_line = line.decode('utf-8', errors='replace').strip()
            print(decoded_line)
except KeyboardInterrupt:
    print("\n⛔ GNSS Reader beendet.")
finally:
    ser.close()