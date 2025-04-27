import serial

ser = serial.Serial('/dev/tty.usbmodem1301', 115200, timeout=1)
while True:
    line = ser.readline()
    print(line.decode('utf-8', errors='replace').strip())
