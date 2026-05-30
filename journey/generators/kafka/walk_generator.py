"""
Challenge Journey – Walk Generator
Liest data/walk.csv (phyphox GPS) und schickt die Daten an Kafka topic: journey.location

Format pro Kafka-Nachricht (CSV):
    timestamp_ms,lat,lon,altitude_m,speed_ms

walk.csv Format (phyphox, Komma-getrennt, wissenschaftliche Notation):
    "Time (s)","Latitude (°)","Longitude (°)","Altitude (m)","Altitude WGS84 (m)",
    "Speed (m/s)","Direction (°)","Distance (km)","Horizontal Accuracy (m)",
    "Vertical Accuracy (m)","Satellites"

Starten:
    python walk_generator.py
"""
from kafka import KafkaProducer
import csv
import time
import math

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'journey.location'
CSV_PATH     = '/home/bfh/rtdp/data/walk.csv'
# Replay-Speed: 1.0 = Echtzeit, 5.0 = 5× schneller, 0.0 = so schnell wie möglich
REPLAY_SPEED = 5.0

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# Startzeitstempel: aktuelle Zeit als Basis für relative CSV-Zeiten
start_wall = time.time()


def parse_float(s: str):
    """Parst float oder gibt None zurück (für NaN-Werte)."""
    s = s.strip()
    if s in ('', 'NaN', 'nan', 'null'):
        return None
    return float(s)


def main():
    print(f"[Walk Generator] Lese {CSV_PATH}")
    print(f"[Walk Generator] Sende an Kafka Topic: {TOPIC}")

    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        print(f"[Walk Generator] Header: {header}")

        prev_time_s = None
        row_count   = 0
        skipped     = 0

        for row in reader:
            if len(row) < 6:
                continue

            try:
                time_s  = parse_float(row[0])
                lat     = parse_float(row[1])
                lon     = parse_float(row[2])
                alt     = parse_float(row[3])  # Altitude (m)
                speed   = parse_float(row[5])  # Speed (m/s), kann NaN sein
            except (ValueError, IndexError) as e:
                print(f"[Walk Generator] Parse-Fehler: {e}")
                continue

            # Zeilen ohne gültige GPS-Koordinaten überspringen
            if time_s is None or lat is None or lon is None:
                skipped += 1
                continue

            # Absoluter Timestamp: Wand-Uhrzeit beim Start + relative CSV-Zeit
            abs_ts_ms = int((start_wall + time_s) * 1000)

            # Speed NaN → 0
            speed_val = speed if speed is not None else 0.0
            # Altitude NaN → 0
            alt_val   = alt if alt is not None else 0.0

            # Replay-Delay
            if prev_time_s is not None and REPLAY_SPEED > 0:
                delay = (time_s - prev_time_s) / REPLAY_SPEED
                if 0 < delay < 5.0:
                    time.sleep(delay)

            prev_time_s = time_s

            value = f"{abs_ts_ms},{lat:.8f},{lon:.8f},{alt_val:.2f},{speed_val:.4f}"
            producer.send(TOPIC, value=value.encode('utf-8'))
            row_count += 1

            if row_count % 50 == 0:
                print(f"[Walk Generator] {row_count} Nachrichten, t={time_s:.1f}s lat={lat:.5f} lon={lon:.5f}")

    producer.flush()
    print(f"[Walk Generator] Fertig. {row_count} Nachrichten gesendet, {skipped} übersprungen (NaN).")


if __name__ == '__main__':
    main()
