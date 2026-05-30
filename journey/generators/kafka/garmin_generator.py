"""
garmin_generator.py – Liest Garmin FIT-Datei und sendet Records an Kafka (journey.garmin).

Format pro Nachricht: timestamp_ms,lat,lon,alt,heart_rate,temperature

Starten:
    python garmin_generator.py

Vorher starten:
    ./start_platform.sh
    kafka-topics.sh --create --topic journey.garmin --partitions 3 \\
        --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
"""
import sys
import time

try:
    import fitparse
except ImportError:
    print("fitparse fehlt. Installieren mit: pip install fitparse")
    sys.exit(1)

from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'

FIT_PATH     = '/home/bfh/rtdp/data/22996735627_ACTIVITY.fit'
KAFKA_TOPIC  = 'journey.garmin'
REPLAY_SPEED = 5.0          # 5× Echtzeit
LOOP         = True         # Datei wiederholen, damit Stream nicht stoppt
SEMICIRCLES  = 180.0 / 2**31


def parse_fit():
    """Liest FIT-Records und gibt Zeilen als Dict zurück."""
    fitfile = fitparse.FitFile(FIT_PATH)
    for msg in fitfile.get_messages('record'):
        row = {f.name: f.value for f in msg}
        yield row


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    print(f'[Garmin Generator] Starte – sende an {KAFKA_TOPIC}')

    run = 0
    while True:
        run += 1
        prev_ts = None
        sent = 0

        for row in parse_fit():
            ts = row.get('timestamp')
            if ts is None:
                continue

            lat = row.get('position_lat')
            lon = row.get('position_long')
            if lat is None or lon is None:
                continue

            lat = float(lat) * SEMICIRCLES
            lon = float(lon) * SEMICIRCLES
            if abs(lat) > 90 or abs(lon) > 180:
                continue

            ts_ms   = int(ts.timestamp() * 1000)
            alt     = float(row.get('enhanced_altitude') or row.get('altitude') or 0)
            hr      = float(row.get('heart_rate')   or 0)
            temp    = float(row.get('temperature')  or 0)

            # Realtime Delay
            if prev_ts is not None and REPLAY_SPEED > 0:
                delta = (ts - prev_ts).total_seconds()
                delay = delta / REPLAY_SPEED
                if 0 < delay < 5.0:
                    time.sleep(delay)
            prev_ts = ts

            msg = f'{ts_ms},{lat:.8f},{lon:.8f},{alt:.2f},{hr:.0f},{temp:.1f}'
            producer.send(KAFKA_TOPIC, value=msg.encode())
            sent += 1
            if sent % 100 == 0:
                print(f'[Garmin Generator] Run {run}: {sent} Nachrichten gesendet')

        producer.flush()
        print(f'[Garmin Generator] Run {run} abgeschlossen – {sent} Nachrichten.')
        if not LOOP:
            break


if __name__ == '__main__':
    main()
