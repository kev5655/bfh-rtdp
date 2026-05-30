import sys
import time

try:
    import fitparse
except ImportError:
    print("fitparse fehlt. Installieren mit: pip install fitparse")
    sys.exit(1)

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC  = 'journey.location'
FIT_PATH     = '/home/bfh/rtdp/data/22834929833_ACTIVITY.fit'
SEMICIRCLES  = 180.0 / 2**31


def purge_topic():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    try:
        admin.delete_topics([KAFKA_TOPIC])
        print(f'[Garmin Generator] Topic {KAFKA_TOPIC} gelöscht.')
        time.sleep(3)
    except Exception:
        pass
    try:
        admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
        print(f'[Garmin Generator] Topic {KAFKA_TOPIC} neu erstellt.')
    except Exception as e:
        print(f'[Garmin Generator] Topic erstellen: {e}')
    admin.close()


def parse_fit():
    fitfile = fitparse.FitFile(FIT_PATH)
    for msg in fitfile.get_messages('record'):
        row = {f.name: f.value for f in msg}

        ts  = row.get('timestamp')
        lat = row.get('position_lat')
        lon = row.get('position_long')
        if ts is None or lat is None or lon is None:
            continue

        lat = float(lat) * SEMICIRCLES
        lon = float(lon) * SEMICIRCLES
        if abs(lat) > 90 or abs(lon) > 180:
            continue

        alt  = float(row.get('altitude') or 0)
        hr   = float(row.get('heart_rate')  or 0)
        temp = float(row.get('temperature') or 0)

        yield lat, lon, alt, hr, temp


def main():
    purge_topic()
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    print(f'[Garmin Generator] Starte – sende an {KAFKA_TOPIC}')

    run = 0
    while True:
        run += 1
        sent = 0

        for lat, lon, alt, hr, temp in parse_fit():
            time.sleep(0.05) 

            ts_ms = int(time.time() * 1000)
            msg = f'{ts_ms},{lat:.8f},{lon:.8f},{alt:.2f},{hr:.0f},{temp:.1f}'
            producer.send(KAFKA_TOPIC, value=msg.encode())
            sent += 1

            if sent % 100 == 0:
                print(f'[Garmin Generator] Run {run}: {sent} Nachrichten gesendet')

        producer.flush()
        print(f'[Garmin Generator] Run {run} abgeschlossen – {sent} Nachrichten.')


if __name__ == '__main__':
    main()
