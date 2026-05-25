"""
Challenge Journey – Gyroscope Generator
Liest bfh-rtdp/roller/RawData.csv und schickt die Daten an Kafka topic: journey.gyroscope

Format pro Kafka-Nachricht (CSV):
    timestamp_ms,gx,gy,gz,absolute

RawData.csv Format (phyphox, Semikolon-getrennt, deutsche Dezimalzahlen):
    "Time (s)";"Gyroscope x (rad/s)";"Gyroscope y (rad/s)";"Gyroscope z (rad/s)";"Absolute (rad/s)"

Starten:
    python gyroscope_generator.py
"""
from kafka import KafkaProducer
import csv
import random
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'journey.gyroscope'
CSV_PATH     = '/home/bfh/rtdp/bfh-rtdp/roller/RawData.csv'
# Replay-Speed: 1.0 = Echtzeit, 0.0 = so schnell wie möglich

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# Startzeitstempel für absolute Timestamps (aktuelle Zeit)
start_wall = time.time()

def parse_de_float(s: str) -> float:
    """Deutsche Dezimalzahl (Komma) in float umwandeln."""
    return float(s.strip().replace(',', '.'))


def main():
    print(f"[Gyroscope Generator] Lese {CSV_PATH}")
    print(f"[Gyroscope Generator] Sende an Kafka Topic: {TOPIC}")

    while True:
        send()

def send():
    
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        print(f"[Gyroscope Generator] Header: {header}")

        prev_time_s = None
        row_count = 0

        for row in reader:
            # if len(row) < 5:
            #     continue

            # try:
            time_s = parse_de_float(row[0])
            gx     = parse_de_float(row[1])
            gy     = parse_de_float(row[2])
            gz     = parse_de_float(row[3])
            abs_v  = parse_de_float(row[4])
            # except ValueError as e:
            #     print(f"[Gyroscope Generator] Parse-Fehler Zeile {row}: {e}")
            #     continue

            # Absolute Timestamp: wall-clock start + relative Zeit aus CSV
            abs_ts_ms = int((start_wall + time_s) * 1000)

            # Replay-Delay: warte die echte Zeit zwischen den Messungen
            # if prev_time_s is not None and REPLAY_SPEED > 0:
            #     delay = (time_s - prev_time_s) / REPLAY_SPEED
            #     if 0 < delay < 2.0:
            #         time.sleep(delay)

            time.sleep(random.uniform(0.01, 0.1))

            # prev_time_s = time_s

            value = f"{abs_ts_ms},{gx:.6f},{gy:.6f},{gz:.6f},{abs_v:.6f}"
            producer.send(TOPIC, value=value.encode('utf-8'))
            row_count += 1

            if row_count % 100 == 0:
                print(f"[Gyroscope Generator] {row_count} Nachrichten gesendet, t={time_s:.2f}s")

    producer.flush()
    print(f"[Gyroscope Generator] Fertig. {row_count} Nachrichten gesendet.")

    

if __name__ == '__main__':
    main()
