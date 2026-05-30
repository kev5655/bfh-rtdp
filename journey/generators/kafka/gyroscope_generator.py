
from kafka import KafkaProducer
import csv
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'journey.gyroscope'
CSV_PATH     = '/home/bfh/rtdp/bfh-rtdp/roller/RawData.csv'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)


def parse_de_float(s: str) -> float:
    """Deutsche Dezimalzahl (Komma) in float umwandeln."""
    return float(s.strip().replace(',', '.'))


def main():
    print(f"[Gyroscope Generator] Lese {CSV_PATH}")
    print(f"[Gyroscope Generator] Sende an Kafka Topic: {TOPIC}")
    start_wall = time.time()

    while True:
        start_wall = time.time()
        send(start_wall)

def send(start_wall):
    
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        print(f"[Gyroscope Generator] Header: {header}")

        prev_time_s = None
        row_count = 0

        for row in reader:

            time_s = parse_de_float(row[0])
            gx     = parse_de_float(row[1])
            gy     = parse_de_float(row[2])
            gz     = parse_de_float(row[3])
            abs_v  = parse_de_float(row[4])

            abs_ts_ms = int((start_wall + time_s) * 1000)

            time.sleep(0.05) 

            value = f"{abs_ts_ms},{gx:.6f},{gy:.6f},{gz:.6f},{abs_v:.6f}"
            producer.send(TOPIC, value=value.encode('utf-8'))
            row_count += 1

            if row_count % 100 == 0:
                print(f"[Gyroscope Generator] {row_count} Nachrichten gesendet, t={time_s:.2f}s")

    producer.flush()
    print(f"[Gyroscope Generator] Fertig. {row_count} Nachrichten gesendet.")

    

if __name__ == '__main__':
    main()
