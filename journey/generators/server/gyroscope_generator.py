import csv
import random
import time
import requests

SERVER_URL = 'http://localhost:5000/gyroscope'
CSV_PATH   = '/home/bfh/rtdp/bfh-rtdp/roller/RawData.csv'

start_wall = time.time()


def main():
    while True:
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader)  # skip header
            for row in reader:
                time_s, gx, gy, gz = (float(v.strip().replace(',', '.')) for v in row[:4])
                payload = {
                    'time': int((start_wall + time_s) * 1000),
                    'x': gx, 'y': gy, 'z': gz,
                }
                requests.post(SERVER_URL, json=payload, timeout=2)
                time.sleep(random.uniform(0.01, 0.1))


if __name__ == '__main__':
    main()
