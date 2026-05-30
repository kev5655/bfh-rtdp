import csv
import time
import requests

SERVER_URL   = 'http://localhost:5000/location'
CSV_PATH     = '/home/bfh/rtdp/data/walk.csv'
REPLAY_SPEED = 5.0

start_wall = time.time()


def parse(s):
    s = s.strip()
    return None if s in ('', 'NaN', 'nan') else float(s)


def main():
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        prev = None
        for row in reader:
            if len(row) < 6:
                continue
            time_s, lat, lon, alt, speed = parse(row[0]), parse(row[1]), parse(row[2]), parse(row[3]), parse(row[5])
            if time_s is None or lat is None or lon is None:
                continue
            if prev is not None and REPLAY_SPEED > 0:
                delay = (time_s - prev) / REPLAY_SPEED
                if 0 < delay < 5.0:
                    time.sleep(delay)
            prev = time_s
            payload = {
                'time': int((start_wall + time_s) * 1000),
                'lat': lat, 'lon': lon,
                'altitude': alt or 0.0, 'speed': speed or 0.0,
            }
            requests.post(SERVER_URL, json=payload, timeout=2)


if __name__ == '__main__':
    main()
