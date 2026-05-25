"""
Challenge Journey – Teil 6: GPS Walk – Kafka → Spark → Redis → Live-HTML-Karte

Ablauf:
    1. Generator liest walk.csv und sendet Zeilen an Kafka (journey.location).
    2. Spark Structured Streaming liest den Topic, aggregiert in 30s-Fenstern,
       schreibt Ergebnisse in Redis (journey:walk:<ts_ms>).
    3. Nach jedem Micro-Batch wird die HTML-Karte neu gebaut → im Browser
       wird die Karte durch Auto-Reload (alle 10s) live aktualisiert.

Modi:
    python part6_geodata.py kafka-send   # walk.csv → Kafka (im eigenen Terminal)
    python part6_geodata.py stream       # Kafka → Spark → Redis + Live-HTML

Vorher starten:
    ./start_platform.sh
"""
import csv
import os
import sys
import time

from kafka import KafkaProducer
from pyspark.sql.functions import avg, col, count, split, to_timestamp, window
from pyspark.sql.functions import min as smin
from pyspark.sql.functions import max as smax
from pyspark.sql.types import DoubleType

from shared import (
    fmt_num,
    kafka_stream,
    make_spark,
    rebuild_live_map,
    redis_client,
    run_stream,
    write_redis_batch,
    KAFKA_BROKER,
    REDIS_TTL,
)

WALK_CSV     = '/home/bfh/rtdp/data/walk.csv'
OUTPUT_DIR   = '/home/bfh/rtdp/bfh-rtdp/journey'
KAFKA_TOPIC  = 'journey.location'
REDIS_PREFIX = 'journey:walk'
WINDOW_SIZE  = 30
REPLAY_SPEED = 5.0
OUT_HTML     = f'{OUTPUT_DIR}/part6_walk_map_live.html'


def parse_float(val):
    text = str(val).strip()
    if text in ('', 'NaN', 'nan', 'null'):
        return None
    return float(text)


def run_kafka_send():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    start_wall = time.time()
    print(f'[Walk Generator] Lese {WALK_CSV}')
    print(f'[Walk Generator] Sende an Kafka Topic: {KAFKA_TOPIC}')

    with open(WALK_CSV, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        print(f'[Walk Generator] Header: {header}')

        prev_time_s = None
        sent = skipped = 0

        for row in reader:
            if len(row) < 6:
                continue
            try:
                time_s = parse_float(row[0])
                lat    = parse_float(row[1])
                lon    = parse_float(row[2])
                alt    = parse_float(row[3])
                speed  = parse_float(row[5])
            except (ValueError, IndexError):
                continue

            if time_s is None or lat is None or lon is None:
                skipped += 1
                continue

            abs_ts_ms = int((start_wall + time_s) * 1000)
            alt_val   = alt   if alt   is not None else 0.0
            speed_val = speed if speed is not None else 0.0

            if prev_time_s is not None and REPLAY_SPEED > 0:
                delay = (time_s - prev_time_s) / REPLAY_SPEED
                if 0 < delay < 5.0:
                    time.sleep(delay)
            prev_time_s = time_s

            msg = f'{abs_ts_ms},{lat:.8f},{lon:.8f},{alt_val:.2f},{speed_val:.4f}'
            producer.send(KAFKA_TOPIC, value=msg.encode())
            sent += 1
            if sent % 50 == 0:
                print(f'[Walk Generator] {sent} Nachrichten, t={time_s:.1f}s')

    producer.flush()
    print(f'[Walk Generator] Fertig. {sent} gesendet, {skipped} uebersprungen.')


def run_stream_mode():
    spark = make_spark('Journey-Part6-Walk-Stream', with_kafka=True)

    df_raw    = kafka_stream(spark, KAFKA_TOPIC)
    df_parsed = (
        df_raw
        .select(split(col('value').cast('string'), ',').alias('f'))
        .select(
            to_timestamp((col('f')[0].cast('long') / 1000).cast('long')).alias('zeit'),
            col('f')[1].cast(DoubleType()).alias('lat'),
            col('f')[2].cast(DoubleType()).alias('lon'),
            col('f')[3].cast(DoubleType()).alias('alt'),
            col('f')[4].cast(DoubleType()).alias('speed'),
        )
    )

    df_result = (
        df_parsed
        .withWatermark('zeit', '30 seconds')
        .groupBy(window(col('zeit'), f'{WINDOW_SIZE} seconds'))
        .agg(
            avg('lat').alias('lat_mean'),
            avg('lon').alias('lon_mean'),
            avg('alt').alias('alt_mean'),
            smin('alt').alias('alt_min'),
            smax('alt').alias('alt_max'),
            avg('speed').alias('speed_mean'),
            count('lat').alias('n_samples'),
        )
        .select(
            col('window.start').alias('fenster_start'),
            col('window.end').alias('fenster_ende'),
            'lat_mean', 'lon_mean', 'alt_mean', 'alt_min', 'alt_max', 'speed_mean', 'n_samples',
        )
    )

    def on_batch(batch_df, epoch_id):
        rows = batch_df.collect()
        if not rows:
            return

        r = redis_client()
        for row in rows:
            ts_ms    = int(row['fenster_start'].timestamp() * 1000)
            key      = f'{REDIS_PREFIX}:{ts_ms}'
            alt_diff = (float(row['alt_max'] or 0)) - (float(row['alt_min'] or 0))
            write_redis_batch(r, key, {
                'fenster_start': str(row['fenster_start']),
                'fenster_ende':  str(row['fenster_ende']),
                'lat_mean':      fmt_num(row['lat_mean']),
                'lon_mean':      fmt_num(row['lon_mean']),
                'alt_mean':      fmt_num(row['alt_mean']),
                'alt_min':       fmt_num(row['alt_min']),
                'alt_max':       fmt_num(row['alt_max']),
                'alt_diff':      fmt_num(alt_diff),
                'speed_mean':    fmt_num(row['speed_mean']),
                'n_samples':     str(row['n_samples']),
            })
        print(f'[Redis] Epoch {epoch_id}: {len(rows)} Fenster geschrieben')

        rebuild_live_map(
            r            = r,
            key_pattern  = f'{REDIS_PREFIX}:*',
            lat_field    = 'lat_mean',
            lon_field    = 'lon_mean',
            color_field  = 'alt_diff',
            color_low    = 0.0,
            color_high   = 10.0,
            popup_fields = ['fenster_start', 'alt_mean', 'alt_diff', 'speed_mean', 'n_samples'],
            out_html     = OUT_HTML,
            title        = 'Walk – Hoehendifferenz',
            colormap     = 'RdYlGn_r',
        )
        print(f'[Map] Karte aktualisiert: {OUT_HTML}')

    print(f'\nStream gestartet. Live-Karte: file://{OUT_HTML}')
    print('Abbrechen mit Ctrl+C\n')

    query = (
        df_result.writeStream
        .outputMode('update')
        .foreachBatch(on_batch)
        .trigger(processingTime='5 seconds')
        .start()
    )
    run_stream(query, on_stop=spark.stop)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'stream'
    if mode == 'kafka-send':
        run_kafka_send()
    elif mode == 'stream':
        run_stream_mode()
    else:
        print('Verwendung: python part6_geodata.py [kafka-send|stream]')
        sys.exit(1)


if __name__ == '__main__':
    main()
