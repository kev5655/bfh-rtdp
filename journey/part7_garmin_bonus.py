"""
Challenge Journey – Teil 7 (Bonus): Garmin FIT – Kafka → Spark → Redis → Live-Map

Ablauf:
    1. garmin_generator.py liest FIT-Datei und sendet Records an journey.garmin.
    2. Spark Structured Streaming aggregiert in 60s-Fenstern.
    3. Ergebnisse → Redis (journey:garmin:<ts_ms>).
    4. Nach jedem Micro-Batch → Live-HTML-Karte überschreiben (Auto-Refresh 10s).

Modi:
    python part7_garmin_bonus.py kafka-send   # garmin_generator.py ist ein eigenes Skript,
                                               # dieser Modus startet es direkt
    python part7_garmin_bonus.py stream        # Kafka → Spark → Redis + Live-Map

Vorher:
    ./start_platform.sh
    kafka-topics.sh --create --topic journey.garmin --partitions 3 \\
        --replication-factor 1 --bootstrap-server localhost:9092 2>/dev/null || true
"""
import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

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
    REDIS_TTL,
)

KAFKA_TOPIC  = 'journey.garmin'
REDIS_PREFIX = 'journey:garmin'
WINDOW_SIZE  = 60
OUTPUT_DIR   = '/home/bfh/rtdp/bfh-rtdp/journey'
OUT_HTML     = f'{OUTPUT_DIR}/part7_garmin_live.html'
OUT_PNG      = f'{OUTPUT_DIR}/part7_garmin_analysis.png'


def run_kafka_send():
    """Startet den garmin_generator als Subprocess."""
    import subprocess
    script = os.path.join(os.path.dirname(__file__), 'garmin_generator.py')
    print(f'[Part7] Starte Generator: {script}')
    subprocess.run([sys.executable, script])


def run_stream_mode():
    spark = make_spark('Journey-Part7-Garmin-Stream', with_kafka=True)

    df_raw    = kafka_stream(spark, KAFKA_TOPIC)
    df_parsed = (
        df_raw
        .select(split(col('value').cast('string'), ',').alias('f'))
        .select(
            to_timestamp((col('f')[0].cast('long') / 1000).cast('long')).alias('zeit'),
            col('f')[1].cast(DoubleType()).alias('lat'),
            col('f')[2].cast(DoubleType()).alias('lon'),
            col('f')[3].cast(DoubleType()).alias('alt'),
            col('f')[4].cast(DoubleType()).alias('hr'),
            col('f')[5].cast(DoubleType()).alias('temp'),
        )
    )

    df_result = (
        df_parsed
        .withWatermark('zeit', '60 seconds')
        .groupBy(window(col('zeit'), f'{WINDOW_SIZE} seconds'))
        .agg(
            avg('lat').alias('lat_mean'),
            avg('lon').alias('lon_mean'),
            avg('alt').alias('alt_mean'),
            smin('alt').alias('alt_min'),
            smax('alt').alias('alt_max'),
            avg('hr').alias('hr_mean'),
            smax('hr').alias('hr_max'),
            avg('temp').alias('temp_mean'),
            count('lat').alias('n_samples'),
        )
        .select(
            col('window.start').alias('fenster_start'),
            col('window.end').alias('fenster_ende'),
            'lat_mean', 'lon_mean', 'alt_mean', 'alt_min', 'alt_max',
            'hr_mean', 'hr_max', 'temp_mean', 'n_samples',
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
                'alt_diff':      fmt_num(alt_diff),
                'hr_mean':       fmt_num(row['hr_mean'], decimals=1),
                'hr_max':        fmt_num(row['hr_max'],  decimals=0),
                'temp_mean':     fmt_num(row['temp_mean'], decimals=1),
                'n_samples':     str(row['n_samples']),
            })
        print(f'[Redis] Epoch {epoch_id}: {len(rows)} Fenster geschrieben')

        rebuild_live_map(
            r            = r,
            key_pattern  = f'{REDIS_PREFIX}:*',
            lat_field    = 'lat_mean',
            lon_field    = 'lon_mean',
            color_field  = 'hr_mean',
            color_low    = 60.0,
            color_high   = 180.0,
            popup_fields = ['fenster_start', 'hr_mean', 'hr_max', 'alt_mean', 'temp_mean', 'n_samples'],
            out_html     = OUT_HTML,
            title        = 'Garmin – Herzfrequenz',
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
        print('Verwendung: python part7_garmin_bonus.py [kafka-send|stream]')
        sys.exit(1)


if __name__ == '__main__':
    main()
