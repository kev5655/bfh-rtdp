"""
Challenge Journey – Teil 5: Redis als Ergebnis-Cache

Ziel: Spark Streaming Ergebnisse in Redis schreiben und aus Redis lesen + visualisieren.

Redis Key-Design:
    Key:   journey:gyro:<fenster_start_unix_ms>
    Typ:   Hash (avg_absolute, avg_gx, avg_gy, avg_gz, n_samples, fenster_start, fenster_ende)
    TTL:   300 Sekunden (5 Minuten)

Begründung TTL: Streaming-Ergebnisse sind Live-Cache – nach 5 Min. veraltet → auto-gelöscht.

Vorher starten:
    ./start_platform.sh

    # Danach in einem separaten Terminal den Generator starten
    python /home/bfh/rtdp/bfh-rtdp/journey/gyroscope_generator.py

Starten (2 Modi):
    python part5_redis.py stream    # Stream starten (schreibt in Redis), Ctrl+C zum Stoppen
    python part5_redis.py read      # Redis lesen und PNG-Plot speichern
"""
import os
import sys
import redis
from shared import make_spark, kafka_stream, fmt_num, redis_client, write_redis_batch, run_stream, REDIS_TTL
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

REDIS_HOST       = 'localhost'
REDIS_PORT       = 6379
REDIS_KEY_PREFIX = 'journey:gyro'
REDIS_TTL        = 300
OUTPUT_DIR       = '/home/bfh/rtdp/bfh-rtdp/journey'

# ---------------------------------------------------------------------------
# Modus bestimmen
# ---------------------------------------------------------------------------
mode = sys.argv[1] if len(sys.argv) > 1 else 'stream'

if mode not in ('stream', 'read'):
    print("Verwendung: python part5_redis.py [stream|read]")
    sys.exit(1)

# ---------------------------------------------------------------------------
# MODUS: stream – Kafka lesen und in Redis schreiben
# ---------------------------------------------------------------------------
if mode == 'stream':
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    spark = make_spark('Journey-Part5-Redis', with_kafka=True)

    # Kafka Stream
    df_raw = kafka_stream(spark, 'journey.gyroscope')

    df_parsed = (
        df_raw
        .select(split(col('value').cast('string'), ',').alias('f'))
        .select(
            to_timestamp((col('f')[0].cast('long') / 1000).cast('long')).alias('zeit'),
            col('f')[1].cast(DoubleType()).alias('gx'),
            col('f')[2].cast(DoubleType()).alias('gy'),
            col('f')[3].cast(DoubleType()).alias('gz'),
            col('f')[4].cast(DoubleType()).alias('absolute'),
        )
    )

    df_result = (
        df_parsed
        .withWatermark('zeit', '10 seconds')
        .groupBy(window(col('zeit'), '10 seconds'))
        .agg(
            avg('absolute').alias('avg_absolute'),
            avg('gx').alias('avg_gx'),
            avg('gy').alias('avg_gy'),
            avg('gz').alias('avg_gz'),
            count('absolute').alias('n_samples'),
        )
        .select(
            col('window.start').alias('fenster_start'),
            col('window.end').alias('fenster_ende'),
            'avg_absolute', 'avg_gx', 'avg_gy', 'avg_gz', 'n_samples',
        )
    )

    def write_to_redis(batch_df, epoch_id):
        rows = batch_df.collect()
        if not rows:
            return
        r = redis_client()

        for row in rows:
            ts_ms = int(row['fenster_start'].timestamp() * 1000)
            key   = f'{REDIS_KEY_PREFIX}:{ts_ms}'
            write_redis_batch(r, key, {
                'fenster_start': str(row['fenster_start']),
                'fenster_ende':  str(row['fenster_ende']),
                'avg_absolute':  fmt_num(row['avg_absolute']),
                'avg_gx':        fmt_num(row['avg_gx']),
                'avg_gy':        fmt_num(row['avg_gy']),
                'avg_gz':        fmt_num(row['avg_gz']),
                'n_samples':     str(row['n_samples']),
            }, ttl=REDIS_TTL)
        print(f"[Redis] Epoch {epoch_id}: {len(rows)} Fenster geschrieben")

    print("\nStream → Redis gestartet. Abbrechen mit Ctrl+C\n")
    query = (
        df_result.writeStream
        .outputMode('update')
        .foreachBatch(write_to_redis)
        .trigger(processingTime='5 seconds')
        .start()
    )
    run_stream(query, on_stop=spark.stop)

# ---------------------------------------------------------------------------
# MODUS: read – Redis lesen und PNG-Plot speichern
# ---------------------------------------------------------------------------
elif mode == 'read':
    r = redis_client()
    keys = r.keys(f'{REDIS_KEY_PREFIX}:*')
    print(f"Gefundene Keys in Redis: {len(keys)}")

    if not keys:
        print("Keine Daten in Redis. Zuerst 'stream' Modus starten und Generator ausführen.")
        sys.exit(0)

    records = []
    for key in keys:
        data = r.hgetall(key)
        decoded = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
        records.append(decoded)

    df = pd.DataFrame(records)
    df['avg_absolute']  = df['avg_absolute'].astype(float)
    df['n_samples']     = df['n_samples'].astype(int)
    df['fenster_start'] = pd.to_datetime(df['fenster_start'])
    df = df.sort_values('fenster_start').reset_index(drop=True)

    print(f"\nRedis Ergebnisse ({len(df)} Fenster):")
    print(df[['fenster_start', 'avg_absolute', 'n_samples']].to_string())

    # Plot
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    rel_time = (df['fenster_start'] - df['fenster_start'].min()).dt.total_seconds()

    axes[0].bar(rel_time, df['avg_absolute'], width=8, color='tomato', alpha=0.7)
    axes[0].set_title('Ø Gyroscop-Absolutwert pro Fenster (aus Redis)')
    axes[0].set_xlabel('Zeit (s)')
    axes[0].set_ylabel('Ø abs (rad/s)')
    axes[0].grid(True, alpha=0.3, axis='y')

    axes[1].bar(rel_time, df['n_samples'], width=8, color='steelblue', alpha=0.7)
    axes[1].set_title('Anzahl Samples pro Fenster')
    axes[1].set_xlabel('Zeit (s)')
    axes[1].set_ylabel('Anzahl Messwerte')
    axes[1].grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    out = f'{OUTPUT_DIR}/part5_redis_result.png'
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"\nPlot gespeichert: {out}")
