"""
Challenge Journey – Teil 4: Kafka + Spark Structured Streaming (Gyroscop)

Ziel: Gyroscop-Daten live aus Kafka lesen, Mittelwert pro 10s-Fenster berechnen.

Vorher starten:
    ./start_platform.sh

    # Danach in einem separaten Terminal den Generator starten
    python /home/bfh/rtdp/bfh-rtdp/journey/gyroscope_generator.py

Starten:
    python part4_kafka_streaming.py

Ergebnisse: werden in der Konsole angezeigt (alle 5 Sekunden)
Abbrechen: Ctrl+C
"""
import os
import sys
from shared import make_spark, kafka_stream

from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------------------------
# Spark Session mit Kafka-Package
# ---------------------------------------------------------------------------
spark = make_spark('Journey-Part4-Streaming', with_kafka=True)

# ---------------------------------------------------------------------------
# 1. Kafka Stream lesen
# ---------------------------------------------------------------------------
# Format pro Nachricht: timestamp_ms,gx,gy,gz,absolute
df_raw = kafka_stream(spark, 'journey.gyroscope')

# ---------------------------------------------------------------------------
# 2. Parsen: bytes → CSV-Felder
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# 3. Window-Aggregation
# Window:    10 Sekunden – genug Datenpunkte bei ~50Hz Gyroscop (~500 Samples)
# Watermark: 10 Sekunden – Toleranz für Netzwerkverzögerung (z.B. VPN)
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# 4. Stream starten – Ausgabe in Konsole
# ---------------------------------------------------------------------------
print("\nStream gestartet. Warte auf Daten vom Generator...")
print("Abbrechen mit Ctrl+C\n")

query = (
    df_result.writeStream
    .outputMode('update')
    .format('console')
    .option('truncate', False)
    .option('numRows', 20)
    .trigger(processingTime='5 seconds')
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStream gestoppt.")
    query.stop()
    spark.stop()
