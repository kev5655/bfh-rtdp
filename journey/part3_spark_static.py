"""
Challenge Journey – Teil 3: Statische Spark-Analyse (Gyroscop)

Ziel: Mittelwert der Gyroscop-Absolutwerte (rad/s) pro Sekunde berechnen.
Daten: bfh-rtdp/roller/RawData.csv

Starten:
    python part3_spark_static.py
"""
import os
import sys
from shared import make_spark

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # kein Display nötig – speichert als PNG
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = make_spark('Journey-Part3')

# ---------------------------------------------------------------------------
# 1. Daten laden
# ---------------------------------------------------------------------------
# RawData.csv: Semikolon-getrennt, deutsches Dezimalkomma
df = pd.read_csv(
    '/home/bfh/rtdp/bfh-rtdp/roller/RawData.csv',
    sep=';',
    decimal=',',
    engine='python'
)
df.columns = ['time_s', 'gx', 'gy', 'gz', 'absolute']
print(f"Zeilen geladen: {len(df)}")
print(df.describe())

# ---------------------------------------------------------------------------
# 2. Timestamps konvertieren (relative Zeit → absolut)
# ---------------------------------------------------------------------------
# Startzeit aus phyphox-Metadaten; hier Beispielwert aus Experiment01.csv
MEASUREMENT_START_UNIX_S = 1732113693.0

df['timestamp'] = df['time_s'] + MEASUREMENT_START_UNIX_S
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
print(f"\nStart:  {df['datetime'].min()}")
print(f"Ende:   {df['datetime'].max()}")
print(f"Dauer:  {df['time_s'].max():.1f} Sekunden")

# ---------------------------------------------------------------------------
# 3. Spark DataFrame
# ---------------------------------------------------------------------------
df_spark = spark.createDataFrame(df[['timestamp', 'gx', 'gy', 'gz', 'absolute']])
df_spark = df_spark.withColumn('zeit', to_timestamp(col('timestamp').cast('long')))
df_spark.printSchema()
df_spark.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 4. Mittelwert pro Sekunde (Window-Aggregation)
# ---------------------------------------------------------------------------
ergebnis = (
    df_spark
    .groupBy(window(col('zeit'), '1 second'))
    .agg(
        avg('absolute').alias('avg_absolute'),
        avg('gx').alias('avg_gx'),
        avg('gy').alias('avg_gy'),
        avg('gz').alias('avg_gz'),
        count('absolute').alias('n_samples'),
    )
    .orderBy('window')
)
ergebnis.show(10, truncate=False)

result_pd = ergebnis.select(
    col('window.start').alias('start'),
    'avg_absolute', 'avg_gx', 'avg_gy', 'avg_gz', 'n_samples'
).toPandas()

print(f"\nAnzahl Fenster: {len(result_pd)}")
print(result_pd.head(10))

# ---------------------------------------------------------------------------
# 5. Visualisierung → PNG speichern
# ---------------------------------------------------------------------------
OUTPUT_DIR = '/home/bfh/rtdp/bfh-rtdp/journey'

# Plot 1: Rohdaten + Spark-Mittelwert
fig, axes = plt.subplots(2, 1, figsize=(14, 8))

axes[0].plot(df['time_s'], df['absolute'], alpha=0.5, linewidth=0.8, color='steelblue')
axes[0].set_title('Gyroscop Rohdaten – Absolutwert (rad/s)')
axes[0].set_xlabel('Zeit (s)')
axes[0].set_ylabel('Absolut (rad/s)')
axes[0].grid(True, alpha=0.3)

relative_start = (result_pd['start'] - result_pd['start'].min()).dt.total_seconds()
axes[1].bar(relative_start, result_pd['avg_absolute'], width=0.8, color='tomato', alpha=0.7)
axes[1].set_title('Spark-Ergebnis: Mittelwert Absolutwert pro Sekunde')
axes[1].set_xlabel('Zeit (s)')
axes[1].set_ylabel('Ø Absolut (rad/s)')
axes[1].grid(True, alpha=0.3, axis='y')

plt.tight_layout()
out1 = f'{OUTPUT_DIR}/part3_gyroscope_analysis.png'
plt.savefig(out1, dpi=150)
plt.close()
print(f"\nPlot gespeichert: {out1}")

# Plot 2: Alle 3 Achsen
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(df['time_s'], df['gx'], label='gx', alpha=0.6, linewidth=0.8)
ax.plot(df['time_s'], df['gy'], label='gy', alpha=0.6, linewidth=0.8)
ax.plot(df['time_s'], df['gz'], label='gz', alpha=0.6, linewidth=0.8)
ax.plot(df['time_s'], df['absolute'], label='absolut', color='black', linewidth=1.2)
ax.set_title('Gyroscop – alle Achsen (rad/s)')
ax.set_xlabel('Zeit (s)')
ax.set_ylabel('rad/s')
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
out2 = f'{OUTPUT_DIR}/part3_gyroscope_axes.png'
plt.savefig(out2, dpi=150)
plt.close()
print(f"Plot gespeichert: {out2}")

spark.stop()
print("\nFertig.")
