import time
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, window, avg, to_timestamp, rand, sqrt
from pyspark.sql import SparkSession

HOST_IP = "10.248.16.109"

spark = (
    SparkSession.builder
    .appName('Journey-Part3')
    .master(f'spark://{HOST_IP}:7077')
    .config('spark.driver.host', HOST_IP)
    .config('spark.driver.bindAddress', HOST_IP)
    .config('spark.eventLog.enabled', 'true')
    .config('spark.eventLog.dir', 'file:///tmp/spark-events')
    .getOrCreate()
)

# Load data
df = pd.read_csv('/home/bfh/rtdp/bfh-rtdp/roller/RawData.csv', sep=';', decimal=',')
df.columns = ['time_s', 'gx', 'gy', 'gz', 'absolute']
df['timestamp'] = df['time_s'] + time.time()

# Spark DataFrame
sdf = spark.createDataFrame(df[['timestamp', 'gx', 'gy', 'gz', 'absolute']]) \
    .withColumn('zeit', to_timestamp(col('timestamp').cast('long')))

# Avg absolute per second
result = (
    sdf.groupBy(window('zeit', '1 second'))
    .agg(avg('absolute').alias('avg_absolute'))
    .orderBy('window')
    .select(col('window.start').alias('start'), 'avg_absolute')
    .toPandas()
)

# Avg gx/gy/gz per second
axes_result = (
    sdf.groupBy(window('zeit', '1 second'))
    .agg(avg('gx').alias('avg_gx'), avg('gy').alias('avg_gy'), avg('gz').alias('avg_gz'))
    .orderBy('window')
    .select(col('window.start').alias('start'), 'avg_gx', 'avg_gy', 'avg_gz')
    .toPandas()
)

# Plot all in one PNG
fig, axes = plt.subplots(3, 1, figsize=(14, 12))

axes[0].plot(df['time_s'], df['absolute'], linewidth=0.8, color='steelblue')
axes[0].set_title('Rohdaten – Absolutwert (rad/s)')
axes[0].set_xlabel('Zeit (s)')
axes[0].set_ylabel('rad/s')

t = (result['start'] - result['start'].min()).dt.total_seconds()
axes[1].bar(t, result['avg_absolute'], width=0.8, color='tomato', alpha=0.8)
axes[1].set_title('Ø Absolutwert pro Sekunde')
axes[1].set_xlabel('Zeit (s)')
axes[1].set_ylabel('rad/s')

t2 = (axes_result['start'] - axes_result['start'].min()).dt.total_seconds()
axes[2].plot(t2, axes_result['avg_gx'], label='gx')
axes[2].plot(t2, axes_result['avg_gy'], label='gy')
axes[2].plot(t2, axes_result['avg_gz'], label='gz')
axes[2].set_title('Ø gx / gy / gz pro Sekunde')
axes[2].set_xlabel('Zeit (s)')
axes[2].set_ylabel('rad/s')
axes[2].legend()

plt.tight_layout()
out = '/home/bfh/rtdp/bfh-rtdp/journey/part3_result.png'
plt.savefig(out, dpi=150)
print(f'Saved: {out}')

spark.stop()
