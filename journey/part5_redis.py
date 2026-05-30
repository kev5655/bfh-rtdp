from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from shared import fmt_num, redis_client, write_redis_batch, run_stream

HOST_IP          = '10.248.16.109'
KAFKA_BROKER     = 'localhost:9092'
TOPIC            = 'journey.gyroscope'
REDIS_KEY_PREFIX = 'journey:gyro'

spark = (SparkSession.builder.appName('Journey-Part5-Redis')
    .master(f'spark://{HOST_IP}:7077')
    .config('spark.driver.host', HOST_IP)
    .config('spark.driver.bindAddress', HOST_IP)
    .config('spark.eventLog.enabled', 'true')
    .config('spark.eventLog.dir', 'file:///tmp/spark-events')
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate())

df_raw = (spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', KAFKA_BROKER)
    .option('subscribe', TOPIC)
    .option('startingOffsets', 'earliest')
    .load())

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
    .groupBy(window(col('zeit'), '1 seconds'))
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
        }, ttl=120)
    print(f"[Redis] Epoch {epoch_id}: {len(rows)} Fenster geschrieben")

print("\nStream → Redis gestartet. Abbrechen mit Ctrl+C\n")
query = (
    df_result.writeStream
    .outputMode('update')
    .foreachBatch(write_to_redis)
    .trigger(processingTime='1 seconds')
    .start()
)
run_stream(query, on_stop=spark.stop)
