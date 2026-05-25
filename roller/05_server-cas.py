
from flask import Flask, request
import json
from datetime import datetime, timezone
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from cassandra.cluster import Cluster

server = Flask(__name__)

# InfluxDB settings (adjust as needed)
url = "http://localhost:8086"
token = "7GI7A2cyVrR7_GsmFI-CQzPFWHbguzIJmHj_XvfrmttOVnEqCKn6NefOYnBdybQVqEmZAu8gfdYbyqjgblmB6w=="
org = "bfh"
bucket = "bfh"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Cassandra settings
cassandra_hosts = ["127.0.0.1"]
cassandra_keyspace = "bfh"
cassandra_table = "sensor"

cassandra_cluster = Cluster(cassandra_hosts)
cassandra_session = cassandra_cluster.connect()

cassandra_session.execute(
    f"""
    CREATE KEYSPACE IF NOT EXISTS {cassandra_keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
)

cassandra_session.set_keyspace(cassandra_keyspace)
cassandra_session.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {cassandra_table} (
        sensor_id text,
        day_bucket date,
        ts timestamp,
        lx double,
        PRIMARY KEY ((sensor_id, day_bucket), ts)
    ) WITH CLUSTERING ORDER BY (ts ASC)
    """
)

insert_cassandra_stmt = cassandra_session.prepare(
    f"""
    INSERT INTO {cassandra_table} (sensor_id, day_bucket, ts, lx)
    VALUES (?, ?, ?, ?)
    """
)

@server.route('/generator', methods=['POST'])
def generator():
    data = json.loads(request.data)
    print(data)

    id = data.get("id", "light")
    time_sec = float(data["time_sec"])
    lx = float(data["lx"])

    ts_dt = datetime.fromtimestamp(time_sec, tz=timezone.utc)
    day_bucket = ts_dt.date()

    try:
        point = influxdb_client.Point(id) \
            .field("lx", lx) \
            .time(int(time_sec * 1_000_000_000), write_precision='ns')
        write_api.write(bucket=bucket, org=org, record=point)
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

    try:
        cassandra_session.execute(
            insert_cassandra_stmt,
            (id, day_bucket, ts_dt, lx),
        )
    except Exception as e:
        print(f"Error writing to Cassandra: {e}")

    return data



# Entry point to this script
if __name__ == '__main__':
    server.run(host='0.0.0.0', port=5000)

