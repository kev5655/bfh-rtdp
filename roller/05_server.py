
from flask import Flask, request
import json
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

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

@server.route('/generator', methods=['POST'])
def generator():
    data = json.loads(request.data)
    print(data)

    try:
        point = influxdb_client.Point("light") \
            .field("lx", float(data["lx"])) \
            .time(int(float(data["time_sec"]) * 1_000_000_000), write_precision='ns')
        write_api.write(bucket=bucket, org=org, record=point)
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

    return data



# Entry point to this script
if __name__ == '__main__':
    server.run(host='0.0.0.0', port=5000)

