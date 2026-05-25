"""
Challenge Journey – Flask Server
Empfängt Sensordaten vom SensorLogger App und schreibt sie an Kafka.

Starten:
    python server.py

Endpunkte:
    GET  /           → Healthcheck
    POST /data        → SensorLogger App-Daten (JSON)
    POST /gyroscope   → Gyroscop-Daten (JSON)
    POST /location    → GPS-Daten (JSON)
"""
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import time

server = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC_GYROSCOPE = 'journey.gyroscope'
TOPIC_LOCATION  = 'journey.location'

# Mapping SensorLogger Sensor-Namen → Kafka Topic
SENSOR_TOPIC_MAP = {
    'gyroscope':  TOPIC_GYROSCOPE,
    'Gyroscope':  TOPIC_GYROSCOPE,
    'location':   TOPIC_LOCATION,
    'Location':   TOPIC_LOCATION,
    'gps':        TOPIC_LOCATION,
}


@server.route('/')
def index():
    return 'Journey Server running'


@server.route('/data', methods=['POST'])
def receive_data():
    """Universeller Endpunkt für SensorLogger App."""
    try:
        data = json.loads(request.data)
        print(f"[/data] received: {data}")

        # SensorLogger schickt entweder einzelne Messung oder Liste
        messages = data if isinstance(data, list) else [data]

        for msg in messages:
            sensor = msg.get('sensor', msg.get('name', 'unknown'))
            topic = SENSOR_TOPIC_MAP.get(sensor)

            if topic == TOPIC_GYROSCOPE:
                ts  = msg.get('time', int(time.time() * 1000))
                gx  = msg.get('x', msg.get('gx', 0))
                gy  = msg.get('y', msg.get('gy', 0))
                gz  = msg.get('z', msg.get('gz', 0))
                import math
                abs_val = math.sqrt(float(gx)**2 + float(gy)**2 + float(gz)**2)
                value = f"{ts},{gx},{gy},{gz},{abs_val:.6f}"
                producer.send(TOPIC_GYROSCOPE, value=value.encode('utf-8'))

            elif topic == TOPIC_LOCATION:
                ts  = msg.get('time', int(time.time() * 1000))
                lat = msg.get('lat', msg.get('latitude', 0))
                lon = msg.get('lon', msg.get('longitude', 0))
                alt = msg.get('altitude', msg.get('alt', 0))
                spd = msg.get('speed', 0)
                value = f"{ts},{lat},{lon},{alt},{spd}"
                producer.send(TOPIC_LOCATION, value=value.encode('utf-8'))

            else:
                print(f"[/data] unbekannter Sensor: {sensor}")

        producer.flush()
        return jsonify({'status': 'ok', 'count': len(messages)})

    except Exception as e:
        print(f"[/data] error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400


@server.route('/gyroscope', methods=['POST'])
def receive_gyroscope():
    """Direkter Endpunkt für Gyroscop-Daten."""
    try:
        data = json.loads(request.data)
        ts  = data.get('time', int(time.time() * 1000))
        gx  = float(data.get('x', data.get('gx', 0)))
        gy  = float(data.get('y', data.get('gy', 0)))
        gz  = float(data.get('z', data.get('gz', 0)))
        import math
        abs_val = math.sqrt(gx**2 + gy**2 + gz**2)
        value = f"{ts},{gx},{gy},{gz},{abs_val:.6f}"
        producer.send(TOPIC_GYROSCOPE, value=value.encode('utf-8'))
        producer.flush()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400


@server.route('/location', methods=['POST'])
def receive_location():
    """Direkter Endpunkt für GPS/Location-Daten."""
    try:
        data = json.loads(request.data)
        ts  = data.get('time', int(time.time() * 1000))
        lat = float(data.get('lat', data.get('latitude', 0)))
        lon = float(data.get('lon', data.get('longitude', 0)))
        alt = float(data.get('altitude', data.get('alt', 0)))
        spd = float(data.get('speed', 0))
        value = f"{ts},{lat},{lon},{alt},{spd}"
        producer.send(TOPIC_LOCATION, value=value.encode('utf-8'))
        producer.flush()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400


if __name__ == '__main__':
    server.run(host='0.0.0.0', port=5000)
