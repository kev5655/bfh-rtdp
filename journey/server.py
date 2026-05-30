"""
Challenge Journey – Flask Server
Starten: python server.py
"""
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import math
import time

server = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC_GYROSCOPE = 'journey.gyroscope'
TOPIC_LOCATION  = 'journey.location'


@server.route('/')
def index():
    return 'Journey Server running'


@server.route('/gyroscope', methods=['POST'])
def receive_gyroscope():
    data = request.get_json()
    ts  = data.get('time', int(time.time() * 1000))
    gx  = float(data.get('x', 0))
    gy  = float(data.get('y', 0))
    gz  = float(data.get('z', 0))
    abs_val = math.sqrt(gx**2 + gy**2 + gz**2)
    producer.send(TOPIC_GYROSCOPE, f"{ts},{gx},{gy},{gz},{abs_val:.6f}".encode())
    producer.flush()
    return jsonify({'status': 'ok'})


@server.route('/location', methods=['POST'])
def receive_location():
    data = request.get_json()
    ts  = data.get('time', int(time.time() * 1000))
    lat = float(data.get('lat', 0))
    lon = float(data.get('lon', 0))
    alt = float(data.get('altitude', 0))
    spd = float(data.get('speed', 0))
    producer.send(TOPIC_LOCATION, f"{ts},{lat},{lon},{alt},{spd}".encode())
    producer.flush()
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    server.run(host='0.0.0.0', port=5000)
