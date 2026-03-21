import argparse
import json
import re
import subprocess
import sys
import time

import requests


DEFAULT_ENDPOINT = "http://127.0.0.1:5000/generator"
DEFAULT_SENSOR_PATTERN = r"light|illuminance"


def run_adb_command(args, serial=None):
	command = ["adb"]
	if serial:
		command.extend(["-s", serial])
	command.extend(args)

	result = subprocess.run(command, capture_output=True, text=True, check=False)
	if result.returncode != 0:
		stderr = result.stderr.strip() or result.stdout.strip()
		raise RuntimeError(f"adb command failed: {' '.join(command)}\n{stderr}")
	return result.stdout


def get_connected_devices():
	output = run_adb_command(["devices"])
	devices = []
	for line in output.splitlines()[1:]:
		line = line.strip()
		if not line:
			continue
		parts = line.split()
		if len(parts) >= 2 and parts[1] == "device":
			devices.append(parts[0])
	return devices


def extract_value_from_line(line):
	keyword_patterns = [
		r"(?:lux|lx|illuminance|value|last(?:\s+value)?|data)\s*[:=]\s*\[?\s*(-?\d+(?:\.\d+)?)",
		r"=\s*\[\s*(-?\d+(?:\.\d+)?)",
	]

	for pattern in keyword_patterns:
		match = re.search(pattern, line, re.IGNORECASE)
		if match:
			return float(match.group(1))

	return None


def extract_sensor_value(dump_text, sensor_pattern):
	sensor_regex = re.compile(sensor_pattern, re.IGNORECASE)
	lines = dump_text.splitlines()

	for index, line in enumerate(lines):
		if not sensor_regex.search(line):
			continue

		window = lines[index:index + 25]

		for candidate in window:
			value = extract_value_from_line(candidate)
			if value is not None:
				return value

		saw_event_section = False
		for candidate in window:
			if "event" in candidate.lower():
				saw_event_section = True
				continue
			if saw_event_section:
				match = re.search(r"\[\s*(-?\d+(?:\.\d+)?)", candidate)
				if match:
					return float(match.group(1))

	raise ValueError(
		"Keinen Sensorwert im dumpsys sensorservice Output gefunden. "
		"Pruefe, ob dein Geraet einen Lichtsensor hat und ob das Regex passt."
	)


def fetch_sensor_value(serial, sensor_pattern):
	dump_text = run_adb_command(["shell", "dumpsys", "sensorservice"], serial=serial)
	return extract_sensor_value(dump_text, sensor_pattern)


def post_reading(endpoint, sensor_value):
	payload = {
		"time_sec": time.time(),
		"lx": sensor_value,
	}
	response = requests.post(endpoint, json=payload, timeout=10)
	response.raise_for_status()
	return payload, response


def parse_args():
	parser = argparse.ArgumentParser(
		description="Liest Sensordaten per adb von einem Android-Geraet und sendet sie an localhost:5000."
	)
	parser.add_argument("--serial", help="ADB serial des Geraets. Wenn leer, wird das erste verbundene Geraet verwendet.")
	parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="HTTP Endpoint fuer den POST Request.")
	parser.add_argument("--interval", type=float, default=1.0, help="Polling-Intervall in Sekunden.")
	parser.add_argument("--sensor-pattern", default=DEFAULT_SENSOR_PATTERN, help="Regex fuer den Sensornamen im dumpsys Output.")
	parser.add_argument("--once", action="store_true", help="Nur einen Messwert lesen und senden.")
	return parser.parse_args()


def resolve_serial(serial):
	if serial:
		return serial

	devices = get_connected_devices()
	if not devices:
		raise RuntimeError("Kein Android-Geraet per adb gefunden.")
	return devices[0]


def main():
	args = parse_args()

	try:
		serial = resolve_serial(args.serial)
	except Exception as error:
		print(error, file=sys.stderr)
		return 1

	print(f"Verwende Geraet: {serial}")
	print(f"Sende an: {args.endpoint}")

	last_value = None

	while True:
		try:
			sensor_value = fetch_sensor_value(serial, args.sensor_pattern)

			if sensor_value != last_value:
				payload, response = post_reading(args.endpoint, sensor_value)
				print(json.dumps({
					"payload": payload,
					"status": response.status_code,
					"response": response.text,
				}, ensure_ascii=True))
				last_value = sensor_value
			else:
				print(f"Unveraenderter Sensorwert: {sensor_value}")
		except KeyboardInterrupt:
			print("Abbruch durch Benutzer.")
			return 0
		except Exception as error:
			print(f"Fehler: {error}", file=sys.stderr)

		if args.once:
			return 0

		time.sleep(args.interval)


if __name__ == "__main__":
	raise SystemExit(main())
