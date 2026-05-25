#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
if [[ -z "${HOST_IP}" ]]; then
  HOST_IP="localhost"
fi

has_systemd() {
  command -v systemctl >/dev/null 2>&1
}

unit_exists() {
  local unit="$1"
  systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -qx "$unit"
}

restart_unit_if_exists() {
  local unit="$1"
  if has_systemd && unit_exists "$unit"; then
    echo "[service] restarting $unit"
    systemctl restart "$unit"
    return 0
  fi
  return 1
}

wait_for_port() {
  local port="$1"
  local name="$2"
  local timeout_s="${3:-30}"
  local i=0
  while [[ $i -lt $timeout_s ]]; do
    if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :$port" | grep -q LISTEN; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  echo "[$name] warning: port $port not ready after ${timeout_s}s"
  return 1
}

wait_for_kafka_api() {
  local timeout_s="${1:-40}"
  local i=0
  while [[ $i -lt $timeout_s ]]; do
    if [[ -x "/opt/kafka/bin/kafka-broker-api-versions.sh" ]] && \
       /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  echo "[kafka] warning: broker API not ready after ${timeout_s}s"
  return 1
}

start_zookeeper() {
  if restart_unit_if_exists "zookeeper.service"; then
    wait_for_port 2181 "zookeeper" 30 || true
    return
  fi
  if [[ -x "/opt/kafka/bin/zookeeper-server-start.sh" ]]; then
    echo "[zookeeper] restarting via Kafka scripts"
    /opt/kafka/bin/zookeeper-server-stop.sh >/dev/null 2>&1 || true
    /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
    wait_for_port 2181 "zookeeper" 30 || true
    return
  fi
  echo "[zookeeper] not found (skipped)"
}

start_kafka() {
  if restart_unit_if_exists "kafka.service"; then
    wait_for_port 9092 "kafka" 40 || true
    wait_for_kafka_api 40 || true
    return
  fi
  if [[ -x "/opt/kafka/bin/kafka-server-start.sh" ]]; then
    echo "[kafka] restarting via Kafka scripts"
    local attempt
    for attempt in 1 2 3; do
      /opt/kafka/bin/kafka-server-stop.sh >/dev/null 2>&1 || true
      sleep 2
      /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
      if wait_for_port 9092 "kafka" 25 && wait_for_kafka_api 25; then
        echo "[kafka] ready (attempt $attempt)"
        return
      fi
      echo "[kafka] startup retry ($attempt/3)"
      sleep 8
    done
    echo "[kafka] failed to become ready after retries"
    return
  fi
  echo "[kafka] not found (skipped)"
}

start_redis() {
  if restart_unit_if_exists "redis-server.service" || restart_unit_if_exists "redis.service"; then
    return
  fi
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli shutdown nosave >/dev/null 2>&1 || true
  fi
  if command -v redis-server >/dev/null 2>&1; then
    echo "[redis] restarting via redis-server"
    redis-server --daemonize yes
    return
  fi
  echo "[redis] not found (skipped)"
}

start_prometheus() {
  if restart_unit_if_exists "prometheus.service"; then
    return
  fi
  echo "[prometheus] restarting via binary"
  pkill -f "[p]rometheus|/opt/prometheus/prometheus" >/dev/null 2>&1 || true

  # Prefer local install style:
  #   cd /opt/prometheus
  #   prometheus &
  if [[ -x "/opt/prometheus/prometheus" ]]; then
    local cfg=""
    if [[ -f "/opt/prometheus/prometheus.yml" ]]; then
      cfg="--config.file=/opt/prometheus/prometheus.yml"
    fi
    nohup /opt/prometheus/prometheus $cfg --web.listen-address=":9090" >"$LOG_DIR/prometheus.log" 2>&1 &
    return
  fi

  if command -v prometheus >/dev/null 2>&1; then
    local cfg=""
    if [[ -f "/etc/prometheus/prometheus.yml" ]]; then
      cfg="--config.file=/etc/prometheus/prometheus.yml"
    elif [[ -f "$BASE_DIR/prometheus.yml" ]]; then
      cfg="--config.file=$BASE_DIR/prometheus.yml"
    fi
    nohup prometheus $cfg --web.listen-address=":9090" >"$LOG_DIR/prometheus.log" 2>&1 &
    return
  fi
  echo "[prometheus] not found (skipped)"
}

start_grafana() {
  if restart_unit_if_exists "grafana-server.service" || restart_unit_if_exists "grafana.service"; then
    return
  fi

  echo "[grafana] restarting via binary"
  pkill -f "[g]rafana-server|/opt/grafana_13_0_1/bin/grafana" >/dev/null 2>&1 || true

  # Prefer grafana-server when available.
  if command -v grafana-server >/dev/null 2>&1; then
    nohup grafana-server --homepath /opt/grafana_13_0_1 >"$LOG_DIR/grafana.log" 2>&1 &
    return
  fi

  # Grafana tar installs often expose only ./bin/grafana; start it in server mode.
  if [[ -x "/opt/grafana_13_0_1/bin/grafana" ]]; then
    nohup /opt/grafana_13_0_1/bin/grafana server --homepath /opt/grafana_13_0_1 >"$LOG_DIR/grafana.log" 2>&1 &
    return
  fi

  echo "[grafana] not found (skipped)"
}

start_spark() {
  if [[ -x "/opt/spark/sbin/start-master.sh" ]]; then
    echo "[spark] restarting standalone + history server"
    /opt/spark/sbin/stop-worker.sh >/dev/null 2>&1 || true
    /opt/spark/sbin/stop-master.sh >/dev/null 2>&1 || true
    /opt/spark/sbin/stop-history-server.sh >/dev/null 2>&1 || true

    mkdir -p /tmp/spark-events
    /opt/spark/sbin/start-master.sh
    /opt/spark/sbin/start-worker.sh "spark://$HOST_IP:7077"
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:/tmp/spark-events -Dspark.history.ui.port=18080" \
      /opt/spark/sbin/start-history-server.sh
    return
  fi
  echo "[spark] not found (skipped)"
}

start_hadoop_optional() {
  if [[ "${ENABLE_HADOOP:-0}" != "1" ]]; then
    echo "[hadoop] skipped (set ENABLE_HADOOP=1 to start)"
    return
  fi
  if [[ -x "/opt/hadoop/sbin/start-dfs.sh" ]]; then
    echo "[hadoop] restarting HDFS"
    /opt/hadoop/sbin/stop-dfs.sh >/dev/null 2>&1 || true
    /opt/hadoop/sbin/start-dfs.sh >/dev/null 2>&1 || true
    if [[ -x "/opt/hadoop/sbin/start-yarn.sh" ]]; then
      /opt/hadoop/sbin/stop-yarn.sh >/dev/null 2>&1 || true
      /opt/hadoop/sbin/start-yarn.sh >/dev/null 2>&1 || true
    fi
    return
  fi
  echo "[hadoop] not found (optional, skipped)"
}

check_port() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :$port" | grep -q LISTEN
  else
    return 1
  fi
}

echo "Starting platform services (restart if running)..."
start_prometheus
start_grafana
start_zookeeper
start_kafka
start_redis
start_spark
start_hadoop_optional

# give services a moment
sleep 2

echo
printf 'Access URLs:\n'
printf 'Prometheus: http://%s:9090\n' "$HOST_IP"
printf 'Grafana:    http://%s:3000\n' "$HOST_IP"
printf 'Spark UI:   http://%s:8080\n' "$HOST_IP"
printf 'Spark Hist: http://%s:18080\n' "$HOST_IP"
printf 'Kafka:      %s:9092\n' "$HOST_IP"
printf 'Zookeeper:  %s:2181\n' "$HOST_IP"
printf 'Redis:      %s:6379\n' "$HOST_IP"
printf 'Hadoop UI:  http://%s:9870 (if installed)\n' "$HOST_IP"

echo
echo "Port checks (LISTEN):"
for p in 9090 3000 2181 9092 6379 8080 18080 9870; do
  if check_port "$p"; then
    echo "  - $p: up"
  else
    echo "  - $p: down or not installed"
  fi
done

echo
echo "You can now run Python scripts, e.g.:"
echo "  cd $BASE_DIR"
echo "  python3 part4_kafka_streaming.py"
