#!/usr/bin/env bash
set -euo pipefail

stop_unit_if_exists() {
  local unit="$1"
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl list-unit-files --type=service --no-legend 2>/dev/null | awk '{print $1}' | grep -qx "$unit"; then
      echo "[service] stopping $unit"
      systemctl stop "$unit" || true
      return 0
    fi
  fi
  return 1
}

echo "Stopping platform services..."

# Prometheus
if ! stop_unit_if_exists "prometheus.service"; then
  pkill -f "[p]rometheus" >/dev/null 2>&1 || true
fi

# Grafana
if ! stop_unit_if_exists "grafana-server.service" && ! stop_unit_if_exists "grafana.service"; then
  pkill -f "[g]rafana-server|/opt/grafana_13_0_1/bin/grafana" >/dev/null 2>&1 || true
fi

# Kafdrop
pkill -f "[k]afdrop" >/dev/null 2>&1 || true

# Kafka + Zookeeper
if ! stop_unit_if_exists "kafka.service"; then
  kafka-server-stop.sh >/dev/null 2>&1 || true
  pkill -f "[k]afka\.Kafka" >/dev/null 2>&1 || true
fi
if ! stop_unit_if_exists "zookeeper.service"; then
  zkServer.sh stop >/dev/null 2>&1 || true
  pkill -f "[Q]uorumPeerMain" >/dev/null 2>&1 || true
fi

# Redis
if ! stop_unit_if_exists "redis-server.service" && ! stop_unit_if_exists "redis.service"; then
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli shutdown nosave >/dev/null 2>&1 || true
  fi
  pkill -f "[r]edis-server" >/dev/null 2>&1 || true
fi

# Spark standalone + history
if [[ -x "/opt/spark/sbin/stop-worker.sh" ]]; then
  /opt/spark/sbin/stop-worker.sh >/dev/null 2>&1 || true
fi
if [[ -x "/opt/spark/sbin/stop-master.sh" ]]; then
  /opt/spark/sbin/stop-master.sh >/dev/null 2>&1 || true
fi
if [[ -x "/opt/spark/sbin/stop-history-server.sh" ]]; then
  /opt/spark/sbin/stop-history-server.sh >/dev/null 2>&1 || true
fi
pkill -f "[o]rg\.apache\.spark\.deploy" >/dev/null 2>&1 || true

# Hadoop (optional)
if [[ -x "/opt/hadoop/sbin/stop-yarn.sh" ]]; then
  /opt/hadoop/sbin/stop-yarn.sh >/dev/null 2>&1 || true
fi
if [[ -x "/opt/hadoop/sbin/stop-dfs.sh" ]]; then
  /opt/hadoop/sbin/stop-dfs.sh >/dev/null 2>&1 || true
fi
pkill -f "[N]ameNode|[D]ataNode|[R]esourceManager|[N]odeManager" >/dev/null 2>&1 || true

echo "All requested services stopped (where available)."
