# bfh-rtdp – Real-Time Data Pipeline

Dieses Repository enthält alle Notebooks und Skripte für die Challenge Journey des Kurses RTDP.

---

## Verzeichnisstruktur

```
bfh-rtdp/
├── journey/                    ← Challenge Journey (Teile 1–7)
│   ├── server.py               ← Flask-Server (SensorLogger → Kafka)
│   ├── gyroscope_generator.py  ← Replay RawData.csv → Kafka
│   ├── walk_generator.py       ← Replay walk.csv → Kafka
│   ├── part3_spark_static.ipynb    ← Statische Spark-Analyse (Gyroscop)
│   ├── part4_kafka_streaming.ipynb ← Kafka + Spark Structured Streaming
│   ├── part5_redis.ipynb           ← Redis Ergebnis-Cache
│   ├── part6_geodata.ipynb         ← Geodaten + Höhenanalyse (Folium)
│   └── part7_garmin_bonus.ipynb    ← Bonus: Garmin FIT-Analyse
├── roller/                     ← Challenge Roller (Teile 1–6)
│   └── RawData.csv             ← Gyroscop-Daten (phyphox)
data/
├── walk.csv                    ← GPS-Walk (phyphox)
├── garmin_walk.csv             ← Garmin CSV (Bonus)
├── 22996735627_ACTIVITY.fit    ← Garmin FIT (Bonus)
├── Experiment01.csv            ← Licht-Experiment
└── Light.csv                   ← Licht-Daten
notebooks/                      ← Kurs-Beispiel-Notebooks (07–13)
```

---

## Schritt-für-Schritt: Alles starten

### 1. Zookeeper + Kafka starten (für Teil 4 + 5)

```bash
# Terminal 1
zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties

# Terminal 2
kafka-server-start.sh /opt/kafka/config/server.properties
```

### 2. Kafka Topics erstellen (nur einmalig nötig)

```bash
kafka-topics.sh --create --topic journey.gyroscope --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic journey.location  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# Prüfen
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 3. Plattform-Dienste zentral starten/stoppen

```bash
cd /home/bfh/rtdp/bfh-rtdp/journey

# Startet oder restarten: Prometheus, Grafana, Zookeeper, Kafka, Redis, Spark
# Hadoop wird optional gestartet (nur wenn /opt/hadoop vorhanden ist)
./start_platform.sh

# Alles wieder stoppen
./stop_platform.sh
```

Am Ende von `./start_platform.sh` bekommst du die URLs mit Server-IP und Port ausgegeben.

### 4. Python-Skripte ausführen

Alle Skripte direkt in der Shell starten – kein Jupyter, kein PySpark-Befehl nötig.

**Teil 3 – Statische Spark-Analyse:**
```bash
cd /home/bfh/rtdp/bfh-rtdp/journey
python3 part3_spark_static.py
# → Speichert: part3_gyroscope_analysis.png, part3_gyroscope_axes.png
```

**Teil 4 – Kafka + Spark Streaming:**
```bash
# Terminal A – Generator starten
python3 gyroscope_generator.py

# Terminal B – Streaming starten (Ctrl+C zum Stoppen)
python3 part4_kafka_streaming.py
# → Gibt Ergebnisse alle 5s in der Konsole aus
```

**Teil 5 – Redis:**
```bash
# Terminal A – Generator starten (falls nicht noch läuft)
python3 gyroscope_generator.py

# Terminal B – Stream in Redis schreiben (Ctrl+C zum Stoppen)
python3 part5_redis.py stream

# Terminal C – Aus Redis lesen und Plot speichern
python3 part5_redis.py read
# → Speichert: part5_redis_result.png
```

**Teil 6 – Geodaten-Karte:**
```bash
python3 part6_geodata.py
# → Speichert: part6_walk_map.html, part6_walk_analysis.png
```

**Teil 7 – Garmin Bonus:**
```bash
pip install fitparse   # einmalig
python3 part7_garmin_bonus.py
# → Speichert: part7_garmin_map.html, part7_garmin_analysis.png
```

---

## Ergebnisse anzeigen

Nach dem Ausführen der Skripte:

| Datei | Inhalt |
|-------|--------|
| `journey/part3_gyroscope_analysis.png` | Gyroscop Rohdaten + Spark-Mittelwert |
| `journey/part3_gyroscope_axes.png` | Alle 3 Gyroscop-Achsen |
| `journey/part5_redis_result.png` | Redis-Ergebnis Visualisierung |
| `journey/part6_walk_map.html` | Interaktive Karte: GPS-Pfad + Höhendifferenz-Marker |
| `journey/part6_walk_analysis.png` | Höhenprofil + Geschwindigkeit + Fenster-Analyse |
| `journey/part7_garmin_map.html` | Bonus: Garmin GPS + HR-Marker |
| `journey/part7_garmin_analysis.png` | Bonus: HR + Höhe + Temperatur |

**Karte im Browser öffnen:**
```
file:///home/bfh/rtdp/bfh-rtdp/journey/part6_walk_map.html
```

---

## Kafka Consumer (Debugging)

```bash
# Gyroscop-Daten live anzeigen
kafka-console-consumer.sh --topic journey.gyroscope --bootstrap-server localhost:9092 --from-beginning

# Walk-Daten live anzeigen
kafka-console-consumer.sh --topic journey.location --bootstrap-server localhost:9092 --from-beginning
```

## Redis löschen (zurücksetzen)

```bash
redis-cli flushdb
```

---

## Kafka Design-Entscheidungen (Screencast)

| Topic | Partitionen | Replikation | Begründung |
|-------|------------|-------------|------------|
| `journey.gyroscope` | 3 | 1* | 3 Partitionen = paralleles Lesen durch mehrere Consumer |
| `journey.location` | 3 | 1* | GPS-Daten pro Nutzer in eigene Partition |

*Replikation 1 = wir haben nur 1 Broker. In Produktion: Replikation 3.

**Partition-Strategie:** Bei vielen Nutzern gleichzeitig → Nutzer-ID als Partition-Key, damit alle Daten eines Nutzers in derselben Partition landen (Ordnung garantiert).

## Redis Design-Entscheidungen (Screencast)

```
Key:    journey:gyro:<fenster_start_unix_ms>
Typ:    Hash (Felder: avg_absolute, avg_gx, avg_gy, avg_gz, n_samples)
TTL:    300 Sekunden (5 Minuten)
```

**Begründung TTL:** Streaming-Ergebnisse sind Live-Daten. Nach 5 Minuten veraltet → automatisch gelöscht. Redis ist Cache, kein persistenter Store.