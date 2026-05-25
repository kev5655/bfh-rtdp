"""
shared.py – Gemeinsame Hilfsfunktionen für alle Challenge-Journey-Skripte.

Importieren mit:
    from shared import make_spark, kafka_stream, fmt_num, write_redis_batch, rebuild_live_map
"""
import builtins
import os
import json
import time
from typing import Dict

import folium
import matplotlib
import matplotlib.colors as mcolors
import numpy as np
import redis as _redis_module

os.environ.setdefault('JAVA_HOME', '/usr/lib/jvm/java-17-openjdk-amd64')

KAFKA_BROKER = 'localhost:9092'
REDIS_HOST   = 'localhost'
REDIS_PORT   = 6379
REDIS_TTL    = 300

SPARK_KAFKA_PACKAGE = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def make_spark(app_name: str, with_kafka: bool = False):
    """Erstellt oder holt eine SparkSession (local[*]).
    
    Args:
        app_name:   Anzeigename in der Spark UI.
        with_kafka: True → Kafka-Package wird mitgeladen (streaming).
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master('local[*]')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.driver.memory', '2g')
    )
    if with_kafka:
        builder = builder.config('spark.jars.packages', SPARK_KAFKA_PACKAGE)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    print(f'Spark {spark.version} gestartet  [{app_name}]')
    return spark


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def kafka_stream(spark, topic: str, starting_offsets: str = 'earliest'):
    """Liest einen Kafka-Topic als unbegrenzten DataFrame (Structured Streaming).

    Args:
        spark:            SparkSession (muss mit with_kafka=True gebaut sein).
        topic:            Kafka-Topic-Name.
        starting_offsets: 'earliest' oder 'latest'.

    Returns:
        Raw-DataFrame mit Spalte 'value' (bytes).
    """
    return (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option('subscribe', topic)
        .option('startingOffsets', starting_offsets)
        .load()
    )


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def fmt_num(value, decimals: int = 6) -> str:
    """Formatiert einen numerischen Wert für Redis-Speicherung (kein Spark-round)."""
    if value is None:
        return ''
    return str(builtins.round(float(value), decimals))


def redis_client() -> _redis_module.Redis:
    """Gibt einen Redis-Client zurück."""
    return _redis_module.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def write_redis_batch(r: _redis_module.Redis, key: str, mapping: Dict[str, str], ttl: int = REDIS_TTL):
    """Schreibt ein Hash in Redis und setzt TTL."""
    r.hset(key, mapping=mapping)
    r.expire(key, ttl)


# ---------------------------------------------------------------------------
# Live-HTML-Karte
# ---------------------------------------------------------------------------

def rebuild_live_map(
    r: _redis_module.Redis,
    key_pattern: str,
    lat_field: str,
    lon_field: str,
    color_field: str,
    color_low: float,
    color_high: float,
    popup_fields: list,
    out_html: str,
    title: str = 'Live Stream Map',
    colormap: str = 'RdYlGn',
    center: tuple = None,
):
    """Liest alle Redis-Keys und schreibt eine aktualisierte Folium-HTML-Karte.

    Diese Funktion wird nach jedem Mikro-Batch aufgerufen und überschreibt die HTML-Datei,
    sodass ein Reload im Browser die neuesten Daten zeigt.

    Args:
        r:             Redis-Client.
        key_pattern:   Glob-Pattern für Keys, z.B. 'journey:walk:*'.
        lat_field:     Redis-Hash-Feld für Breitengrad.
        lon_field:     Redis-Hash-Feld für Längengrad.
        color_field:   Redis-Hash-Feld, nach dem eingefärbt wird.
        color_low/high: Min/Max-Wert für die Farbskala.
        popup_fields:  Liste von Redis-Feldern, die im Popup erscheinen.
        out_html:      Zielpfad der HTML-Datei.
        title:         Titel für Legende.
        colormap:      matplotlib-Colormap-Name.
        center:        (lat, lon) Kartenmitte; wenn None → erster Datenpunkt.
    """
    keys = r.keys(key_pattern)
    if not keys:
        return

    cmap = matplotlib.colormaps[colormap]

    records = []
    for k in keys:
        data = r.hgetall(k)
        records.append({dk.decode(): dv.decode() for dk, dv in data.items()})

    records.sort(key=lambda rec: rec.get('fenster_start', ''))

    # Mittelpunkt
    lats = [float(rec[lat_field]) for rec in records if lat_field in rec and rec[lat_field]]
    lons = [float(rec[lon_field]) for rec in records if lon_field in rec and rec[lon_field]]
    if not lats:
        return

    map_center = center if center else (lats[0], lons[0])
    karte = folium.Map(location=map_center, zoom_start=15, control_scale=True)
    folium.PolyLine(list(zip(lats, lons)), color='royalblue', weight=2, opacity=0.6,
                    tooltip='GPS-Pfad').add_to(karte)

    color_range = color_high - color_low if color_high > color_low else 1.0

    for rec in records:
        try:
            lat = float(rec.get(lat_field, ''))
            lon = float(rec.get(lon_field, ''))
        except ValueError:
            continue

        try:
            val = float(rec.get(color_field, '0') or '0')
        except ValueError:
            val = 0.0

        norm  = min(max((val - color_low) / color_range, 0.0), 1.0)
        color = mcolors.to_hex(cmap(norm))

        popup_lines = [f'<b>{title}</b>']
        for field in popup_fields:
            if field in rec and rec[field]:
                popup_lines.append(f'{field}: {rec[field]}')

        folium.CircleMarker(
            location=[lat, lon],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            popup=folium.Popup('<br>'.join(popup_lines), max_width=280),
            tooltip=f'{color_field}={rec.get(color_field, "?")}'
        ).add_to(karte)

    # Auto-Refresh alle 10s damit Browser automatisch nachlädt
    refresh_html = (
        '<script>setTimeout(function(){ location.reload(); }, 10000);</script>'
        f'<div style="position:fixed;top:10px;right:10px;z-index:999;background:white;'
        f'padding:8px;border-radius:5px;font-size:12px;">'
        f'Live | {len(records)} Fenster | Auto-Reload 10s</div>'
    )
    karte.get_root().html.add_child(folium.Element(refresh_html))
    karte.save(out_html)


# ---------------------------------------------------------------------------
# Streaming query helper
# ---------------------------------------------------------------------------

def run_stream(query, on_stop=None):
    """Wartet auf Streaming-Query, bricht bei Ctrl+C sauber ab."""
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print('\nStream gestoppt.')
        query.stop()
    finally:
        if on_stop:
            on_stop()
