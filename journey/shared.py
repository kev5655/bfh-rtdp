
import builtins
import os
import json
import time
from typing import Dict

import numpy as np
import redis as _redis_module

os.environ.setdefault('JAVA_HOME', '/usr/lib/jvm/java-17-openjdk-amd64')

KAFKA_BROKER = 'localhost:9092'


def fmt_num(value, decimals: int = 6) -> str:
    """Formatiert einen numerischen Wert für Redis-Speicherung (kein Spark-round)."""
    if value is None:
        return ''
    return str(builtins.round(float(value), decimals))


def redis_client() -> _redis_module.Redis:
    """Gibt einen Redis-Client zurück."""
    return _redis_module.Redis(host='localhost', port=6379, db=0)


def write_redis_batch(r: _redis_module.Redis, key: str, mapping: Dict[str, str], ttl: int = 30):
    r.hset(key, mapping=mapping)
    r.expire(key, ttl)


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
