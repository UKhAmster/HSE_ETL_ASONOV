#!/usr/bin/env python3
"""Продюсер: отправляет JSON-события кредитных заявок в топик Managed Kafka.

Использование: producer.py <jsonl_file> [topic]
Подключение: SASL_SSL / SCRAM-SHA-512, сертификат Yandex Cloud CA.
"""
import os
import sys
import time

from kafka import KafkaProducer

BROKER = os.environ.get("KAFKA_BROKER", "rc1a-brfpc0kqggaiv8dp.mdb.yandexcloud.net:9091")
USER = os.environ.get("KAFKA_USER", "etl_user")
PASSWORD = os.environ["KAFKA_PASSWORD"]
CA = os.environ.get("KAFKA_CA", os.path.join(os.path.dirname(__file__), "..", "scripts", "YandexInternalRootCA.crt"))

path = sys.argv[1]
topic = sys.argv[2] if len(sys.argv) > 2 else "loan_applications"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USER,
    sasl_plain_password=PASSWORD,
    ssl_cafile=CA,
    linger_ms=50,
    batch_size=256 * 1024,
    compression_type="gzip",
)

t0 = time.time()
sent = size = 0
with open(path, "rb") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        producer.send(topic, value=line)
        sent += 1
        size += len(line)
producer.flush()
print(f"sent {sent} messages, {size / 1024 / 1024:.1f} MB, {time.time() - t0:.1f}s")
