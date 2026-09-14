"""Общий S3-клиент для Yandex Object Storage (ключ сервисного аккаунта etl-sa)."""
import json
import os

import boto3

KEY_FILE = os.path.expanduser("~/.config/yandex-cloud/etl-sa-s3-key.json")
BUCKET = os.environ.get("ETL_BUCKET", "hse-etl-asonov-2026")


def client():
    key = json.load(open(KEY_FILE))
    return boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        region_name="ru-central1",
        aws_access_key_id=key["access_key"]["key_id"],
        aws_secret_access_key=key["secret"],
    )
