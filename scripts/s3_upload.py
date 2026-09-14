#!/usr/bin/env python3
"""Загрузка файла в бакет: s3_upload.py <local_path> <s3_key>"""
import sys

from s3_client import BUCKET, client

local, key = sys.argv[1], sys.argv[2]
client().upload_file(local, BUCKET, key)
print(f"s3://{BUCKET}/{key} <- {local}")
