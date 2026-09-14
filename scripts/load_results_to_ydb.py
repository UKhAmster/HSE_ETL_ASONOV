#!/usr/bin/env python3
"""Загружает результаты PySpark-заданий (parquet в Object Storage) в витрины YDB
для DataLens. Использует bulk_upsert через YDB Python SDK.

Переменные окружения: YDB_TOKEN (IAM-токен), опционально YDB_ENDPOINT, YDB_DATABASE.
"""
import io
import os
import sys
from datetime import date, datetime

import pyarrow.parquet as pq
import ydb

from s3_client import BUCKET, client

ENDPOINT = os.environ.get("YDB_ENDPOINT", "grpcs://ydb.serverless.yandexcloud.net:2135")
DATABASE = os.environ.get("YDB_DATABASE", "/ru-central1/b1gf5j4p8fki6rqpi029/etn949epluer3vl045t3")

TABLES = {
    # префикс parquet в бакете -> (таблица YDB, колонки: имя -> тип)
    "output/loans/loan_applications_flat/": ("loan_applications_flat", {
        "application_id": ydb.PrimitiveType.Utf8, "customer_id": ydb.PrimitiveType.Utf8,
        "region_code": ydb.PrimitiveType.Utf8, "loan_amount": ydb.PrimitiveType.Int64,
        "term_months": ydb.PrimitiveType.Int32, "credit_score": ydb.PrimitiveType.Int32,
        "risk_level": ydb.PrimitiveType.Utf8, "documents_count": ydb.PrimitiveType.Int32,
        "documents_verified": ydb.PrimitiveType.Int32, "document_types": ydb.PrimitiveType.Utf8,
        "decision_status": ydb.PrimitiveType.Utf8, "submitted_at": ydb.PrimitiveType.Datetime}),
    "output/applications/2026-05/region_channel_conversion/": ("applications_region_channel", {
        "region_code": ydb.PrimitiveType.Utf8, "channel": ydb.PrimitiveType.Utf8,
        "applications": ydb.PrimitiveType.Int64, "approved": ydb.PrimitiveType.Int64,
        "avg_requested": ydb.PrimitiveType.Double, "approval_rate": ydb.PrimitiveType.Double}),
    "output/applications/2026-05/daily_by_product/": ("applications_daily", {
        "event_date": ydb.PrimitiveType.Date, "product_type": ydb.PrimitiveType.Utf8,
        "decision_status": ydb.PrimitiveType.Utf8, "applications": ydb.PrimitiveType.Int64,
        "requested_total": ydb.PrimitiveType.Int64, "approved_total": ydb.PrimitiveType.Int64,
        "avg_credit_score": ydb.PrimitiveType.Double, "avg_processing_sec": ydb.PrimitiveType.Double}),
}


def read_parquet_prefix(s3, prefix):
    frames = []
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            frames.append(pq.read_table(io.BytesIO(body)).to_pandas())
    if not frames:
        raise SystemExit(f"parquet не найден по префиксу {prefix}")
    import pandas as pd
    return pd.concat(frames, ignore_index=True)


def to_row(rec, cols):
    out = {}
    for name, typ in cols.items():
        v = rec[name]
        if typ == ydb.PrimitiveType.Datetime and not isinstance(v, datetime):
            v = v.to_pydatetime()
        if typ == ydb.PrimitiveType.Date:
            v = v.date() if hasattr(v, "date") else v
        elif typ in (ydb.PrimitiveType.Int64, ydb.PrimitiveType.Int32):
            v = int(v)
        elif typ == ydb.PrimitiveType.Double:
            v = float(v)
        elif typ == ydb.PrimitiveType.Utf8:
            v = str(v)
        out[name] = v
    return out


def main():
    only = set(sys.argv[1:])  # необязательный список таблиц для загрузки
    s3 = client()
    driver = ydb.Driver(endpoint=ENDPOINT, database=DATABASE,
                        credentials=ydb.AccessTokenCredentials(os.environ["YDB_TOKEN"]))
    driver.wait(timeout=15)
    for prefix, (table, cols) in TABLES.items():
        if only and table not in only:
            continue
        df = read_parquet_prefix(s3, prefix)
        column_types = ydb.BulkUpsertColumns()
        for name, typ in cols.items():
            column_types.add_column(name, ydb.OptionalType(typ))
        rows = [to_row(r, cols) for r in df[list(cols)].to_dict("records")]
        for i in range(0, len(rows), 5000):
            driver.table_client.bulk_upsert(f"{DATABASE}/{table}", rows[i:i + 5000], column_types)
        print(f"{table}: {len(rows)} строк загружено из {prefix}")
    driver.stop()


if __name__ == "__main__":
    main()
