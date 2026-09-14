#!/usr/bin/env python3
"""Генерация тестовых данных для трёх заданий ДЗ.

  transactions_v2.csv   >= 30 МБ  (задание 1, YDB -> Object Storage)
  applications.csv      >= 50 МБ  (задание 2, Airflow + Data Proc)
  loan_events.jsonl     >= 20 МБ  (задание 3, Kafka + PySpark)

Данные синтетические, детерминированные (seed=42).
"""
import csv
import json
import os
import random
import sys
from datetime import datetime, timedelta

SEED = 42
OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "data"
REGIONS = ["DE-HE", "DE-BY", "DE-BE", "DE-NW", "DE-BW", "DE-SN", "DE-HH", "DE-NI"]
CAMPAIGNS = ["credit_card_offer", "cash_loan_offer", "mortgage_refi", "deposit_promo", "insurance_upsell"]
CALL_STATUSES = ["answered", "no_answer", "busy", "voicemail", "declined"]
RESPONSES = ["interested", "not_interested", "callback_later", "already_client", "unknown"]
PRODUCTS = ["cash_loan", "credit_card", "mortgage", "auto_loan", "consumer_loan"]
RISKS = ["low", "medium", "high"]
DECISIONS = ["approved", "rejected", "manual_review", "pending"]
CHANNELS = ["mobile", "web", "branch", "call_center", "partner"]
DOC_TYPES = ["passport", "income_statement", "employment_proof", "utility_bill", "tax_return"]
DOC_STATUSES = ["verified", "pending", "rejected"]
START = datetime(2026, 5, 1)


def ts(rnd, day_span=31):
    return START + timedelta(seconds=rnd.randint(0, day_span * 86400 - 1))


def gen_transactions(path, target_mb=32):
    rnd = random.Random(SEED)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["call_id", "call_time", "client_id", "region_code", "campaign_type",
                    "call_status", "client_response", "duration_sec", "follow_up_required"])
        i = 0
        while f.tell() < target_mb * 1024 * 1024:
            i += 1
            t = ts(rnd)
            status = rnd.choice(CALL_STATUSES)
            resp = rnd.choice(RESPONSES) if status == "answered" else "unknown"
            dur = rnd.randint(30, 900) if status == "answered" else rnd.randint(0, 40)
            w.writerow([f"call_{t:%Y%m%d}_{i:07d}", f"{t:%Y-%m-%d %H:%M:%S}",
                        f"client_{rnd.randint(1000, 99999)}", rnd.choice(REGIONS), rnd.choice(CAMPAIGNS),
                        status, resp, dur, str(resp in ("interested", "callback_later")).lower()])
    return i


def gen_applications(path, target_mb=52):
    rnd = random.Random(SEED + 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["application_id", "event_time", "customer_id", "region_code", "product_type",
                    "requested_amount", "term_months", "credit_score", "risk_level", "decision_status",
                    "approved_amount", "channel", "employee_review_flag", "processing_time_sec"])
        i = 0
        while f.tell() < target_mb * 1024 * 1024:
            i += 1
            t = ts(rnd)
            score = rnd.randint(300, 850)
            risk = "low" if score >= 700 else "medium" if score >= 580 else "high"
            req = rnd.choice([3000, 5000, 8000, 12000, 15000, 20000, 30000, 50000, 100000])
            decision = rnd.choices(DECISIONS, weights=[55, 25, 15, 5])[0]
            approved = req if decision == "approved" else (req // 2 if decision == "manual_review" else 0)
            w.writerow([f"app_{t:%Y%m%d}_{i:07d}", f"{t:%Y-%m-%d %H:%M:%S}",
                        f"cust_{rnd.randint(10000, 99999)}", rnd.choice(REGIONS), rnd.choice(PRODUCTS),
                        req, rnd.choice([6, 12, 24, 36, 48, 60]), score, risk, decision, approved,
                        rnd.choice(CHANNELS), str(decision == "manual_review").lower(), rnd.randint(5, 600)])
    return i


def gen_loan_events(path, target_mb=22):
    rnd = random.Random(SEED + 2)
    with open(path, "w") as f:
        i = 0
        while f.tell() < target_mb * 1024 * 1024:
            i += 1
            t = ts(rnd)
            score = rnd.randint(300, 850)
            event = {
                "application_id": f"loan_{i:07d}",
                "customer": {"customer_id": f"cust_{rnd.randint(100, 9999)}", "region": rnd.choice(REGIONS)},
                "loan": {"amount": rnd.choice([5000, 10000, 15000, 20000, 30000, 50000]),
                         "term_months": rnd.choice([12, 24, 36, 48, 60])},
                "scoring": {"score": score,
                            "risk_level": "low" if score >= 700 else "medium" if score >= 580 else "high"},
                "documents": [{"type": d, "status": rnd.choice(DOC_STATUSES)}
                              for d in rnd.sample(DOC_TYPES, rnd.randint(1, 3))],
                "decision_status": rnd.choices(DECISIONS, weights=[55, 25, 15, 5])[0],
                "submitted_at": f"{t:%Y-%m-%dT%H:%M:%S}Z",
            }
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    return i


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, fn in [("transactions_v2.csv", gen_transactions),
                     ("applications.csv", gen_applications),
                     ("loan_events.jsonl", gen_loan_events)]:
        p = os.path.join(OUT_DIR, name)
        n = fn(p)
        print(f"{name}: {n} строк, {os.path.getsize(p) / 1024 / 1024:.1f} МБ")
