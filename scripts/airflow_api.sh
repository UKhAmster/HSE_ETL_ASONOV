#!/usr/bin/env bash
# Обёртка над REST API Managed Service for Apache Airflow (Airflow 3, /api/v2).
# Авторизация двухслойная: IAM-токен для прокси Yandex Cloud (X-Cloud-Authorization)
# и JWT самого Airflow (получается по логину/паролю через /auth/token).
#   scripts/airflow_api.sh GET  /api/v2/dags
#   scripts/airflow_api.sh POST /api/v2/dags/<dag_id>/dagRuns '{"logical_date": null}'
set -euo pipefail
AF=${AIRFLOW_URL:-https://c-c9qpcptsmvu9pj24lmcs.airflow.yandexcloud.net}
YC=${YC:-yc}
IAM=$($YC iam create-token)
JWT=$(curl -s -H "X-Cloud-Authorization: Bearer $IAM" -X POST "$AF/auth/token" \
      -H "Content-Type: application/json" \
      -d "{\"username\":\"admin\",\"password\":\"$AIRFLOW_ADMIN_PASSWORD\"}" \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
METHOD=$1; PATH_=$2; BODY=${3:-}
if [ -n "$BODY" ]; then
  curl -s -X "$METHOD" -H "X-Cloud-Authorization: Bearer $IAM" -H "Authorization: Bearer $JWT" \
       -H "Content-Type: application/json" -d "$BODY" "$AF$PATH_"
else
  curl -s -X "$METHOD" -H "X-Cloud-Authorization: Bearer $IAM" -H "Authorization: Bearer $JWT" "$AF$PATH_"
fi
