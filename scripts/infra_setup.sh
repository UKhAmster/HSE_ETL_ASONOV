#!/usr/bin/env bash
# Подготовка инфраструктуры Yandex Cloud для ДЗ (выполнялось через yc CLI 1.34.0).
# Идентификаторы каталога/подсети подставлены из облака cloud-blizzard-240.
set -euo pipefail
YC=${YC:-yc}
FOLDER_ID=b1g7b3ak2b38r1kktup8
SUBNET_A=e9b4akil835tq5ic4q1e          # default-ru-central1-a, 10.128.0.0/24
BUCKET=hse-etl-asonov-2026

# 1. Сервисный аккаунт и роли
$YC iam service-account create --name etl-sa --description "HSE ETL homework service account"
SA_ID=$($YC iam service-account get etl-sa --format json | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
for role in editor storage.editor dataproc.agent dataproc.editor managed-airflow.integrationProvider \
            iam.serviceAccounts.user ydb.editor vpc.user kafka.editor data-transfer.editor; do
  $YC resource-manager folder add-access-binding $FOLDER_ID --role $role --subject serviceAccount:$SA_ID
done

# 2. Статический ключ для доступа к Object Storage по S3 API (хранится вне репозитория)
$YC iam access-key create --service-account-name etl-sa --format json > ~/.config/yandex-cloud/etl-sa-s3-key.json
chmod 600 ~/.config/yandex-cloud/etl-sa-s3-key.json

# 3. Бакет Object Storage
$YC storage bucket create --name $BUCKET --default-storage-class standard --max-size 5368709120

# 4. NAT-шлюз, чтобы хосты Data Proc без публичных IP ходили в Object Storage и Maven
$YC vpc gateway create --name etl-nat
GW_ID=$($YC vpc gateway get etl-nat --format json | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
$YC vpc route-table create --name etl-rt --network-name default --route destination=0.0.0.0/0,gateway-id=$GW_ID
$YC vpc subnet update default-ru-central1-a --route-table-name etl-rt

# 5. Группа безопасности
$YC vpc security-group create --name etl-sg --network-name default \
  --rule "direction=ingress,protocol=any,from-port=0,to-port=65535,v4-cidrs=[10.128.0.0/16],description=internal" \
  --rule "direction=ingress,protocol=tcp,port=443,v4-cidrs=[0.0.0.0/0],description=https" \
  --rule "direction=ingress,protocol=tcp,port=9091,v4-cidrs=[0.0.0.0/0],description=kafka-tls" \
  --rule "direction=ingress,protocol=tcp,port=8443,v4-cidrs=[0.0.0.0/0],description=dataproc-ui" \
  --rule "direction=ingress,protocol=tcp,port=22,v4-cidrs=[0.0.0.0/0],description=ssh" \
  --rule "direction=egress,protocol=any,from-port=0,to-port=65535,v4-cidrs=[0.0.0.0/0],description=all-out"
$YC vpc security-group update-rules etl-sg \
  --add-rule "direction=ingress,protocol=any,from-port=0,to-port=65535,predefined=self_security_group,description=self"
$YC vpc security-group update-rules etl-sg \
  --add-rule "direction=egress,protocol=any,from-port=0,to-port=65535,predefined=self_security_group,description=self-out"
SG_ID=$($YC vpc security-group get etl-sg --format json | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

# 6. Managed Service for YDB (serverless) — задание 1
$YC ydb database create etl-db --serverless

# 7. Managed Service for Apache Kafka — задание 3
$YC managed-kafka cluster create --name etl-kafka --environment production --version 3.9 --network-name default \
  --subnet-ids $SUBNET_A --zone-ids ru-central1-a --brokers-count 1 \
  --resource-preset s3-c2-m8 --disk-type network-ssd --disk-size 32 --assign-public-ip \
  --security-group-ids $SG_ID
$YC managed-kafka topic create loan_applications --cluster-name etl-kafka --partitions 3 --replication-factor 1 --retention-ms 604800000
$YC managed-kafka user create etl_user --cluster-name etl-kafka --password "$KAFKA_PASSWORD" \
  --permission topic=loan_applications,role=producer --permission topic=loan_applications,role=consumer

# 8. Managed Service for Apache Airflow — задание 2 (DAG-файлы читаются из папки dags/ бакета)
$YC managed-airflow cluster create --name etl-airflow \
  --subnet-ids $SUBNET_A --security-group-ids $SG_ID \
  --service-account-id $SA_ID --dags-bucket $BUCKET \
  --webserver count=1,resource-preset-id=c1-m4 --scheduler count=1,resource-preset-id=c1-m4 \
  --dag-processor count=1,resource-preset-id=c1-m4 \
  --worker min-count=1,max-count=1,resource-preset-id=c1-m4 \
  --admin-password "$AIRFLOW_ADMIN_PASSWORD"

# 9. Кластер Data Proc для задания 3 (для задания 2 кластер создаёт и удаляет DAG)
$YC dataproc cluster create --name etl-dp-kafka --zone ru-central1-a --version 2.1 --services YARN,SPARK \
  --service-account-name etl-sa --bucket $BUCKET --security-group-ids $SG_ID \
  --ssh-public-keys-file ~/.ssh/id_ed25519.pub \
  --subcluster name=master,role=masternode,resource-preset=s2.small,disk-type=network-ssd,disk-size=40,subnet-id=$SUBNET_A,hosts-count=1 \
  --subcluster name=compute,role=computenode,resource-preset=s2.small,disk-type=network-ssd,disk-size=40,subnet-id=$SUBNET_A,hosts-count=1 \
  --property spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 \
  --ui-proxy
