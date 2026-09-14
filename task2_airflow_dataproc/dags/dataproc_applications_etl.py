"""DAG для Managed Service for Apache Airflow (задание 2).

Жизненный цикл: создать кластер Yandex Data Processing -> запустить
PySpark-задание обработки заявок -> удалить кластер (в том числе при ошибке).
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

FOLDER_ID = "b1g7b3ak2b38r1kktup8"
SERVICE_ACCOUNT_ID = "aje511ve2gp8gm5kvdqd"
SUBNET_ID = "e9b4akil835tq5ic4q1e"
SECURITY_GROUP_ID = "enpu9akb0cnadmce5kta"
ZONE = "ru-central1-a"
BUCKET = "hse-etl-asonov-2026"
# Авторизация в Yandex Cloud идёт через сервисный аккаунт кластера Airflow (etl-sa),
# поэтому отдельное подключение с ключом не нужно: используется yandexcloud_default.
YC_CONN_ID = "yandexcloud_default"
SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFmSLZQeEUsawswbPFMNmp3frzIin+Yvu4SVIlQl0Z2P"

with DAG(
    dag_id="dataproc_applications_etl",
    description="Data Proc: create cluster -> PySpark applications ETL -> delete cluster",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["hse", "etl", "dataproc"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name="airflow-dp-applications",
        folder_id=FOLDER_ID,
        zone=ZONE,
        subnet_id=SUBNET_ID,
        s3_bucket=BUCKET,
        service_account_id=SERVICE_ACCOUNT_ID,
        security_group_ids=[SECURITY_GROUP_ID],
        ssh_public_keys=SSH_PUBLIC_KEY,
        cluster_image_version="2.1",
        services=["YARN", "SPARK"],
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-ssd",
        masternode_disk_size=40,
        computenode_resource_preset="s2.small",
        computenode_disk_type="network-ssd",
        computenode_disk_size=40,
        computenode_count=1,
        datanode_count=0,
        properties={"spark:spark.hadoop.fs.s3a.committer.name": "directory"},
        connection_id=YC_CONN_ID,
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_process_applications",
        name="process_applications",
        main_python_file_uri=f"s3a://{BUCKET}/jobs/process_applications.py",
        args=[
            f"s3a://{BUCKET}/input/applications/2026-05/",
            f"s3a://{BUCKET}/output/applications/2026-05/",
        ],
        connection_id=YC_CONN_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        trigger_rule="all_done",  # удалить кластер даже если задание упало
        connection_id=YC_CONN_ID,
    )

    create_cluster >> run_pyspark >> delete_cluster
