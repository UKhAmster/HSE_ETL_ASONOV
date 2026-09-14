# Итоговое ДЗ, модуль 4 (экзамен). ETL-процессы в Yandex Cloud

**Студент:** Асонов В.
**Курс:** ETL-процессы, практическая работа «Реализация ETL-процесса»
**Облако:** `cloud-blizzard-240`, каталог `default`, зона `ru-central1-a`

Всё, кроме создания приёмника Object Storage в Data Transfer и дашборда DataLens, выполнено
через `yc` CLI 1.34.0, `ydb` CLI 2.33.0 и REST API сервисов. Команды сохранены в
[`scripts/infra_setup.sh`](scripts/infra_setup.sh), скриншоты консоли — в [`screenshots/`](screenshots/).

## Содержание

1. [Общая инфраструктура](#0-общая-инфраструктура)
2. [Задание 1. Data Transfer: YDB → Object Storage](#задание-1-yandex-datatransfer-ydb--object-storage)
3. [Задание 2. Airflow + Data Processing](#задание-2-автоматизация-yandex-data-processing-через-managed-airflow)
4. [Задание 3. Kafka + PySpark](#задание-3-топики-apache-kafka-и-pyspark-задание-в-data-processing)
5. [Задание 4. DataLens](#задание-4-визуализация-в-datalens)
6. [Уборка ресурсов и стоимость](#уборка-ресурсов-и-стоимость)

Структура репозитория:

```
scripts/                     генератор данных, S3-клиент, загрузчик витрин в YDB, обёртка API Airflow, инфраструктура
task1_datatransfer/yql/      YQL: создание таблицы и проверочные запросы
task2_airflow_dataproc/      dags/ — DAG для Managed Airflow, jobs/ — PySpark-задание
task3_kafka_pyspark/         producer.py — отправка JSON в Kafka, jobs/ — PySpark-раскладка JSON
task4_datalens/              YQL витрин и пошаговая инструкция по дашборду
screenshots/                 скриншоты из консоли Yandex Cloud
```

## 0. Общая инфраструктура

| Ресурс | Имя / ID | Назначение |
|---|---|---|
| Сервисный аккаунт | `etl-sa` (`aje511ve2gp8gm5kvdqd`) | роли `editor`, `storage.editor`, `dataproc.agent`, `dataproc.editor`, `managed-airflow.integrationProvider`, `iam.serviceAccounts.user`, `ydb.editor`, `vpc.user`, `kafka.editor`, `data-transfer.editor` |
| Статический ключ | `YCAJEXIFkdXtapziYOR3TKqu2` | доступ к Object Storage по S3 API (секрет вне репозитория) |
| Бакет | `hse-etl-asonov-2026` | входные данные, PySpark-задания, DAG-файлы, результаты, логи Data Proc |
| Сеть | `default`, подсеть `default-ru-central1-a` (10.128.0.0/24) | все кластеры |
| NAT-шлюз | `etl-nat` + таблица маршрутов `etl-rt` | хосты Data Proc без публичных IP ходят в Object Storage и Maven Central |
| Группа безопасности | `etl-sg` (`enpu9akb0cnadmce5kta`) | внутренний трафик, self-rule (обязательное требование Data Proc), 443/9091/8443/22 снаружи |

Тестовые данные синтетические, генерируются детерминированно скриптом
[`scripts/generate_data.py`](scripts/generate_data.py) (seed = 42):

| Файл | Строк | Размер | Требование ДЗ |
|---|---|---|---|
| `transactions_v2.csv` | 322 110 | 32,0 МБ | ≥ 30 МБ |
| `applications.csv` | 456 884 | 52,0 МБ | ≥ 50 МБ |
| `loan_events.jsonl` | 61 728 | 22,0 МБ | ≥ 20 МБ |

## Задание 1. Yandex DataTransfer: YDB → Object Storage

### 1.1. База Managed Service for YDB

```bash
yc ydb database create etl-db --serverless
# id etn949epluer3vl045t3, endpoint grpcs://ydb.serverless.yandexcloud.net:2135
# database /ru-central1/b1gf5j4p8fki6rqpi029/etn949epluer3vl045t3
```

### 1.2. Таблица и данные

YQL: [`task1_datatransfer/yql/01_create_table.sql`](task1_datatransfer/yql/01_create_table.sql)
— таблица `transactions_v2` с первичным ключом `call_id`, `call_time` типа `Datetime`,
`duration_sec` `Int32`, `follow_up_required` `Bool`.

Загрузка CSV через `ydb` CLI. Нюанс: колонка `Datetime` при импорте CSV принимает только ISO 8601
(`2026-05-01T11:42:15Z`), поэтому перед импортом формат даты из ДЗ (`2026-05-01 11:42:15`)
переведён `sed`-ом (см. раздел в [`scripts/infra_setup.sh`](scripts/infra_setup.sh)).

```bash
ydb -e $EP -d $DB yql -f task1_datatransfer/yql/01_create_table.sql
ydb -e $EP -d $DB import file csv --path transactions_v2 --header --newline-delimited data/transactions_v2_iso.csv
# 100% 2.12 MiB/s | 32.3 MiB / 32.3 MiB  Elapsed: 15s
```

Проверка ([`02_check_data.sql`](task1_datatransfer/yql/02_check_data.sql)): `COUNT(*) = 322 110`;
средняя длительность отвеченного звонка ≈ 465 с, неотвеченного ≈ 20 с — данные согласованы с генератором.

### 1.3. Трансфер

- **Источник** `ydb-transactions-source` (`dtefudhjg35fpfu8lfjq`) создан через REST API
  `POST https://datatransfer.api.cloud.yandex.net/v1/endpoint` с `settings.ydbSource`
  (`database`, `instance = ydb.serverless.yandexcloud.net:2135`, `paths = ["transactions_v2"]`,
  аутентификация сервисным аккаунтом `etl-sa`). CLI `yc datatransfer endpoint create` не поддерживает
  тип YDB, публичный API не поддерживает приёмник Object Storage.
- **Приёмник** `s3-transactions-target`: тип Object Storage, бакет `hse-etl-asonov-2026`,
  формат CSV, префикс `transfer/transactions_v2`, сервисный аккаунт `etl-sa` — создан в консоли.
- **Трансфер** `ydb-to-s3-transactions`, тип «Копирование» (snapshot), активирован из консоли.

### 1.4. Проверка работоспособности

_(результат заполняется после активации трансфера, см. скриншоты `screenshots/task1_*.png`)_

## Задание 2. Автоматизация Yandex Data Processing через Managed Airflow

### 2.1. Инфраструктура

- Входной файл `input/applications/2026-05/applications.csv` (54,5 МБ) загружен в бакет
  скриптом [`scripts/s3_upload.py`](scripts/s3_upload.py).
- Кластер **Managed Service for Apache Airflow** `etl-airflow` (Airflow 3.1, провайдер
  `apache-airflow-providers-yandex` 4.3.3): webserver, scheduler, dag-processor, worker — по `c1-m4`;
  сервисный аккаунт `etl-sa`; DAG-файлы читаются из папки `dags/` бакета `hse-etl-asonov-2026`.
- Отдельного подключения с ключом в Airflow не создавалось: провайдер берёт учётные данные
  сервисного аккаунта кластера из metadata-сервиса (в логе: `using metadata service as credentials`).

### 2.2. PySpark-задание

[`task2_airflow_dataproc/jobs/process_applications.py`](task2_airflow_dataproc/jobs/process_applications.py)
читает CSV из `s3a://`, приводит `event_time` к timestamp и строит три витрины:

- `daily_by_product` — заявки по дню × продукту × решению (количество, суммы, средний скоринг, время обработки);
- `region_channel_conversion` — доля одобрений по регионам и каналам;
- `risk_distribution` — распределение риска по продуктам и число ручных проверок.

Результат — parquet (`coalesce(1)`) и CSV-копии в `output/applications/2026-05/`.

### 2.3. DAG

[`task2_airflow_dataproc/dags/dataproc_applications_etl.py`](task2_airflow_dataproc/dags/dataproc_applications_etl.py):

```
create_dataproc_cluster  ->  run_process_applications  ->  delete_dataproc_cluster (trigger_rule=all_done)
DataprocCreateClusterOperator   DataprocCreatePysparkJobOperator   DataprocDeleteClusterOperator
```

Кластер `airflow-dp-applications`: образ 2.1, сервисы YARN + SPARK, master `s2.small` + 1 compute
`s2.small`, диски network-ssd 40 ГБ, без датанод (данные в Object Storage). Удаление стоит с
`trigger_rule="all_done"`, чтобы кластер не оставался жить после ошибки задания.

Грабли по пути: `cluster_name` у оператора не шаблонизируется, имя `airflow-dp-{{ ds_nodash }}`
уходило в API буквально и отклонялось (`Cluster.Name: pattern mismatch`). Заменено на статичное имя.

### 2.4. Запуск и результат

DAG снят с паузы и запущен через REST API (`scripts/airflow_api.sh`; авторизация двухслойная:
`X-Cloud-Authorization: Bearer <IAM>` для прокси Yandex Cloud + JWT самого Airflow из `/auth/token`).

_(результаты запуска заполняются ниже после завершения DAG)_

## Задание 3. Топики Apache Kafka и PySpark-задание в Data Processing

### 3.1. Архитектура

```
generate_data.py -> loan_events.jsonl (22 МБ, 61 728 JSON)
        |  producer.py (kafka-python, SASL_SSL / SCRAM-SHA-512)
        v
Managed Kafka etl-kafka (3.9, 1 брокер s3-c2-m8, топик loan_applications, 3 партиции)
        |  spark.read.format("kafka"), пакет spark-sql-kafka-0-10_2.12:3.0.3
        v
Data Proc etl-dp-kafka (2.1, master+compute s2.small) — kafka_flatten_loans.py
        |  from_json(schema) -> плоские колонки, explode(documents)
        v
Object Storage output/loans/{loan_applications_flat, loan_documents, csv/...}  ->  YDB loan_applications_flat
```

### 3.2. Отправка данных

```bash
yc managed-kafka topic create loan_applications --cluster-name etl-kafka --partitions 3 --replication-factor 1
yc managed-kafka user create etl_user --cluster-name etl-kafka --permission topic=loan_applications,role=producer ...
KAFKA_PASSWORD=... python task3_kafka_pyspark/producer.py data/loan_events.jsonl
# sent 61728 messages, 21.9 MB, 16.1s
```

### 3.3. PySpark-задание

[`task3_kafka_pyspark/jobs/kafka_flatten_loans.py`](task3_kafka_pyspark/jobs/kafka_flatten_loans.py):
batch-чтение топика от `earliest` до `latest`, явная схема `StructType` для JSON, раскладка:

| JSON | Колонка |
|---|---|
| `customer.customer_id`, `customer.region` | `customer_id`, `region_code` |
| `loan.amount`, `loan.term_months` | `loan_amount`, `term_months` |
| `scoring.score`, `scoring.risk_level` | `credit_score`, `risk_level` |
| `documents[]` | `documents_count`, `documents_verified`, `document_types` (агрегаты) и отдельная таблица `loan_documents` через `explode` |
| `submitted_at` | `submitted_at` timestamp + `submitted_date` |

Плюс служебные `partition`, `offset`, `kafka_ts`.

```bash
yc dataproc job create-pyspark --cluster-name etl-dp-kafka --name kafka_flatten_loans \
  --main-python-file-uri s3a://hse-etl-asonov-2026/jobs/kafka_flatten_loans.py \
  --args <broker>:9091 --args loan_applications --args etl_user --args '***' --args s3a://hse-etl-asonov-2026/output/loans \
  --properties spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3
```

Результат (лог задания `c9qv90scmthoq0jcannr`, статус DONE, ~2 мин):

```
=== events read from kafka: 61728
root
 |-- application_id: string
 |-- customer_id: string
 |-- region_code: string
 |-- loan_amount: integer
 ...
 |-- submitted_at: timestamp
```

В бакете: `loan_applications_flat` parquet 1,66 МБ + CSV 9,7 МБ, `loan_documents` parquet 0,6 МБ,
`csv/loan_summary`. Плоская таблица дополнительно загружена в YDB (`loan_applications_flat`, 61 728 строк)
скриптом [`scripts/load_results_to_ydb.py`](scripts/load_results_to_ydb.py) для DataLens.

## Задание 4. Визуализация в DataLens

Пошаговая инструкция и состав дашборда: [`task4_datalens/README.md`](task4_datalens/README.md).
Источник — YDB `etl-db` (подключение через сервисный аккаунт `etl-sa`), четыре датасета,
восемь чартов и три индикатора с селекторами по региону и типу кампании.

Скриншоты: `screenshots/task4_*.png`.

## Уборка ресурсов и стоимость

После снятия скриншотов удалены: `etl-airflow`, `etl-kafka`, `etl-dp-kafka`, кластер `airflow-dp-applications`
(его удаляет сам DAG), NAT-шлюз. Оставлены YDB serverless (оплата по запросам, в бесплатном пакете)
и бакет (~65 МБ, в бесплатном 1 ГБ).

Ориентировочная стоимость прогона: Airflow ~27 ₽/ч × 3 ч, Kafka ~6 ₽/ч × 3 ч, Data Proc ~15 ₽/ч × 2 ч,
итого около 130 ₽ из стартового гранта 4000 ₽.
