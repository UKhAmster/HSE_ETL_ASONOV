# Итоговое ДЗ, модуль 4 (экзамен). ETL-процессы в Yandex Cloud

**Студент:** Асонов В.
**Курс:** ETL-процессы, практическая работа «Реализация ETL-процесса»
**Облако:** `cloud-blizzard-240`, каталог `default`, зона `ru-central1-a`

PDF-версия отчёта: [`Отчёт_Асонов_ETL_модуль4.pdf`](Отчёт_Асонов_ETL_модуль4.pdf)
(собирается из этого README скриптом `scripts/build_report_pdf.py`).

Всё, кроме создания приёмника Object Storage в Data Transfer и дашборда DataLens, выполнено
через `yc` CLI 1.34.0, `ydb` CLI 2.33.0 и REST API сервисов. Команды сохранены в
[`scripts/infra_setup.sh`](scripts/infra_setup.sh), скриншоты консоли — в [`screenshots/`](screenshots/).

## Содержание

1. [Общая инфраструктура](#0-общая-инфраструктура)
2. [Задание 1. Data Transfer: YDB → Object Storage](#задание-1-yandex-datatransfer-ydb--object-storage)
3. [Задание 2. Airflow + Data Processing](#задание-2-автоматизация-yandex-data-processing-через-managed-airflow)
4. [Задание 3. Kafka + PySpark](#задание-3-топики-apache-kafka-и-pyspark-задание-в-data-processing)
5. [Задание 4. DataLens](#задание-4-визуализация-в-datalens)

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

Проверка ([`02_check_data.sql`](task1_datatransfer/yql/02_check_data.sql)): `COUNT(*) = 322 110`
(скриншот консоли: [`screenshots/task1_ydb_tables_and_count.png`](screenshots/task1_ydb_tables_and_count.png));
средняя длительность отвеченного звонка ≈ 465 с, неотвеченного ≈ 20 с — данные согласованы с генератором.

### 1.3. Трансфер

- **Источник** `ydb-transactions-source` (`dtefudhjg35fpfu8lfjq`) создан через REST API
  `POST https://datatransfer.api.cloud.yandex.net/v1/endpoint` с `settings.ydbSource`
  (`database`, `instance = ydb.serverless.yandexcloud.net:2135`, `paths = ["transactions_v2"]`,
  аутентификация сервисным аккаунтом `etl-sa`). CLI `yc datatransfer endpoint create` не поддерживает
  тип YDB, а публичный API не поддерживает приёмник Object Storage, поэтому приёмник создан в консоли.
- **Приёмник** `s3-transactions-target` (`dtetbhol3fmloehgrm0e`): тип Object Storage, бакет `hse-etl-asonov-2026`,
  формат CSV, путь `transfer/transactions_v2`, сервисный аккаунт `etl-sa`.
- **Трансфер** `ydb-to-s3-transactions` (`dttutjs3u94sna1f30lm`), тип «Копирование» (`SNAPSHOT_ONLY`),
  создан 14.09.2026 в 17:32, активирован из консоли.

### 1.4. Проверка работоспособности

Статус трансфера — **Завершён** (`yc datatransfer transfer get … → DONE`). В бакете появился объект

```
transfer/transactions_v2/transactions_v2/part-1789396328-c21f969b.00000.csv   36 453 392 байт (34,8 МБ)
```

Контроль полноты: в файле **322 110 строк**, столько же, сколько `COUNT(*)` в YDB. Первые строки:

```
call_20260501_0000027,2026-05-01 22:41:31 +0000 UTC,client_41306,DE-NW,credit_card_offer,answered,callback_later,881,true
call_20260501_0000105,2026-05-01 22:34:59 +0000 UTC,client_46870,DE-NW,credit_card_offer,declined,unknown,3,false
```

Data Transfer выгружает `Datetime` в формате `2026-05-01 22:41:31 +0000 UTC`, а `Bool` — как `true/false`.

Скриншоты:

| Файл | Что видно |
|---|---|
| [`task1_ydb_tables_and_count.png`](screenshots/task1_ydb_tables_and_count.png) | YDB `etl-db`: таблицы и `SELECT COUNT(*) FROM transactions_v2` = 322 110 |
| [`task1_dt_endpoints.png`](screenshots/task1_dt_endpoints.png) | эндпоинты: источник YDB и приёмник Object Storage |
| [`task1_dt_transfer_done.png`](screenshots/task1_dt_transfer_done.png) | трансфер `ydb-to-s3-transactions`, тип «Копировать», статус «Завершён» |
| [`task1_bucket_transfer.png`](screenshots/task1_bucket_transfer.png) | папка `transfer/transactions_v2` в бакете |

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

Хронология успешного прогона `manual__2026-09-14T12:35:00` (время UTC):

| Задача | Оператор | Старт | Финиш | Итог |
|---|---|---|---|---|
| `create_dataproc_cluster` | `DataprocCreateClusterOperator` | 12:35:20 | 12:38:17 | кластер `c9q1l32sgdb4642ut5bg` создан, id ушёл в XCom |
| `run_process_applications` | `DataprocCreatePysparkJobOperator` | 12:38:18 | 12:39:47 | задание `c9qmonhtv5llvijlf8cu`, статус DONE |
| `delete_dataproc_cluster` | `DataprocDeleteClusterOperator` | 12:39:48 | ~12:42 | кластер удалён, `yc dataproc cluster list` его не показывает |

Итог DAG — `success`, весь цикл занял 7 минут.

Скриншоты Airflow и консоли:

| Файл | Что видно |
|---|---|
| [`task2_airflow_cluster_overview.png`](screenshots/task2_airflow_cluster_overview.png) | кластер `etl-airflow`: Airflow 3.1, Python 3.12, SA `etl-sa`, 4 компонента по `c1-m4` |
| [`task2_bucket_input.png`](screenshots/task2_bucket_input.png), [`task2_bucket_jobs.png`](screenshots/task2_bucket_jobs.png), [`task2_bucket_dags.png`](screenshots/task2_bucket_dags.png) | бакет: входной CSV, PySpark-задания, DAG-файл |
| [`task2_airflow_dags_list.png`](screenshots/task2_airflow_dags_list.png) | список DAG, последний запуск `success` |
| [`task2_airflow_dag_overview_runs.png`](screenshots/task2_airflow_dag_overview_runs.png) | три запуска: два отладочных `failed`, третий `success`, график длительности |
| [`task2_airflow_run_success_tasks.png`](screenshots/task2_airflow_run_success_tasks.png) | успешный запуск: все три задачи `Success`, операторы и длительности |
| [`task2_airflow_audit_log.png`](screenshots/task2_airflow_audit_log.png) | журнал: снятие с паузы и три ручных триггера через REST API |
| [`task2_airflow_task_operator.png`](screenshots/task2_airflow_task_operator.png) | задача `run_process_applications`: `DataprocCreatePysparkJobOperator`, `all_success` | Результат в бакете `output/applications/2026-05/`
(время объектов 12:39:23–12:39:42 совпадает с окном задания):

```
daily_by_product/               parquet 21 КБ   620 строк (день × продукт × решение)
region_channel_conversion/      parquet  3 КБ    40 строк (регион × канал)
risk_distribution/              parquet  2 КБ    15 строк
csv/daily_by_product/, csv/region_channel_conversion/   копии для DataLens
```

Витрины `daily_by_product` и `region_channel_conversion` дополнительно загружены в YDB
(`applications_daily`, `applications_region_channel`) для дашборда.

Ещё одни грабли по дороге: провайдер ищет `cluster_id` через `xcom_pull(key="cluster_id")` без
`task_ids`, а в Airflow 3 такой вызов читает XCom только своей задачи — обе следующие задачи падали с
`Cluster id must be specified`, а кластер оставался жить. Решение: `cluster_id` передаётся явно
шаблоном `{{ ti.xcom_pull(task_ids='create_dataproc_cluster', key='cluster_id') }}` (поле templated).

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

Скриншоты:

| Файл | Что видно |
|---|---|
| [`task3_kafka_cluster_overview.png`](screenshots/task3_kafka_cluster_overview.png) | кластер `etl-kafka` 3.9.2, `s3-c2-m8`, 32 ГБ SSD, публичный доступ, SG `etl-sg` |
| [`task3_kafka_topics.png`](screenshots/task3_kafka_topics.png) | топик `loan_applications`, 3 раздела, RF 1 |
| [`task3_kafka_users.png`](screenshots/task3_kafka_users.png) | пользователь `etl_user` с ролями producer и consumer |
| [`task3_dataproc_cluster_overview.png`](screenshots/task3_dataproc_cluster_overview.png) | кластер `etl-dp-kafka` 2.1, SPARK + YARN, SA `etl-sa`, бакет, UI Proxy |
| [`task3_dataproc_job_done.png`](screenshots/task3_dataproc_job_done.png) | задание `kafka_flatten_loans` PYSPARK, статус Done, 15:19–15:21 |
| [`task3_dataproc_logs_all.png`](screenshots/task3_dataproc_logs_all.png), [`task3_dataproc_logs_info.png`](screenshots/task3_dataproc_logs_info.png) | логи кластера |
| [`task3_bucket_output_loans.png`](screenshots/task3_bucket_output_loans.png) | результат в бакете: `loan_applications_flat`, `loan_documents`, `csv/` |

В бакете: `loan_applications_flat` parquet 1,66 МБ + CSV 9,7 МБ, `loan_documents` parquet 0,6 МБ,
`csv/loan_summary`. Плоская таблица дополнительно загружена в YDB (`loan_applications_flat`, 61 728 строк)
скриптом [`scripts/load_results_to_ydb.py`](scripts/load_results_to_ydb.py) для DataLens.

## Задание 4. Визуализация в DataLens

Пошаговая инструкция и состав дашборда: [`task4_datalens/README.md`](task4_datalens/README.md).
Источник — YDB `etl-db`, подключение `ydb-etl-db` через сервисный аккаунт `etl-sa`, воркбук `HSE ETL`.

Датасеты (по одному на таблицу): `ds_calls` (transactions_v2), `ds_loans` (loan_applications_flat),
`ds_region_channel` (applications_region_channel), `ds_daily` (applications_daily).
Вычисляемые поля построены на флагах `IF(..., 1, 0)` с агрегацией в чарте: `COUNTIF` для
YDB-источника DataLens не поддерживает.

Чарты (Wizard) и что на них видно:

| Файл | Чарт | Датасет | Наблюдение |
|---|---|---|---|
| [`task4_indicator_calls_total.png`](screenshots/task4_indicator_calls_total.png) | Звонков всего | `ds_calls` | 322 110 — совпадает с `COUNT(*)` в YDB |
| [`task4_indicator_kafka_applications.png`](screenshots/task4_indicator_kafka_applications.png) | Заявок из Kafka | `ds_loans` | 61 728 — совпадает с числом сообщений продюсера |
| [`task4_indicator_approved_total.png`](screenshots/task4_indicator_approved_total.png) | Одобрено, сумма | `ds_daily` | 7 720 345 000 |
| [`task4_chart_calls_by_campaign.png`](screenshots/task4_chart_calls_by_campaign.png) | Звонки по кампаниям и статусам | `ds_calls` | ~64 тыс. звонков на кампанию, статусы распределены равномерно |
| [`task4_chart_answer_rate_by_region.png`](screenshots/task4_chart_answer_rate_by_region.png) | Доля дозвонов по регионам | `ds_calls` | ~0,20 во всех регионах, лидер DE-NW |
| [`task4_chart_duration_by_day.png`](screenshots/task4_chart_duration_by_day.png) | Длительность разговора по дням | `ds_calls` | фильтр `call_status = answered`, средняя ~465 с |
| [`task4_chart_risk_x_decision_pivot.png`](screenshots/task4_chart_risk_x_decision_pivot.png) | Риск × решение (сводная с раскраской) | `ds_loans` | high/approved 17 216, цифры совпадают с YQL-проверкой |
| [`task4_chart_loan_amount_by_day.png`](screenshots/task4_chart_loan_amount_by_day.png) | Сумма кредитов по дням и риску | `ds_loans` | стек high/low/medium по дням мая |
| [`task4_chart_region_channel_table.png`](screenshots/task4_chart_region_channel_table.png) | Одобрение по регионам и каналам | `ds_region_channel` | approval_rate 0,54–0,56, ~11,4 тыс. заявок на пару |
| [`task4_chart_applications_by_product.png`](screenshots/task4_chart_applications_by_product.png) | Заявки по продуктам (Airflow-витрина) | `ds_daily` | ~14,7 тыс. заявок в день, 5 продуктов поровну |

Объекты воркбука `HSE ETL`: [`task4_datalens_connection.png`](screenshots/task4_datalens_connection.png)
(подключение `ydb-etl-db`), [`task4_datalens_datasets.png`](screenshots/task4_datalens_datasets.png)
(4 датасета), [`task4_datalens_charts.png`](screenshots/task4_datalens_charts.png) (10 чартов).

Дашборд `HSE ETL — кредитная аналитика`: селекторы `Кампания` (ds_calls → 4 виджета) и `Регион`
(ds_loans → 3 виджета), три секции с заголовками.

| Файл | Что видно |
|---|---|
| [`task4_dashboard_full.png`](screenshots/task4_dashboard_full.png) | дашборд без фильтров: 322 110 звонков, 61 728 заявок, 7,72 млрд одобрено |
| [`task4_dashboard_filtered.png`](screenshots/task4_dashboard_filtered.png) | выбраны кампания `cash_loan_offer` и регион `DE-BE`: индикаторы пересчитались (64 234 звонка, 7 608 заявок), витрины Airflow не зависят от селекторов |
