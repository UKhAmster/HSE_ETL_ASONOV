# Задание 4. Визуализация в DataLens

Источник данных для дашборда — база **Managed Service for YDB `etl-db`** (та же, что в задании 1).
В ней четыре таблицы:

| Таблица | Откуда | Строк |
|---|---|---|
| `transactions_v2` | задание 1, импорт CSV через `ydb import file csv` | 322 110 |
| `loan_applications_flat` | задание 3, результат PySpark-раскладки JSON из Kafka (parquet → `scripts/load_results_to_ydb.py`) | 61 728 |
| `applications_region_channel` | задание 2, витрина PySpark из Airflow-DAG | 40 |
| `applications_daily` | задание 2, витрина PySpark из Airflow-DAG | ~600 |

YQL создания витрин: [`yql/03_result_tables.sql`](yql/03_result_tables.sql).

## Шаги в интерфейсе DataLens

1. Открыть <https://datalens.yandex.cloud>, войти под тем же аккаунтом, что и консоль. При первом входе выбрать организацию и включить DataLens (бесплатный тариф для индивидуального использования).
2. **Подключение**: «Создать» → «Подключение» → «YDB».
   Тип «Managed Service for YDB», облако `cloud-blizzard-240`, каталог `default`, база `etl-db`,
   аутентификация — «Сервисный аккаунт» `etl-sa` (у него роль `ydb.editor`). Имя `ydb-etl-db`. Проверить подключение, сохранить.
3. **Датасеты** (по одному на таблицу): «Создать датасет» → выбрать подключение → перетащить таблицу.
   - `ds_calls` ← `transactions_v2`. Добавить вычисляемые поля:
     `answered = COUNTIF([call_status] = "answered")`, `answer_rate = [answered] / COUNT()`,
     `interested = COUNTIF([client_response] = "interested")`.
   - `ds_loans` ← `loan_applications_flat`. Поле `approved_share = COUNTIF([decision_status] = "approved") / COUNT()`.
   - `ds_region_channel` ← `applications_region_channel`.
   - `ds_daily` ← `applications_daily`.
4. **Чарты** («Создать» → «Чарт», датасет из списка):
   - «Звонки по кампаниям и статусам» — столбчатая: X `campaign_type`, Y `COUNT()`, цвет `call_status`.
   - «Доля дозвонов по регионам» — линейчатая: Y `region_code`, X `answer_rate`.
   - «Длительность разговора по дням» — линейная: X `call_time` (день), Y `AVG([duration_sec])`, фильтр `call_status = answered`.
   - «Заявки из Kafka: риск × решение» — тепловая карта: строки `risk_level`, столбцы `decision_status`, значение `COUNT()`.
   - «Сумма кредитов по дням и уровню риска» — область: X `submitted_date`, Y `SUM([loan_amount])`, цвет `risk_level`.
   - «Одобрение по регионам и каналам» — таблица из `ds_region_channel`: `region_code`, `channel`, `applications`, `approval_rate`.
   - «Заявки по продуктам (Airflow-витрина)» — столбчатая из `ds_daily`: X `event_date`, Y `SUM([applications])`, цвет `product_type`.
   - Индикаторы: `COUNT()` звонков, `COUNT()` заявок Kafka, `SUM([approved_total])` из `ds_daily`.
5. **Дашборд**: «Создать» → «Дашборд» `HSE ETL — кредитная аналитика`. Добавить чарты, сверху селекторы
   `region_code` (из `ds_loans`) и `campaign_type` (из `ds_calls`) со связями на чарты. Сохранить, при желании «Опубликовать».

## Что приложить в отчёт
- скриншот подключения `ydb-etl-db` с успешной проверкой;
- скриншот списка датасетов;
- скриншот дашборда целиком и 2–3 отдельных чартов;
- ссылка на опубликованный дашборд (если публиковали).
