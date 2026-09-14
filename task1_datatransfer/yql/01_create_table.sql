-- Задание 1. Таблица звонков колл-центра (transactions_v2) в Managed Service for YDB
CREATE TABLE transactions_v2 (
    call_id            Utf8      NOT NULL,
    call_time          Datetime,
    client_id          Utf8,
    region_code        Utf8,
    campaign_type      Utf8,
    call_status        Utf8,
    client_response    Utf8,
    duration_sec       Int32,
    follow_up_required Bool,
    PRIMARY KEY (call_id)
);
