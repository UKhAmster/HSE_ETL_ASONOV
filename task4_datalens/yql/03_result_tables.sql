-- Задание 4. Таблицы-витрины в YDB для подключения DataLens
CREATE TABLE loan_applications_flat (
    application_id     Utf8 NOT NULL,
    customer_id        Utf8,
    region_code        Utf8,
    loan_amount        Int64,
    term_months        Int32,
    credit_score       Int32,
    risk_level         Utf8,
    documents_count    Int32,
    documents_verified Int32,
    document_types     Utf8,
    decision_status    Utf8,
    submitted_at       Datetime,
    PRIMARY KEY (application_id)
);

CREATE TABLE applications_region_channel (
    region_code    Utf8 NOT NULL,
    channel        Utf8 NOT NULL,
    applications   Int64,
    approved       Int64,
    avg_requested  Double,
    approval_rate  Double,
    PRIMARY KEY (region_code, channel)
);

CREATE TABLE applications_daily (
    event_date         Date NOT NULL,
    product_type       Utf8 NOT NULL,
    decision_status    Utf8 NOT NULL,
    applications       Int64,
    requested_total    Int64,
    approved_total     Int64,
    avg_credit_score   Double,
    avg_processing_sec Double,
    PRIMARY KEY (event_date, product_type, decision_status)
);
