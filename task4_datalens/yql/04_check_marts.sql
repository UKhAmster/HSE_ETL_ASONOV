-- Контроль наполнения витрин перед подключением DataLens
SELECT "transactions_v2" AS t, COUNT(*) AS n FROM transactions_v2
UNION ALL SELECT "loan_applications_flat", COUNT(*) FROM loan_applications_flat
UNION ALL SELECT "applications_region_channel", COUNT(*) FROM applications_region_channel
UNION ALL SELECT "applications_daily", COUNT(*) FROM applications_daily;

SELECT region_code, channel, applications, approved, approval_rate
FROM applications_region_channel ORDER BY approval_rate DESC LIMIT 5;

SELECT risk_level, decision_status, COUNT(*) AS n, SUM(loan_amount) AS amount
FROM loan_applications_flat GROUP BY risk_level, decision_status ORDER BY risk_level, decision_status;
