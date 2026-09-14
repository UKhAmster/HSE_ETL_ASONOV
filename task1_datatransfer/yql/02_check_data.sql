-- Проверка загруженных данных
SELECT COUNT(*) AS rows_total FROM transactions_v2;

SELECT campaign_type, call_status, COUNT(*) AS calls, AVG(duration_sec) AS avg_duration
FROM transactions_v2
GROUP BY campaign_type, call_status
ORDER BY campaign_type, call_status;
