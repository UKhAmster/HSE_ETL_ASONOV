"""PySpark-задание для Yandex Data Processing (задание 3).

Читает JSON-события из топика Apache Kafka (Managed Service for Apache Kafka),
раскладывает вложенную структуру в плоскую таблицу и пишет результат
в Object Storage (parquet + csv). Массив documents разворачивается в строки
(одна строка = одна пара заявка/документ), плюс агрегат по заявке.

Аргументы: <bootstrap_servers> <topic> <user> <password> <output_path>
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField, StructType)

SCHEMA = StructType([
    StructField("application_id", StringType()),
    StructField("customer", StructType([
        StructField("customer_id", StringType()),
        StructField("region", StringType()),
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType()),
        StructField("term_months", IntegerType()),
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType()),
        StructField("risk_level", StringType()),
    ])),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType()),
        StructField("status", StringType()),
    ]))),
    StructField("decision_status", StringType()),
    StructField("submitted_at", StringType()),
])


def main(bootstrap: str, topic: str, user: str, password: str, output: str) -> None:
    spark = SparkSession.builder.appName("hse-etl-kafka-flatten-loans").getOrCreate()

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .option("kafka.security.protocol", "SASL_SSL")
           .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
           .option("kafka.sasl.jaas.config",
                   "org.apache.kafka.common.security.scram.ScramLoginModule required "
                   f'username="{user}" password="{password}";')
           .load())

    events = (raw.select(F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_ts"),
                         F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
              .select("partition", "offset", "kafka_ts", "e.*"))

    # Плоская таблица заявок: вложенные поля -> колонки, массив документов -> агрегаты
    flat = (events
            .withColumn("submitted_at", F.to_timestamp("submitted_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .select(
                "application_id",
                F.col("customer.customer_id").alias("customer_id"),
                F.col("customer.region").alias("region_code"),
                F.col("loan.amount").alias("loan_amount"),
                F.col("loan.term_months").alias("term_months"),
                F.col("scoring.score").alias("credit_score"),
                F.col("scoring.risk_level").alias("risk_level"),
                F.size("documents").alias("documents_count"),
                F.expr("size(filter(documents, d -> d.status = 'verified'))").alias("documents_verified"),
                F.expr("concat_ws(',', transform(documents, d -> d.type))").alias("document_types"),
                "decision_status",
                "submitted_at",
                F.to_date("submitted_at").alias("submitted_date"),
                "partition", "offset", "kafka_ts"))

    # Развёрнутые документы: одна строка на документ
    docs = (events.select("application_id", F.explode("documents").alias("d"))
            .select("application_id", F.col("d.type").alias("document_type"),
                    F.col("d.status").alias("document_status")))

    n = flat.count()
    print(f"=== events read from kafka: {n}")
    flat.printSchema()
    flat.show(10, truncate=False)

    flat.coalesce(1).write.mode("overwrite").parquet(f"{output}/loan_applications_flat")
    docs.coalesce(1).write.mode("overwrite").parquet(f"{output}/loan_documents")
    flat.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output}/csv/loan_applications_flat")

    summary = (flat.groupBy("submitted_date", "risk_level", "decision_status")
               .agg(F.count("*").alias("applications"), F.sum("loan_amount").alias("amount_total"),
                    F.avg("credit_score").alias("avg_score")))
    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output}/csv/loan_summary")
    summary.orderBy("submitted_date", "risk_level").show(20, truncate=False)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        sys.exit("usage: kafka_flatten_loans.py <bootstrap> <topic> <user> <password> <output_path>")
    main(*sys.argv[1:])
