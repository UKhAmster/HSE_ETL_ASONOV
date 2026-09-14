"""PySpark-задание для Yandex Data Processing (задание 2).

Читает CSV с кредитными заявками из Object Storage, считает витрины
и пишет результат в parquet обратно в бакет.

Аргументы: <input_path> <output_path>
Пример:
  s3a://hse-etl-asonov-2026/input/applications/2026-05/
  s3a://hse-etl-asonov-2026/output/applications/2026-05/
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(input_path: str, output_path: str) -> None:
    spark = SparkSession.builder.appName("hse-etl-process-applications").getOrCreate()

    df = (spark.read.option("header", True).option("inferSchema", True).csv(input_path)
          .withColumn("event_time", F.to_timestamp("event_time"))
          .withColumn("event_date", F.to_date("event_time")))

    total = df.count()
    print(f"=== rows read: {total}")

    # Витрина 1: заявки по дням, продуктам и решениям
    daily = (df.groupBy("event_date", "product_type", "decision_status")
             .agg(F.count("*").alias("applications"),
                  F.sum("requested_amount").alias("requested_total"),
                  F.sum("approved_amount").alias("approved_total"),
                  F.avg("credit_score").alias("avg_credit_score"),
                  F.avg("processing_time_sec").alias("avg_processing_sec")))

    # Витрина 2: конверсия одобрения по регионам и каналам
    region = (df.groupBy("region_code", "channel")
              .agg(F.count("*").alias("applications"),
                   F.sum(F.when(F.col("decision_status") == "approved", 1).otherwise(0)).alias("approved"),
                   F.avg("requested_amount").alias("avg_requested"))
              .withColumn("approval_rate", F.round(F.col("approved") / F.col("applications"), 4)))

    # Витрина 3: распределение риска
    risk = (df.groupBy("risk_level", "product_type")
            .agg(F.count("*").alias("applications"),
                 F.avg("credit_score").alias("avg_credit_score"),
                 F.sum(F.when(F.col("employee_review_flag") == True, 1).otherwise(0)).alias("manual_reviews")))

    daily.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/daily_by_product")
    region.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/region_channel_conversion")
    risk.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/risk_distribution")
    # Копия в CSV для загрузки в DataLens
    region.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/csv/region_channel_conversion")
    daily.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_path}/csv/daily_by_product")

    print("=== daily_by_product sample")
    daily.orderBy("event_date", "product_type").show(10, truncate=False)
    print("=== region_channel_conversion")
    region.orderBy("region_code", "channel").show(40, truncate=False)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: process_applications.py <input_path> <output_path>")
    main(sys.argv[1], sys.argv[2])
