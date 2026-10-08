import argparse
import json
import os
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    expr,
    lit,
    max as spark_max,
    row_number,
    sum as spark_sum,
    to_timestamp,
    when,
)

from bigdata_scale_pyspark import build_dataset


def parse_rows(value):
    value = value.strip().lower().replace("_", "")
    multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}

    if value[-1:] in multipliers:
        rows = int(float(value[:-1]) * multipliers[value[-1]])
    else:
        rows = int(value)

    if rows <= 0:
        raise argparse.ArgumentTypeError("rows must be > 0")

    return rows


def scale_name(rows):
    if rows % 1_000_000 == 0:
        return f"{rows // 1_000_000}m"
    if rows % 1_000 == 0:
        return f"{rows // 1_000}k"
    return str(rows)


def clean_orders(raw_df):
    typed = (
        raw_df
        .withColumn("id", col("id").cast("long"))
        .withColumn("age", col("age").cast("int"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("discount", col("discount").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("order_date", col("order_date").cast("date"))
        .withColumn("ship_date", col("ship_date").cast("date"))
        .withColumn("delivery_days", col("delivery_days").cast("int"))
        .withColumn("shipping_cost", col("shipping_cost").cast("double"))
        .withColumn("customer_rating", col("customer_rating").cast("int"))
        .withColumn("is_member", col("is_member").cast("boolean"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
    )

    return (
        typed
        .filter(col("id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .filter(col("age").between(18, 100))
        .filter(col("quantity") > 0)
        .filter(col("unit_price") >= 0)
        .filter(col("discount").between(0, 1))
        .filter(col("customer_rating").between(1, 5))
        .dropDuplicates(["id"])
    )


def build_cdc_events(orders_df):
    # Every base order starts as an INSERT event.
    inserts = orders_df.select(
        col("id").alias("order_id"),
        lit("INSERT").alias("operation"),
        to_timestamp(
            date_format(col("order_date"), "yyyy-MM-dd")
        ).alias("event_ts"),
        lit(1).cast("long").alias("event_seq"),
        col("customer_id"),
        col("country"),
        col("product_category"),
        col("quantity"),
        col("total_amount"),
        col("order_status"),
    )

    # Simulate updates for roughly 5% of orders.
    updates = (
        orders_df
        .filter(expr("pmod(id * 37, 100) < 5"))
        .select(
            col("id").alias("order_id"),
            lit("UPDATE").alias("operation"),
            to_timestamp(
                date_format(
                    expr("date_add(order_date, 30)"),
                    "yyyy-MM-dd"
                )
            ).alias("event_ts"),
            lit(2).cast("long").alias("event_seq"),
            col("customer_id"),
            col("country"),
            col("product_category"),
            (col("quantity") + lit(1)).alias("quantity"),
            expr("round(total_amount * 1.03, 2)").alias("total_amount"),
            when(
                col("order_status") == "PROCESSING",
                lit("SHIPPED"),
            ).otherwise(col("order_status")).alias("order_status"),
        )
    )

    # Simulate deletes for roughly 1% of orders.
    deletes = (
        orders_df
        .filter(expr("pmod(id * 41, 100) < 1"))
        .select(
            col("id").alias("order_id"),
            lit("DELETE").alias("operation"),
            to_timestamp(
                date_format(
                    expr("date_add(order_date, 45)"),
                    "yyyy-MM-dd"
                )
            ).alias("event_ts"),
            lit(3).cast("long").alias("event_seq"),
            col("customer_id"),
            col("country"),
            col("product_category"),
            col("quantity"),
            col("total_amount"),
            col("order_status"),
        )
    )

    return inserts.unionByName(updates).unionByName(deletes)


def latest_event_per_order(events_df):
    event_window = Window.partitionBy("order_id").orderBy(
        col("event_ts").desc(),
        col("event_seq").desc(),
    )

    return (
        events_df
        .withColumn("event_rank", row_number().over(event_window))
        .filter(col("event_rank") == 1)
        .drop("event_rank")
    )


def write_summary(df, path):
    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Incremental ETL + CDC PySpark exercise for 1M / 10M / 100M rows."
    )
    parser.add_argument("--rows", type=parse_rows, default=1_000_000)
    parser.add_argument("--output-dir", default="output/exercise_03")
    parser.add_argument("--shuffle-partitions", type=int, default=None)
    args = parser.parse_args()

    name = scale_name(args.rows)
    partitions = args.shuffle_partitions or max(
        16, min(200, args.rows // 100_000)
    )

    spark = (
        SparkSession.builder
        .appName(f"IncrementalCDC-{name}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    output_dir = os.path.join(args.output_dir, name)
    bronze_path = os.path.join(output_dir, "bronze_cdc")
    silver_path = os.path.join(output_dir, "silver_current_orders")
    gold_path = os.path.join(output_dir, "gold_business_metrics")
    os.makedirs(output_dir, exist_ok=True)

    timings = {}

    # --------------------------------------------------------------
    # 1. Build a clean source dataset.
    # --------------------------------------------------------------
    t0 = time.perf_counter()
    raw_df, _ = build_dataset(spark, args.rows)
    orders_df = clean_orders(raw_df).persist(StorageLevel.MEMORY_AND_DISK)
    base_count = orders_df.count()
    timings["source_materialization_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    # --------------------------------------------------------------
    # 2. Bronze: CDC events.
    # --------------------------------------------------------------
    t0 = time.perf_counter()
    events_df = build_cdc_events(orders_df).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    event_count = events_df.count()
    event_operation_counts = {
        row["operation"]: row["count"]
        for row in events_df.groupBy("operation").count().collect()
    }

    (
        events_df
        .withColumn("event_date", date_format("event_ts", "yyyy-MM-dd"))
        .repartition("event_date")
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(bronze_path)
    )
    timings["bronze_cdc_seconds"] = round(time.perf_counter() - t0, 2)

    # --------------------------------------------------------------
    # 3. Silver: latest event wins, DELETE removes the current row.
    # --------------------------------------------------------------
    t0 = time.perf_counter()
    latest_events = latest_event_per_order(events_df).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    current_orders = (
        latest_events
        .filter(col("operation") != "DELETE")
        .drop("operation", "event_ts", "event_seq")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    current_count = current_orders.count()
    deleted_count = (
        latest_events
        .filter(col("operation") == "DELETE")
        .count()
    )

    current_orders.write.mode("overwrite").parquet(silver_path)
    timings["silver_cdc_merge_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    # --------------------------------------------------------------
    # 4. Gold: business metrics from the current-state Silver table.
    # --------------------------------------------------------------
    t0 = time.perf_counter()
    # Keep the final gold table simple and stable for all scales.
    gold_metrics = (
        current_orders
        .groupBy("country", "product_category")
        .agg(
            count("order_id").alias("orders"),
            spark_sum("quantity").alias("units"),
            spark_sum("total_amount").alias("revenue"),
            avg("total_amount").alias("avg_order_value"),
        )
        .orderBy("country", col("revenue").desc())
    )

    gold_rows = gold_metrics.collect()
    gold_metrics.write.mode("overwrite").parquet(gold_path)
    timings["gold_aggregation_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    # --------------------------------------------------------------
    # 5. Watermark simulation:
    #    determine the latest processed event timestamp.
    # --------------------------------------------------------------
    watermark = (
        events_df
        .agg(spark_max("event_ts").alias("max_event_ts"))
        .first()["max_event_ts"]
    )

    # --------------------------------------------------------------
    # 6. Idempotency check:
    #    processing the same CDC feed twice must not change the
    #    final current-state row count.
    # --------------------------------------------------------------
    first_snapshot = latest_event_per_order(events_df)

    second_snapshot = latest_event_per_order(
        events_df.unionByName(events_df)
    )

    first_count = first_snapshot.count()
    second_count = second_snapshot.count()

    idempotent = first_count == second_count

    metrics = {
        "exercise": "incremental_etl_cdc",
        "scale": name,
        "base_rows": args.rows,
        "source_clean_rows": base_count,
        "cdc_event_rows": event_count,
        "insert_events": event_operation_counts.get("INSERT", 0),
        "update_events": event_operation_counts.get("UPDATE", 0),
        "delete_events": event_operation_counts.get("DELETE", 0),
        "latest_event_rows": first_count,
        "current_silver_rows": current_count,
        "deleted_current_rows": deleted_count,
        "gold_result_groups": len(gold_rows),
        "latest_event_timestamp": str(watermark),
        "idempotency_check": idempotent,
        "shuffle_partitions": partitions,
        "timings_seconds": timings,
    }

    with open(
        os.path.join(output_dir, "cdc_metrics.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(metrics, f, indent=2)

    print("\n===== EXERCISE 03 RESULT =====")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    print("\n===== OUTPUTS =====")
    for path in [
        bronze_path,
        silver_path,
        gold_path,
        os.path.join(output_dir, "cdc_metrics.json"),
    ]:
        print(path)

    second_snapshot.unpersist()
    first_snapshot.unpersist()
    current_orders.unpersist()
    latest_events.unpersist()
    events_df.unpersist()
    orders_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
