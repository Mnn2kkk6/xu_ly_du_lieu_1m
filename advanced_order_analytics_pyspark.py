import argparse
import csv
import json
import os
import time

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    date_format,
    dense_rank,
    expr,
    lit,
    max as spark_max,
    row_number,
    stddev,
    sum as spark_sum,
    when,
)

# Reuse the same synthetic 30-field order generator as the existing scale exercise.
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


def prepare_clean_orders(raw_df):
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


def write_small_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

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
        description="Advanced PySpark analytics exercise for 1M / 10M / 100M rows."
    )
    parser.add_argument("--rows", type=parse_rows, default=1_000_000)
    parser.add_argument("--output-dir", default="output/advanced")
    parser.add_argument("--shuffle-partitions", type=int, default=None)
    args = parser.parse_args()

    name = scale_name(args.rows)
    partitions = args.shuffle_partitions or max(
        16, min(200, args.rows // 100_000)
    )

    spark = (
        __import__("pyspark").sql.SparkSession.builder
        .appName(f"AdvancedOrderAnalytics-{name}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    output_dir = os.path.join(args.output_dir, name)
    os.makedirs(output_dir, exist_ok=True)

    timings = {}

    # ------------------------------------------------------------------
    # 1. Generate and clean the same 30-field order dataset.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    raw_df, _ = build_dataset(spark, args.rows)
    raw_count = raw_df.count()
    timings["generate_and_materialize_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    t0 = time.perf_counter()
    clean_df = prepare_clean_orders(raw_df).withColumn(
        "order_month", date_format("order_date", "yyyy-MM")
    )
    clean_df = clean_df.persist(StorageLevel.MEMORY_AND_DISK)
    clean_count = clean_df.count()
    timings["clean_and_deduplicate_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    # ------------------------------------------------------------------
    # 2. Customer 360:
    #    Aggregate revenue, orders and delivery performance by customer.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    customer_metrics = (
        clean_df
        .groupBy("customer_id", "country", "city")
        .agg(
            count("id").alias("orders"),
            spark_sum("total_amount").alias("lifetime_revenue"),
            avg("total_amount").alias("avg_order_value"),
            avg("delivery_days").alias("avg_delivery_days"),
            avg("customer_rating").alias("avg_rating"),
        )
    )

    customer_metrics = customer_metrics.persist(StorageLevel.MEMORY_ONLY)\n\n    revenue_p50, revenue_p90 = customer_metrics.approxQuantile(
        "lifetime_revenue",
        [0.50, 0.90],
        0.01,
    )

    customer_segments = (
        customer_metrics
        .withColumn(
            "customer_segment",
            when(col("lifetime_revenue") >= lit(revenue_p90), "VIP")
            .when(col("lifetime_revenue") >= lit(revenue_p50), "CORE")
            .otherwise("STANDARD"),
        )
        .orderBy(col("lifetime_revenue").desc())
    )

    customer_segment_summary = (
        customer_segments
        .groupBy("country", "customer_segment")
        .agg(
            count("*").alias("customers"),
            spark_sum("lifetime_revenue").alias("segment_revenue"),
            avg("avg_order_value").alias("avg_order_value"),
        )
        .orderBy("country", "customer_segment")
    )

    # Force execution of the segment summary.
    segment_rows = customer_segment_summary.collect()
    timings["customer_360_seconds"] = round(time.perf_counter() - t0, 2)

    write_small_csv(
        customer_segment_summary,
        os.path.join(output_dir, "customer_segment_summary"),
    )

    # ------------------------------------------------------------------
    # 3. Top-3 products per country using a Window function.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    product_country = (
        clean_df
        .groupBy("country", "product_id", "product_category")
        .agg(
            count("id").alias("orders"),
            spark_sum("quantity").alias("units"),
            spark_sum("total_amount").alias("revenue"),
        )
    )

    product_window = Window.partitionBy("country").orderBy(
        col("revenue").desc()
    )

    top_products = (
        product_country
        .withColumn("revenue_rank", dense_rank().over(product_window))
        .filter(col("revenue_rank") <= 3)
        .orderBy("country", "revenue_rank", col("revenue").desc())
    )

    top_product_rows = top_products.collect()
    timings["top3_products_window_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    write_small_csv(
        top_products,
        os.path.join(output_dir, "top3_products_by_country"),
    )

    # ------------------------------------------------------------------
    # 4. Seven-day rolling revenue by country.
    #    rowsBetween(-6, 0) works because the synthetic dataset contains
    #    all calendar dates in the generated 365-day range.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    daily_revenue = (
        clean_df
        .groupBy("country", "order_date")
        .agg(
            spark_sum("total_amount").alias("daily_revenue"),
            count("id").alias("daily_orders"),
        )
    )

    rolling_window = (
        Window
        .partitionBy("country")
        .orderBy("order_date")
        .rowsBetween(-6, 0)
    )

    rolling_revenue = (
        daily_revenue
        .withColumn(
            "rolling_7d_revenue",
            spark_sum("daily_revenue").over(rolling_window),
        )
        .withColumn(
            "rolling_7d_orders",
            spark_sum("daily_orders").over(rolling_window),
        )
        .orderBy("country", "order_date")
    )

    rolling_tail = (
        rolling_revenue
        .withColumn(
            "latest_day_rank",
            row_number().over(
                Window.partitionBy("country").orderBy(
                    col("order_date").desc()
                )
            ),
        )
        .filter(col("latest_day_rank") <= 10)
        .drop("latest_day_rank")
    )

    rolling_rows = rolling_tail.collect()
    timings["rolling_7d_window_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    write_small_csv(
        rolling_tail,
        os.path.join(output_dir, "rolling_7d_revenue_latest_10_days"),
    )

    # ------------------------------------------------------------------
    # 5. Warehouse utilization: aggregate orders per warehouse/day,
    #    then broadcast-join a tiny warehouse dimension.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    warehouse_dim = (
        spark.range(0, 30)
        .withColumnRenamed("id", "warehouse_num")
        .withColumn(
            "warehouse_id",
            expr("concat('W', lpad(cast(warehouse_num as string), 3, '0'))"),
        )
        .withColumn(
            "daily_capacity",
            (lit(4_000) + col("warehouse_num") * lit(250)).cast("double"),
        )
        .select("warehouse_id", "daily_capacity")
    )

    warehouse_daily = (
        clean_df
        .groupBy("warehouse_id", "order_date")
        .agg(
            count("id").alias("orders"),
            spark_sum("quantity").alias("units"),
            spark_sum("total_amount").alias("revenue"),
        )
    )

    warehouse_utilization = (
        warehouse_daily
        .join(
            broadcast(warehouse_dim),
            on="warehouse_id",
            how="left",
        )
        .withColumn(
            "capacity_utilization_pct",
            spark_round(
                col("units") / col("daily_capacity") * lit(100),
                2,
            ),
        )
    )

    busiest_warehouses = (
        warehouse_utilization
        .orderBy(col("capacity_utilization_pct").desc())
        .limit(20)
    )

    warehouse_rows = busiest_warehouses.collect()
    timings["warehouse_broadcast_join_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    write_small_csv(
        busiest_warehouses,
        os.path.join(output_dir, "busiest_warehouse_days"),
    )

    # ------------------------------------------------------------------
    # 6. Customer-level anomaly detection with a partitioned window.
    #    Orders far above a customer's typical order value are flagged.
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    customer_order_window = Window.partitionBy("customer_id")

    customer_anomalies = (
        clean_df
        .withColumn(
            "customer_avg_order",
            avg("total_amount").over(customer_order_window),
        )
        .withColumn(
            "customer_order_stddev",
            stddev("total_amount").over(customer_order_window),
        )
        .withColumn(
            "high_value_flag",
            when(
                (col("customer_order_stddev") > lit(0))
                & (
                    col("total_amount")
                    > col("customer_avg_order")
                    + lit(3) * col("customer_order_stddev")
                ),
                lit(1),
            ).otherwise(lit(0)),
        )
        .filter(col("high_value_flag") == 1)
        .select(
            "id",
            "customer_id",
            "country",
            "order_date",
            "total_amount",
            "customer_avg_order",
            "customer_order_stddev",
        )
        .orderBy(col("total_amount").desc())
        .limit(100)
    )

    anomaly_rows = customer_anomalies.collect()
    timings["customer_anomaly_window_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    write_small_csv(
        customer_anomalies,
        os.path.join(output_dir, "top_customer_order_anomalies"),
    )

    # ------------------------------------------------------------------
    # 7. Compact benchmark + business result metadata.
    # ------------------------------------------------------------------
    total_revenue = clean_df.agg(
        spark_sum("total_amount").alias("revenue")
    ).first()["revenue"]

    average_delivery = clean_df.agg(
        avg("delivery_days").alias("avg_delivery_days")
    ).first()["avg_delivery_days"]

    metrics = {
        "exercise": "advanced_order_analytics",
        "scale": name,
        "base_rows": args.rows,
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "shuffle_partitions": partitions,
        "customer_count": customer_metrics.count(),
        "top_product_rows": len(top_product_rows),
        "rolling_rows": len(rolling_rows),
        "warehouse_result_rows": len(warehouse_rows),
        "anomaly_result_rows": len(anomaly_rows),
        "customer_revenue_p50": round(float(revenue_p50), 4),
        "customer_revenue_p90": round(float(revenue_p90), 4),
        "total_revenue": round(float(total_revenue or 0), 4),
        "average_delivery_days": round(float(average_delivery or 0), 4),
        "timings_seconds": timings,
    }

    with open(
        os.path.join(output_dir, "advanced_metrics.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(metrics, f, indent=2)

    print("\n===== ADVANCED ANALYTICS RESULT =====")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    print("\n===== OUTPUTS =====")
    for filename in [
        "customer_segment_summary",
        "top3_products_by_country",
        "rolling_7d_revenue_latest_10_days",
        "busiest_warehouse_days",
        "top_customer_order_anomalies",
        "advanced_metrics.json",
    ]:
        print(os.path.join(output_dir, filename))

    clean_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
