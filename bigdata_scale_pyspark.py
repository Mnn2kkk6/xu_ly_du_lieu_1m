import argparse
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    count,
    date_add,
    date_format,
    date_sub,
    lpad,
    lit,
    pmod,
    rand,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
)


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


def build_dataset(spark, n):
    base = spark.range(0, n).withColumnRenamed("id", "row_id")
    base_date = to_date(lit("2026-10-05"))

    df = (
        base
        .withColumn("id", col("row_id"))
        .withColumn(
            "customer_id",
            concat(lit("C"), lpad((col("row_id") % 200_000).cast("string"), 6, "0"))
        )
        .withColumn(
            "order_id",
            concat(lit("O"), lpad(col("row_id").cast("string"), 8, "0"))
        )
        .withColumn("age", (18 + (col("row_id") % 63)).cast("int"))
        .withColumn("gender", when(col("row_id") % 2 == 0, "M").otherwise("F"))
        .withColumn(
            "city",
            when(col("row_id") % 5 == 0, "Hanoi")
            .when(col("row_id") % 5 == 1, "HCM")
            .when(col("row_id") % 5 == 2, "Da Nang")
            .when(col("row_id") % 5 == 3, "Hai Phong")
            .otherwise("Can Tho")
        )
        .withColumn(
            "country",
            when(col("row_id") % 5 == 0, "VN")
            .when(col("row_id") % 5 == 1, "VN")
            .when(col("row_id") % 5 == 2, "TH")
            .when(col("row_id") % 5 == 3, "SG")
            .otherwise("MY")
        )
        .withColumn(
            "product_id",
            concat(lit("P"), lpad((col("row_id") % 50_000).cast("string"), 6, "0"))
        )
        .withColumn(
            "product_category",
            when(col("row_id") % 6 == 0, "Electronics")
            .when(col("row_id") % 6 == 1, "Fashion")
            .when(col("row_id") % 6 == 2, "Home")
            .when(col("row_id") % 6 == 3, "Beauty")
            .when(col("row_id") % 6 == 4, "Sports")
            .otherwise("Books")
        )
        .withColumn("quantity", (1 + (col("row_id") % 10)).cast("int"))
        .withColumn("unit_price", spark_round(rand(7) * 490 + 10, 2))
        .withColumn("discount", spark_round(rand(8) * 0.30, 4))
        .withColumn(
            "total_amount",
            spark_round(
                (col("quantity") * col("unit_price")) * (1 - col("discount")),
                2,
            ),
        )
        .withColumn(
            "payment_method",
            when(col("row_id") % 4 == 0, "CARD")
            .when(col("row_id") % 4 == 1, "CASH")
            .when(col("row_id") % 4 == 2, "BANK_TRANSFER")
            .otherwise("EWALLET")
        )
        .withColumn(
            "order_status",
            when(col("row_id") % 5 == 0, "COMPLETED")
            .when(col("row_id") % 5 == 1, "SHIPPED")
            .when(col("row_id") % 5 == 2, "PROCESSING")
            .when(col("row_id") % 5 == 3, "CANCELLED")
            .otherwise("RETURNED")
        )
        .withColumn(
            "order_date",
            date_sub(base_date, (col("row_id") % 365).cast("int"))
        )
        .withColumn("delivery_days", (1 + (col("row_id") % 14)).cast("int"))
        .withColumn("ship_date", date_add(col("order_date"), col("delivery_days")))
        .withColumn("shipping_cost", spark_round(rand(9) * 45 + 5, 2))
        .withColumn(
            "warehouse_id",
            concat(lit("W"), lpad((col("row_id") % 30).cast("string"), 3, "0"))
        )
        .withColumn(
            "seller_id",
            concat(lit("S"), lpad((col("row_id") % 10_000).cast("string"), 5, "0"))
        )
        .withColumn("customer_rating", (1 + (col("row_id") % 5)).cast("int"))
        .withColumn(
            "coupon_code",
            when(
                col("row_id") % 3 == 0,
                concat(lit("CPN"), (col("row_id") % 100).cast("string")),
            ).otherwise(""),
        )
        .withColumn("is_member", col("row_id") % 2 == 0)
        .withColumn(
            "device_type",
            when(col("row_id") % 3 == 0, "mobile")
            .when(col("row_id") % 3 == 1, "desktop")
            .otherwise("tablet")
        )
        .withColumn(
            "channel",
            when(col("row_id") % 3 == 0, "web")
            .when(col("row_id") % 3 == 1, "app")
            .otherwise("store")
        )
        .withColumn(
            "source",
            when(col("row_id") % 4 == 0, "organic")
            .when(col("row_id") % 4 == 1, "ads")
            .when(col("row_id") % 4 == 2, "social")
            .otherwise("email")
        )
        .withColumn("latitude", spark_round(rand(10) * 25 + 1, 6))
        .withColumn("longitude", spark_round(rand(11) * 104 + 95, 6))
        .withColumn(
            "created_at",
            date_format(
                date_sub(base_date, (col("row_id") % 365).cast("int")),
                "yyyy-MM-dd",
            ),
        )
        .drop("row_id")
    )

    columns = [
        "id", "customer_id", "order_id", "age", "gender", "city", "country",
        "product_id", "product_category", "quantity", "unit_price", "discount",
        "total_amount", "payment_method", "order_status", "order_date", "ship_date",
        "delivery_days", "shipping_cost", "warehouse_id", "seller_id",
        "customer_rating", "coupon_code", "is_member", "device_type", "channel",
        "source", "latitude", "longitude", "created_at",
    ]
    assert len(columns) == 30

    raw = df.select(*[col(c).cast("string").alias(c) for c in columns])

    null_rules = (
        ("age", 100),
        ("quantity", 101),
        ("unit_price", 102),
        ("customer_rating", 103),
        ("city", 104),
    )
    for field_name, salt in null_rules:
        raw = raw.withColumn(
            field_name,
            when(
                pmod(col("id") * (salt + 17), 100_000) < 200,
                None,
            ).otherwise(col(field_name)),
        )

    raw = (
        raw
        .withColumn(
            "age",
            when(pmod(col("id") * 307, 100_000) < 150, "abc")
            .otherwise(col("age")),
        )
        .withColumn(
            "quantity",
            when(pmod(col("id") * 311, 100_000) < 150, "-3")
            .otherwise(col("quantity")),
        )
        .withColumn(
            "unit_price",
            when(pmod(col("id") * 313, 100_000) < 150, "N/A")
            .otherwise(col("unit_price")),
        )
        .withColumn(
            "customer_rating",
            when(pmod(col("id") * 317, 100_000) < 150, "6")
            .otherwise(col("customer_rating")),
        )
    )

    duplicate_rows = max(1, int(n * 0.01))
    return raw.unionByName(raw.limit(duplicate_rows)), columns


def main():
    parser = argparse.ArgumentParser(
        description="Scale data-quality exercise with PySpark."
    )
    parser.add_argument("--rows", type=parse_rows, default=1_000_000)
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--shuffle-partitions", type=int, default=None)
    args = parser.parse_args()

    name = scale_name(args.rows)
    partitions = args.shuffle_partitions or max(
        16, min(200, args.rows // 100_000)
    )

    spark = (
        SparkSession.builder
        .appName(f"LargeDataQualityPractice-{name}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    timings = {}

    t0 = time.perf_counter()
    raw_df, columns = build_dataset(spark, args.rows)
    raw_count = raw_df.count()
    timings["generate_and_materialize_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    t0 = time.perf_counter()
    distinct_id_count = raw_df.select("id").distinct().count()
    timings["distinct_id_seconds"] = round(time.perf_counter() - t0, 2)

    invalid_condition = (
        (
            col("age").isNotNull()
            & (
                ~col("age").rlike(r"^[0-9]+$")
                | (col("age").cast("int") < 0)
                | (col("age").cast("int") > 120)
            )
        )
        | (
            col("quantity").isNotNull()
            & (
                ~col("quantity").rlike(r"^[0-9]+$")
                | (col("quantity").cast("int") <= 0)
            )
        )
        | (
            col("unit_price").isNotNull()
            & (
                ~col("unit_price").rlike(r"^[0-9]+(\.[0-9]+)?$")
                | (col("unit_price").cast("double") < 0)
            )
        )
        | (
            col("customer_rating").isNotNull()
            & (
                ~col("customer_rating").rlike(r"^[0-9]+$")
                | ~col("customer_rating").cast("int").between(1, 5)
            )
        )
    )

    t0 = time.perf_counter()
    invalid_count = raw_df.filter(invalid_condition).count()
    timings["invalid_check_seconds"] = round(time.perf_counter() - t0, 2)

    clean_candidate = (
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

    t0 = time.perf_counter()
    clean_df = (
        clean_candidate
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
    clean_count = clean_df.count()
    timings["clean_and_deduplicate_seconds"] = round(
        time.perf_counter() - t0, 2
    )

    t0 = time.perf_counter()
    summary = (
        clean_df
        .groupBy("country", "product_category")
        .agg(
            count("id").alias("orders"),
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("total_amount").alias("revenue"),
            avg("delivery_days").alias("avg_delivery_days"),
        )
        .orderBy(col("revenue").desc())
    )
    summary_count = summary.count()
    timings["groupby_seconds"] = round(time.perf_counter() - t0, 2)

    output_path = os.path.join(args.output_dir, name, "orders_parquet")
    t0 = time.perf_counter()
    (
        clean_df
        .repartition(max(4, min(64, partitions // 2)), "country")
        .write
        .mode("overwrite")
        .partitionBy("country")
        .parquet(output_path)
    )
    timings["parquet_write_seconds"] = round(time.perf_counter() - t0, 2)

    metrics = {
        "scale": name,
        "base_rows": args.rows,
        "raw_rows": raw_count,
        "distinct_ids": distinct_id_count,
        "invalid_rows": invalid_count,
        "clean_rows": clean_count,
        "summary_groups": summary_count,
        "shuffle_partitions": partitions,
        "output_path": output_path,
        "timings_seconds": timings,
    }

    metrics_dir = os.path.join(args.output_dir, name)
    os.makedirs(metrics_dir, exist_ok=True)
    metrics_path = os.path.join(metrics_dir, "benchmark_metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("\n===== SCALE RESULT =====")
    for key, value in metrics.items():
        print(f"{key}: {value}")
    print(f"metrics_path: {metrics_path}")

    spark.stop()


if __name__ == "__main__":
    main()
