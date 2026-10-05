from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, rand, lpad, date_sub, date_add,
    date_format, to_date, concat, count, sum as spark_sum, avg,
    round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType

N = 1_000_000
DUP_ROWS = 10_000
BASE_DATE = "2026-10-05"

RAW_PATH = "data/raw/orders_csv"
PARQUET_PATH = "data/processed/orders_parquet"
SUMMARY_PATH = "data/processed/orders_summary"

spark = (
    SparkSession.builder
    .appName("LargeDataQualityPractice")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# 1. Generate 1M base records with exactly 30 fields
base = spark.range(0, N).withColumnRenamed("id", "row_id")

base = (
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
    .withColumn("gender", when(col("row_id") % 2 == 0, lit("M")).otherwise(lit("F")))
    .withColumn(
        "city",
        when(col("row_id") % 5 == 0, lit("Hanoi"))
        .when(col("row_id") % 5 == 1, lit("HCM"))
        .when(col("row_id") % 5 == 2, lit("Da Nang"))
        .when(col("row_id") % 5 == 3, lit("Hai Phong"))
        .otherwise(lit("Can Tho"))
    )
    .withColumn(
        "country",
        when(col("row_id") % 5 == 0, lit("VN"))
        .when(col("row_id") % 5 == 1, lit("VN"))
        .when(col("row_id") % 5 == 2, lit("TH"))
        .when(col("row_id") % 5 == 3, lit("SG"))
        .otherwise(lit("MY"))
    )
    .withColumn(
        "product_id",
        concat(lit("P"), lpad((col("row_id") % 50_000).cast("string"), 6, "0"))
    )
    .withColumn(
        "product_category",
        when(col("row_id") % 6 == 0, lit("Electronics"))
        .when(col("row_id") % 6 == 1, lit("Fashion"))
        .when(col("row_id") % 6 == 2, lit("Home"))
        .when(col("row_id") % 6 == 3, lit("Beauty"))
        .when(col("row_id") % 6 == 4, lit("Sports"))
        .otherwise(lit("Books"))
    )
    .withColumn("quantity", (1 + (col("row_id") % 10)).cast("int"))
    .withColumn("unit_price", spark_round(rand(7) * 490 + 10, 2))
    .withColumn("discount", spark_round(rand(8) * 0.30, 4))
    .withColumn(
        "total_amount",
        spark_round(
            (col("quantity") * col("unit_price")) * (1 - col("discount")),
            2
        )
    )
    .withColumn(
        "payment_method",
        when(col("row_id") % 4 == 0, lit("CARD"))
        .when(col("row_id") % 4 == 1, lit("CASH"))
        .when(col("row_id") % 4 == 2, lit("BANK_TRANSFER"))
        .otherwise(lit("EWALLET"))
    )
    .withColumn(
        "order_status",
        when(col("row_id") % 5 == 0, lit("COMPLETED"))
        .when(col("row_id") % 5 == 1, lit("SHIPPED"))
        .when(col("row_id") % 5 == 2, lit("PROCESSING"))
        .when(col("row_id") % 5 == 3, lit("CANCELLED"))
        .otherwise(lit("RETURNED"))
    )
    .withColumn(
        "order_date",
        date_sub(to_date(lit(BASE_DATE)), (col("row_id") % 365).cast("int"))
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
            concat(lit("CPN"), (col("row_id") % 100).cast("string"))
        ).otherwise(lit(""))
    )
    .withColumn("is_member", col("row_id") % 2 == 0)
    .withColumn(
        "device_type",
        when(col("row_id") % 3 == 0, lit("mobile"))
        .when(col("row_id") % 3 == 1, lit("desktop"))
        .otherwise(lit("tablet"))
    )
    .withColumn(
        "channel",
        when(col("row_id") % 3 == 0, lit("web"))
        .when(col("row_id") % 3 == 1, lit("app"))
        .otherwise(lit("store"))
    )
    .withColumn(
        "source",
        when(col("row_id") % 4 == 0, lit("organic"))
        .when(col("row_id") % 4 == 1, lit("ads"))
        .when(col("row_id") % 4 == 2, lit("social"))
        .otherwise(lit("email"))
    )
    .withColumn("latitude", spark_round(rand(10) * 25 + 1, 6))
    .withColumn("longitude", spark_round(rand(11) * 104 + 95, 6))
    .withColumn(
        "created_at",
        date_format(
            date_sub(to_date(lit(BASE_DATE)), (col("row_id") % 365).cast("int")),
            "yyyy-MM-dd"
        )
    )
    .drop("row_id")
)

columns = [
    "id", "customer_id", "order_id", "age", "gender", "city", "country",
    "product_id", "product_category", "quantity", "unit_price", "discount",
    "total_amount", "payment_method", "order_status", "order_date", "ship_date",
    "delivery_days", "shipping_cost", "warehouse_id", "seller_id",
    "customer_rating", "coupon_code", "is_member", "device_type", "channel",
    "source", "latitude", "longitude", "created_at"
]

assert len(columns) == 30, f"Expected 30 columns, got {len(columns)}"


# 2. Convert to strings so the raw source can contain bad values
raw = base.select(*[col(c).cast("string").alias(c) for c in columns])


# 3. Inject NULLs and invalid values
for idx, field_name in enumerate(
    ["age", "quantity", "unit_price", "customer_rating", "city"]
):
    raw = raw.withColumn(
        field_name,
        when(rand(100 + idx) < 0.002, lit(None)).otherwise(col(field_name))
    )

raw = raw.withColumn(
    "age",
    when(rand(200) < 0.0015, lit("abc")).otherwise(col("age"))
)
raw = raw.withColumn(
    "quantity",
    when(rand(201) < 0.0015, lit("-3")).otherwise(col("quantity"))
)
raw = raw.withColumn(
    "unit_price",
    when(rand(202) < 0.0015, lit("N/A")).otherwise(col("unit_price"))
)
raw = raw.withColumn(
    "customer_rating",
    when(rand(203) < 0.0015, lit("6")).otherwise(col("customer_rating"))
)


# 4. Add duplicate records
raw = raw.unionByName(raw.limit(DUP_ROWS))


# 5. Write raw data as CSV
(
    raw
    .repartition(8)
    .write
    .mode("overwrite")
    .option("header", True)
    .option("emptyValue", "")
    .csv(RAW_PATH)
)


# 6. Read raw data with an explicit string schema
raw_schema = StructType([
    StructField(field_name, StringType(), True)
    for field_name in columns
])

raw_df = (
    spark.read
    .option("header", True)
    .schema(raw_schema)
    .csv(RAW_PATH)
)

print("\n===== RAW SCHEMA =====")
raw_df.printSchema()

raw_count = raw_df.count()
distinct_id_count = raw_df.select("id").distinct().count()

print("RAW ROWS:", raw_count)
print("RAW DISTINCT ID:", distinct_id_count)


# 7. Data quality checks
print("\n===== NULL / EMPTY CHECK =====")
null_exprs = [
    spark_sum(
        when(col(field_name).isNull() | (col(field_name) == ""), 1).otherwise(0)
    ).alias(field_name)
    for field_name in columns
]
raw_df.select(null_exprs).show(truncate=False)


print("\n===== INVALID VALUE CHECK =====")
invalid_df = raw_df.filter(
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

invalid_count = invalid_df.count()
print("INVALID ROWS:", invalid_count)

invalid_df.select(
    "id", "age", "quantity", "unit_price", "customer_rating"
).show(20, truncate=False)


# 8. Cast columns to business types
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


# 9. Remove invalid / missing required records and duplicates
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
clean_distinct_id_count = clean_df.select("id").distinct().count()

print("\n===== CLEAN RESULT =====")
print("CLEAN ROWS:", clean_count)
print("CLEAN DISTINCT ID:", clean_distinct_id_count)


# 10. select + filter
selected = clean_df.select(
    "id", "country", "product_category", "quantity", "total_amount"
)

print("\n===== FILTER + SELECT =====")
selected.filter(col("total_amount") > 500).show(10, truncate=False)


# 11. groupBy + aggregation
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

print("\n===== GROUP BY =====")
summary.show(20, truncate=False)


# 12. Write cleaned data to partitioned Parquet
(
    clean_df
    .repartition(4, "country")
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet(PARQUET_PATH)
)

summary.write.mode("overwrite").parquet(SUMMARY_PATH)


# 13. Verify Parquet schema and query plan
print("\n===== PARQUET SCHEMA =====")
spark.read.parquet(PARQUET_PATH).printSchema()

print("\n===== EXPLAIN =====")
(
    clean_df
    .filter(col("total_amount") > 500)
    .groupBy("country")
    .count()
    .explain("formatted")
)

spark.stop()
