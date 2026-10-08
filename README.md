# PySpark Large-Scale Data Quality Practice

Bài thực hành xử lý dữ liệu giả lập 1M, 10M và 100M records, mỗi record có 30 fields. Bài tập tập trung vào data quality, cleaning, aggregation, Parquet và quan sát workload khi dữ liệu tăng 10x và 100x.

## Quy mô dữ liệu

| Scale | Base records | Duplicate records | Raw records | Mục đích |
|---|---:|---:|---:|---|
| 1M | 1,000,000 | 10,000 | 1,010,000 | Dataset mẫu được commit trong repo |
| 10M | 10,000,000 | 100,000 | 10,100,000 | Benchmark / xử lý cỡ trung |
| 100M | 100,000,000 | 1,000,000 | 101,000,000 | Benchmark dữ liệu lớn |

Dữ liệu được cố tình chèn NULL và invalid values vào các trường như age, quantity, unit_price, customer_rating, đồng thời có duplicate theo id.

## Nội dung bài

- Sinh dataset với 30 fields
- Kiểm tra schema, NULL, duplicate và invalid values
- Cast raw string sang business types
- Filter dữ liệu lỗi
- dropDuplicates
- select, filter, groupBy, aggregation
- Ghi Parquet partition theo country
- Kiểm tra execution plan
- So sánh thời gian xử lý giữa 1M / 10M / 100M
- Theo dõi tác động của shuffle partitions, disk I/O và data volume

## Cấu trúc dữ liệu

~~~text
data/
├── raw/
│   └── orders_raw_1m.csv.gz
├── processed/
│   ├── orders_clean_1m.csv.gz
│   ├── orders_summary.csv
│   └── orders_parquet/
├── 10m/
│   ├── README.md
│   ├── raw/
│   │   └── orders_raw_10m.csv.gz
│   └── processed/
│       ├── orders_clean_10m.csv.gz
│       ├── orders_summary_10m.csv
│       └── orders_parquet/
└── 100m/
    ├── README.md
    ├── raw/
    │   └── orders_raw_100m.csv.gz
    └── processed/
        ├── orders_clean_100m.csv.gz
        ├── orders_summary_100m.csv
        └── orders_parquet/
~~~

Bộ 1M hiện tại được giữ lại làm dataset mẫu để clone về kiểm tra.

Các run 10M và 100M không được commit vào Git vì kích thước file tăng rất nhanh. Repo chỉ lưu code để tái tạo dữ liệu và benchmark; thư mục output/ được gitignore.

## 1. Sinh CSV.gz theo quy mô

Script generate_repository_data.py hỗ trợ tham số:

~~~bash
python generate_repository_data.py --rows 10m
python generate_repository_data.py --rows 100m
~~~

Có thể chỉ định thư mục output:

~~~bash
python generate_repository_data.py --rows 10m
python generate_repository_data.py --rows 100m
~~~

Output sẽ có dạng:

~~~text
data/10m/
├── README.md
├── raw/
│   └── orders_raw_10m.csv.gz
└── processed/
    ├── orders_clean_10m.csv.gz
    └── orders_summary_10m.csv
~~~

Với 100M, CSV.gz có thể chiếm nhiều GB nên phải chạy trên ổ đĩa còn trống đủ lớn. Dataset lớn không được commit trực tiếp vào Git; GitHub Actions có thể tạo và upload dưới dạng artifact.

## 2. Chuyển CSV.gz sang Parquet theo chunk

Không nên đọc toàn bộ CSV vào RAM khi lên 10M/100M. Script generate_parquet_output.py đã được đổi sang streaming/chunk:

~~~bash
python generate_parquet_output.py \
  --input data/10m/processed/orders_clean_10m.csv.gz \
  --output data/10m/processed/orders_parquet
~~~

Có thể chỉnh buffer:

~~~bash
python generate_parquet_output.py \
  --input data/scale_csv/processed/orders_clean_10m.csv.gz \
  --output data/scale_parquet/10m/orders_parquet \
  --chunk-size 250000
~~~

Kết quả được chia thành nhiều part-xxxxx.parquet theo country thay vì gom toàn bộ một country vào RAM.

## 3. Benchmark trực tiếp bằng PySpark

Đây là cách phù hợp hơn để so sánh 1M → 10M → 100M:

~~~bash
spark-submit bigdata_scale_pyspark.py --rows 1m
spark-submit bigdata_scale_pyspark.py --rows 10m
spark-submit bigdata_scale_pyspark.py --rows 100m
~~~

Output mặc định:

~~~text
output/
├── 1m/
│   ├── benchmark_metrics.json
│   ├── summary.csv
│   └── orders_parquet/
├── 10m/
│   ├── benchmark_metrics.json
│   ├── summary.csv
│   └── orders_parquet/
└── 100m/
    ├── benchmark_metrics.json
    ├── summary.csv
    └── orders_parquet/
~~~

Script ghi lại raw row count, distinct ID, invalid rows, clean row count, số summary groups, số shuffle partitions và thời gian của từng stage chính.

## 4. GitHub Actions

Có workflow thủ công:

.github/workflows/scale-benchmark.yml

Vào Actions → Scale Benchmark (1M / 10M / 100M) → Run workflow rồi chọn 1m, 10m hoặc 100m.

Workflow chỉ lưu benchmark_metrics.json dưới dạng artifact, không commit dataset lớn vào repository.

## 5. Chạy bài 1M hiện tại

~~~bash
spark-submit bigdata_1m_pyspark.py
~~~

Script cũ vẫn được giữ để không làm mất bài 1M đang có.

## Gợi ý báo cáo so sánh

| Metric | 1M | 10M | 100M |
|---|---:|---:|---:|
| Raw rows | | | |
| Clean rows | | | |
| Invalid rows | | | |
| Distinct IDs | | | |
| Generate / materialize | | | |
| Clean + dedup | | | |
| GroupBy | | | |
| Parquet write | | | |
| Disk size | | | |

Điểm cần phân tích không chỉ là thời gian tăng bao nhiêu lần, mà còn phải nhìn vào shuffle, số partition, memory pressure, disk I/O và khả năng mở rộng của pipeline.


## 6. Bài 2 — Advanced Order Analytics

Bài 2 sử dụng lại generator 30 fields của bài scale hiện tại nhưng chuyển trọng tâm từ **Data Quality** sang các pattern thường gặp khi làm Data Engineering:

- **Customer 360:** tổng hợp số đơn, doanh thu, giá trị đơn trung bình, thời gian giao hàng và rating theo khách hàng.
- **Customer segmentation:** chia khách hàng thành STANDARD / CORE / VIP dựa trên P50 và P90 lifetime revenue.
- **Top-3 products per country:** dùng `dense_rank()` + Window để tìm sản phẩm có doanh thu cao nhất theo từng quốc gia.
- **7-day rolling revenue:** tính doanh thu và số đơn lũy kế 7 ngày bằng Window.
- **Warehouse capacity:** aggregate theo warehouse/ngày rồi `broadcast join` với bảng dimension kho nhỏ.
- **Order anomaly detection:** dùng Window theo customer để tính average/stddev và đánh dấu đơn có giá trị cao bất thường.

### Chạy 3 quy mô

~~~bash
spark-submit advanced_order_analytics_pyspark.py --rows 1m
spark-submit advanced_order_analytics_pyspark.py --rows 10m
spark-submit advanced_order_analytics_pyspark.py --rows 100m
~~~

Có thể thay đổi số shuffle partitions:

~~~bash
spark-submit advanced_order_analytics_pyspark.py \
  --rows 100m \
  --shuffle-partitions 128
~~~

Output mặc định:

~~~text
output/
└── advanced/
    ├── 1m/
    │   ├── advanced_metrics.json
    │   ├── customer_segment_summary/
    │   ├── top3_products_by_country/
    │   ├── rolling_7d_revenue_latest_10_days/
    │   ├── busiest_warehouse_days/
    │   └── top_customer_order_anomalies/
    ├── 10m/
    │   └── ...
    └── 100m/
        └── ...
~~~

Bài 2 không commit thêm dataset 10M/100M vào Git. Script tái sử dụng `build_dataset()` trong `bigdata_scale_pyspark.py`, vì vậy cả ba scale được tạo và xử lý bằng cùng một pipeline.

### Mục tiêu thực hành

Sau khi làm xong cả hai bài, có thể đối chiếu:

| Kỹ thuật | Bài 1 | Bài 2 |
|---|:---:|:---:|
| Schema / casting | ✓ | ✓ |
| NULL / invalid values | ✓ | ✓ |
| Deduplication | ✓ | ✓ |
| GroupBy / aggregation | ✓ | ✓ |
| Parquet / partition | ✓ |  |
| Execution plan | ✓ |  |
| Window function |  | ✓ |
| Rolling metrics |  | ✓ |
| Ranking / Top-N |  | ✓ |
| Broadcast join |  | ✓ |
| Anomaly detection |  | ✓ |
| Scale 1M → 10M → 100M | ✓ | ✓ |

Điểm đáng phân tích ở Bài 2 là **shuffle**, **sort trong Window**, tác động của **broadcast join**, memory pressure và việc thời gian xử lý thay đổi thế nào khi từ 1M → 10M → 100M.


## 7. Docker

Repository có Docker để tạo môi trường PySpark reproducible, không cần cài Spark trực tiếp trên máy.

### Build image

~~~bash
docker compose build
~~~

### Chạy bài Data Quality

~~~bash
docker compose run --rm spark \
  spark-submit /app/bigdata_scale_pyspark.py --rows 1m

docker compose run --rm spark \
  spark-submit /app/bigdata_scale_pyspark.py --rows 10m

docker compose run --rm spark \
  spark-submit /app/bigdata_scale_pyspark.py --rows 100m
~~~

### Chạy bài Advanced Analytics

~~~bash
docker compose run --rm spark \
  spark-submit /app/advanced_order_analytics_pyspark.py --rows 1m

docker compose run --rm spark \
  spark-submit /app/advanced_order_analytics_pyspark.py --rows 10m

docker compose run --rm spark \
  spark-submit /app/advanced_order_analytics_pyspark.py --rows 100m
~~~

Có thể override số shuffle partitions:

~~~bash
docker compose run --rm spark \
  spark-submit /app/advanced_order_analytics_pyspark.py \
  --rows 100m \
  --shuffle-partitions 128
~~~

### Volume

Docker Compose mount:

~~~text
./data   → /app/data
./output → /app/output
~~~

Dataset và output không được đóng gói vào Docker image. Điều này tránh làm image phình to khi chạy với dataset 10M/100M.

### Kiểm tra môi trường

~~~bash
docker compose run --rm spark \
  python -c "import pyspark; print(pyspark.__version__)"

docker compose run --rm spark \
  java -version
~~~



## 8. Bài 3 — Incremental ETL & CDC

Bài 3 mô phỏng một pipeline e-commerce nhận thay đổi đơn hàng dưới dạng **CDC events** thay vì xử lý lại toàn bộ dữ liệu.

Các phần chính:

- **Bronze:** sinh và lưu INSERT / UPDATE / DELETE events dưới dạng Parquet partition theo event date.
- **Silver:** dùng Window để lấy latest event cho từng `order_id`, sau đó loại các order có latest operation là DELETE.
- **Gold:** aggregate current-state orders theo country và product category cho BI/reporting.
- **Watermark:** ghi nhận `max(event_ts)` của batch đã xử lý.
- **Idempotency:** kiểm tra kết quả khi cùng một CDC feed được xử lý lại.

### Chạy bằng Docker

~~~bash
docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py --rows 1m

docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py --rows 10m

docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py --rows 100m
~~~

Có thể benchmark nhiều mức shuffle partitions:

~~~bash
docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py \
  --rows 100m \
  --shuffle-partitions 64
~~~

### Output

~~~text
output/exercise_03/
├── 1m/
│   ├── bronze_cdc/
│   ├── silver_current_orders/
│   ├── gold_business_metrics/
│   └── cdc_metrics.json
├── 10m/
│   └── ...
└── 100m/
    └── ...
~~~

### Kỹ thuật được bổ sung

| Kỹ thuật | Bài 1 | Bài 2 | Bài 3 |
|---|:---:|:---:|:---:|
| Data quality / casting | ✓ | ✓ | ✓ |
| Deduplication | ✓ | ✓ | ✓ |
| GroupBy / aggregation | ✓ | ✓ | ✓ |
| Parquet / partitioning | ✓ |  | ✓ |
| Window function |  | ✓ | ✓ |
| Rolling metrics |  | ✓ |  |
| Ranking / Top-N |  | ✓ |  |
| Broadcast join |  | ✓ |  |
| Anomaly detection |  | ✓ |  |
| CDC |  |  | ✓ |
| Incremental ETL |  |  | ✓ |
| Bronze / Silver / Gold |  |  | ✓ |
| Watermark |  |  | ✓ |
| Idempotency |  |  | ✓ |
| Scale 1M → 10M → 100M | ✓ | ✓ | ✓ |
| Docker | ✓ | ✓ | ✓ |

Bài 3 giúp repo tiến gần hơn tới một mini data platform workflow thay vì chỉ là các phép xử lý DataFrame độc lập.
