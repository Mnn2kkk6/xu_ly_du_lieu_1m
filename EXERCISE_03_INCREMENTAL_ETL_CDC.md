# Bài 3 — Incremental ETL & CDC với PySpark

## Bối cảnh

Một hệ thống e-commerce liên tục phát sinh thay đổi trên đơn hàng. Thay vì xử lý lại toàn bộ dataset sau mỗi lần chạy, Data Engineer cần thiết kế pipeline có khả năng nhận các **CDC events** và tạo ra bảng current-state để phục vụ BI/analytics.

Bài thực hành mô phỏng các event:

- `INSERT`
- `UPDATE`
- `DELETE`

và chạy trên cùng ba quy mô:

- 1M records
- 10M records
- 100M records

## Yêu cầu

### 1. Bronze — CDC event ingestion

Sinh CDC events từ dataset orders hiện tại.

Mỗi event cần có:

- `order_id`
- `operation`
- `event_ts`
- `event_seq`
- các business fields cần thiết

Lưu Bronze dưới dạng **Parquet partition theo event date**.

Theo dõi số lượng:

- INSERT
- UPDATE
- DELETE

### 2. Silver — Current state

Một `order_id` có thể xuất hiện nhiều lần trong CDC feed.

Sử dụng:

`Window.partitionBy("order_id").orderBy(event_ts DESC, event_seq DESC)`

để lấy **latest event**.

Quy tắc:

- latest INSERT/UPDATE → giữ lại order
- latest DELETE → loại order khỏi current-state table

Kết quả là bảng:

`silver_current_orders`

### 3. Gold — Business aggregation

Từ current-state Silver table, tạo bảng Gold gồm:

- số đơn
- tổng units
- revenue
- average order value

theo:

- country
- product_category

Đây là dữ liệu cuối cho BI/reporting.

### 4. Watermark

Xác định timestamp lớn nhất đã xử lý:

`max(event_ts)`

và lưu lại trong benchmark metadata.

Mục tiêu của phần này là hiểu khái niệm **watermark / processed-through timestamp** trong incremental pipelines.

### 5. Idempotency

Một pipeline production phải an toàn khi cùng một batch được retry.

Kiểm tra:

> Nếu cùng CDC feed được đưa vào pipeline hai lần, current-state row count có thay đổi không?

Pipeline được xem là idempotent khi kết quả latest-state sau khi xử lý một lần và hai lần là tương đương.

## Scale

Chạy:

```bash
spark-submit incremental_etl_cdc_pyspark.py --rows 1m
spark-submit incremental_etl_cdc_pyspark.py --rows 10m
spark-submit incremental_etl_cdc_pyspark.py --rows 100m
```

Docker:

```bash
docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py --rows 1m
```

Tương tự cho `10m` và `100m`.

Có thể benchmark partition tuning:

```bash
docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py \
  --rows 100m \
  --shuffle-partitions 64
```

và:

```bash
docker compose run --rm spark \
  spark-submit /app/incremental_etl_cdc_pyspark.py \
  --rows 100m \
  --shuffle-partitions 128
```

## Output

Mỗi scale tạo:

```text
output/exercise_03/<scale>/
├── bronze_cdc/
├── silver_current_orders/
├── gold_daily_metrics/
└── cdc_metrics.json
```

Trong `cdc_metrics.json` cần có:

- source row count
- CDC event count
- INSERT / UPDATE / DELETE count
- latest event row count
- current Silver row count
- delete count
- watermark
- idempotency check
- shuffle partitions
- thời gian từng stage

## Câu hỏi phân tích

1. Tại sao không nên xử lý lại toàn bộ 100M records khi chỉ có một phần nhỏ records thay đổi?
2. Vì sao CDC phải có `event_ts` và / hoặc `event_seq` để xác định latest state?
3. Tại sao việc lấy latest event bằng Window có thể gây shuffle?
4. DELETE event nên được xử lý thế nào khi xây dựng current-state table?
5. Idempotency quan trọng thế nào khi Spark job bị retry?
6. Watermark khác gì so với processing time?
7. Khi dữ liệu CDC tăng lên hàng trăm triệu events, nên lưu Bronze/Silver/Gold ở đâu thay vì local filesystem?
8. Có thể thay Window bằng cách nào khác để giảm chi phí khi pipeline production?

## Mục tiêu kiến thức

Sau bài này cần hiểu được flow:

```text
Source
  ↓
CDC Events
  ↓
Bronze
  ↓
Latest Event per Key
  ↓
Silver Current State
  ↓
Gold Aggregation
  ↓
BI / Analytics
```

Các khái niệm trọng tâm:

**CDC · Incremental ETL · Latest-state deduplication · Watermark · Idempotency · Bronze/Silver/Gold · Partitioning · Shuffle**
