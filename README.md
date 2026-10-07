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

## Cấu trúc hiện tại

~~~text
data/
├── raw/
│   └── orders_raw_1m.csv.gz
└── processed/
    ├── orders_clean_1m.csv.gz
    ├── orders_summary.csv
    └── orders_parquet/
        ├── country=MY/
        ├── country=SG/
        ├── country=TH/
        └── country=VN/
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
python generate_repository_data.py --rows 10m --output-dir data/scale_csv
~~~

Output sẽ có dạng:

~~~text
data/scale_csv/
├── raw/orders_raw_10m.csv.gz
└── processed/
    ├── orders_clean_10m.csv.gz
    └── orders_summary_10m.csv
~~~

Với 100M, CSV.gz có thể chiếm nhiều GB nên phải chạy trên ổ đĩa còn trống đủ lớn.

## 2. Chuyển CSV.gz sang Parquet theo chunk

Không nên đọc toàn bộ CSV vào RAM khi lên 10M/100M. Script generate_parquet_output.py đã được đổi sang streaming/chunk:

~~~bash
python generate_parquet_output.py \
  --input data/scale_csv/processed/orders_clean_10m.csv.gz \
  --output data/scale_parquet/10m/orders_parquet
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
data/scale/
├── 1m/
│   ├── benchmark_metrics.json
│   └── orders_parquet/
├── 10m/
│   ├── benchmark_metrics.json
│   └── orders_parquet/
└── 100m/
    ├── benchmark_metrics.json
    └── orders_parquet/
~~~

Script ghi lại raw row count, distinct ID, invalid rows, clean row count, số summary groups, số shuffle partitions và thời gian của từng stage chính.

## 4. GitHub Actions

Có workflow thủ công:

.github/workflows/scale-benchmark.yml

Vào Actions → Scale Benchmark (1M / 10M / 100M) → Run workflow rồi chọn 10m hoặc 100m.

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
