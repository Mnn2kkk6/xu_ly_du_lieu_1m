# PySpark 1M Records / 30 Fields

Bài thực hành xử lý dữ liệu giả lập khoảng **1 triệu bản ghi**, mỗi bản ghi có **30 fields**, tập trung vào data quality và các thao tác xử lý dữ liệu bằng PySpark.

## Nội dung

- Sinh 1,000,000 records bằng Spark
- Kiểm tra schema và đúng 30 fields
- Tạo dữ liệu lỗi: NULL, duplicate và invalid values
- Đọc raw CSV với explicit schema
- Cast dữ liệu sang business types
- Lọc dữ liệu lỗi và loại duplicate
- Thực hành select, filter, groupBy
- Ghi dữ liệu sạch ra Parquet
- Partition Parquet theo country
- Kiểm tra execution plan bằng explain("formatted")

## Data / Output

Repo có kèm dữ liệu nén để có thể clone về và kiểm tra trực tiếp:

~~~text
data/
├── raw/
│   └── orders_raw_1m.csv.gz
└── processed/
    ├── orders_clean_1m.csv.gz
    └── orders_summary.csv
~~~

Raw gồm **1,010,000 rows** (1,000,000 records + 10,000 duplicate rows).

Processed gồm dữ liệu sau khi validate/cast logic và loại duplicate theo id.

File \`orders_summary.csv\` là kết quả tổng hợp theo \`country\` và \`product_category\`.

## Chạy PySpark

~~~bash
spark-submit bigdata_1m_pyspark.py
~~~

Script PySpark sẽ tạo output Parquet tại:

~~~text
data/processed/orders_parquet/
data/processed/orders_summary/
~~~

GitHub Actions của repo cũng tự sinh và commit bộ raw/processed CSV nén khi có commit mới vào \`main\` (trừ commit sinh dữ liệu).
