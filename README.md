# PySpark 1M Records / 30 Fields

Bài thực hành xử lý dữ liệu giả lập khoảng 1 triệu bản ghi, mỗi bản ghi có 30 fields, tập trung vào data quality và các thao tác xử lý dữ liệu bằng PySpark.

## Nội dung

- Sinh 1,000,000 records bằng Spark
- Kiểm tra schema và đúng 30 fields
- Cố tình tạo dữ liệu lỗi: NULL, duplicate và invalid values
- Đọc raw CSV với explicit schema
- Cast dữ liệu sang business types
- Lọc dữ liệu lỗi và loại duplicate
- Thực hành select, filter, groupBy
- Ghi dữ liệu sạch ra Parquet
- Partition Parquet theo country
- Kiểm tra execution plan bằng explain("formatted")

## Data / Output

Repo có kèm data và output để clone về kiểm tra:

```text
data/
├── raw/
│   └── orders_raw_1m.csv.gz
└── processed/
    ├── orders_clean_1m.csv.gz
    ├── orders_summary.csv
    └── orders_parquet/
        ├── country=MY/
        │   └── part-00000.parquet
        ├── country=SG/
        │   └── part-00000.parquet
        ├── country=TH/
        │   └── part-00000.parquet
        └── country=VN/
            └── part-00000.parquet
```

Raw gồm 1,010,000 rows (1,000,000 records + 10,000 duplicate rows).

Processed gồm khoảng 984,100 rows sau validation và loại duplicate theo id.

Parquet được nén bằng Snappy và partition theo country.

## Chạy PySpark

```bash
spark-submit bigdata_1m_pyspark.py
```

Script PySpark sẽ tạo raw CSV và output Parquet tại:

data/raw/orders_csv/
data/processed/orders_parquet/
data/processed/orders_summary/

## GitHub Actions

Workflow .github/workflows/generate-data.yml tự sinh raw/processed data và partitioned Parquet rồi commit vào main.
