# PySpark 1M Records / 30 Fields

Bài thực hành xử lý dữ liệu giả lập khoảng **1 triệu bản ghi**, mỗi bản ghi có **30 fields**, tập trung vào data quality và các thao tác xử lý dữ liệu bằng PySpark.

## Nội dung

- Sinh 1,000,000 records bằng Spark
- Kiểm tra đúng 30 fields và schema
- Cố tình tạo dữ liệu lỗi:
  - NULL / empty
  - Duplicate
  - Tuổi không hợp lệ
  - Quantity âm
  - Unit price không phải số
  - Rating ngoài khoảng 1-5
- Đọc raw CSV với explicit schema
- Cast dữ liệu sang business types
- Lọc dữ liệu lỗi và loại duplicate
- Thực hành select, filter, groupBy
- Ghi dữ liệu sạch ra Parquet
- Partition Parquet theo country
- Kiểm tra execution plan bằng explain("formatted")

## Chạy

~~~bash
spark-submit bigdata_1m_pyspark.py
~~~

Hoặc:

~~~bash
python bigdata_1m_pyspark.py
~~~

## Output

~~~text
data/
├── raw/
│   └── orders_csv/
└── processed/
    ├── orders_parquet/
    │   ├── country=VN/
    │   ├── country=TH/
    │   ├── country=SG/
    │   └── country=MY/
    └── orders_summary/
~~~

Các thư mục data/ là output runtime, không commit lên GitHub.
