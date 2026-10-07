# 10M Dataset

Quy mô: **10,000,000 base records / 30 fields**.

Raw sau khi thêm duplicate 1%: **10,100,000 rows**.

```text
data/10m/
├── raw/
│   └── orders_raw_10m.csv.gz
└── processed/
    ├── orders_clean_10m.csv.gz
    ├── orders_summary_10m.csv
    └── orders_parquet/
        ├── country=MY/
        ├── country=SG/
        ├── country=TH/
        └── country=VN/
```

Tạo dataset:

```bash
python generate_repository_data.py --rows 10m
```

Tạo Parquet:

```bash
python generate_parquet_output.py \
  --input data/10m/processed/orders_clean_10m.csv.gz \
  --output data/10m/processed/orders_parquet
```

Dataset 10M thực tế không được commit vào Git thường vì kích thước lớn; dùng GitHub Actions để tạo và lấy artifact.