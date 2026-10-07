# 100M Dataset

Quy mô: **100,000,000 base records / 30 fields**.

Raw sau khi thêm duplicate 1%: **101,000,000 rows**.

```text
data/100m/
├── raw/
│   └── orders_raw_100m.csv.gz
└── processed/
    ├── orders_clean_100m.csv.gz
    ├── orders_summary_100m.csv
    └── orders_parquet/
        ├── country=MY/
        ├── country=SG/
        ├── country=TH/
        └── country=VN/
```

Tạo dataset:

```bash
python generate_repository_data.py --rows 100m --output-dir data/100m
```

Tạo Parquet theo chunk:

```bash
python generate_parquet_output.py \
  --input data/100m/processed/orders_clean_100m.csv.gz \
  --output data/100m/processed/orders_parquet \
  --chunk-size 250000
```

Dataset 100M rất lớn nên không nên commit trực tiếp vào Git repository. Dùng GitHub Actions artifact hoặc object storage/Git LFS.