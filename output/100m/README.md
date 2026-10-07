# 100m output

Quy mô: **100,000,000 base records / 30 fields**

Raw logical rows sau khi thêm duplicate 1%: **101,000,000**

Kết quả chạy nằm trong:

```text
output/100m/
├── benchmark_metrics.json
└── orders_parquet/
    ├── country=MY/
    ├── country=SG/
    ├── country=TH/
    └── country=VN/
```

Chạy:

```bash
spark-submit bigdata_scale_pyspark.py --rows 100m --output-dir output
```

`benchmark_metrics.json` được tạo sau khi pipeline hoàn tất.

summary.csv và benchmark_metrics.json là output tổng hợp nhỏ; các file Parquet thực tế của scale lớn không được commit vào Git. Dùng workflow **Scale Benchmark (1M / 10M / 100M)** để tạo và tải toàn bộ output artifact.
