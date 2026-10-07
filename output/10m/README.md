# 10m output

Quy mô: **10,000,000 base records / 30 fields**

Raw logical rows sau khi thêm duplicate 1%: **10,100,000**

Kết quả chạy nằm trong:

```text
output/10m/
├── benchmark_metrics.json
└── orders_parquet/
    ├── country=MY/
    ├── country=SG/
    ├── country=TH/
    └── country=VN/
```

Chạy:

```bash
spark-submit bigdata_scale_pyspark.py --rows 10m --output-dir output
```

`benchmark_metrics.json` được tạo sau khi pipeline hoàn tất.

Các file Parquet thực tế của scale lớn không được commit vào Git; dùng workflow **Scale Benchmark (1M / 10M / 100M)** để tạo và tải artifact.
