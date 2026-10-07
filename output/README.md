# Outputs

Thư mục này dùng cho kết quả chạy benchmark theo từng quy mô:

\`\`\`text
output/
├── 1m/
├── 10m/
└── 100m/
\`\`\`

Mỗi scale có cùng cấu trúc:

\`\`\`text
<scale>/
├── benchmark_metrics.json
├── summary.csv
└── orders_parquet/
    ├── country=MY/
    ├── country=SG/
    ├── country=TH/
    └── country=VN/
\`\`\`

\`benchmark_metrics.json\` lưu row counts, data-quality counts, số shuffle partitions và thời gian các stage.

\`orders_parquet/\` là output Parquet partition theo \`country\`.

Do 10M và 100M có kích thước rất lớn, các file Parquet thực tế không được commit vào Git. GitHub Actions sẽ sinh và upload chúng dưới dạng workflow artifacts.
