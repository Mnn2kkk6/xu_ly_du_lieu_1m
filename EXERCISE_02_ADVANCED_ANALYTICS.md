# Bài 2 — Advanced Order Analytics với PySpark

## Bối cảnh

Bạn đang xây dựng một pipeline phân tích đơn hàng cho một hệ thống thương mại điện tử. Dữ liệu có thể tăng từ **1M → 10M → 100M records**, vì vậy pipeline phải xử lý được scale lớn mà không đưa toàn bộ dữ liệu vào Python/Pandas.

Dataset sử dụng chính là bộ dữ liệu giả lập **30 fields** đã có trong repository.

## Yêu cầu

### 1. Customer 360

Tạo bảng phân tích theo `customer_id` gồm:

- số lượng đơn hàng
- lifetime revenue
- average order value
- average delivery days
- average customer rating

Sau đó chia khách hàng thành:

- `VIP`: lifetime revenue >= P90
- `CORE`: P50 <= lifetime revenue < P90
- `STANDARD`: lifetime revenue < P50

Kết quả cần tổng hợp tiếp theo `country` và `customer_segment`.

### 2. Top-N sản phẩm theo quốc gia

Tính doanh thu của từng sản phẩm theo `country`.

Sử dụng **Window Function** để tìm Top 3 sản phẩm có doanh thu cao nhất ở mỗi quốc gia.

Không dùng vòng lặp Python để xử lý từng quốc gia.

### 3. Rolling 7-day revenue

Tính theo từng quốc gia:

- doanh thu trong ngày
- số đơn trong ngày
- doanh thu rolling 7 ngày
- số đơn rolling 7 ngày

Kết quả cần giữ được thứ tự theo ngày để quan sát xu hướng.

### 4. Warehouse capacity

Tạo một dimension table nhỏ gồm:

- `warehouse_id`
- `daily_capacity`

Tính số lượng units xử lý theo warehouse/ngày, sau đó **broadcast join** với dimension table.

Tạo thêm:

`capacity_utilization_pct = units / daily_capacity * 100`

Xác định các warehouse-day có mức utilization cao nhất.

### 5. Phát hiện đơn hàng bất thường

Theo từng customer, dùng Window Function để tính:

- average order value
- standard deviation

Đánh dấu một order là bất thường khi:

`total_amount > customer_avg_order + 3 * customer_order_stddev`

Xuất Top 100 order có giá trị bất thường cao nhất.

## Yêu cầu về scale

Pipeline phải chạy được với:

```bash
spark-submit advanced_order_analytics_pyspark.py --rows 1m
spark-submit advanced_order_analytics_pyspark.py --rows 10m
spark-submit advanced_order_analytics_pyspark.py --rows 100m
```

Không commit dataset 10M/100M vào Git.

## Yêu cầu về Data Engineering

Trong quá trình làm bài cần quan sát:

- shuffle do `groupBy`
- shuffle + sort do Window
- lợi ích của `broadcast join`
- tác động của `spark.sql.shuffle.partitions`
- memory pressure khi scale từ 1M lên 100M
- sự khác nhau giữa execution time của từng stage

Có thể thử:

```bash
spark-submit advanced_order_analytics_pyspark.py \
  --rows 100m \
  --shuffle-partitions 64
```

và so sánh với:

```bash
spark-submit advanced_order_analytics_pyspark.py \
  --rows 100m \
  --shuffle-partitions 128
```

## Output bắt buộc

Mỗi scale cần tạo:

```text
output/advanced/<scale>/
├── advanced_metrics.json
├── customer_segment_summary/
├── top3_products_by_country/
├── rolling_7d_revenue_latest_10_days/
├── busiest_warehouse_days/
└── top_customer_order_anomalies/
```

## Câu hỏi phân tích

1. Vì sao Window thường tốn tài nguyên hơn một phép `groupBy` đơn giản?
2. Tại sao dimension table warehouse phù hợp với `broadcast join`?
3. 1M → 10M → 100M có làm thời gian chạy tăng đúng 10x và 100x không? Vì sao?
4. Khi nào tăng `shuffle partitions` giúp nhanh hơn và khi nào lại làm chậm?
5. Stage nào trở thành bottleneck khi dữ liệu tăng lên 100M?
6. Nếu đây là pipeline production, bạn sẽ chuyển output từ local filesystem sang hệ thống lưu trữ nào và vì sao?

## File chạy mẫu

`advanced_order_analytics_pyspark.py` là implementation tham khảo của bài này.
