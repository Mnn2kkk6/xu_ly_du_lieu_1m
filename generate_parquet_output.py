import csv
import gzip
import os
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

INPUT = "data/processed/orders_clean_1m.csv.gz"
OUTPUT = "data/processed/orders_parquet"

NUMERIC_INT = {"id", "age", "quantity", "delivery_days", "customer_rating"}
NUMERIC_FLOAT = {
    "unit_price", "discount", "total_amount",
    "shipping_cost", "latitude", "longitude"
}
BOOL = {"is_member"}
DATE_FIELDS = {"order_date", "ship_date", "created_at"}

os.makedirs(OUTPUT, exist_ok=True)

with gzip.open(INPUT, "rt", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    columns = reader.fieldnames
    partition_rows = {}
    for row in reader:
        partition_rows.setdefault(row["country"], []).append(row)

for country, rows in sorted(partition_rows.items()):
    for row in rows:
        for name in NUMERIC_INT:
            row[name] = int(row[name])
        for name in NUMERIC_FLOAT:
            row[name] = float(row[name])
        for name in BOOL:
            row[name] = row[name].lower() == "true"
        for name in DATE_FIELDS:
            row[name] = date.fromisoformat(row[name])

    arrays = {}
    for name in columns:
        values = [r[name] for r in rows]
        if name in DATE_FIELDS:
            arrays[name] = pa.array(values, type=pa.date32())
        elif name in NUMERIC_INT:
            arrays[name] = pa.array(values, type=pa.int64())
        elif name in NUMERIC_FLOAT:
            arrays[name] = pa.array(values, type=pa.float64())
        elif name in BOOL:
            arrays[name] = pa.array(values, type=pa.bool_())
        else:
            arrays[name] = pa.array(values, type=pa.string())

    table = pa.table(arrays)
    path = os.path.join(OUTPUT, "country=" + country, "part-00000.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    print(f"Wrote {path}: {table.num_rows} rows, {table.num_columns} columns")
