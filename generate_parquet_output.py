import argparse
import csv
import gzip
import os
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq


NUMERIC_INT = {"id", "age", "quantity", "delivery_days", "customer_rating"}
NUMERIC_FLOAT = {
    "unit_price", "discount", "total_amount",
    "shipping_cost", "latitude", "longitude"
}
BOOL = {"is_member"}
DATE_FIELDS = {"order_date", "ship_date", "created_at"}

COUNTRIES = ("MY", "SG", "TH", "VN")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream CSV.gz into partitioned Parquet without loading all rows into memory."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--chunk-size", type=int, default=250_000,
        help="Rows buffered per country before flushing a Parquet part."
    )
    return parser.parse_args()


def convert_row(row):
    for name in NUMERIC_INT:
        row[name] = int(row[name])
    for name in NUMERIC_FLOAT:
        row[name] = float(row[name])
    for name in BOOL:
        row[name] = row[name].lower() == "true"
    for name in DATE_FIELDS:
        row[name] = date.fromisoformat(row[name])
    return row


def arrow_schema(columns):
    fields = []
    for name in columns:
        if name in DATE_FIELDS:
            field_type = pa.date32()
        elif name in NUMERIC_INT:
            field_type = pa.int64()
        elif name in NUMERIC_FLOAT:
            field_type = pa.float64()
        elif name in BOOL:
            field_type = pa.bool_()
        else:
            field_type = pa.string()
        fields.append(pa.field(name, field_type))
    return pa.schema(fields)


def rows_to_table(rows, columns, schema):
    arrays = {}
    for name in columns:
        values = [row[name] for row in rows]
        arrays[name] = pa.array(values, type=schema.field(name).type)
    return pa.table(arrays, schema=schema)


def main():
    args = parse_args()
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be > 0")

    os.makedirs(args.output, exist_ok=True)

    writers = {}
    buffers = {country: [] for country in COUNTRIES}
    part_numbers = {country: 0 for country in COUNTRIES}
    total_rows = 0
    schema = None
    columns = None

    def flush(country):
        nonlocal total_rows
        rows = buffers[country]
        if not rows:
            return

        country_dir = os.path.join(args.output, f"country={country}")
        os.makedirs(country_dir, exist_ok=True)

        path = os.path.join(
            country_dir, f"part-{part_numbers[country]:05d}.parquet"
        )
        writer = pq.ParquetWriter(
            path,
            schema=schema,
            compression="snappy",
            use_dictionary=True,
        )
        part_numbers[country] += 1

        table = rows_to_table(rows, columns, schema)
        writer.write_table(table)
        writer.close()

        total_rows += table.num_rows
        buffers[country] = []

    with gzip.open(args.input, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        if not columns:
            raise SystemExit("Input CSV has no header")

        schema = arrow_schema(columns)

        for raw_row in reader:
            row = convert_row(raw_row)
            country = row["country"]
            if country not in buffers:
                raise ValueError(f"Unexpected country value: {country!r}")

            buffers[country].append(row)
            if len(buffers[country]) >= args.chunk_size:
                flush(country)

        for country in COUNTRIES:
            flush(country)

    parts = sum(part_numbers.values())
    print(f"WROTE_ROWS={total_rows:,}")
    print(f"PARQUET_PARTS={parts:,}")
    print(f"OUTPUT={args.output}")


if __name__ == "__main__":
    main()
