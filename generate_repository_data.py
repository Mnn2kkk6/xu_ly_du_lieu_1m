import csv
import gzip
import os
from datetime import date, timedelta

N = 1_000_000
DUP_ROWS = 10_000
ROOT = "data"
RAW_PATH = f"{ROOT}/raw/orders_raw_1m.csv.gz"
CLEAN_PATH = f"{ROOT}/processed/orders_clean_1m.csv.gz"
SUMMARY_PATH = f"{ROOT}/processed/orders_summary.csv"

COLUMNS = [
    "id","customer_id","order_id","age","gender","city","country",
    "product_id","product_category","quantity","unit_price","discount",
    "total_amount","payment_method","order_status","order_date","ship_date",
    "delivery_days","shipping_cost","warehouse_id","seller_id",
    "customer_rating","coupon_code","is_member","device_type","channel",
    "source","latitude","longitude","created_at"
]

def pseudo(i, salt, mod=1_000_000):
    return (i * (1_103_515_245 + salt * 97) + 12_345 + salt * 17) % mod

def values(i, inject=True):
    age = 18 + i % 63
    qty = 1 + i % 10
    unit = 10 + pseudo(i, 7, 49001) / 100
    discount = pseudo(i, 8, 3001) / 10_000
    total = qty * unit * (1 - discount)
    delivery = 1 + i % 14
    shipping = 5 + pseudo(i, 9, 4501) / 100

    countries = ("VN", "VN", "TH", "SG", "MY")
    cities = ("Hanoi", "HCM", "Da Nang", "Hai Phong", "Can Tho")
    categories = ("Electronics", "Fashion", "Home", "Beauty", "Sports", "Books")
    payments = ("CARD", "CASH", "BANK_TRANSFER", "EWALLET")
    statuses = ("COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "RETURNED")
    devices = ("mobile", "desktop", "tablet")
    channels = ("web", "app", "store")
    sources = ("organic", "ads", "social", "email")

    day_offset = i % 365
    order_date = DATES[day_offset]
    ship_date = DATES[day_offset - delivery] if day_offset >= delivery else (
        BASE - timedelta(days=day_offset - delivery)
    ).isoformat()

    row = [
        str(i),
        f"C{i % 200000:06d}",
        f"O{i:08d}",
        str(age),
        "M" if i % 2 == 0 else "F",
        cities[i % 5],
        countries[i % 5],
        f"P{i % 50000:06d}",
        categories[i % 6],
        str(qty),
        f"{unit:.2f}",
        f"{discount:.4f}",
        f"{total:.2f}",
        payments[i % 4],
        statuses[i % 5],
        order_date,
        ship_date,
        str(delivery),
        f"{shipping:.2f}",
        f"W{i % 30:03d}",
        f"S{i % 10000:05d}",
        str(1 + i % 5),
        f"CPN{i % 100}" if i % 3 == 0 else "",
        "true" if i % 2 == 0 else "false",
        devices[i % 3],
        channels[i % 3],
        sources[i % 4],
        f"{1 + pseudo(i, 10, 25_000_000) / 1_000_000:.6f}",
        f"{95 + pseudo(i, 11, 104_000_000) / 1_000_000:.6f}",
        order_date,
    ]

    if inject:
        # ~0.2% nulls
        null_rules = (
            (3, 100 + 0), (9, 100 + 1), (10, 100 + 2),
            (21, 100 + 3), (5, 100 + 4)
        )
        for idx, salt in null_rules:
            if pseudo(i, salt, 100_000) < 200:
                row[idx] = ""

        # ~0.15% invalid values
        if pseudo(i, 200, 100_000) < 150:
            row[3] = "abc"
        if pseudo(i, 201, 100_000) < 150:
            row[9] = "-3"
        if pseudo(i, 202, 100_000) < 150:
            row[10] = "N/A"
        if pseudo(i, 203, 100_000) < 150:
            row[21] = "6"

    return row

def is_valid(row):
    try:
        if not row[0] or not row[1] or not row[7] or not row[15]:
            return False
        age = int(row[3])
        qty = int(row[9])
        price = float(row[10])
        discount = float(row[11])
        rating = int(row[21])
        return (
            18 <= age <= 100
            and qty > 0
            and price >= 0
            and 0 <= discount <= 1
            and 1 <= rating <= 5
            and bool(row[5])
        )
    except (TypeError, ValueError):
        return False

def main():
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)

    clean_seen = set()
    aggregates = {}
    clean_count = 0

    with gzip.open(RAW_PATH, "wt", encoding="utf-8", newline="", compresslevel=6) as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for i in range(N):
            writer.writerow(values(i, True))
        for i in range(DUP_ROWS):
            writer.writerow(values(i, True))

    with gzip.open(CLEAN_PATH, "wt", encoding="utf-8", newline="", compresslevel=6) as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for i in range(N):
            row = values(i, True)
            if not is_valid(row):
                continue
            row_id = row[0]
            if row_id in clean_seen:
                continue
            clean_seen.add(row_id)
            writer.writerow(row)
            clean_count += 1

            key = (row[6], row[8])
            if key not in aggregates:
                aggregates[key] = [0, 0, 0.0, 0]
            a = aggregates[key]
            a[0] += 1
            a[1] += int(row[9])
            a[2] += float(row[12])
            a[3] += int(row[17])

    with open(SUMMARY_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "country","product_category","orders",
            "total_quantity","revenue","avg_delivery_days"
        ])
        for (country, category), a in sorted(
            aggregates.items(), key=lambda item: item[1][2], reverse=True
        ):
            writer.writerow([
                country, category, a[0], a[1],
                f"{a[2]:.2f}", f"{a[3] / a[0]:.4f}"
            ])

    print(f"RAW_ROWS={N + DUP_ROWS}")
    print(f"CLEAN_ROWS={clean_count}")
    print(f"DISTINCT_IDS={len(clean_seen)}")

if __name__ == "__main__":
    BASE = date(2026, 10, 5)
    DATES = [(BASE - timedelta(days=i)).isoformat() for i in range(365)]
    main()
