"""
generate_dataset.py
-------------------
Generates a synthetic Supply Chain Management (SCM) orders dataset
with realistic dirty data for use in QA testing and analysis.

Output: data/scm_orders.csv (~500 rows)

Dirty data intentionally injected:
  - Null values (missing supplier, quantity, delivery date)
  - Duplicate rows
  - Outliers (extreme quantities, negative values)
  - Invalid status values
  - Inconsistent date formats
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# ── Reproducibility ──────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ── Configuration ────────────────────────────────────────────────────────────
NUM_CLEAN_ROWS = 450
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "scm_orders.csv")

# ── Reference Data ───────────────────────────────────────────────────────────
PRODUCTS = [
    "Microcontroller Unit", "Industrial Sensor", "Power Supply Module",
    "Conveyor Belt Segment", "Hydraulic Pump", "Circuit Breaker",
    "Steel Coupling", "Pneumatic Valve", "LED Driver Board", "Heat Exchanger"
]

SUPPLIERS = [
    "Bosch Rexroth HU", "Schneider Electric", "Siemens AG",
    "ABB Limited", "Honeywell Supply", "Eaton Corporation",
    "Parker Hannifin", "Emerson Electric", "Rockwell Automation", "TE Connectivity"
]

VALID_STATUSES = ["Pending", "In Transit", "Delivered", "Cancelled", "Returned"]
INVALID_STATUSES = ["UNKNOWN", "N/A", "delivered", "transit", ""]  # dirty values

REGIONS = ["Central Europe", "Western Europe", "Eastern Europe", "Asia-Pacific", "North America"]

# ── Helper Functions ─────────────────────────────────────────────────────────

def random_date(start_year: int = 2023, end_year: int = 2025) -> datetime:
    """Returns a random datetime between start_year and end_year."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def format_date(dt: datetime, style: str = "iso") -> str:
    """Formats a datetime in different styles to simulate inconsistency."""
    formats = {
        "iso": "%Y-%m-%d",
        "eu": "%d.%m.%Y",
        "us": "%m/%d/%Y",
        "verbose": "%B %d, %Y",
    }
    return dt.strftime(formats.get(style, "%Y-%m-%d"))


def generate_order_id(index: int) -> str:
    return f"ORD-{str(index).zfill(5)}"


# ── Clean Data Generation ────────────────────────────────────────────────────

def generate_clean_rows(n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        order_date = random_date()
        delivery_date = order_date + timedelta(days=random.randint(3, 60))
        rows.append({
            "order_id":        generate_order_id(i),
            "product":         random.choice(PRODUCTS),
            "quantity":        random.randint(1, 500),
            "unit_price_eur":  round(random.uniform(5.0, 2000.0), 2),
            "supplier":        random.choice(SUPPLIERS),
            "region":          random.choice(REGIONS),
            "order_date":      format_date(order_date, "iso"),
            "delivery_date":   format_date(delivery_date, "iso"),
            "status":          random.choice(VALID_STATUSES),
            "warehouse_id":    f"WH-{random.randint(1, 10):02d}",
        })
    return rows


# ── Dirty Data Injection ─────────────────────────────────────────────────────

def inject_nulls(rows: list[dict], null_rate: float = 0.04) -> list[dict]:
    """Randomly nullify values in key columns."""
    nullable_cols = ["supplier", "quantity", "delivery_date", "region"]
    for row in rows:
        for col in nullable_cols:
            if random.random() < null_rate:
                row[col] = None
    return rows


def inject_duplicates(rows: list[dict], n: int = 15) -> list[dict]:
    """Duplicate n random rows and append them."""
    duplicates = random.choices(rows[:NUM_CLEAN_ROWS // 2], k=n)
    return rows + duplicates


def inject_outliers(rows: list[dict], n: int = 10) -> list[dict]:
    """Inject extreme or negative quantity values."""
    outlier_values = [-50, -1, 0, 99999, 100000, -999]
    targets = random.choices(range(len(rows)), k=n)
    for idx in targets:
        rows[idx]["quantity"] = random.choice(outlier_values)
    return rows


def inject_invalid_statuses(rows: list[dict], n: int = 12) -> list[dict]:
    """Replace valid statuses with invalid ones."""
    targets = random.choices(range(len(rows)), k=n)
    for idx in targets:
        rows[idx]["status"] = random.choice(INVALID_STATUSES)
    return rows


def inject_date_format_inconsistencies(rows: list[dict], n: int = 20) -> list[dict]:
    """Change date format on some rows to simulate real-world inconsistency."""
    targets = random.choices(range(len(rows)), k=n)
    for idx in targets:
        try:
            dt = datetime.strptime(rows[idx]["order_date"], "%Y-%m-%d")
            rows[idx]["order_date"] = format_date(dt, random.choice(["eu", "us", "verbose"]))
        except (TypeError, ValueError):
            pass
    return rows


def inject_negative_prices(rows: list[dict], n: int = 8) -> list[dict]:
    """Inject negative unit prices."""
    targets = random.choices(range(len(rows)), k=n)
    for idx in targets:
        rows[idx]["unit_price_eur"] = round(-random.uniform(5.0, 500.0), 2)
    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("🏭 SCM Dataset Generator")
    print("=" * 40)

    # Generate clean base
    print(f"  Generating {NUM_CLEAN_ROWS} clean rows...")
    rows = generate_clean_rows(NUM_CLEAN_ROWS)

    # Inject dirty data
    print("  Injecting dirty data...")
    rows = inject_nulls(rows, null_rate=0.04)
    rows = inject_duplicates(rows, n=15)
    rows = inject_outliers(rows, n=10)
    rows = inject_invalid_statuses(rows, n=12)
    rows = inject_date_format_inconsistencies(rows, n=20)
    rows = inject_negative_prices(rows, n=8)

    # Shuffle so dirty rows aren't all at the end
    random.shuffle(rows)

    # Build DataFrame
    df = pd.DataFrame(rows)
    total_rows = len(df)

    # Save to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    # ── Summary Report ───────────────────────────────────────────────────────
    print(f"\n✅ Dataset saved to: {OUTPUT_FILE}")
    print(f"\n📊 Dataset Summary")
    print(f"  Total rows       : {total_rows}")
    print(f"  Columns          : {list(df.columns)}")
    print(f"\n🧟 Dirty Data Injected")
    print(f"  Null values      : {df.isnull().sum().sum()} across all columns")
    print(f"  Duplicate rows   : {df.duplicated().sum()}")
    print(f"  Outlier qty rows : {((df['quantity'] <= 0) | (df['quantity'] > 10000)).sum()}")
    print(f"  Invalid statuses : {(~df['status'].isin(VALID_STATUSES + [None])).sum()}")
    print(f"  Negative prices  : {(df['unit_price_eur'] < 0).sum()}")
    print(f"\n  Null breakdown by column:")
    for col, count in df.isnull().sum().items():
        if count > 0:
            print(f"    {col:<20}: {count}")
    print("\n🎉 Done. Ready for quality_checker.py!")


if __name__ == "__main__":
    main()
