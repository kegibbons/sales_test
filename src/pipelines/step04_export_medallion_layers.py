import json
from pathlib import Path

import duckdb

"""
step04_export_medallion_layers.py

Exports the medallion tables from DuckDB to disk:

- Bronze  -> JSON + metadata under data/bronze
- Silver  -> JSON + metadata under data/silver
- Gold    -> Parquet + metadata under data/gold

This script is location-agnostic: paths are resolved from this file's
location (…/src/pipelines), not from the current working directory.
"""

# ----------------------------
# Paths
# ----------------------------

THIS_FILE = Path(__file__).resolve()        # …/src/pipelines/step04_export_medallion_layers.py
SRC_DIR = THIS_FILE.parents[1]              # …/src
PROJECT_ROOT = THIS_FILE.parents[2]         # …/sales_test

DB_PATH = SRC_DIR / "sales.duckdb"          # …/src/sales.duckdb

DATA_DIR = PROJECT_ROOT / "data"            # …/data
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# --------------------------------------------------------
# Table collections
# --------------------------------------------------------

BRONZE_TABLES = [
    "bronze_customers",
    "bronze_orders",
    "bronze_sales",
    "bronze_products",
    "bronze_countries",
]

SILVER_TABLES = [
    "silver_customers",
    "silver_orders",
    "silver_sales",
    "silver_products",
    "silver_countries",
    "silver_fact_sales",
]

GOLD_TABLES = [
    "gold_dim_customer",
    "gold_dim_product",
    "gold_dim_country",
    "gold_dim_date",
    "gold_fact_sales",
]


# ----------------------------
# Directory setup
# ----------------------------

def ensure_dirs() -> None:
    for d in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        d.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Export functions
# ----------------------------

def export_table_json(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    out_dir: Path,
) -> Path:
    out_path = out_dir / f"{table_name}.json"
    sql = f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT JSON);"
    con.execute(sql, [str(out_path)])
    return out_path


def export_table_parquet(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    out_dir: Path,
) -> Path:
    out_path = out_dir / f"{table_name}.parquet"
    sql = f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT PARQUET);"
    con.execute(sql, [str(out_path)])
    return out_path


def write_table_metadata(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    out_dir: Path,
) -> Path:
    (row_count,) = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()

    schema_rows = con.execute(
        f"PRAGMA table_info({table_name})"
    ).fetchall()

    columns = [
        {
            "name": r[1],
            "type": r[2],
            "not_null": bool(r[3]),
            "primary_key": bool(r[5]),
        }
        for r in schema_rows
    ]

    meta = {
        "table": table_name,
        "row_count": row_count,
        "columns": columns,
    }

    out_path = out_dir / f"{table_name}.meta.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    return out_path


def dump_table_set(
    con: duckdb.DuckDBPyConnection,
    tables: list[str],
    out_dir: Path,
    label: str,
) -> None:
    print(f"\n=== Exporting {label} tables to {out_dir} ===")
    for t in tables:
        print(f"  -> {t} ...", end="", flush=True)
        try:
            data_path = export_table_json(con, t, out_dir)
            meta_path = write_table_metadata(con, t, out_dir)
            print(f" done [{data_path.name}, {meta_path.name}]")
        except Exception as e:
            print(f" FAILED ({e})")


def dump_gold_tables(
    con: duckdb.DuckDBPyConnection,
    tables: list[str],
    out_dir: Path,
) -> None:
    print(f"\n=== Exporting gold tables to {out_dir} ===")
    for t in tables:
        print(f"  -> {t} ...", end="", flush=True)
        try:
            data_path = export_table_parquet(con, t, out_dir)
            meta_path = write_table_metadata(con, t, out_dir)
            print(f" done [{data_path.name}, {meta_path.name}]")
        except Exception as e:
            print(f" FAILED ({e})")


# ----------------------------
# Main entry
# ----------------------------

def main() -> None:
    ensure_dirs()

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found at {DB_PATH}")

    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(str(DB_PATH))

    try:
        # bronze snapshot (json)
        dump_table_set(con, BRONZE_TABLES, BRONZE_DIR, label="bronze")

        # silver snapshot (json)
        dump_table_set(con, SILVER_TABLES, SILVER_DIR, label="silver")

        # gold snapshot (parquet)
        dump_gold_tables(con, GOLD_TABLES, GOLD_DIR)

        print("\nAll exports complete.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
