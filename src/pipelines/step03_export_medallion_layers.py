import json
from pathlib import Path
import duckdb

# ----------------------------
# Paths
# ----------------------------

# control.py lives in: sales_test/src

SRC_DIR = Path(__file__).resolve().parents[1]   # -> src/
BASE_DIR = SRC_DIR

PROJECT_ROOT = BASE_DIR.parent                       # -> sales_test

DB_PATH = BASE_DIR / "sales.duckdb"                  # -> sales_test/src/sales.duckdb

DATA_DIR = PROJECT_ROOT / "data"                     # -> sales_test/data
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

# Gold later... 


# ----------------------------
# Directory Setup
# ----------------------------

def ensure_dirs() -> None:
    for d in (DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        d.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Export Functions
# ----------------------------

def export_table_json(con: duckdb.DuckDBPyConnection,
                      table_name: str,
                      out_dir: Path) -> Path:
    out_path = out_dir / f"{table_name}.json"
    sql = f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT JSON);"
    con.execute(sql, [str(out_path)])
    return out_path


def write_table_metadata(con: duckdb.DuckDBPyConnection,
                         table_name: str,
                         out_dir: Path) -> Path:

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]

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


def dump_table_set(con: duckdb.DuckDBPyConnection,
                   tables: list[str],
                   out_dir: Path,
                   label: str) -> None:

    print(f"\n=== Exporting {label} tables to {out_dir} ===")
    for t in tables:
        print(f"  -> {t} ...", end="", flush=True)
        try:
            data_path = export_table_json(con, t, out_dir)
            meta_path = write_table_metadata(con, t, out_dir)
            print(f" done [{data_path.name}, {meta_path.name}]")
        except Exception as e:
            print(f" FAILED ({e})")

# ----------------------------
# Gold Layer placeholder
# ----------------------------

def export_gold(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== Gold export not implemented yet ===")

# ----------------------------
# Main Entry
# ----------------------------

def main() -> None:
    ensure_dirs()

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found at {DB_PATH}")

    print(f"Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(str(DB_PATH))

    dump_table_set(con, BRONZE_TABLES, BRONZE_DIR, label="bronze")
    dump_table_set(con, SILVER_TABLES, SILVER_DIR, label="silver")

    export_gold(con)

    print("\nAll exports complete.")

if __name__ == "__main__":
    main()
