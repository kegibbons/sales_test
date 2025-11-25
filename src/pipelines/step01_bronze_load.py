from pathlib import Path
import json
import duckdb


"""
bronze_load.py
---------

Ingest from raw JSON sources

- Basic processing to get into DuckDB without data loss

Gibbons 2025_11_24

"""

# ---------------------------
# Project paths
# ---------------------------

# This file:  .../sales_test/src/pipelines/bronze.py
THIS_FILE = Path(__file__).resolve()

# parents[0] = pipelines, [1] = src, [2] = sales_test
PROJECT_ROOT = THIS_FILE.parents[2]

DB_PATH = THIS_FILE.parents[1] / "sales.duckdb"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


# ---------------------------
# JSON validator (optional, for debugging)
# ---------------------------

def validate_json_lines(path: Path) -> None:
    """
    Quick sanity check: try to parse each non-empty line as JSON.

    """
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue

            # Many line-delimited JSON files use one object per line.
            # If there are trailing commas, strip them before validating.
            if text.endswith(","):
                text = text[:-1]

            try:
                json.loads(text)
            except json.JSONDecodeError as e:
                print(f"[VALIDATION] Possible malformed JSON in {path} at line {i}: {e}")
                break


# ---------------------------
# Cleaner: loose JSON -> NDJSON
# ---------------------------

def fix_to_ndjson(path: Path) -> Path:
    """
    Convert loosely formatted JSON list into NDJSON (one JSON object per line)
    Removes trailing commas and blank lines, write a temporary fixed file
    """
    fixed_path = path.with_name(path.stem + "_fixed.json")

    with path.open("r", encoding="utf-8") as src, fixed_path.open("w", encoding="utf-8") as dst:
        for line in src:
            cleaned = line.strip()

            if not cleaned:
                continue  # skip blank lines

            if cleaned.endswith(","):
                cleaned = cleaned[:-1]  # remove trailing comma

            dst.write(cleaned + "\n")

    return fixed_path


# ---------------------------
# Bronze loader
# ---------------------------

def load_bronze_table(
    con: duckdb.DuckDBPyConnection,
    filename: str,
    table_name: str,
) -> None:
    """
    Load a single JSON file from data/raw into a DuckDB table in the bronze layer

    Strategy:
     1. Try to load as normal JSON with read_json_auto().
     2. If that fails, being malformed, clean it into NDJSON and retry with read_ndjson_auto().
    """
    file_path = RAW_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    print(f"Loading {file_path} -> {table_name}")

    # First attempt: assume the file is valid JSON.
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_json_auto(?)
            """,
            [str(file_path)],
        )
        return  # success, we're done

    except duckdb.InvalidInputException as e:
        print(f"[WARN] Initial JSON load failed for {file_path}")
        print(f"[WARN] DuckDB said: {e}")
        print("[INFO] Attempting to clean file into NDJSON and reload...")

        # validate_json_lines(file_path)

        fixed_path = fix_to_ndjson(file_path)

        try:
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_ndjson_auto(?)
                """,
                [str(fixed_path)],
            )
            print(f"[INFO] Loaded cleaned NDJSON file {fixed_path} -> {table_name}")
        except duckdb.InvalidInputException as e2:
            print(f"[ERROR] Even cleaned NDJSON version failed for {file_path}")
            print(f"[ERROR] DuckDB reported: {e2}")
            # make pipeline fail loudly if this happens
            raise


# ---------------------------
# Main 
# ---------------------------

def main() -> None:
    print(f"Connecting to DuckDB at {DB_PATH}")
    print(f"Raw data folder: {RAW_DIR}")

    con = duckdb.connect(str(DB_PATH))

    tables_to_load = [
        ("customers.json", "bronze_customers"),
        ("products.json", "bronze_products"),
        ("orders.json", "bronze_orders"),
        ("sales.json", "bronze_sales"),
        ("countries.json", "bronze_countries"),
    ]

    for filename, table_name in tables_to_load:
        load_bronze_table(con, filename, table_name)

    con.close()
    print("Bronze layer finished.")


if __name__ == "__main__":
    main()
