from pathlib import Path
from datetime import datetime

import duckdb

"""
step03_gold_load.py

Gold layer builder on top of the Silver tables.

Notes:
* Bronze/Silver stay literal (plural table names).
* In Gold we intentionally de-pluralize the dimensions so they read
  like a semantic model (gold_dim_customer, gold_dim_product, etc.),
  while the fact table keeps the plural 'gold_fact_sales'.
* Date dimension is generated from order history with 1-day granularity
  for use in Power BI (ISO-style DateKey).

Gibbons 2025-11-25
"""

# --------------------------------------------------------------------
# Paths / config
# --------------------------------------------------------------------

THIS_FILE = Path(__file__).resolve()

# parents[1] = src, parents[2] = sales_test
SRC_DIR = THIS_FILE.parents[1]
PROJECT_ROOT = THIS_FILE.parents[2]

DB_PATH = SRC_DIR / "sales.duckdb"
LOG_PATH = SRC_DIR / "gold_pipeline.log"


# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------

def log(message: str) -> None:
    """Tiny logger for the gold pipeline."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [gold] {message}"

    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_row_count(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    (count,) = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    log(f"row count for {table_name}: {count}")


# --------------------------------------------------------------------
# Gold dimension builders
# --------------------------------------------------------------------

def create_gold_dim_customer(con: duckdb.DuckDBPyConnection) -> None:
    log("creating gold_dim_customer...")
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_dim_customer AS
        SELECT
            c.CustomerId,
            c.Name    AS CustomerName,
            c.City    AS CustomerCity,
            c.Country AS CustomerCountry,
            c.Active,
            c.Email
        FROM silver_customers c
        WHERE c.CustomerId IS NOT NULL;
        """
    )
    log_row_count(con, "gold_dim_customer")


def create_gold_dim_product(con: duckdb.DuckDBPyConnection) -> None:
    log("creating gold_dim_product...")
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_dim_product AS
        SELECT
            p.ProductId,
            p.Name                AS ProductName,
            p.ManufacturedCountry AS ProductCountry,
            p.WeightGrams
        FROM silver_products p
        WHERE p.ProductId IS NOT NULL;
        """
    )
    log_row_count(con, "gold_dim_product")


def create_gold_dim_country(con: duckdb.DuckDBPyConnection) -> None:
    log("creating gold_dim_country...")
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_dim_country AS
        SELECT
            c.Country,
            c.Region,
            c.Currency,
            c.Population,
            c.AreaSqMi,
            c.PopDensity,
            c.CoastlineRatio,
            c.NetMigration,
            c.InfantMortality,
            c.GDPPerCapita,
            c.LiteracyPct,
            c.PhonesPer1000,
            c.ArablePct,
            c.CropsPct,
            c.OtherLandPct,
            c.Climate,
            c.Birthrate,
            c.Deathrate,
            c.Agriculture,
            c.Industry,
            c.Service
        FROM silver_countries c
        WHERE c.Country IS NOT NULL;
        """
    )
    log_row_count(con, "gold_dim_country")


def create_gold_dim_date(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build a reusable calendar dimension from the min/max order dates.
    1 row per calendar day, with an ISO-style integer DateKey (YYYYMMDD).
    """
    log("creating gold_dim_date...")
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_dim_date AS
        WITH bounds AS (
            SELECT
                MIN(Date) AS min_date,
                MAX(Date) AS max_date
            FROM silver_orders
        ),
        dates AS (
            SELECT
                -- unnest the series so it becomes rows, not a timestamp[]
                CAST(UNNEST(GENERATE_SERIES(min_date, max_date, INTERVAL 1 DAY)) AS DATE) AS Date
            FROM bounds
        )
        SELECT
            CAST(STRFTIME(Date, '%Y%m%d') AS INTEGER)                   AS DateKey,
            Date,
            EXTRACT(YEAR    FROM Date)                AS Year,
            EXTRACT(QUARTER FROM Date)                AS Quarter,
            EXTRACT(MONTH   FROM Date)                  AS Month,
            STRFTIME(Date, '%B')                        AS MonthName,
            (EXTRACT(YEAR FROM Date) * 100
                + EXTRACT(MONTH FROM Date))              AS YearMonth,
            EXTRACT(WEEK     FROM Date)                  AS WeekOfYear,
            EXTRACT(DOW      FROM Date)                                 AS DayOfWeek,
            STRFTIME(Date, '%A')                                        AS DayOfWeekName,
            CASE
                WHEN EXTRACT(DOW FROM Date) IN (0, 6)
                    THEN TRUE
                ELSE FALSE
            END                                                         AS IsWeekend
        FROM dates
        ORDER BY Date;
        """
    )
    log_row_count(con, "gold_dim_date")


# --------------------------------------------------------------------
# Gold fact table
# --------------------------------------------------------------------

def create_gold_fact_sales(con: duckdb.DuckDBPyConnection) -> None:
    """
    Gold fact table: one row per sale, with:
    - DateKey from gold_dim_date
    - Product info via silver_products
    - Pre-calculated TotalWeightGrams for convenience
    """
    log("creating gold_fact_sales...")
    con.execute(
        """
        CREATE OR REPLACE TABLE gold_fact_sales AS
        SELECT
            f.SaleId,
            f.OrderId,
            d.DateKey,
            f.CustomerId,
            f.ProductId,
            f.CustomerCountry AS Country,
            f.Quantity,
            p.WeightGrams,
            f.Quantity * p.WeightGrams AS TotalWeightGrams
        FROM silver_fact_sales f
        LEFT JOIN gold_dim_date d
            ON f.OrderDate = d.Date
        LEFT JOIN silver_products p
            ON f.ProductId = p.ProductId;
        """
    )
    log_row_count(con, "gold_fact_sales")


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def main() -> None:
    log(f"starting gold pipeline. db: {DB_PATH}")

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found at {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    try:
        create_gold_dim_customer(con)
        create_gold_dim_product(con)
        create_gold_dim_country(con)
        create_gold_dim_date(con)
        create_gold_fact_sales(con)
        log("gold pipeline finished.")
    finally:
        con.close()
        log("connection closed.")


if __name__ == "__main__":
    main()
