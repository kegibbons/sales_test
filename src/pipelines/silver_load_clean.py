from pathlib import Path
from datetime import datetime
import duckdb


"""
silver_load_clean.py
---------
Build the silver layer from bronze tables:

- Apply light cleaning
- Standardize data types since JSON is all over the place (DATE, DOUBLE, INTEGER)
- Create clean silver dimension tables
- Create conformed sales fact table so Power BI can easily read
- Logs activity and row counts
- Detail level
- Fact table 

Gibbons 2025_11_24

"""

# --------------------------------------------------------------------
# Paths / configuration
# --------------------------------------------------------------------

# This file lives at: .../sales_test/src/pipelines/silver.py
THIS_FILE = Path(__file__).resolve()

# Go up two levels: pipelines -> src -> sales_test
PROJECT_ROOT = THIS_FILE.parents[2]

DB_PATH = PROJECT_ROOT / "sales.duckdb"
# Logs go under src directory 
LOG_PATH = PROJECT_ROOT / "src" / "silver_pipeline.log"


# --------------------------------------------------------------------
# Logging helpers, append not overwrite
# --------------------------------------------------------------------

def log(message: str) -> None:
    """Simple structured log writer for the Silver pipeline."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [SILVER] {message}"
    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_row_count(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Log the row count for a given table."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    log(f"Row count for {table_name}: {count}")


# --------------------------------------------------------------------
# Silver table builders
# (keep column names same as bronze, just clean & cast)
# --------------------------------------------------------------------

def create_silver_customers(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver customers: same columns as bronze_customers, but types enforced
    and obvious junk rows (null CustomerId) removed.
    """
    log("Creating silver_customers...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_customers AS
        SELECT
            CAST(bc.CustomerId AS BIGINT)   AS CustomerId,
            CAST(bc.Active     AS BOOLEAN)  AS Active,
            CAST(bc.Name       AS VARCHAR)  AS Name,
            CAST(bc.Address    AS VARCHAR)  AS Address,
            CAST(bc.City       AS VARCHAR)  AS City,
            CAST(bc.Country    AS VARCHAR)  AS Country,
            CAST(bc.Email      AS VARCHAR)  AS Email
        FROM bronze_customers bc
        WHERE bc.CustomerId IS NOT NULL
    """)
    log_row_count(con, "silver_customers")

# --------------------------------------------------------------------
def create_silver_products(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver products: clean ProductId/WeightGrams types, keep all columns.
    """
    log("Creating silver_products...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_products AS
        SELECT
            CAST(bp.ProductId          AS BIGINT)  AS ProductId,
            CAST(bp.Name               AS VARCHAR) AS Name,
            CAST(bp.ManufacturedCountry AS VARCHAR) AS ManufacturedCountry,
            CAST(bp.WeightGrams        AS DOUBLE)  AS WeightGrams
        FROM bronze_products bp
        WHERE bp.ProductId IS NOT NULL
    """)
    log_row_count(con, "silver_products")

# --------------------------------------------------------------------
def create_silver_orders(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver orders: enforce numeric IDs and a clean DATE column.
    """
    log("Creating silver_orders...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_orders AS
        SELECT
            CAST(bo.OrderId    AS BIGINT) AS OrderId,
            CAST(bo.CustomerId AS BIGINT) AS CustomerId,
            CAST(bo.Date       AS DATE)   AS Date
        FROM bronze_orders bo
        WHERE bo.OrderId IS NOT NULL
    """)
    log_row_count(con, "silver_orders")

# --------------------------------------------------------------------
def create_silver_sales(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver sales: clean numeric types; this is by individual transaction (for rollup)
    """
    log("Creating silver_sales...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_sales AS
        SELECT
            CAST(bs.SaleId    AS BIGINT) AS SaleId,
            CAST(bs.OrderId   AS BIGINT) AS OrderId,
            CAST(bs.ProductId AS BIGINT) AS ProductId,
            CAST(bs.Quantity  AS DOUBLE) AS Quantity
        FROM bronze_sales bs
        WHERE bs.SaleId IS NOT NULL
    """)
    log_row_count(con, "silver_sales")

# --------------------------------------------------------------------
def create_silver_countries(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver countries, ensure types are stable
    Standardize column aliases (aliaii?)
    """
    log("Creating silver_countries...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_countries AS
        SELECT
            CAST(bc.Country       AS VARCHAR) AS Country,
            CAST(bc.Currency      AS VARCHAR) AS Currency,
            CAST(bc.Name          AS VARCHAR) AS Name,
            CAST(bc.Region        AS VARCHAR) AS Region,
            CAST(bc.Population    AS BIGINT)  AS Population,
            CAST(bc."Area (sq. mi.)"        AS BIGINT)  AS AreaSqMi,
            CAST(bc."Pop. Density (per sq. mi.)" AS DOUBLE) AS PopDensity,
            CAST(bc."Coastline (coast per area ratio)" AS DOUBLE) AS CoastlineRatio,
            CAST(bc."Net migration"       AS DOUBLE) AS NetMigration,
            CAST(bc."Infant mortality (per 1000 births)" AS DOUBLE) AS InfantMortality,
            CAST(bc."GDP ($ per capita)"  AS BIGINT)  AS GDPPerCapita,
            CAST(bc."Literacy (%)"       AS DOUBLE) AS LiteracyPct,
            CAST(bc."Phones (per 1000)"  AS DOUBLE) AS PhonesPer1000,
            CAST(bc."Arable (%)"         AS DOUBLE) AS ArablePct,
            CAST(bc."Crops (%)"          AS DOUBLE) AS CropsPct,
            CAST(bc."Other (%)"          AS DOUBLE) AS OtherLandPct,
            CAST(bc.Climate              AS DOUBLE) AS Climate,
            CAST(bc.Birthrate            AS DOUBLE) AS Birthrate,
            CAST(bc.Deathrate            AS DOUBLE) AS Deathrate,
            CAST(bc.Agriculture          AS DOUBLE) AS Agriculture,
            CAST(bc.Industry             AS DOUBLE) AS Industry,
            CAST(bc.Service              AS DOUBLE) AS Service
        FROM bronze_countries bc
        WHERE bc.Country IS NOT NULL
    """)
    log_row_count(con, "silver_countries")


# --------------------------------------------------------------------
# Silver fact table - Each row now represents a full transaction with all the related attributes
# --------------------------------------------------------------------

def create_silver_fact_sales(con: duckdb.DuckDBPyConnection) -> None:
    """
    Silver fact table - joins sales, orders, customers, products, countries.
    Grain - one row per SaleId (line item)
    """
    log("Creating silver_fact_sales...")
    con.execute("""
        CREATE OR REPLACE TABLE silver_fact_sales AS
        SELECT
            ss.SaleId,
            ss.Quantity,

            so.OrderId,
            so.Date AS OrderDate,

            sc.CustomerId,
            sc.Name    AS CustomerName,
            sc.Country AS CustomerCountry,
            sc.City    AS CustomerCity,

            sp.ProductId,
            sp.Name    AS ProductName,
            sp.ManufacturedCountry AS ProductCountry,

            c.Currency AS CountryCurrency,
            c.GDPPerCapita AS CountryGDPPerCapita
        FROM silver_sales ss
        LEFT JOIN silver_orders   so ON ss.OrderId   = so.OrderId
        LEFT JOIN silver_customers sc ON so.CustomerId = sc.CustomerId
        LEFT JOIN silver_products  sp ON ss.ProductId = sp.ProductId
        LEFT JOIN silver_countries c  ON sc.Country   = c.Country
    """)
    log_row_count(con, "silver_fact_sales")


# --------------------------------------------------------------------
# Basic data quality checks
# --------------------------------------------------------------------

def run_quality_checks(con: duckdb.DuckDBPyConnection) -> None:
    """
    Referential integrity checks
    Just log the counts
    """
    log("Running simple data quality checks...")

    # Orders with no matching customer
    missing_customers = con.execute("""
        SELECT COUNT(*) 
        FROM silver_orders o
        LEFT JOIN silver_customers c
            ON o.CustomerId = c.CustomerId
        WHERE c.CustomerId IS NULL
    """).fetchone()[0]
    log(f"Orders with missing customers: {missing_customers}")

    # Sales with no matching order
    missing_orders = con.execute("""
        SELECT COUNT(*)
        FROM silver_sales s
        LEFT JOIN silver_orders o
            ON s.OrderId = o.OrderId
        WHERE o.OrderId IS NULL
    """).fetchone()[0]
    log(f"Sales with missing orders: {missing_orders}")

    # Sales with no matching product
    missing_products = con.execute("""
        SELECT COUNT(*)
        FROM silver_sales s
        LEFT JOIN silver_products p
            ON s.ProductId = p.ProductId
        WHERE p.ProductId IS NULL
    """).fetchone()[0]
    log(f"Sales with missing products: {missing_products}")

# --------------------------------------------------------------------
def log_bronze_vs_silver(con: duckdb.DuckDBPyConnection) -> None:
    """Compare Bronze vs Silver row counts and log the differences."""
    log("Comparing Bronze vs Silver row counts...")

    comparisons = con.execute("""
        SELECT * FROM (
            SELECT 'customers' AS dataset,
                   (SELECT COUNT(*) FROM bronze_customers) AS bronze_count,
                   (SELECT COUNT(*) FROM silver_customers) AS silver_count
            UNION ALL
            SELECT 'products',
                   (SELECT COUNT(*) FROM bronze_products),
                   (SELECT COUNT(*) FROM silver_products)
            UNION ALL
            SELECT 'orders',
                   (SELECT COUNT(*) FROM bronze_orders),
                   (SELECT COUNT(*) FROM silver_orders)
            UNION ALL
            SELECT 'sales',
                   (SELECT COUNT(*) FROM bronze_sales),
                   (SELECT COUNT(*) FROM silver_sales)
            UNION ALL
            SELECT 'countries',
                   (SELECT COUNT(*) FROM bronze_countries),
                   (SELECT COUNT(*) FROM silver_countries)
        )
        ORDER BY dataset;
    """).fetchall()

    for dataset, bronze_count, silver_count in comparisons:
        diff = silver_count - bronze_count
        log(f"[ROWCHECK] {dataset}: Bronze={bronze_count}, Silver={silver_count}, Diff={diff}")


# --------------------------------------------------------------------
# Main 
# --------------------------------------------------------------------

def main() -> None:
    log(f"Starting Silver pipeline. DB: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))

    try:
        create_silver_customers(con)
        create_silver_products(con)
        create_silver_orders(con)
        create_silver_sales(con)
        create_silver_countries(con)
        create_silver_fact_sales(con)
        run_quality_checks(con)
        log_bronze_vs_silver(con)

    finally:
        con.close()
        log("Silver pipeline finished!")


if __name__ == "__main__":
    main()
