# Module 8: Python Integration

**Duration**: 1.5 hours
**Objective**: Use DuckDB from Python for data pipelines

## Setup

### Install Packages

```bash
pip install duckdb pandas polars pyarrow jupyter
```

## Basic DuckDB from Python

### Connect and Query

```python
import duckdb

# In-memory database
con = duckdb.connect()

# Persistent database
con = duckdb.connect('my_database.db')

# Execute query
result = con.execute("SELECT 42 AS answer").fetchall()
print(result)  # [(42,)]

# Get as DataFrame
df = con.execute("SELECT * FROM 'data.csv'").df()

# Close connection
con.close()
```

### Query Files Directly

```python
# Query CSV
sales = duckdb.query("SELECT * FROM 'datasets/raw/sales.csv' LIMIT 10").df()

# Query Parquet
sales = duckdb.query("""
    SELECT product_category, SUM(amount) AS total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY product_category
""").df()

# Query multiple files
logs = duckdb.query("SELECT * FROM 'datasets/raw/logs_*.csv'").df()
```

## DuckDB + pandas

### pandas DataFrame to DuckDB

```python
import pandas as pd

# Create pandas DataFrame
df = pd.read_csv('datasets/raw/employees.csv')

# Query pandas DataFrame directly!
result = duckdb.query("""
    SELECT department, AVG(salary) AS avg_salary
    FROM df
    WHERE salary > 80000
    GROUP BY department
""").df()
```

### Register DataFrame

```python
# Register for reuse
con.register('employees', df)

# Query registered table
result = con.execute("""
    SELECT * FROM employees WHERE department = 'Engineering'
""").df()
```

### Performance Comparison

```python
import time

# pandas aggregation
start = time.time()
pandas_result = df.groupby('department')['salary'].mean()
pandas_time = time.time() - start

# DuckDB aggregation
start = time.time()
duckdb_result = duckdb.query("""
    SELECT department, AVG(salary) AS avg_salary
    FROM df
    GROUP BY department
""").df()
duckdb_time = time.time() - start

print(f"pandas: {pandas_time:.4f}s")
print(f"DuckDB: {duckdb_time:.4f}s")
```

## DuckDB + Polars

Polars is a fast DataFrame library similar to pandas.

```python
import polars as pl

# Read with Polars
sales_pl = pl.read_parquet('datasets/raw/sales.parquet')

# Query Polars DataFrame with DuckDB
result = duckdb.query("""
    SELECT product_category, SUM(amount) AS total
    FROM sales_pl
    GROUP BY product_category
""").df()  # Returns pandas DataFrame

# Or get Polars DataFrame
result_pl = duckdb.query("""
    SELECT product_category, SUM(amount) AS total
    FROM sales_pl
    GROUP BY product_category
""").pl()  # Returns Polars DataFrame
```

## Arrow Integration

Zero-copy data transfer.

```python
import pyarrow as pa
import pyarrow.parquet as pq

# DuckDB to Arrow
arrow_table = con.execute("SELECT * FROM 'data.parquet'").arrow()

# Arrow to pandas (if needed)
df = arrow_table.to_pandas()

# Arrow to DuckDB
arrow_data = pq.read_table('data.parquet')
result = duckdb.query("SELECT * FROM arrow_data").df()
```

## Building ETL Pipelines

### Example: Data Cleaning Pipeline

```python
import duckdb

con = duckdb.connect()

# Multi-step pipeline
result = con.execute("""
    WITH raw_data AS (
        SELECT * FROM 'datasets/raw/sales.csv'
        WHERE amount > 0
    ),
    cleaned AS (
        SELECT
            *,
            EXTRACT(YEAR FROM sale_date) AS year,
            EXTRACT(MONTH FROM sale_date) AS month
        FROM raw_data
    ),
    aggregated AS (
        SELECT
            year,
            month,
            product_category,
            SUM(amount) AS total_sales,
            COUNT(*) AS transactions
        FROM cleaned
        GROUP BY year, month, product_category
    )
    SELECT * FROM aggregated ORDER BY year, month
""").df()

# Export results
con.execute("""
    COPY (SELECT * FROM result)
    TO 'output/monthly_summary.parquet' (FORMAT PARQUET)
""")
```

### Example: Incremental Processing

```python
def process_daily_data(date_str):
    """Process sales data for a specific date"""
    con = duckdb.connect('analytics.db')

    # Load and process
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS processed_sales (
            sale_date DATE,
            product_category VARCHAR,
            total_sales DOUBLE,
            transaction_count INTEGER
        );

        INSERT INTO processed_sales
        SELECT
            sale_date,
            product_category,
            SUM(amount) AS total_sales,
            COUNT(*) AS transaction_count
        FROM 'raw_data/sales_{date_str}.csv'
        GROUP BY sale_date, product_category;
    """)

    con.close()

# Use it
process_daily_data('2024-01-15')
```

## Jupyter Notebook Integration

```python
# In Jupyter notebook
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Query data
sales = duckdb.query("""
    SELECT
        DATE_TRUNC('day', sale_date) AS date,
        SUM(amount) AS total_sales
    FROM 'datasets/raw/sales.parquet'
    GROUP BY date
    ORDER BY date
""").df()

# Visualize
plt.figure(figsize=(12, 6))
plt.plot(sales['date'], sales['total_sales'])
plt.title('Daily Sales Trend')
plt.xlabel('Date')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Statistics
print(sales['total_sales'].describe())
```

## Working with Large Files

```python
# Process large file in chunks with DuckDB
def analyze_large_file(filename):
    """Analyze large file without loading into memory"""

    con = duckdb.connect()

    # DuckDB handles memory management
    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            AVG(amount) AS avg_amount,
            MIN(sale_date) AS first_date,
            MAX(sale_date) AS last_date
        FROM '{filename}'
    """).df()

    return stats

stats = analyze_large_file('datasets/raw/sales.parquet')
print(stats)
```

## Example: Complete Analysis Workflow

```python
import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Connect
con = duckdb.connect()

# 1. Data extraction and transformation
monthly_trends = con.execute("""
    WITH daily_sales AS (
        SELECT
            DATE_TRUNC('day', sale_date) AS date,
            product_category,
            SUM(amount) AS daily_total
        FROM 'datasets/raw/sales.parquet'
        GROUP BY date, product_category
    ),
    with_moving_avg AS (
        SELECT
            *,
            AVG(daily_total) OVER (
                PARTITION BY product_category
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma_7day
        FROM daily_sales
    )
    SELECT * FROM with_moving_avg
    ORDER BY product_category, date
""").df()

# 2. Visualization
plt.figure(figsize=(14, 8))
for category in monthly_trends['product_category'].unique():
    data = monthly_trends[monthly_trends['product_category'] == category]
    plt.plot(data['date'], data['ma_7day'], label=category)

plt.title('7-Day Moving Average by Product Category')
plt.xlabel('Date')
plt.ylabel('Average Daily Sales')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('output/sales_trends.png', dpi=300)
plt.show()

# 3. Export processed data
con.execute("""
    COPY monthly_trends
    TO 'output/processed_trends.parquet' (FORMAT PARQUET)
""")

con.close()
```

## Command-Line Scripts

### Script: `analyze_sales.py`

```python
#!/usr/bin/env python3
import duckdb
import sys
from datetime import datetime

def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_sales.py <category>")
        sys.exit(1)

    category = sys.argv[1]

    con = duckdb.connect()

    result = con.execute(f"""
        SELECT
            DATE_TRUNC('month', sale_date) AS month,
            COUNT(*) AS transactions,
            SUM(amount) AS total_sales,
            AVG(amount) AS avg_transaction
        FROM 'datasets/raw/sales.parquet'
        WHERE product_category = '{category}'
        GROUP BY month
        ORDER BY month
    """).df()

    print(f"\nSales Analysis for {category}")
    print(f"Generated: {datetime.now()}\n")
    print(result.to_string(index=False))

    con.close()

if __name__ == "__main__":
    main()
```

Usage: `python analyze_sales.py Electronics`

## When to Use DuckDB vs pandas/polars

### Use DuckDB when:
- **Large files**: Bigger than RAM
- **File-based workflows**: Query without loading
- **Complex SQL**: Window functions, CTEs, JOINs
- **Fast aggregations**: Vectorized execution

### Use pandas when:
- **Data fits in memory**: Convenient API
- **Data manipulation**: Rich ecosystem
- **Machine learning**: Integration with sklearn, etc.

### Use polars when:
- **Speed-critical**: Faster than pandas
- **Lazy evaluation**: Optimize query plans
- **Modern API**: More consistent than pandas

### Use together:
- DuckDB for heavy lifting, pandas/polars for results
- Best of both worlds!

## Key Takeaways

✅ **Query pandas/polars DataFrames** directly with SQL
✅ **DuckDB handles large files** without memory issues
✅ **Arrow integration** for zero-copy transfer
✅ **Seamless with Jupyter** notebooks
✅ **Combine with Python ecosystem** for complete workflows

## Practice Exercises

See `exercises/08_python_integration.py` and `08_jupyter_examples.ipynb`.

---

## Course Complete!

You now have the skills to:
- Query files directly with DuckDB CLI
- Write advanced analytical SQL (window functions, CTEs)
- Optimize queries for performance
- Build data pipelines and workflows
- Integrate DuckDB with R and Python

**Next steps**: Apply these skills to your real-world data problems!
