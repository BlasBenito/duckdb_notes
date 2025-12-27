# Module 7: R Integration

**Duration**: 1.5 hours
**Objective**: Use DuckDB from R for enhanced data analysis

## Setup

### Install Packages

```r
install.packages("duckdb")
install.packages("dplyr")
install.packages("dbplyr")
install.packages("arrow")  # Optional: for zero-copy transfer
```

## Basic DuckDB from R

### Connect to DuckDB

```r
library(duckdb)

# In-memory database
con <- dbConnect(duckdb::duckdb())

# Persistent database
con <- dbConnect(duckdb::duckdb(), dbdir = "my_database.db")

# Always disconnect when done
dbDisconnect(con, shutdown = TRUE)
```

### Execute Queries

```r
# Run query, get results as data.frame
result <- dbGetQuery(con, "SELECT * FROM 'data.csv' LIMIT 10")

# Execute without returning results
dbExecute(con, "CREATE TABLE test AS SELECT * FROM 'data.csv'")

# List tables
dbListTables(con)
```

## Querying Files Directly

```r
# Query CSV file
sales <- dbGetQuery(con, "SELECT * FROM 'datasets/raw/sales.csv'")

# Query Parquet
sales <- dbGetQuery(con, "
    SELECT product_category, SUM(amount) AS total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY product_category
")

# Query multiple files
all_logs <- dbGetQuery(con, "SELECT * FROM 'datasets/raw/logs_*.csv'")
```

## dplyr Integration

### Register Data Frame as Table

```r
library(dplyr)
library(dbplyr)

# Register R data.frame in DuckDB
employees_df <- read.csv("datasets/raw/employees.csv")
dbWriteTable(con, "employees", employees_df)

# Or use dplyr::copy_to
employees_tbl <- copy_to(con, employees_df, "employees")
```

### Query with dplyr Syntax

```r
# Create dplyr reference to DuckDB table
employees <- tbl(con, "employees")

# Use dplyr verbs (translated to SQL)
result <- employees %>%
    filter(salary > 80000) %>%
    group_by(department) %>%
    summarise(
        count = n(),
        avg_salary = mean(salary, na.rm = TRUE)
    ) %>%
    arrange(desc(avg_salary))

# Show generated SQL
result %>% show_query()

# Collect results to R
result_df <- result %>% collect()
```

### Query Files with dplyr

```r
# Reference file directly
sales <- tbl(con, "read_parquet('datasets/raw/sales.parquet')")

# Use dplyr
monthly_sales <- sales %>%
    mutate(month = date_trunc('month', sale_date)) %>%
    group_by(month, product_category) %>%
    summarise(total = sum(amount, na.rm = TRUE)) %>%
    collect()
```

## When to Use DuckDB vs Base R

### Use DuckDB when:
- **Data larger than RAM**: DuckDB handles out-of-memory
- **File-based workflows**: Query CSV/Parquet directly
- **Complex SQL**: Window functions, CTEs
- **Fast aggregations**: Vectorized execution

### Use R when:
- **Data fits in memory**: Native R is fine
- **Statistical modeling**: R's strength
- **Visualization**: ggplot2, etc.
- **R-specific packages**: No SQL equivalent

## Performance Comparison

```r
library(microbenchmark)

# Load data
sales_df <- read.csv("datasets/raw/sales.csv")

# R aggregation
benchmark_r <- function() {
    aggregate(amount ~ product_category, sales_df, sum)
}

# DuckDB aggregation
benchmark_duckdb <- function() {
    dbGetQuery(con, "
        SELECT product_category, SUM(amount) AS total
        FROM 'datasets/raw/sales.csv'
        GROUP BY product_category
    ")
}

microbenchmark(
    r = benchmark_r(),
    duckdb = benchmark_duckdb(),
    times = 10
)
```

## Combining DuckDB and ggplot2

```r
library(ggplot2)

# Query with DuckDB, visualize with ggplot2
daily_sales <- dbGetQuery(con, "
    SELECT
        sale_date,
        SUM(amount) AS total_sales
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
    ORDER BY sale_date
")

ggplot(daily_sales, aes(x = sale_date, y = total_sales)) +
    geom_line() +
    geom_smooth(method = "loess") +
    theme_minimal() +
    labs(title = "Daily Sales Trend", x = "Date", y = "Total Sales")
```

## Arrow Integration

Zero-copy data transfer between DuckDB and R.

```r
library(arrow)

# DuckDB -> Arrow -> R (efficient)
arrow_table <- dbGetQuery(con, "SELECT * FROM 'data.parquet'", arrow = TRUE)

# Convert to R data.frame if needed
df <- as.data.frame(arrow_table)

# R -> Arrow -> DuckDB
arrow_table <- arrow::as_arrow_table(mtcars)
dbWriteTable(con, "mtcars", arrow_table)
```

## Working with Partitioned Data

```r
# Read partitioned dataset
result <- dbGetQuery(con, "
    SELECT * FROM 'datasets/processed/sales_partitioned/**/*.parquet'
    WHERE year = 2023
")

# Create partitioned dataset from R
dbExecute(con, "
    COPY (
        SELECT *, EXTRACT(YEAR FROM sale_date) AS year
        FROM 'datasets/raw/sales.csv'
    ) TO 'output/sales_partitioned' (
        FORMAT PARQUET,
        PARTITION_BY (year),
        COMPRESSION 'ZSTD'
    )
")
```

## Example Workflow: R + DuckDB

```r
library(duckdb)
library(dplyr)
library(ggplot2)

# Connect
con <- dbConnect(duckdb::duckdb())

# 1. Query and aggregate with DuckDB
monthly_sales <- dbGetQuery(con, "
    WITH daily_sales AS (
        SELECT
            DATE_TRUNC('month', sale_date) AS month,
            product_category,
            SUM(amount) AS total
        FROM 'datasets/raw/sales.parquet'
        GROUP BY month, product_category
    )
    SELECT
        month,
        product_category,
        total,
        SUM(total) OVER (
            PARTITION BY product_category
            ORDER BY month
        ) AS cumulative_total
    FROM daily_sales
    ORDER BY product_category, month
")

# 2. Visualize with ggplot2
ggplot(monthly_sales, aes(x = month, y = total, color = product_category)) +
    geom_line() +
    facet_wrap(~product_category, scales = "free_y") +
    theme_minimal() +
    labs(title = "Monthly Sales by Category")

# 3. Statistical analysis in R
model <- lm(total ~ month + product_category, data = monthly_sales)
summary(model)

# Cleanup
dbDisconnect(con, shutdown = TRUE)
```

## Key Takeaways

✅ **DuckDB integrates seamlessly with R**
✅ **dplyr syntax translates to SQL** - familiar interface
✅ **Query files without loading** - same as CLI
✅ **Combine strengths**: DuckDB for data, R for stats/viz
✅ **Arrow for efficient transfer** - zero-copy

## Practice Exercises

See `exercises/07_r_integration.R` for hands-on R examples.

## Next Module

**Module 8: Python Integration** - Using DuckDB with pandas and polars.
