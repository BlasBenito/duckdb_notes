# Common Table Expressions (CTEs) - Patterns Guide

## Basic CTE Syntax

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;
```

## When to Use CTEs

✅ **Use CTEs when**:
- Breaking down complex queries into logical steps
- Reusing the same subquery multiple times
- Improving query readability
- Debugging complex transformations
- Creating temporary data transformations

❌ **Skip CTEs when**:
- Simple queries (single SELECT)
- Performance-critical code (sometimes subqueries optimize better)
- The CTE is used only once and isn't complex

## Multiple CTEs (Chaining)

```sql
WITH
-- Step 1: Filter
filtered_data AS (
    SELECT * FROM sales WHERE amount > 100
),
-- Step 2: Aggregate
aggregated AS (
    SELECT
        product_id,
        SUM(amount) AS total_sales
    FROM filtered_data
    GROUP BY product_id
),
-- Step 3: Rank
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY total_sales DESC) AS rank
    FROM aggregated
)
-- Final query
SELECT * FROM ranked WHERE rank <= 10;
```

## Common CTE Patterns

### 1. Data Cleaning Pipeline

```sql
WITH
-- Step 1: Remove nulls and outliers
cleaned AS (
    SELECT *
    FROM raw_data
    WHERE value IS NOT NULL
      AND value BETWEEN 0 AND 10000
),
-- Step 2: Standardize formats
standardized AS (
    SELECT
        UPPER(TRIM(name)) AS name,
        ROUND(value, 2) AS value,
        DATE_TRUNC('day', timestamp) AS date
    FROM cleaned
),
-- Step 3: Deduplicate
deduplicated AS (
    SELECT * FROM standardized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY name, date ORDER BY timestamp DESC) = 1
)
SELECT * FROM deduplicated;
```

### 2. Cohort Analysis

```sql
WITH
-- Define cohorts
cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month
    FROM users
),
-- User activity by month
user_activity AS (
    SELECT
        user_id,
        DATE_TRUNC('month', activity_date) AS activity_month
    FROM events
),
-- Join cohorts with activity
cohort_activity AS (
    SELECT
        c.cohort_month,
        ua.activity_month,
        COUNT(DISTINCT ua.user_id) AS active_users
    FROM cohorts c
    JOIN user_activity ua ON c.user_id = ua.user_id
    GROUP BY c.cohort_month, ua.activity_month
)
SELECT
    cohort_month,
    activity_month,
    DATE_DIFF('month', cohort_month, activity_month) AS months_since_cohort,
    active_users
FROM cohort_activity
ORDER BY cohort_month, months_since_cohort;
```

### 3. Time Series Gap Filling

```sql
WITH RECURSIVE
-- Generate complete date range
date_series AS (
    SELECT MIN(sale_date) AS date, MAX(sale_date) AS max_date
    FROM sales

    UNION ALL

    SELECT date + INTERVAL 1 DAY, max_date
    FROM date_series
    WHERE date < max_date
),
-- Actual sales by date
actual_sales AS (
    SELECT sale_date, SUM(amount) AS total
    FROM sales
    GROUP BY sale_date
)
-- Fill gaps with zeros
SELECT
    ds.date,
    COALESCE(asa.total, 0) AS sales
FROM date_series ds
LEFT JOIN actual_sales asa ON ds.date = asa.sale_date;
```

### 4. Running Calculations with Context

```sql
WITH
-- Daily aggregates
daily_stats AS (
    SELECT
        date,
        SUM(amount) AS daily_total,
        COUNT(*) AS daily_count
    FROM transactions
    GROUP BY date
),
-- Add running totals and averages
with_running AS (
    SELECT
        date,
        daily_total,
        daily_count,
        SUM(daily_total) OVER (ORDER BY date) AS running_total,
        AVG(daily_total) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7day
    FROM daily_stats
),
-- Classify days
classified AS (
    SELECT *,
           CASE
               WHEN daily_total > ma_7day * 1.5 THEN 'High'
               WHEN daily_total < ma_7day * 0.5 THEN 'Low'
               ELSE 'Normal'
           END AS performance
    FROM with_running
)
SELECT * FROM classified;
```

### 5. Hierarchical Aggregation

```sql
WITH
-- Leaf level aggregates
product_sales AS (
    SELECT
        product_id,
        category_id,
        SUM(amount) AS sales
    FROM transactions
    GROUP BY product_id, category_id
),
-- Category level
category_sales AS (
    SELECT
        category_id,
        SUM(sales) AS sales
    FROM product_sales
    GROUP BY category_id
),
-- Overall total
total_sales AS (
    SELECT SUM(sales) AS sales
    FROM category_sales
)
-- Combine with percentages
SELECT
    'Product' AS level,
    product_id AS id,
    sales,
    100.0 * sales / (SELECT sales FROM total_sales) AS pct_of_total
FROM product_sales
UNION ALL
SELECT
    'Category',
    category_id::VARCHAR,
    sales,
    100.0 * sales / (SELECT sales FROM total_sales)
FROM category_sales
UNION ALL
SELECT
    'Total',
    'ALL',
    sales,
    100.0
FROM total_sales;
```

### 6. Self-Referencing for Comparisons

```sql
WITH
monthly_sales AS (
    SELECT
        DATE_TRUNC('month', sale_date) AS month,
        SUM(amount) AS total
    FROM sales
    GROUP BY month
)
SELECT
    curr.month,
    curr.total AS current_month,
    prev.total AS previous_month,
    curr.total - prev.total AS mom_change,
    100.0 * (curr.total - prev.total) / prev.total AS mom_pct_change,
    yoy.total AS year_ago,
    curr.total - yoy.total AS yoy_change,
    100.0 * (curr.total - yoy.total) / yoy.total AS yoy_pct_change
FROM monthly_sales curr
LEFT JOIN monthly_sales prev
    ON prev.month = curr.month - INTERVAL 1 MONTH
LEFT JOIN monthly_sales yoy
    ON yoy.month = curr.month - INTERVAL 1 YEAR;
```

## Recursive CTEs

### Syntax

```sql
WITH RECURSIVE cte_name AS (
    -- Anchor (base case)
    SELECT ...

    UNION ALL

    -- Recursive term
    SELECT ...
    FROM cte_name
    [JOIN other_tables]
    WHERE termination_condition
)
SELECT * FROM cte_name;
```

### Pattern 1: Number/Date Sequences

```sql
-- Generate numbers 1-100
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < 100
)
SELECT * FROM numbers;

-- Generate date range
WITH RECURSIVE dates AS (
    SELECT DATE '2024-01-01' AS date
    UNION ALL
    SELECT date + INTERVAL 1 DAY
    FROM dates
    WHERE date < DATE '2024-12-31'
)
SELECT * FROM dates;
```

### Pattern 2: Hierarchical Data

```sql
-- Organization tree (employees with manager_id)
WITH RECURSIVE org_tree AS (
    -- Anchor: Top of hierarchy (no manager)
    SELECT id, name, manager_id, 1 AS level, name AS path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: Add direct reports
    SELECT
        e.id,
        e.name,
        e.manager_id,
        ot.level + 1,
        ot.path || ' > ' || e.name
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.id
)
SELECT * FROM org_tree ORDER BY path;
```

### Pattern 3: Graph Traversal

```sql
-- Find all paths in a graph
WITH RECURSIVE paths AS (
    -- Anchor: Start nodes
    SELECT
        node_id,
        node_id AS path,
        0 AS depth
    FROM nodes
    WHERE is_start = true

    UNION ALL

    -- Recursive: Follow edges
    SELECT
        e.to_node,
        p.path || ' -> ' || e.to_node,
        p.depth + 1
    FROM paths p
    JOIN edges e ON p.node_id = e.from_node
    WHERE p.depth < 10  -- Prevent infinite loops
)
SELECT * FROM paths;
```

### Pattern 4: Cumulative Calculations

```sql
-- Running product (factorial-like)
WITH RECURSIVE factorial AS (
    SELECT 1 AS n, 1 AS product
    UNION ALL
    SELECT n + 1, product * (n + 1)
    FROM factorial
    WHERE n < 10
)
SELECT n, product FROM factorial;
```

## CTE vs Subquery vs Temp Table

### Use CTE when:
- Query is run once
- Logical steps need names for readability
- Result set is small-to-medium
- Need to reference result multiple times in same query

### Use Subquery when:
- Very simple transformation
- Used only once
- Optimizer can push down predicates

### Use Temp Table when:
- Result is reused across multiple queries
- Large intermediate result
- Need to create indexes on intermediate data

```sql
-- CTE (good for single-query, multi-reference)
WITH top_sellers AS (SELECT * FROM products WHERE sales > 10000)
SELECT * FROM top_sellers ts1 JOIN top_sellers ts2 ON ...;

-- Temp table (good for multi-query reuse)
CREATE TEMP TABLE top_sellers AS
SELECT * FROM products WHERE sales > 10000;

-- Use in multiple queries
SELECT * FROM top_sellers WHERE category = 'Electronics';
SELECT * FROM top_sellers WHERE region = 'West';
```

## CTE Best Practices

1. **Name CTEs descriptively**
   ```sql
   WITH high_value_customers AS (...), recent_purchases AS (...)
   ```

2. **One transformation per CTE**
   - Each CTE should do ONE logical thing
   - Makes debugging easier

3. **Comment complex CTEs**
   ```sql
   WITH
   -- Calculate customer lifetime value for active customers
   customer_ltv AS (...)
   ```

4. **Order CTEs logically**
   - List CTEs in the order they're used
   - Later CTEs can reference earlier ones

5. **Use QUALIFY when possible**
   ```sql
   -- Instead of
   WITH ranked AS (
       SELECT *, RANK() OVER (...) AS rnk FROM data
   )
   SELECT * FROM ranked WHERE rnk = 1;

   -- Use
   SELECT * FROM data
   QUALIFY RANK() OVER (...) = 1;
   ```

6. **Materialize when needed**
   - DuckDB optimizes CTEs automatically
   - If CTE is used multiple times and is expensive, it may be materialized
   - For explicit control, use temp tables

## Debugging CTEs

1. **Test each CTE individually**
   ```sql
   WITH cte1 AS (...), cte2 AS (...)
   SELECT * FROM cte1;  -- Test first CTE
   -- SELECT * FROM cte2;  -- Then test second
   ```

2. **Add row counts**
   ```sql
   WITH
   step1 AS (SELECT * FROM ... ),
   step2 AS (SELECT * FROM step1 WHERE ...)
   SELECT
       (SELECT COUNT(*) FROM step1) AS step1_count,
       (SELECT COUNT(*) FROM step2) AS step2_count;
   ```

3. **Use EXPLAIN**
   ```sql
   EXPLAIN WITH cte AS (...) SELECT * FROM cte;
   ```

## Common Pitfalls

1. **Circular references** - Can't reference later CTEs in earlier ones
2. **Recursive infinite loops** - Always include termination condition
3. **Performance** - Very complex CTEs might be slower than temp tables
4. **Scope** - CTEs are only available in the query that defines them

## Advanced: Materialized CTEs (Future)

Some databases support MATERIALIZED hint:
```sql
WITH MATERIALIZED expensive_cte AS (
    -- Complex calculation
)
SELECT * FROM expensive_cte e1
JOIN expensive_cte e2 ON ...;
```

DuckDB doesn't currently support the MATERIALIZED keyword but may optimize automatically.
