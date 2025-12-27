# Module 5: DuckDB Extensions & Advanced Features

**Duration**: 2 hours
**Objective**: Use essential DuckDB extensions and advanced features

## Extension System

Extensions add functionality to DuckDB: new file formats, functions, or capabilities.

### Managing Extensions

```sql
-- List available extensions
SELECT * FROM duckdb_extensions();

-- Install extension
INSTALL extension_name;

-- Load extension (must load each session)
LOAD extension_name;

-- Auto-load on startup
SET autoinstall_known_extensions=true;
SET autoload_known_extensions=true;
```

## httpfs Extension: Remote Files

Access files over HTTP/HTTPS and S3.

### Installation

```sql
INSTALL httpfs;
LOAD httpfs;
```

### HTTP/HTTPS Access

```sql
-- Read from URL
SELECT * FROM 'https://example.com/data.csv';

-- Read Parquet from URL
SELECT * FROM 'https://example.com/data.parquet';
```

### S3 Access

```sql
-- Configure (if using private buckets)
SET s3_region='us-east-1';
SET s3_access_key_id='YOUR_KEY';
SET s3_secret_access_key='YOUR_SECRET';

-- Query S3 file
SELECT * FROM 's3://bucket-name/path/to/file.parquet';

-- Query with glob
SELECT * FROM 's3://bucket-name/data/*.parquet';
```

### Public Datasets

```sql
-- AWS Open Data (no credentials needed)
LOAD httpfs;
SELECT * FROM 's3://noaa-ghcn-pds/csv/2024.csv' LIMIT 10;
```

## Spatial Extension: GIS Operations

Basic geospatial analysis.

### Installation

```sql
INSTALL spatial;
LOAD spatial;
```

### Creating Geometries

```sql
-- Create points
SELECT ST_Point(lon, lat) AS geom
FROM locations;

-- Create from WKT (Well-Known Text)
SELECT ST_GeomFromText('POINT(-73.98 40.75)') AS nyc;

SELECT ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS square;
```

### Spatial Operations

```sql
-- Distance between points (in degrees)
SELECT ST_Distance(
    ST_Point(-73.98, 40.75),  -- NYC
    ST_Point(-118.24, 34.05)  -- LA
) AS distance;

-- Distance in meters (use geography)
SELECT ST_Distance(
    ST_GeogFromText('POINT(-73.98 40.75)'),
    ST_GeogFromText('POINT(-118.24 34.05)')
) / 1000 AS distance_km;

-- Point in polygon
SELECT ST_Within(
    ST_Point(-73.98, 40.75),
    ST_GeomFromText('POLYGON((-74 40, -74 41, -73 41, -73 40, -74 40))')
) AS is_within;

-- Buffer (expand geometry)
SELECT ST_Buffer(ST_Point(0, 0), 1.0) AS circle;
```

### Spatial Joins

```sql
-- Find points within regions
SELECT p.name, r.region_name
FROM points p
JOIN regions r ON ST_Within(p.geom, r.geom);

-- Find nearest neighbor
SELECT p1.name, p2.name,
       ST_Distance(p1.geom, p2.geom) AS distance
FROM points p1
CROSS JOIN points p2
WHERE p1.id != p2.id
QUALIFY ROW_NUMBER() OVER (PARTITION BY p1.id ORDER BY ST_Distance(p1.geom, p2.geom)) = 1;
```

### Reading Geospatial Files

```sql
-- Read GeoJSON
SELECT * FROM ST_Read('data.geojson');

-- Read Shapefile
SELECT * FROM ST_Read('data.shp');

-- Export to GeoJSON
COPY (SELECT * FROM spatial_table)
TO 'output.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
```

## Parquet Extension (Built-in)

Advanced Parquet operations.

### Metadata Inspection

```sql
-- File metadata
SELECT * FROM parquet_metadata('file.parquet');

-- Schema information
SELECT * FROM parquet_schema('file.parquet');

-- File statistics
SELECT * FROM parquet_file_metadata('file.parquet');
```

### Row Group Statistics

```sql
-- See min/max per row group (enables pruning)
SELECT * FROM parquet_kv_metadata('file.parquet');
```

## User-Defined Functions: Macros

Create reusable custom functions.

### Scalar Macros

```sql
-- Simple macro
CREATE MACRO add_tax(amount) AS amount * 1.08;

-- Use it
SELECT add_tax(100) AS total;

-- Macro with multiple parameters
CREATE MACRO full_name(first, last) AS first || ' ' || last;

SELECT full_name(first_name, last_name) FROM users;
```

### Table Macros

```sql
-- Macro that returns a table
CREATE MACRO top_n(tbl, n) AS TABLE
    SELECT * FROM tbl ORDER BY amount DESC LIMIT n;

-- Use it
SELECT * FROM top_n('sales.parquet', 10);
```

### Parameterized Queries

```sql
-- Create template query
CREATE MACRO sales_by_category(cat) AS TABLE
    SELECT * FROM 'sales.parquet'
    WHERE product_category = cat;

-- Execute with parameter
SELECT * FROM sales_by_category('Electronics');
```

### Common Macro Patterns

```sql
-- Percent change
CREATE MACRO pct_change(new_val, old_val) AS
    ROUND(100.0 * (new_val - old_val) / old_val, 2);

-- Clean string
CREATE MACRO clean_string(s) AS
    UPPER(TRIM(s));

-- Date range filter
CREATE MACRO in_date_range(dt, start_dt, end_dt) AS
    dt BETWEEN start_dt AND end_dt;
```

## Time-Zone Aware Operations

Handle timestamps with time zones.

```sql
-- Create timestamp with timezone
SELECT '2024-01-01 12:00:00'::TIMESTAMP AS no_tz,
       '2024-01-01 12:00:00'::TIMESTAMPTZ AS with_tz;

-- Convert between timezones
SELECT timezone('America/New_York', '2024-01-01 12:00:00'::TIMESTAMPTZ) AS ny_time,
       timezone('Europe/London', '2024-01-01 12:00:00'::TIMESTAMPTZ) AS london_time;

-- Current time in timezone
SELECT current_timestamp AT TIME ZONE 'America/Los_Angeles';
```

## Prepared Statements

Parameterize queries for reuse.

```sql
-- Prepare statement
PREPARE sales_query AS
    SELECT * FROM sales WHERE amount > $1 AND category = $2;

-- Execute with parameters
EXECUTE sales_query(1000, 'Electronics');

-- Execute with different parameters
EXECUTE sales_query(500, 'Clothing');
```

## JSON Extension (Built-in)

Advanced JSON operations.

```sql
-- Extract JSON fields
SELECT data->>'name' AS name,
       data->'address'->>'city' AS city
FROM json_data;

-- JSON aggregation
SELECT json_group_array(name) FROM users;

SELECT json_group_object(key, value) FROM key_value_pairs;
```

## Key Takeaways

✅ **Extensions add powerful features** - httpfs, spatial, and more
✅ **httpfs enables remote queries** - HTTP, S3 without downloading
✅ **Spatial extension** handles basic GIS operations
✅ **Macros create reusable functions** - DRY principle
✅ **Parquet metadata** useful for debugging and optimization

## Practice Exercises

See `exercises/05_extensions.sql` for hands-on exercises with extensions.

## Next Module

**Module 6: Data Analysis Workflows** - Build complete end-to-end analytical pipelines.
