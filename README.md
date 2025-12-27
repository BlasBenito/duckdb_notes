# DuckDB & SQL Mini-Course

A hands-on 20-hour course focused on DuckDB and modern SQL for data analysis.

## 🎯 Course Overview

Learn to use DuckDB, the "SQLite for analytics," to efficiently analyze data files using SQL. This course emphasizes:
- **DuckDB-specific features**: Query files directly, modern SQL syntax, performance
- **Data analysis workflows**: Build reproducible analytical pipelines
- **Hands-on learning**: Real datasets and practical exercises throughout
- **Console-first approach**: Master the CLI, then apply to R/Python

## 👤 Who This Course Is For

- Intermediate SQL users looking to deepen skills
- Data analysts wanting faster file-based workflows
- Anyone who works with CSV, Parquet, or JSON files
- R/Python users seeking performance improvements

## ⏱️ Time Commitment

**Total**: 20 hours
- **Core modules** (1-6): 18 hours on DuckDB CLI
- **Integration modules** (7-8): 2 hours on R and Python

Work at your own pace. Each module includes:
- Comprehensive lesson notes
- Hands-on exercises with solutions
- Reference materials and cheat sheets

## 📚 Course Structure

### Module 1: DuckDB Fundamentals (3 hours)
Learn DuckDB basics: installation, CLI, reading files directly, and core architecture.

**Key topics**: CLI commands, file formats (CSV/Parquet/JSON), DESCRIBE/SUMMARIZE

**Hands-on**: Query CSV files, compare CSV vs Parquet performance, explore datasets

---

### Module 2: Modern SQL with DuckDB (6 hours)
Master advanced analytical SQL: window functions, CTEs, and DuckDB-specific features.

**Key topics**:
- Window functions (ranking, offset, aggregates, custom frames)
- CTEs and recursive CTEs
- GROUPING SETS, ROLLUP, CUBE
- PIVOT/UNPIVOT, QUALIFY clause
- List comprehensions, EXCLUDE/REPLACE

**Hands-on**: Time-series analysis, cohort analysis, hierarchical queries

---

### Module 3: Advanced File Operations (3 hours)
Master file reading/writing: glob patterns, format conversions, partitioned datasets.

**Key topics**: Glob patterns, Parquet compression, partitioning strategies, remote files

**Hands-on**: Process 100+ files, create partitioned datasets, build ETL pipelines

---

### Module 4: Query Optimization & Performance (3 hours)
Write efficient queries using EXPLAIN, understand execution plans, optimize performance.

**Key topics**: EXPLAIN ANALYZE, filter/projection pushdown, partition pruning, sampling

**Hands-on**: Diagnose slow queries, optimize joins, benchmark different approaches

---

### Module 5: DuckDB Extensions & Advanced Features (2 hours)
Use DuckDB extensions for specialized tasks: remote files, spatial operations, macros.

**Key topics**: httpfs (S3/HTTP), spatial extension (GIS), macros (UDFs), Parquet metadata

**Hands-on**: Query S3 files, basic geospatial analysis, create reusable macros

---

### Module 6: Data Analysis Workflows (2 hours)
Build complete, reproducible analytical workflows combining all learned skills.

**Key topics**: ETL pipelines, analysis scripts, error handling, documentation

**Hands-on**: Daily ETL pipeline, parameterized analysis, shell integration

---

### Module 7: R Integration (1.5 hours)
Apply DuckDB skills in R using the {duckdb} package and dplyr integration.

**Key topics**: duckdb package, dplyr integration, performance comparison, Arrow

**Hands-on**: Query files from R, combine DuckDB + ggplot2, statistical analysis

---

### Module 8: Python Integration (1.5 hours)
Apply DuckDB skills in Python with pandas, polars, and Jupyter notebooks.

**Key topics**: duckdb-python, pandas/polars integration, ETL pipelines, Jupyter

**Hands-on**: Build data pipelines, combine DuckDB + matplotlib, notebook workflows

---

## 🚀 Getting Started

### Prerequisites

- **System**: Ubuntu 24.04 (or similar Linux/macOS)
- **DuckDB**: v1.4.1+ (already installed at `/home/blas/.local/bin/duckdb`)
- **SQL Knowledge**: Comfortable with SELECT, JOIN, WHERE, GROUP BY
- **Optional**: R and Python for integration modules

### Installation Check

```bash
# Verify DuckDB is installed
duckdb --version

# Should show: v1.4.1 or later
```

### Repository Structure

```
duckdb_notes/
├── lessons/           # Lesson notes for each module
│   ├── 01_fundamentals/
│   ├── 02_modern_sql/
│   ├── 03_file_operations/
│   ├── 04_optimization/
│   ├── 05_extensions/
│   ├── 06_workflows/
│   ├── 07_r_integration/
│   └── 08_python_integration/
├── datasets/          # Sample datasets
│   ├── raw/          # Original data files
│   └── processed/    # Processed/transformed data
├── exercises/         # Hands-on exercises with solutions
├── reference/         # Quick reference materials
│   └── cheatsheets/
└── README.md          # This file
```

### Quick Start

1. **Read Module 1 lesson notes**:
   ```bash
   cat lessons/01_fundamentals/lesson.md
   ```

2. **Start DuckDB and run first query**:
   ```bash
   duckdb
   ```
   ```sql
   SELECT 'Hello DuckDB!' AS greeting;
   .quit
   ```

3. **Try querying a file directly**:
   ```bash
   duckdb -c "SELECT * FROM 'datasets/raw/employees.csv' LIMIT 5"
   ```

4. **Work through exercises**:
   ```bash
   duckdb < exercises/01_fundamentals.sql
   ```

## 📖 Learning Path

### Recommended Progression

1. **Start with Module 1** - Get comfortable with DuckDB basics
2. **Work through Modules 2-6 sequentially** - Core skills build on each other
3. **Try exercises after each lesson** - Hands-on practice is essential
4. **Use reference materials** - Cheat sheets for quick lookup
5. **Complete Modules 7-8** - Apply skills in your preferred language
6. **Apply to real problems** - Use your own datasets!

### Study Tips

- **Enable timing**: Always use `.timer on` to see query performance
- **Experiment**: Try variations of exercises
- **Use EXPLAIN**: Understand what DuckDB is doing
- **Sample first**: Develop on small datasets, then scale up
- **Document**: Comment your queries, build a personal reference

## 📊 Datasets Included

All datasets are in `datasets/raw/`:

- **employees.csv** (20 rows): Employee data with salary, department, hire date
- **sales.csv / sales.parquet** (100K rows): Sales transactions for performance testing
- **logs_*.csv** (15K rows): Event logs across multiple files
- **users.json**: User data with nested structure
- **wide_table.csv** (10K rows, 55 columns): For testing columnar performance

Additional datasets will be generated as part of exercises.

## 🎓 Learning Outcomes

After completing this course, you will be able to:

✅ Query CSV, Parquet, and JSON files without loading into a database
✅ Write optimized analytical queries using window functions and CTEs
✅ Build data transformation pipelines using DuckDB
✅ Use EXPLAIN to understand and optimize query performance
✅ Leverage DuckDB extensions for specialized tasks (S3, spatial)
✅ Integrate DuckDB into R and Python workflows
✅ Choose the right tool (DuckDB vs pandas/dplyr) for each task
✅ Create reproducible analytical workflows
✅ Work efficiently with partitioned datasets
✅ Apply modern SQL features (QUALIFY, EXCLUDE, list comprehensions)

## 🔧 Troubleshooting

### DuckDB not found
```bash
# Add to PATH if needed
export PATH="$HOME/.local/bin:$PATH"

# Or install/update DuckDB
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
mv duckdb ~/.local/bin/
chmod +x ~/.local/bin/duckdb
```

### Permission denied on datasets
```bash
chmod -R u+rw datasets/
```

### Out of memory errors
```sql
-- Reduce memory limit
SET memory_limit='2GB';

-- Use sampling for large files
SELECT * FROM 'huge_file.parquet' USING SAMPLE 1 PERCENT;
```

## 📚 Additional Resources

### Official Documentation
- [DuckDB Docs](https://duckdb.org/docs/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [DuckDB GitHub](https://github.com/duckdb/duckdb)

### Reference Materials in This Course
- `reference/cheatsheets/cli_basics.md` - CLI commands
- `reference/cheatsheets/window_functions.md` - Window functions reference
- `reference/cheatsheets/cte_patterns.md` - CTE patterns

### Community
- [DuckDB Discord](https://discord.duckdb.org/)
- [DuckDB Discussions](https://github.com/duckdb/duckdb/discussions)

## 💡 Tips for Success

1. **Practice daily**: Even 30 minutes of hands-on work is valuable
2. **Use real data**: Apply concepts to your own datasets
3. **Build a reference**: Create your own cheat sheets
4. **Share knowledge**: Explain concepts to others (best way to learn!)
5. **Start simple**: Don't try to optimize prematurely
6. **Experiment**: DuckDB is fast - try different approaches
7. **Version control**: Keep your SQL scripts in git
8. **Document**: Future you will thank present you

## 🤝 Contributing

Found an error? Have a suggestion? Feel free to:
- Open an issue
- Submit a pull request
- Share your own examples

## 📝 License

This course material is provided for educational purposes.

---

**Ready to start?** Head to `lessons/01_fundamentals/lesson.md` and begin your DuckDB journey!

**Questions?** Check the troubleshooting section or refer to official DuckDB documentation.

Happy querying! 🦆
