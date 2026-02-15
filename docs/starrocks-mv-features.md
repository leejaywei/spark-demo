# StarRocks Materialized View Features

This document lists all StarRocks materialized view features and their implementation status in the `spark-mv-plugin`.

## Feature Overview

| Feature | Status | Description |
|---------|--------|-------------|
| CREATE MATERIALIZED VIEW | ✅ Implemented | Create an MV with a defining SQL query |
| DROP MATERIALIZED VIEW | ✅ Implemented | Drop an MV and its backing table |
| REFRESH MATERIALIZED VIEW | ✅ Implemented | Full refresh of MV data |
| REFRESH MATERIALIZED VIEW INCREMENTAL | ✅ Implemented | Incremental refresh (fallback to full) |
| SHOW MATERIALIZED VIEWS | ✅ Implemented | List all MVs with metadata |
| Exact query match rewrite | ✅ Implemented | Rewrite identical query to scan MV |
| Aggregate rewrite | ✅ Implemented | Rewrite aggregate queries to use MV |
| Roll-up (上卷) rewrite | ✅ Implemented | Re-aggregate from MV for coarser GROUP BY |
| Predicate compensation | ✅ Implemented | Apply extra WHERE on top of MV scan |
| Column subset selection | ✅ Implemented | Query fewer columns than MV stores |
| AVG derivation | ✅ Implemented | Compute AVG from SUM/COUNT in MV |
| JOIN matching | ✅ Implemented | Rewrite join queries to use pre-joined MV |
| JOIN + aggregate matching | ✅ Implemented | Rewrite aggregated join queries |
| View data storage metadata | ✅ Implemented | Track format, location, row count, size |

## Detailed Feature Descriptions

### 1. CREATE MATERIALIZED VIEW

Creates a materialized view by executing the defining query and storing
the result into a backing Hive table.

```sql
CREATE MATERIALIZED VIEW mv_name AS
SELECT region, SUM(amount) AS total
FROM sales
GROUP BY region;
```

### 2. DROP MATERIALIZED VIEW

Drops the materialized view, removing both the catalog entry and the
backing table that stores the data.

```sql
DROP MATERIALIZED VIEW mv_name;
```

### 3. REFRESH MATERIALIZED VIEW

Re-executes the defining query and overwrites the backing table with
fresh data.

```sql
-- Full refresh
REFRESH MATERIALIZED VIEW mv_name;

-- Incremental refresh (currently falls back to full)
REFRESH MATERIALIZED VIEW mv_name INCREMENTAL;
```

### 4. SHOW MATERIALIZED VIEWS

Lists all registered materialized views with their metadata, including
storage information.

```sql
SHOW MATERIALIZED VIEWS;
```

Returns columns: `name`, `query`, `backing_table`, `last_refresh_ts`,
`storage_format`, `storage_location`, `row_count`, `size_in_bytes`.

### 5. Exact Query Match Rewrite

When a query's canonical plan is identical to an MV's defining query,
the optimizer replaces the entire plan with a scan of the MV's backing
table.

```sql
-- MV definition
CREATE MATERIALIZED VIEW mv1 AS
SELECT id, amount FROM orders WHERE status = 'completed';

-- This query is rewritten to scan mv_backing_mv1
SELECT id, amount FROM orders WHERE status = 'completed';
```

### 6. Aggregate Rewrite

Rewrites aggregate queries whose GROUP BY columns and aggregate
functions match the MV exactly.

```sql
-- MV definition
CREATE MATERIALIZED VIEW mv_region AS
SELECT region, SUM(amount) AS total FROM sales GROUP BY region;

-- This query uses the MV directly
SELECT region, SUM(amount) AS total FROM sales GROUP BY region;
```

### 7. Roll-up (上卷) Rewrite

When a query groups by a *subset* of the MV's GROUP BY columns, the
optimizer re-aggregates from the MV data:

- `SUM` → `SUM(sum_col)`
- `COUNT` → `SUM(count_col)`
- `MIN` → `MIN(min_col)`
- `MAX` → `MAX(max_col)`
- `AVG` → `SUM(sum_col) / SUM(count_col)` (when MV stores both)

```sql
-- MV groups by (region, city)
CREATE MATERIALIZED VIEW mv_city AS
SELECT region, city, SUM(amount) AS total FROM sales GROUP BY region, city;

-- Query groups by (region) only → roll-up
SELECT region, SUM(amount) AS total FROM sales GROUP BY region;
-- Rewrites to: SELECT region, SUM(total) FROM mv_backing_mv_city GROUP BY region
```

### 8. Predicate Compensation

If the query has extra WHERE predicates that the MV doesn't, they are
applied on top of the MV scan (provided the MV predicates are a subset
of the query's).

```sql
-- MV has no WHERE
CREATE MATERIALIZED VIEW mv_all AS
SELECT region, SUM(amount) AS total FROM sales GROUP BY region;

-- Query adds WHERE → compensated on MV scan
SELECT region, SUM(amount) AS total FROM sales WHERE region = 'east' GROUP BY region;
```

### 9. Column Subset Selection

The query may select fewer columns than the MV stores; a Project
operator prunes the extra columns.

```sql
-- MV has many aggregate columns
CREATE MATERIALIZED VIEW mv_full AS
SELECT region, SUM(amount) AS total, COUNT(amount) AS cnt, MIN(amount) AS mn
FROM sales GROUP BY region;

-- Query selects only SUM
SELECT region, SUM(amount) AS total FROM sales GROUP BY region;
```

### 10. JOIN Matching

Rewrites queries containing JOINs to use MVs that pre-compute the same
join. Supports:

- **Exact join match**: same tables, join type, and conditions
- **Join with predicate compensation**: extra WHERE on join query
- **Join with column subset**: fewer columns from the join result
- **Join + aggregate**: aggregated join queries

```sql
-- MV pre-computes a join
CREATE MATERIALIZED VIEW mv_order_customer AS
SELECT o.order_id, o.amount, c.name, c.city
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id;

-- This query is rewritten to scan the MV
SELECT o.order_id, o.amount, c.name, c.city
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id;

-- Join with extra WHERE → predicate compensation
SELECT o.order_id, o.amount, c.name, c.city
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.region = 'east';
```

### 11. View Data Storage Metadata

The MV catalog tracks storage information for each materialized view:

- **`storageFormat`**: The format used to store MV data (e.g., "parquet")
- **`storageLocation`**: Physical path to the backing table data
- **`rowCount`**: Number of rows after last refresh
- **`sizeInBytes`**: Data size in bytes after last refresh

This metadata is visible via `SHOW MATERIALIZED VIEWS` and is updated
on CREATE and REFRESH operations.

## Architecture

```
MaterializedViewExtensions
├── MaterializedViewParser         — intercepts MV DDL commands
├── MaterializedViewOptimizationRule — post-hoc resolution rule
│   ├── Exact canonical match
│   ├── JoinRewriter               — join tree matching
│   │   ├── Exact join match
│   │   ├── Join + predicate compensation
│   │   ├── Join + column subset
│   │   └── Join + aggregate match
│   └── AggregateRewriter          — aggregate/roll-up matching
│       ├── Exact aggregate match
│       ├── Roll-up rewrite
│       ├── Predicate compensation
│       ├── Column subset selection
│       └── AVG derivation
├── MaterializedViewCatalog        — in-memory metadata store
│   └── MaterializedViewMeta       — name, query, backing table, storage info
└── MaterializedViewCommands       — CREATE/DROP/REFRESH/SHOW commands
```

## Test Coverage

- **MaterializedViewSuite**: 27 tests covering CREATE, DROP, REFRESH,
  SHOW, storage metadata, query optimization, case insensitivity, and
  end-to-end lifecycle.
- **AggregateRewriteSuite**: 14 tests covering aggregate rewrite,
  roll-up, predicate compensation, column subset, AVG derivation, and
  negative cases.
- **JoinRewriteSuite**: 9 tests covering exact join match, join +
  predicate compensation, join + column subset, join + aggregate, and
  negative cases.
