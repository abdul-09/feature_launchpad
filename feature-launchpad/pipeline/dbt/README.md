# Feature Launchpad - dbt Models

This dbt project transforms raw event data into actionable analytics for measuring product feature impact.

## Model Layers

### Staging (`models/staging/`)

Raw data cleaning and standardization:

| Model | Description |
|-------|-------------|
| `stg_events` | Cleans raw event data, flattens JSON, filters bot traffic |

### Core Marts (`models/marts/core/`)

Foundational fact and dimension tables:

| Model | Description |
|-------|-------------|
| `fact_events` | Enriched event fact table with computed metrics |
| `dim_users` | User dimension with lifetime metrics and segmentation |

### Engagement Marts (`models/marts/engagement/`)

Business-focused analytics models:

| Model | Description |
|-------|-------------|
| `engagement_metrics` | Daily KPIs including adoption, completion, retention lift |
| `funnel_analysis` | Step-by-step conversion funnel with drop-off rates |

## Key Metrics

```sql
-- Example: Query retention lift
SELECT 
    metric_date,
    completer_return_rate,
    non_completer_return_rate,
    (completer_return_rate - non_completer_return_rate) / 
        non_completer_return_rate * 100 as retention_lift_pct
FROM engagement_metrics
ORDER BY metric_date DESC;
```

## Running the Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select engagement_metrics

# Test all models
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```

## Model DAG

```
stg_events
    │
    ├──► fact_events ──► engagement_metrics
    │                         │
    └──► dim_users ──────────►│
                              │
                              ▼
                      funnel_analysis
```

## Configuration

- **Target**: DuckDB (local analytics warehouse)
- **Materialization**: Tables for marts, views for staging
- **Testing**: Unique keys, not-null constraints, accepted values

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [DuckDB dbt Adapter](https://github.com/duckdb/dbt-duckdb)
