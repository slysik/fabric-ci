# Sales Analytics medallion

Fabric item source for the medallion architecture built in the
`lakeshore-ontology-lab` workspace (lakehouse `SalesLakehouse`).

Layout is the Fabric Git-integration format (`<item>.<type>/` with `.platform`
plus content), so the folder can be imported to, or synced with, a workspace.

## Items

| Item | Type | Purpose |
|---|---|---|
| `SalesLakehouse` | Lakehouse | Single lakehouse holding all layers as Delta tables |
| `nb_bronze_load` | Notebook | Landing CSV → Bronze Delta, raw strings preserved, ingestion metadata appended |
| `nb_silver_transform` | Notebook | Type casting, validation, dedupe on business keys, rejections, quarantine, joins |
| `nb_gold_build` | Notebook | Builds `gold_stg_*` staging tables only |
| `nb_validate_promote` | Notebook | 47 validation checks, then promotes `gold_stg_*` → `gold_*` only on full pass |

## Execution order

```
nb_bronze_load  ->  nb_silver_transform  ->  nb_gold_build  ->  nb_validate_promote
```

All notebooks are idempotent (overwrite semantics with deterministic dedupe
ordering); re-running the chain reproduces identical row counts.

## Layers

- Bronze: `bronze_customer`, `bronze_product`, `bronze_order`, `bronze_order_line`
- Silver: `silver_customer`, `silver_product`, `silver_order`, `silver_order_line`,
  `silver_quarantine`, `silver_rejected_order`, `silver_rejected_order_line`
- Gold: `gold_daily_sales`, `gold_customer_sales`, `gold_product_performance`,
  `gold_monthly_revenue_by_segment`
- Ops: `ops_notebook_runs`, `ops_validation_results`

Gold is only promoted after all validation checks pass; on failure the gate
records `BLOCKED` and leaves any existing Gold tables untouched.

## Source data

Read from `SalesLakehouse/Files/landing` (`customer.csv`, `product.csv`,
`order.csv`, `order_line.csv`). The landing files are treated as immutable
read-only inputs — nothing in this pipeline writes to them.
