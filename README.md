# Patchit Databricks Data Quality Demo

This bundle provisions 6 production-style pipelines on Databricks. Each pipeline has
three tasks (ingest → transform → quality/publish) and intentionally injects a real-world
failure mode so PATCHIT can generate RCA and fix proposals.

## Pipelines included
- `retail_orders` — schema drift on `order_ts`
- `payments_recon` — duplicate transaction IDs
- `inventory_snapshot` — negative inventory values
- `customer_360` — missing email column
- `clickstream_sessions` — invalid timestamp strings
- `finance_close` — missing FX rates

## Prerequisites
- Databricks CLI v0.200+ (bundle support)
- `DATABRICKS_HOST` + `DATABRICKS_TOKEN` exported
- Existing all-purpose cluster id
- Unity Catalog catalog + schema (defaults: `patchit.dq`)

## Deploy
```
cd /Users/ankurchopra/.cursor/worktrees/repo_projects/dvh/projects/patchit-databricks-dq-demo

# Set once per shell to avoid repeating --var cluster_id
export BUNDLE_VAR_cluster_id=YOUR_CLUSTER_ID

databricks bundle validate -t dev

# Deploy using an existing cluster id
databricks bundle deploy -t dev --var cluster_id=YOUR_CLUSTER_ID
```

## Run a pipeline
```
# Example: retail orders
databricks bundle run -t dev retail_orders_pipeline
```

## Make it healthy after the demo
Edit `resources/jobs.yml` and change `fail_mode` to empty or `none` for the pipeline,
then re-run the job.

See `RUNBOOK_DEMO.md` for step-by-step demo flow and how to open the DQ monitoring UI.
