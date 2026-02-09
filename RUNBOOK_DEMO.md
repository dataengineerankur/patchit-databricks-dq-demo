# Demo Runbook — Databricks DQ + PATCHIT

This runbook walks through a full demo: deploy six pipelines, induce failures,
show Databricks monitoring, then showcase PATCHIT’s RCA + fix workflow.

## 1) Deploy the bundle
```
cd /Users/ankurchopra/.cursor/worktrees/repo_projects/dvh/projects/patchit-databricks-dq-demo
export BUNDLE_VAR_cluster_id=YOUR_CLUSTER_ID
databricks bundle validate -t dev
databricks bundle deploy -t dev --var cluster_id=YOUR_CLUSTER_ID
```

## 2) Run the pipelines (failures on purpose)
The jobs are preconfigured with a failure mode. Run any or all:
```
databricks bundle run -t dev retail_orders_pipeline
databricks bundle run -t dev payments_recon_pipeline
databricks bundle run -t dev inventory_snapshot_pipeline
databricks bundle run -t dev customer_360_pipeline
databricks bundle run -t dev clickstream_sessions_pipeline
databricks bundle run -t dev finance_close_pipeline
```

## 3) Show failed jobs and logs
In Databricks:
1. Go to **Workflows** → **Jobs**.
2. Open a job (e.g., `patchit-retail-orders`).
3. Click the latest **Run** → open the failed task.
4. Show the error stack trace (this is what PATCHIT will ingest).

## 4) Enable Data Quality Monitoring (Unity Catalog)
From the Databricks blog, DQ Monitoring is enabled from Unity Catalog and surfaces
root cause links back to upstream jobs and pipelines.

Steps:
1. Go to **Catalog** → select your catalog (default: `patchit`) → schema (`dq`).
2. Click **Data Quality** (or **Monitor**) → **Enable monitoring**.
3. Choose anomaly detection at the schema level.
4. Wait for the first scan. You should see table health cards and unhealthy tables.
5. Click an unhealthy table to view **Root cause** and jump to its upstream job.

Reference: https://www.databricks.com/blog/data-quality-monitoring-scale-agentic-ai

## 5) PATCHIT demo flow
1. Ensure PATCHIT is running and ingesting Databricks logs.
2. Trigger the failed job again and show the PATCHIT UI:
   - RCA report generated
   - Proposed fix (PR or patch)
   - Apply fix
3. Re-run the job with `fail_mode` set to `none` or empty in `resources/jobs.yml`.
4. Confirm the run succeeds and the table health improves.

## Failure scenarios & expected fixes
- **Retail orders**: `order_ts` missing → add fallback for `order_time` or fill `order_ts`.
- **Payments recon**: duplicate `txn_id` → dedupe before publish.
- **Inventory**: negative `on_hand` → filter or clamp invalid values.
- **Customer 360**: missing `email` column → fallback to `contact_email`.
- **Clickstream**: invalid timestamps → multi-format parsing with safe fallback.
- **Finance close**: missing FX rate → default to latest FX or drop invalid currency.

## Make jobs healthy
Edit `resources/jobs.yml` and set `fail_mode` to empty or `none` for the pipeline,
then run the job again.
