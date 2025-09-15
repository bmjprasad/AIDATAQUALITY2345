Runbook: AI-Assisted DQ for Delta on Azure Databricks (Llama)

Purpose
Step-by-step to run the AI DQ demo using Databricks Foundation Models.

1) Prerequisites
- Azure Databricks workspace with Unity Catalog enabled
- Access to a Databricks Model Serving endpoint for Llama
- Cluster: DBR ML (includes MLflow, scikit-learn) or add scikit-learn to cluster libs

2) Create or choose a UC location
- Decide on CATALOG and SCHEMA (e.g., `main.ai_dq_demo`)
- In SQL: `CREATE SCHEMA IF NOT EXISTS main.ai_dq_demo;`

3) Import the notebook script
- Path in repo: `notebooks/ai_dq_llama_databricks.py`
- In Databricks Workspace: Import the .py as a notebook or run via Repos

4) Configure the notebook
- Set `CATALOG`, `SCHEMA`, and `ENDPOINT` at the top
- Optional: Replace the sample table with your own table path

5) Execute
- Run all cells. The notebook will:
  - Create `ai_dq_sales` if missing (demo)
  - Profile metadata and top values
  - Call the Llama endpoint to generate JSON rules
  - Apply rules and write results to `ai_dq_results` and `ai_dq_violations`
  - Optionally train IsolationForest and display top anomalies

6) Review results
- In SQL:
  - `SELECT * FROM <catalog>.<schema>.ai_dq_results ORDER BY ts_epoch DESC;`
  - `SELECT * FROM <catalog>.<schema>.ai_dq_violations ORDER BY __run_id DESC LIMIT 100;`
- In Lakeview/DBSQL: build a simple dashboard from these tables

7) Streaming variant (optional)
- Use the generated rules to route bad rows to a quarantine table with Structured Streaming

8) Scheduling and alerts
- Create a Databricks Job to run daily/hourly
- Add a SQL alert: notify when any error-rule pass_rate < threshold

9) Troubleshooting
- LLM returns non-JSON: ensure low temperature and JSON-only instruction; we strip code fences and validate JSON
- Permission denied: ensure UC and Model Serving permissions for your user/cluster
- No numeric columns: anomaly step will be skipped; this is expected
- Large tables: profiling uses approximate quantiles and top-k; adjust limits as needed

10) Clean-up
- Drop demo tables when done:
  - `DROP TABLE IF EXISTS <catalog>.<schema>.ai_dq_sales;`
  - `DROP TABLE IF EXISTS <catalog>.<schema>.ai_dq_results;`
  - `DROP TABLE IF EXISTS <catalog>.<schema>.ai_dq_violations;`

