AI-Assisted Data Quality for Delta on Azure Databricks (Databricks Llama)

Overview
This project auto-generates and applies data quality (DQ) rules for Delta tables on Azure Databricks using Databricks Foundation Models (Llama) and PySpark. It profiles table metadata, asks a served LLM to return compact SQL rules in JSON, executes them at Spark scale, and writes metrics and violations back to Unity Catalog tables.

Key features
- AI-assisted rule generation from schema, comments, quantiles, and top values
- Spark-scale rule execution with pass/fail metrics and violation rows
- Results persisted to Unity Catalog Delta tables for governance and BI
- Optional anomaly detection using MLflow and IsolationForest

Repo layout
- docs/Slides_Notes.md: PPT-ready bullets for a hackathon demo
- docs/Runbook.md: Step-by-step setup and execution guide
- notebooks/ai_dq_llama_databricks.py: Databricks notebook script (importable)

Prerequisites
- Azure Databricks workspace with Unity Catalog enabled
- Access to Databricks Foundation Model Serving (e.g., databricks-llama-3-8b-instruct)
- Cluster runtime with MLflow and scikit-learn (DBR ML runtimes work best)

Quickstart
1) Choose or deploy a Databricks Model Serving endpoint for Llama (e.g., "databricks-llama-3-8b-instruct"). Ensure you have permission to query it.
2) In Databricks, create a schema to hold demo tables, e.g., `main.ai_dq_demo`.
3) Import `notebooks/ai_dq_llama_databricks.py` into your workspace (Repos or Workspace import) and open it.
4) At the top of the notebook, set:
   - CATALOG and SCHEMA (e.g., main / ai_dq_demo)
   - ENDPOINT to your chosen Llama endpoint
5) Run the notebook. It will:
   - Create a small demo Delta table if missing (`ai_dq_sales`)
   - Profile metadata and prompt the model for JSON rules
   - Apply rules and write results to `ai_dq_results` and `ai_dq_violations`
   - Optionally train a tiny IsolationForest and display top outliers
6) Explore results in SQL or Lakeview/DBSQL dashboards.

Core tables (Unity Catalog)
- <catalog>.<schema>.ai_dq_sales: Sample input table (replace with your own)
- <catalog>.<schema>.ai_dq_results: Aggregated pass/fail metrics per rule per run
- <catalog>.<schema>.ai_dq_violations: Row-level failures for severity=error rules

Notes and tips
- Keep LLM temperature low (0.1) and ask for JSON-only responses.
- Prefer small models for hackathons to reduce latency/cost.
- Start with severity=warn; promote to error after review.
- Use Jobs to schedule and alert when error rules fail beyond thresholds.

License
MIT

