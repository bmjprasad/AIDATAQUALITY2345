AI-Assisted Data Quality for Delta on Azure Databricks

1. Problem & Motivation
- Manual DQ rules don’t scale across many Delta tables
- Context is buried in schema, comments, logs, and profiling data
- Data teams need fast, explainable checks with governance in Unity Catalog

2. Solution Overview
- Use Unity Catalog metadata + lightweight profiles to prompt a Databricks Llama model
- LLM returns compact SQL boolean rules (JSON)
- Spark applies rules at scale; results and violations written back to UC
- Optional anomaly scoring with MLflow IsolationForest

3. Architecture
- Delta table: source data in UC
- Metadata/profile: DESCRIBE DETAIL/EXTENDED, quantiles, top values
- Databricks Foundation Model (Llama) for rule generation
- Rule executor: PySpark applies SQL expressions
- Results: `<catalog>.<schema>.ai_dq_results` and `<catalog>.<schema>.ai_dq_violations`

4. Demo Flow (what you’ll show)
- Open notebook `ai_dq_llama_databricks.py`
- Configure CATALOG/SCHEMA and Llama endpoint
- Run: it builds demo table (if missing), profiles, calls LLM, applies rules
- Show generated rules JSON, metrics table, and sample violations
- (Optional) Show anomaly top-20 with IsolationForest

5. Example Rules the LLM Generates
- NOT NULL: `id IS NOT NULL`
- Allowed set: `status IN ('A','B','C')`
- Range via quantiles: `amount BETWEEN p01 AND p99`
- Date recency: `event_date >= date_sub(current_date(), 365)`

6. Metrics to Highlight
- Pass rate per rule
- Count of failed rows per rule
- Trend over time by `ts_epoch` (rerun to simulate)

7. Governance & Ops
- Tables live in Unity Catalog; secure with UC permissions
- Schedule with Databricks Jobs; alert if any error rule pass_rate < threshold
- Keep a curated, approved rule set per table (optional)

8. Extensibility Ideas
- Add regex/PII checks, cross-column constraints, temporal drift
- Stream variant: quarantine failing rows to `<schema>_quarantine`
- Swap in other models/endpoints or pre-approved rule libraries

9. Why This Works for Hackathons
- Small, deterministic prompt; quick profiling; minimal dependencies
- One notebook, a few Delta tables, live results in Lakeview/DBSQL

10. Call to Action
- Point at your own table in UC, run the notebook, dashboard results

