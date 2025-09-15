from pyspark.sql import functions as F, types as T
import json, time, uuid
import mlflow
from mlflow.deployments import get_deploy_client

# ----- CONFIG (Unity Catalog + Endpoint) -----
CATALOG = "main"
SCHEMA = "ai_dq_demo"
BASE = f"{CATALOG}.{SCHEMA}"
TABLE_NAME = f"{BASE}.ai_dq_sales"         # change to your Delta table if needed
RESULTS_TABLE = f"{BASE}.ai_dq_results"
VIOLATIONS_TABLE = f"{BASE}.ai_dq_violations"

# Databricks Foundation Model endpoint (pick one available in your workspace):
# "databricks-llama-3-8b-instruct" (fast/cost-effective)
# "databricks-llama-3-70b-instruct" (higher quality)
ENDPOINT = "databricks-llama-3-8b-instruct"

# ----- SETUP (UC schema + demo data if needed) -----
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
if not spark._jsparkSession.catalog().tableExists(CATALOG, SCHEMA, "ai_dq_sales"):
    demo_df = spark.createDataFrame(
        [
            (1, "A", 23.0, "2024-01-02"),
            (2, "B", 41.5, "2024-02-10"),
            (3, "A", None,  "2024-03-22"),
            (4, "C", 17.2,  "2023-12-31"),
            (5, None,  95.0,"2024-05-01"),
        ],
        schema="id INT, status STRING, amount DOUBLE, event_date STRING"
    ).withColumn("event_date", F.to_date("event_date"))
    demo_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

# ----- METADATA + PROFILING -----
def describe_schema_and_comments(table_name: str):
    rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").select("col_name","data_type","comment").collect()
    cols = []
    for r in rows:
        col = r["col_name"]
        if col and not col.startswith("#"):
            cols.append({"name": col, "type": r["data_type"], "comment": r["comment"]})
    return cols

def profile_table(df, quantiles=(0.01, 0.5, 0.99)):
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.ShortType, T.DecimalType))]
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    date_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (T.DateType, T.TimestampType))]

    agg_exprs = []
    for c in df.columns:
        agg_exprs += [
            F.count(F.col(c)).alias(f"{c}__count"),
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}__nulls"),
            F.approx_count_distinct(c).alias(f"{c}__distinct"),
        ]
    for c in numeric_cols:
        agg_exprs += [F.min(c).alias(f"{c}__min"), F.max(c).alias(f"{c}__max"), F.mean(c).alias(f"{c}__mean")]
    for c in date_cols:
        agg_exprs += [F.min(c).alias(f"{c}__min"), F.max(c).alias(f"{c}__max")]

    agg = df.agg(*agg_exprs).first().asDict()

    qmap = {}
    for c in numeric_cols:
        qs = df.approxQuantile(c, list(quantiles), 0.01)
        qmap[c] = {str(q): v for q, v in zip(quantiles, qs)}

    samples = {}
    for c in string_cols:
        vals = (df.select(c).where(F.col(c).isNotNull())
                  .groupBy(c).count().orderBy(F.desc("count")).limit(10).collect())
        samples[c] = [r[c] for r in vals]

    return {
        "row_count": df.count(),
        "numeric_quantiles": qmap,
        "column_stats": agg,
        "string_top_values": samples,
    }

# ----- DATABRICKS LLM RULE GENERATOR -----
def _strip_code_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = t.split("```", 2)[1] if "```" in t[3:] else t
    return t.strip()

def generate_rules_with_databricks_llm(meta: dict, endpoint: str = ENDPOINT):
    client = get_deploy_client("databricks")
    instruction = (
        "Generate concise SQL boolean expressions for data quality checks on a Delta table. "
        "Use only portable Spark SQL syntax. Prefer rules: NOT NULL, range checks using quantiles, "
        "allowed categories from top values, and reasonable date windows. "
        "Return ONLY a JSON list of objects: {name, expr, severity in ['error','warn'], rationale}."
    )
    inputs = {
        "messages": [
            {"role":"system", "content": instruction},
            {"role":"user", "content": json.dumps(meta)}
        ],
        "temperature": 0.1,
        "max_tokens": 800,
    }
    res = client.predict(endpoint=endpoint, inputs=inputs)
    raw = res["choices"][0]["message"]["content"]
    raw = _strip_code_fences(raw)
    rules = []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and "rules" in parsed:
            parsed = parsed["rules"]
        for i, r in enumerate(parsed or []):
            name = r.get("name") or f"rule_{i}"
            expr = r.get("expr")
            sev = (r.get("severity") or "error").lower()
            if expr:
                rules.append({"name": name, "expr": expr, "severity": "error" if sev not in ("warn","error") else sev})
    except Exception:
        rules = []
    return rules

# ----- APPLY RULES + PERSIST -----
def apply_rules_and_report(df, table_name: str, rules, run_id: str):
    if not rules:
        print("No rules to apply.")
        return
    with_flags = df
    for r in rules:
        with_flags = with_flags.withColumn(f"dq__{r['name']}", F.expr(r["expr"]).cast("boolean"))

    metrics_exprs = []
    for r in rules:
        c = f"dq__{r['name']}"
        metrics_exprs += [
            F.sum(F.when(F.col(c) == True, 1).otherwise(0)).alias(f"{r['name']}__passed"),
            F.sum(F.when(F.col(c) == False, 1).otherwise(0)).alias(f"{r['name']}__failed"),
        ]
    agg_row = with_flags.agg(*metrics_exprs).first().asDict()

    now = int(time.time())
    rows = []
    for r in rules:
        passed = int(agg_row.get(f"{r['name']}__passed", 0) or 0)
        failed = int(agg_row.get(f"{r['name']}__failed", 0) or 0)
        total = passed + failed
        pass_rate = (float(passed) / total) if total > 0 else 1.0
        rows.append((run_id, table_name, r["name"], r["expr"], r["severity"], passed, failed, pass_rate, now))
    results_df = spark.createDataFrame(rows, schema="""
        run_id STRING, table_name STRING, rule_name STRING, rule_expr STRING, severity STRING,
        passed BIGINT, failed BIGINT, pass_rate DOUBLE, ts_epoch BIGINT
    """)
    results_df.write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)

    error_rules = [r for r in rules if r["severity"] == "error"]
    if error_rules:
        any_fail = None
        for r in error_rules:
            colname = f"dq__{r['name']}"
            fail = F.when(F.col(colname) == False, F.lit(r["name"]))
            any_fail = fail if any_fail is None else F.coalesce(any_fail, fail)
        viol = (with_flags.where(any_fail.isNotNull())
                .withColumn("__violated_rule", any_fail)
                .withColumn("__run_id", F.lit(run_id))
                .withColumn("__table_name", F.lit(table_name)))
        (viol.select("__run_id","__table_name","__violated_rule", *df.columns)
             .write.format("delta").mode("append").saveAsTable(VIOLATIONS_TABLE))

# ----- OPTIONAL: SIMPLE ANOMALY DETECTION -----
def ensure_anomaly_udf(df, run_name="iforest_demo"):
    numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.ShortType, T.DecimalType))]
    if not numeric_cols:
        return None, []
    pdf = df.select(*numeric_cols).dropna().limit(5000).toPandas()
    if pdf.empty:
        return None, []
    from sklearn.ensemble import IsolationForest
    with mlflow.start_run(run_name=run_name):
        model = IsolationForest(n_estimators=100, contamination=0.02, random_state=13).fit(pdf)
        mlflow.sklearn.log_model(model, artifact_path="model")
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
    udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="double")
    return udf, numeric_cols

# ----- RUN END-TO-END -----
def run_ai_dq(table_name: str):
    run_id = str(uuid.uuid4())
    df = spark.table(table_name)

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first().asDict()
    cols = describe_schema_and_comments(table_name)
    profile = profile_table(df)
    meta = {"detail": detail, "columns": cols, "profile": profile}

    rules = generate_rules_with_databricks_llm(meta, endpoint=ENDPOINT)
    if not rules:
        baseline = []
        if any(c["name"] == "id" for c in cols):
            baseline.append({"name":"id_not_null","expr":"id IS NOT NULL","severity":"error"})
        if any(c["name"] == "amount" for c in cols):
            q = profile["numeric_quantiles"].get("amount", {})
            p01, p99 = q.get("0.01", 0), q.get("0.99", 1e9)
            baseline.append({"name":"amount_reasonable","expr":f"amount BETWEEN {p01} AND {p99}","severity":"warn"})
        rules = baseline

    apply_rules_and_report(df, table_name, rules, run_id)

    anomaly_udf, num_cols = ensure_anomaly_udf(df)
    if anomaly_udf:
        scored = df.select("*", anomaly_udf(F.array(*[F.col(c).cast("double") for c in num_cols])).alias("anomaly_score"))
        scored.orderBy(F.desc("anomaly_score")).limit(20).display()

    print(f"AI DQ run complete. run_id={run_id}")
    print("Rule metrics preview:")
    spark.table(RESULTS_TABLE).orderBy(F.desc("ts_epoch")).show(50, truncate=False)
    print("Sample violations preview:")
    spark.table(VIOLATIONS_TABLE).orderBy(F.desc("__run_id")).show(20, truncate=False)


if __name__ == "__main__":
    run_ai_dq(TABLE_NAME)

