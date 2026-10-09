# Databricks notebook source
# 06_data_tests  (NOTEBOOK; last task of the payer_refresh job)
# Each failed assert fails the job, which fails CI.
dbutils.widgets.text("catalog", "payer_demo")
dbutils.widgets.text("gold_schema", "payor_gold")
G = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("gold_schema")}'

# COMMAND ----------

# 1. Grain: one row per claim
dupes = spark.sql(f"SELECT COUNT(*) - COUNT(DISTINCT claim_id) AS d FROM {G}.fact_claims").first().d
assert dupes == 0, f"fact_claims grain broken: {dupes} duplicate claim_ids"

# 2. Tie-out: the metric view matches the table
mv, tbl = spark.sql(f"""
  SELECT (SELECT MEASURE(total_charges) FROM {G}.claims_metrics) AS mv,
         (SELECT SUM(total_charge) FROM {G}.fact_claims) AS tbl""").first()
assert abs(mv - tbl) < 0.01, f"metric view does not tie out: {mv} vs {tbl}"

# 3. Volume: the load didn't silently drop data
n = spark.table(f"{G}.fact_claims").count()
assert n >= 10000, f"expected at least 10,000 claims, found {n}"

# 4. Status values: the denial measures depend on lowercase 'denied'
denied = spark.sql(f"SELECT COUNT_IF(claim_status = 'denied') AS d FROM {G}.fact_claims").first().d
assert denied > 0, "no claims with status 'denied'; denial rate would be 0"

print(f"All data tests passed on {G}: {n} claims, {denied} denied")
