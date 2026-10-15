# Databricks notebook source
# 00_setup  (NOTEBOOK; first task of the payer_refresh job)
# Values come from job parameters; the defaults let it run on its own too.
for name, default in [("catalog", "payer_demo"), ("bronze_schema", "payor_bronze"),
                      ("silver_schema", "payor_silver"), ("gold_schema", "payor_gold")]:
    dbutils.widgets.text(name, default)

catalog   = dbutils.widgets.get("catalog")
bronze_db = dbutils.widgets.get("bronze_schema")
silver_db = dbutils.widgets.get("silver_schema")
gold_db   = dbutils.widgets.get("gold_schema")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
for schema in (bronze_db, silver_db, gold_db):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_db}.payor")

# COMMAND ----------

import requests, os

RAW = "https://raw.githubusercontent.com/chetanp1390/payer-medallion-lakeflow/main/data/"
base = f"/Volumes/{catalog}/{bronze_db}/payor/files"

# left = filename in your GitHub data/ folder ; right = Volume subfolder bronze reads from
files = {
    "claims.csv":     "claims",
    "diagnosis.csv":  "diagnosis",
    "procedures.csv": "procedures",
    "members.csv":    "members",
    "providers.csv":  "providers",
}

for fname, subfolder in files.items():
    dest_dir = f"{base}/{subfolder}"
    os.makedirs(dest_dir, exist_ok=True)
    r = requests.get(f"{RAW}{fname}")
    r.raise_for_status()
    with open(f"{dest_dir}/{fname}", "wb") as f:
        f.write(r.content)
    print("Loaded", f"{subfolder}/{fname}", f"({len(r.content)} bytes)")
