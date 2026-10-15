# Databricks notebook source
# 07_claim_denial_model  (NOTEBOOK; Step 6, run on demand, not part of payer_refresh)
# Trains a denial classifier, registers it in Unity Catalog as @champion,
# and writes per-claim risk scores to gold.
%pip install -q "mlflow>=3.1" scikit-learn
dbutils.library.restartPython()

# COMMAND ----------

import json
import mlflow, pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

dbutils.widgets.text("catalog", "payer_demo")
dbutils.widgets.text("gold_schema", "payor_gold")
CATALOG, GOLD = dbutils.widgets.get("catalog"), dbutils.widgets.get("gold_schema")
MODEL_NAME = f"{CATALOG}.{GOLD}.claim_denial_model"

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")   # register models in Unity Catalog
mlflow.set_experiment(f"/Users/{user}/payer_claim_denial")

# COMMAND ----------

# A. Training data: only fields known before adjudication (no leakage)
df = spark.sql(f"""
  SELECT f.claim_id, f.total_charge, f.diagnosis_count,
         MONTH(f.claim_date) AS claim_month, DAYOFWEEK(f.claim_date) AS claim_dow,
         m.gender, m.plan_id, p.specialty, p.state,
         CASE WHEN f.claim_status = 'denied' THEN 1 ELSE 0 END AS is_denied
  FROM {CATALOG}.{GOLD}.fact_claims f
  LEFT JOIN {CATALOG}.{GOLD}.dim_member   m USING (member_id)
  LEFT JOIN {CATALOG}.{GOLD}.dim_provider p USING (provider_id)
""").toPandas()

NUM = ["total_charge", "diagnosis_count", "claim_month", "claim_dow"]
CAT = ["gender", "plan_id", "specialty", "state"]

def features(frame):
    X = frame[NUM + CAT].copy()
    X[NUM] = X[NUM].fillna(0)
    X[CAT] = X[CAT].fillna("UNKNOWN")
    return X

denial_share = round(float(df.is_denied.mean()), 3)
print(len(df), "claims; denial share =", denial_share)
assert 0 < denial_share < 1, "is_denied has one class only; check the denied status value"

# COMMAND ----------

# B. Train, track and register
X, y = features(df), df["is_denied"]
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42)

pipe = Pipeline([
    ("prep", ColumnTransformer([
        ("num", StandardScaler(), NUM),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CAT)])),
    ("clf", LogisticRegression(max_iter=1000, class_weight="balanced"))])

with mlflow.start_run(run_name="logreg_baseline") as run:
    pipe.fit(X_train, y_train)
    auc = roc_auc_score(y_test, pipe.predict_proba(X_test)[:, 1])
    mlflow.log_params({"model": "logistic_regression", "class_weight": "balanced"})
    mlflow.log_metric("test_auc", auc)
    mlflow.sklearn.log_model(
        pipe, name="model",
        signature=infer_signature(X_train, pipe.predict(X_train)),
        input_example=X_train.head(5),
        registered_model_name=MODEL_NAME)
print(f"Test AUC: {auc:.3f}")

# COMMAND ----------

# C. Promote the newest version with the champion alias
client = MlflowClient()
latest = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'"))
client.set_registered_model_alias(MODEL_NAME, "champion", latest)
print(f"{MODEL_NAME} version {latest} is now @champion")

# COMMAND ----------

# D. Batch-score every claim with @champion and publish to gold
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")
df["denial_risk"] = model.predict_proba(features(df))[:, 1]

(spark.createDataFrame(df[["claim_id", "denial_risk"]])
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(f"{CATALOG}.{GOLD}.claim_denial_scores"))
spark.sql(f"""COMMENT ON TABLE {CATALOG}.{GOLD}.claim_denial_scores IS
  'Predicted probability (0 to 1) that a claim is denied, from claim_denial_model@champion. One row per claim; join to fact_claims on claim_id.'""")

high_risk = int((df.denial_risk > 0.7).sum())
dbutils.notebook.exit(json.dumps({
    "claims": len(df), "denial_share": denial_share, "test_auc": round(auc, 3),
    "champion_version": latest, "high_risk_claims": high_risk}))
