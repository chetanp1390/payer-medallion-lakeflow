# 01_bronze  (PIPELINE TRANSFORMATION FILE)
import dlt

catalog_name = spark.conf.get("conf.catalog_name")
bronze_schema_name = spark.conf.get("conf.bronze_schema_name")

BASE = f"/Volumes/{catalog_name}/{bronze_schema_name}/payor/files"
PATHS = {
    "members":    f"{BASE}/members/",
    "claims":     f"{BASE}/claims/",
    "providers":  f"{BASE}/providers/",
    "diagnoses":  f"{BASE}/diagnosis/",
    "procedures": f"{BASE}/procedures/",
}
SCHEMAS = {
    "members":    "member_id INTEGER, first_name STRING, last_name STRING, birth_date DATE, gender STRING, plan_id STRING, effective_date DATE",
    "claims":     "claim_id STRING, member_id STRING, provider_id STRING, claim_date DATE, total_charge DOUBLE, claim_status STRING",
    "providers":  "provider_id STRING, npi STRING, provider_name STRING, specialty STRING, address STRING, city STRING, state STRING",
    "diagnoses":  "claim_id STRING, diagnosis_code STRING, diagnosis_desc STRING",
    "procedures": "claim_id STRING, procedure_code STRING, procedure_desc STRING, amount STRING",
}

def bronze(name):
    @dlt.table(name=f"{name}_raw")
    def _reader():
        return (spark.readStream
                .format("csv")
                .option("header", True)
                .schema(SCHEMAS[name])
                .load(PATHS[name]))
    return _reader

for entity in PATHS:
    bronze(entity)
