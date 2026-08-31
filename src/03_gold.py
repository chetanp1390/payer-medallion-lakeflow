# 03_gold  (PIPELINE TRANSFORMATION FILE)
import dlt
from pyspark.sql.functions import countDistinct, sum as spark_sum, max as spark_max, min as spark_min

catalog_name = spark.conf.get("conf.catalog_name")
silver_schema_name = spark.conf.get("conf.silver_schema_name")
gold_schema_name = spark.conf.get("conf.gold_schema_name")
S = {n: f"{catalog_name}.{silver_schema_name}.{n}" for n in
     ["members","claims","providers","diagnoses","procedures"]}
G = lambda n: f"{catalog_name}.{gold_schema_name}.{n}"

@dlt.table(name=G("claims_enriched"))
def claims_enriched():
    claims    = dlt.read(S["claims"])
    members   = dlt.read(S["members"])
    providers = dlt.read(S["providers"])
    diagnoses = dlt.read(S["diagnoses"])   # NOTE: fans out on multi-diagnosis claims; fixed in Phase 3.3
    return (claims
            .join(members, "member_id", "left")
            .join(providers, "provider_id", "left")
            .join(diagnoses, "claim_id", "left")
            .select("claim_id","claim_date","total_charge","claim_status",
                    "member_id","first_name","last_name","gender","plan_id",
                    "provider_id","provider_name","specialty","city","state",
                    "diagnosis_code","diagnosis_desc"))

@dlt.table(name=G("member_claim_summary"))
def member_claim_summary():
    return (dlt.read(S["claims"]).groupBy("member_id")
            .agg(countDistinct("claim_id").alias("total_claims"),
                 spark_sum("total_charge").alias("sum_claims"),
                 spark_max("total_charge").alias("max_claim"),
                 spark_min("total_charge").alias("min_claim")))

@dlt.table(name=G("diagnosis_frequency"))
def diagnosis_frequency():
    return (dlt.read(S["diagnoses"]).groupBy("diagnosis_code")
            .agg(countDistinct("claim_id").alias("claim_count")))

@dlt.table(name=G("procedure_agg"))
def procedure_agg():
    return (dlt.read(S["procedures"]).groupBy("procedure_code")
            .agg(countDistinct("claim_id").alias("claim_count"),
                 spark_sum("amount").alias("total_amount")))
