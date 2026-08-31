# 02_silver  (PIPELINE TRANSFORMATION FILE)
import dlt
from pyspark.sql.functions import col, trim, upper, lower, regexp_replace, round as spark_round

catalog_name = spark.conf.get("conf.catalog_name")
silver_schema_name = spark.conf.get("conf.silver_schema_name")
bronze_schema_name = spark.conf.get("conf.bronze_schema_name")
B = {n: f"{n}_raw" for n in
     ["members","claims","providers","diagnoses","procedures"]}
S = lambda n: f"{catalog_name}.{silver_schema_name}.{n}"

@dlt.table(name=S("members"))
def s_members():
    return (dlt.read_stream(B["members"])
            .filter(col("member_id").isNotNull())
            .selectExpr(
                "cast(member_id as string) as member_id",
                "trim(first_name) as first_name",
                "trim(last_name) as last_name",
                "cast(birth_date as date) as birth_date",
                "gender", "plan_id",
                "cast(effective_date as date) as effective_date")
            .distinct())

@dlt.table(name=S("claims"))
def s_claims():
    return (dlt.read_stream(B["claims"])
            .filter((col("claim_id").isNotNull()) & (col("total_charge") > 0))
            .select("claim_id","member_id","provider_id",
                    col("claim_date").cast("date").alias("claim_date"),
                    spark_round("total_charge",2).alias("total_charge"),
                    lower(col("claim_status")).alias("claim_status"))
            .distinct())

@dlt.table(name=S("providers"))
def s_providers():
    return (dlt.read_stream(B["providers"])
            .filter(col("provider_id").isNotNull())
            .select("provider_id","npi","provider_name","specialty",
                    "address","city","state").distinct())

@dlt.table(name=S("diagnoses"))
def s_diagnoses():
    return (dlt.read_stream(B["diagnoses"])
            .filter(col("claim_id").isNotNull() & col("diagnosis_code").isNotNull())
            .select("claim_id",
                    upper(col("diagnosis_code")).alias("diagnosis_code"),
                    trim(col("diagnosis_desc")).alias("diagnosis_desc"))
            .distinct())

@dlt.table(name=S("procedures"))
def s_procedures():
    return (dlt.read_stream(B["procedures"])
            .filter(col("claim_id").isNotNull() & col("procedure_code").isNotNull())
            .select("claim_id",
                    upper(col("procedure_code")).alias("procedure_code"),
                    "procedure_desc",
                    regexp_replace(col("amount"), r"\$", "").cast("DOUBLE").alias("amount"))
            .distinct())
