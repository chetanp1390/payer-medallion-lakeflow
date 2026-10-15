# 03_gold  (PIPELINE TRANSFORMATION FILE)
import dlt
from pyspark.sql.functions import countDistinct, sum as spark_sum, max as spark_max, min as spark_min
from pyspark.sql import functions as F

catalog_name = spark.conf.get("conf.catalog_name")
silver_schema_name = spark.conf.get("conf.silver_schema_name")
gold_schema_name = spark.conf.get("conf.gold_schema_name")
S = {n: f"{catalog_name}.{silver_schema_name}.{n}" for n in
     ["members","claims","providers","diagnoses","procedures"]}
G = lambda n: f"{catalog_name}.{gold_schema_name}.{n}"

REF = f"{catalog_name}.{gold_schema_name}"

@dlt.table(name=G("claims_enriched"),
           comment="One row per claim per diagnosis. Do not sum total_charge here; use fact_claims.")
def claims_enriched():
    claims    = dlt.read(S["claims"])
    members   = dlt.read(S["members"])
    providers = dlt.read(S["providers"])
    diagnoses = dlt.read(S["diagnoses"])   # NOTE: fans out on multi-diagnosis claims
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


@dlt.table(
    name=G("dim_member"),
    comment="One row per health plan member. Use for member gender and plan. Join to fact_claims on member_id.",
    schema="""
      member_id STRING NOT NULL PRIMARY KEY COMMENT 'Unique member identifier',
      gender    STRING COMMENT 'Member gender as recorded at enrollment',
      plan_id   STRING COMMENT 'Health plan the member is enrolled in'
    """)
def dim_member():
    return (dlt.read(S["members"])
            .select(F.col("member_id").cast("string"),
                    F.col("gender").cast("string"),
                    F.col("plan_id").cast("string"))
            .dropDuplicates(["member_id"]))

@dlt.table(
    name=G("dim_provider"),
    comment="One row per billing provider. Use for specialty and location. Join to fact_claims on provider_id.",
    schema="""
      provider_id   STRING NOT NULL PRIMARY KEY COMMENT 'Unique provider identifier',
      provider_name STRING COMMENT 'Provider or practice name',
      specialty     STRING COMMENT 'Clinical specialty of the provider',
      city          STRING COMMENT 'Provider practice city',
      state         STRING COMMENT 'Provider practice state, two-letter code'
    """)
def dim_provider():
    return (dlt.read(S["providers"])
            .select(*[F.col(c).cast("string") for c in
                      ["provider_id", "provider_name", "specialty", "city", "state"]])
            .dropDuplicates(["provider_id"]))

@dlt.table(
    name=G("fact_claims"),
    comment=("One row per claim (claim grain). Use this table for claim counts, "
             "charges and status. Join dim_member on member_id and dim_provider on provider_id."),
    schema=f"""
      claim_id        STRING NOT NULL PRIMARY KEY COMMENT 'Unique claim identifier',
      claim_date      DATE   COMMENT 'Date of service on the claim',
      total_charge    DOUBLE COMMENT 'Total billed charge for the claim, in USD',
      claim_status    STRING COMMENT 'Adjudication status. Values: denied, paid, pending',
      member_id       STRING COMMENT 'Member who received the service',
      provider_id     STRING COMMENT 'Provider who billed the claim',
      diagnosis_count BIGINT COMMENT 'Number of distinct diagnosis codes on the claim',
      CONSTRAINT fk_claim_member   FOREIGN KEY (member_id)   REFERENCES {REF}.dim_member(member_id),
      CONSTRAINT fk_claim_provider FOREIGN KEY (provider_id) REFERENCES {REF}.dim_provider(provider_id)
    """)
def fact_claims():
    dx = (dlt.read(S["diagnoses"])
          .groupBy(F.col("claim_id").cast("string").alias("claim_id"))
          .agg(F.countDistinct("diagnosis_code").alias("diagnosis_count")))
    return (dlt.read(S["claims"])
            .select(F.col("claim_id").cast("string"),
                    F.col("claim_date").cast("date"),
                    F.col("total_charge").cast("double"),
                    F.col("claim_status").cast("string"),
                    F.col("member_id").cast("string"),
                    F.col("provider_id").cast("string"))
            .dropDuplicates(["claim_id"])
            .join(dx, "claim_id", "left")
            .fillna(0, subset=["diagnosis_count"]))

@dlt.table(
    name=G("claim_diagnosis"),
    comment=("One row per claim and diagnosis code. Use for diagnosis questions; count claims "
             "with COUNT(DISTINCT claim_id). Never sum charges from this table."),
    schema=f"""
      claim_id       STRING NOT NULL COMMENT 'Claim the diagnosis is billed on',
      diagnosis_code STRING NOT NULL COMMENT 'Diagnosis code on the claim',
      diagnosis_desc STRING COMMENT 'Plain-English description of the diagnosis code',
      CONSTRAINT pk_claim_dx PRIMARY KEY (claim_id, diagnosis_code),
      CONSTRAINT fk_dx_claim FOREIGN KEY (claim_id) REFERENCES {REF}.fact_claims(claim_id)
    """)
def claim_diagnosis():
    return (dlt.read(S["diagnoses"])
            .select(*[F.col(c).cast("string") for c in
                      ["claim_id", "diagnosis_code", "diagnosis_desc"]])
            .dropna(subset=["claim_id", "diagnosis_code"])
            .dropDuplicates(["claim_id", "diagnosis_code"]))