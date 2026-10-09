# Databricks notebook source
# 05_semantic_layer  (NOTEBOOK; runs after the pipeline in the payer_refresh job)
# Rebuilds the metric view and trusted function in whichever gold schema the target uses.
dbutils.widgets.text("catalog", "payer_demo")
dbutils.widgets.text("gold_schema", "payor_gold")
G = f'{dbutils.widgets.get("catalog")}.{dbutils.widgets.get("gold_schema")}'

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.claims_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Governed claims KPIs at claim grain. Source of truth for claim counts,
  billed charges and denial rate. Built on fact_claims, never claims_enriched.
source: {G}.fact_claims
joins:
  - name: member
    source: {G}.dim_member
    'on': source.member_id = member.member_id
  - name: provider
    source: {G}.dim_provider
    'on': source.provider_id = provider.provider_id
dimensions:
  - name: claim_date
    expr: claim_date
    display_name: Claim Date
  - name: claim_month
    expr: "DATE_TRUNC('MONTH', claim_date)"
    display_name: Claim Month
  - name: claim_status
    expr: claim_status
    display_name: Claim Status
    synonyms: [status, adjudication status]
  - name: plan_id
    expr: member.plan_id
    display_name: Plan
    synonyms: [health plan, product]
  - name: member_gender
    expr: member.gender
    display_name: Member Gender
  - name: provider_specialty
    expr: provider.specialty
    display_name: Provider Specialty
    synonyms: [specialty]
  - name: provider_state
    expr: provider.state
    display_name: Provider State
  - name: provider_name
    expr: provider.provider_name
    display_name: Provider Name
measures:
  - name: claim_count
    expr: COUNT(DISTINCT claim_id)
    display_name: Claim Count
    synonyms: [number of claims, claim volume]
  - name: total_charges
    expr: SUM(total_charge)
    display_name: Total Charges
    synonyms: [billed amount, charges, spend]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
  - name: avg_charge_per_claim
    expr: MEASURE(total_charges) / MEASURE(claim_count)
    display_name: Average Charge per Claim
  - name: members_with_claims
    expr: COUNT(DISTINCT member_id)
    display_name: Members with Claims
  - name: charges_per_member
    expr: MEASURE(total_charges) / MEASURE(members_with_claims)
    display_name: Charges per Member
  - name: denied_claims
    expr: COUNT(DISTINCT claim_id) FILTER (WHERE claim_status = 'denied')
    display_name: Denied Claims
  - name: denial_rate
    expr: MEASURE(denied_claims) / MEASURE(claim_count)
    display_name: Denial Rate
    synonyms: [rejection rate, deny rate]
$$
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {G}.top_providers_by_denial_rate(min_claims INT)
RETURNS TABLE (provider_id STRING, provider_name STRING, specialty STRING,
               claims BIGINT, denied BIGINT, denial_rate DOUBLE)
COMMENT 'Providers ranked by denial rate, limited to providers with at least min_claims claims. Use for questions about which providers have the most denials or the highest denial rate.'
RETURN
  SELECT p.provider_id, p.provider_name, p.specialty,
         COUNT(*) AS claims,
         COUNT_IF(f.claim_status = 'denied') AS denied,
         ROUND(COUNT_IF(f.claim_status = 'denied') / COUNT(*), 4) AS denial_rate
  FROM {G}.fact_claims f
  JOIN {G}.dim_provider p ON f.provider_id = p.provider_id
  GROUP BY p.provider_id, p.provider_name, p.specialty
  HAVING COUNT(*) >= min_claims
  ORDER BY denial_rate DESC
""")

print(f"Semantic layer rebuilt in {G}")
