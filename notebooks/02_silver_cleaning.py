# Databricks notebook source
bronze_df = spark.table("workspace.default.bronze_vf_ghana_raw")
display(bronze_df.limit(5))
print("Rows:", bronze_df.count())
print(bronze_df.columns)

# COMMAND ----------

import json
import re
import ast

from pyspark.sql.functions import col, udf, trim, lower
from pyspark.sql.types import ArrayType, StringType, DoubleType

NULL_LIKE = {"", "null", "none", "nan", "[]", "{}", '""', "n/a", "na"}

def normalize_scalar(value):
    if value is None:
        return None
    s = str(value).strip()
    s = s.strip('"').strip("'").strip()
    if s.lower() in NULL_LIKE:
        return None
    return s

def parse_listish(value):
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in NULL_LIKE:
        return []

    # normalize weird doubled quotes
    s2 = s.replace('""', '"').strip()

    # try JSON
    try:
        parsed = json.loads(s2)
        if isinstance(parsed, list):
            out = []
            for item in parsed:
                item = normalize_scalar(item)
                if item:
                    out.append(item)
            return out
    except Exception:
        pass

    # try Python literal
    try:
        parsed = ast.literal_eval(s2)
        if isinstance(parsed, list):
            out = []
            for item in parsed:
                item = normalize_scalar(item)
                if item:
                    out.append(item)
            return out
    except Exception:
        pass

    # fallback: split on semicolon first, then comma only if not bracketed
    raw = s2.strip("[]")
    parts = re.split(r";|\|", raw)
    if len(parts) == 1:
        parts = [raw]

    out = []
    for p in parts:
        p = normalize_scalar(p)
        if p:
            out.append(p)
    return out

def parse_number(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in NULL_LIKE:
        return None
    m = re.search(r"-?\d+(\.\d+)?", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

normalize_scalar_udf = udf(normalize_scalar, StringType())
parse_listish_udf = udf(parse_listish, ArrayType(StringType()))
parse_number_udf = udf(parse_number, DoubleType())

# COMMAND ----------

silver_df = bronze_df

for c in silver_df.columns:
    silver_df = silver_df.withColumn(c, normalize_scalar_udf(trim(col(c))))

# COMMAND ----------

silver_df = (
    silver_df
    .withColumn("region", normalize_scalar_udf(col("address_stateorregion")))
    .withColumn("city", normalize_scalar_udf(col("address_city")))
    .withColumn("country", normalize_scalar_udf(col("address_country")))
    .withColumn("facility_type_id", normalize_scalar_udf(col("facilitytypeid")))
    .withColumn("operator_type_id", normalize_scalar_udf(col("operatortypeid")))
    .withColumn("doctor_count", parse_number_udf(col("numberdoctors")))
    .withColumn("capacity_num", parse_number_udf(col("capacity")))
    .withColumn("area_num", parse_number_udf(col("area")))
)

# COMMAND ----------

list_cols = ["specialties", "procedure", "equipment", "capability", "phone_numbers", "websites", "affiliationtypeids", "countries"]

for c in list_cols:
    if c in silver_df.columns:
        silver_df = silver_df.withColumn(c, parse_listish_udf(col(c)))

# COMMAND ----------

from pyspark.sql.functions import size, when, lit

silver_df = (
    silver_df
    .withColumn("num_specialties", size(col("specialties")))
    .withColumn("num_procedures", size(col("procedure")))
    .withColumn("num_equipment", size(col("equipment")))
    .withColumn("num_capabilities", size(col("capability")))
    .withColumn("has_specialty", when(size(col("specialties")) > 0, lit(True)).otherwise(lit(False)))
    .withColumn("has_procedure", when(size(col("procedure")) > 0, lit(True)).otherwise(lit(False)))
    .withColumn("has_equipment", when(size(col("equipment")) > 0, lit(True)).otherwise(lit(False)))
    .withColumn("has_capability", when(size(col("capability")) > 0, lit(True)).otherwise(lit(False)))
)

# COMMAND ----------

display(
    silver_df.select(
        "name",
        "region",
        "city",
        "country",
        "facility_type_id",
        "operator_type_id",
        "specialties",
        "procedure",
        "equipment",
        "capability",
        "doctor_count",
        "capacity_num",
        "area_num"
    ).limit(20)
)

# COMMAND ----------

silver_table = "workspace.default.silver_vf_ghana"

silver_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_table)
print("Saved:", silver_table)

# COMMAND ----------

