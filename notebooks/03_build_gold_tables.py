# Databricks notebook source
silver_df = spark.table("workspace.default.silver_vf_ghana")
display(silver_df.limit(5))
print("Rows:", silver_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, size, lit, when

gold_facility_master = (
    silver_df
    .withColumn("support_score", size(col("equipment")) + size(col("capability")))
    .withColumn("evidence_count", size(col("specialties")) + size(col("procedure")) + size(col("equipment")) + size(col("capability")))
    .withColumn(
        "anomaly_score",
        (
            when((size(col("procedure")) > 0) & (size(col("equipment")) == 0) & (size(col("capability")) == 0), lit(2.0)).otherwise(lit(0.0))
            +
            when((size(col("specialties")) + size(col("procedure"))) >= 6, lit(1.0)).otherwise(lit(0.0))
            +
            when(col("doctor_count").isNull() & col("capacity_num").isNull(), lit(1.0)).otherwise(lit(0.0))
        )
    )
    .select(
        col("unique_id"),
        col("name"),
        col("facility_type_id"),
        col("operator_type_id"),
        col("region"),
        col("city"),
        col("country"),
        col("specialties"),
        col("procedure"),
        col("equipment"),
        col("capability"),
        col("description"),
        col("doctor_count"),
        col("capacity_num").alias("capacity"),
        col("area_num").alias("area"),
        col("support_score"),
        col("anomaly_score"),
        col("evidence_count")
    )
)

display(gold_facility_master.limit(20))

# COMMAND ----------

gold_facility_master.withColumn("doctor_count", col("doctor_count").cast("string")).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    "workspace.default.gold_facility_master"
)
print("Saved: workspace.default.gold_facility_master")

# COMMAND ----------

from pyspark.sql.functions import count, sum as spark_sum, avg

gold_region_summary = (
    gold_facility_master
    .filter(col("region").isNotNull())
    .groupBy("region")
    .agg(
        count("*").alias("facility_count"),
        spark_sum("doctor_count").alias("total_doctors"),
        spark_sum("capacity").alias("total_capacity"),
        avg("support_score").alias("avg_support_score"),
        avg("evidence_count").alias("avg_evidence_count"),
        avg("anomaly_score").alias("avg_anomaly_score")
    )
)

display(gold_region_summary)

# COMMAND ----------

gold_region_summary.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    "workspace.default.gold_region_summary"
)
print("Saved: workspace.default.gold_region_summary")

# COMMAND ----------

from pyspark.sql.functions import explode, lit

specialty_facts = (
    gold_facility_master
    .select("unique_id", "name", "region", "facility_type_id", explode(col("specialties")).alias("fact_text"))
    .filter(col("fact_text").isNotNull())
    .withColumn("fact_type", lit("specialty"))
)

procedure_facts = (
    gold_facility_master
    .select("unique_id", "name", "region", "facility_type_id", explode(col("procedure")).alias("fact_text"))
    .filter(col("fact_text").isNotNull())
    .withColumn("fact_type", lit("procedure"))
)

equipment_facts = (
    gold_facility_master
    .select("unique_id", "name", "region", "facility_type_id", explode(col("equipment")).alias("fact_text"))
    .filter(col("fact_text").isNotNull())
    .withColumn("fact_type", lit("equipment"))
)

capability_facts = (
    gold_facility_master
    .select("unique_id", "name", "region", "facility_type_id", explode(col("capability")).alias("fact_text"))
    .filter(col("fact_text").isNotNull())
    .withColumn("fact_type", lit("capability"))
)

description_facts = (
    gold_facility_master
    .select("unique_id", "name", "region", "facility_type_id", col("description").alias("fact_text"))
    .filter(col("fact_text").isNotNull())
    .withColumn("fact_type", lit("description"))
)

gold_facility_facts_long = (
    specialty_facts
    .unionByName(procedure_facts)
    .unionByName(equipment_facts)
    .unionByName(capability_facts)
    .unionByName(description_facts)
)

display(gold_facility_facts_long.limit(20))

# COMMAND ----------

gold_facility_facts_long.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_facility_facts_long"
)
print("Saved: workspace.default.gold_facility_facts_long")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

display(spark.table("workspace.default.gold_facility_master").limit(10))
display(spark.table("workspace.default.gold_region_summary"))
display(spark.table("workspace.default.gold_facility_facts_long").limit(20))

# COMMAND ----------

