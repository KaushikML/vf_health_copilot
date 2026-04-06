# Databricks notebook source
facility_df = spark.table("workspace.default.gold_facility_master")
region_df = spark.table("workspace.default.gold_region_summary")
facts_df = spark.table("workspace.default.gold_facility_facts_long")

display(facility_df.limit(5))
display(region_df.limit(5))
display(facts_df.limit(5))

# COMMAND ----------

facility_pdf = facility_df.toPandas()
region_pdf = region_df.toPandas()
facts_pdf = facts_df.toPandas()

print(facility_pdf.shape, region_pdf.shape, facts_pdf.shape)

# COMMAND ----------

# MAGIC %pip install faiss-cpu

# COMMAND ----------

import hashlib
import math
import faiss
import numpy as np

def tokenize(text):
    return [t for t in "".join(ch.lower() if ch.isalnum() else " " for ch in str(text)).split() if t]

def embed_text(text, dimension=256):
    vector = [0.0] * dimension
    for token in tokenize(text):
        token_hash = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
        vector[token_hash % dimension] += 1.0
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]

documents = []
for _, row in facts_pdf.iterrows():
    text = f"{row['name']} | {row['region']} | {row['facility_type_id']} | {row['fact_type']} | {row['fact_text']}"
    documents.append({
        "text": text,
        "metadata": {
            "unique_id": row["unique_id"],
            "name": row["name"],
            "region": row["region"],
            "fact_type": row["fact_type"],
            "facility_type_id": row["facility_type_id"],
        }
    })

vectors = np.array([embed_text(doc["text"]) for doc in documents], dtype="float32")
index = faiss.IndexFlatIP(256)
index.add(vectors)

def vector_search(query, top_k=5):
    q = np.array([embed_text(query)], dtype="float32")
    scores, indices = index.search(q, top_k)
    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        results.append({
            "score": float(score),
            "text": documents[idx]["text"],
            "metadata": documents[idx]["metadata"]
        })
    return results

# COMMAND ----------

def classify_query(query: str) -> str:
    q = query.lower()
    if "how many" in q or "count" in q or "most" in q:
        return "count_ranking"
    if "which regions" in q or "prioritize" in q or "underserved" in q or "cold spots" in q:
        return "planner"
    if "overstating" in q or "unrealistic" in q or "suspicious" in q:
        return "anomaly"
    if "what services" in q or "offer" in q:
        return "facility_lookup"
    return "service_search"

# COMMAND ----------

def run_query(query: str):
    family = classify_query(query)
    print("Query family:", family)

    if family == "count_ranking":
        if "cardiology" in query.lower():
            result = facility_pdf[facility_pdf["specialties"].astype(str).str.contains("cardiology", case=False, na=False)]
            return {
                "summary": f"Found {len(result)} facilities matching cardiology.",
                "rows": result[["name", "region", "city"]].head(10).to_dict(orient="records")
            }
        return {"summary": "Count/ranking path triggered.", "rows": []}

    if family == "facility_lookup":
        results = vector_search(query, top_k=8)
        return {
            "summary": "Facility lookup completed using semantic retrieval.",
            "rows": results
        }

    if family == "planner":
        ranked = region_pdf.sort_values("facility_count", ascending=True).head(10)
        return {
            "summary": "Planner path completed using region summary table.",
            "rows": ranked.to_dict(orient="records")
        }

    if family == "anomaly":
        suspicious = facility_pdf.sort_values("anomaly_score", ascending=False).head(10)
        return {
            "summary": "Anomaly path completed using anomaly score ranking.",
            "rows": suspicious.to_dict(orient="records")
        }

    results = vector_search(query, top_k=5)
    return {
        "summary": "Service search completed using semantic retrieval.",
        "rows": results
    }

# COMMAND ----------

run_query("How many hospitals have cardiology?")

# COMMAND ----------

run_query("What services does Korle Bu Teaching Hospital offer?")

# COMMAND ----------

run_query("Which regions should VF prioritize for emergency care investment?")

# COMMAND ----------

run_query("Which facilities may be overstating capabilities?")

# COMMAND ----------

demo_outputs = {
    "q1": run_query("How many hospitals have cardiology?"),
    "q2": run_query("What services does Korle Bu Teaching Hospital offer?"),
    "q3": run_query("Which regions should VF prioritize for emergency care investment?"),
    "q4": run_query("Which facilities may be overstating capabilities?")
}
demo_outputs

# COMMAND ----------

