# COMMAND ----------

# DBTITLE 1,Install faiss package
# %pip install faiss-cpu


# Databricks notebook source
facts_df = spark.table("workspace.default.gold_facility_facts_long")
display(facts_df.limit(10))
print("Rows:", facts_df.count())

# COMMAND ----------

facts_pdf = facts_df.toPandas()
print(facts_pdf.shape)
facts_pdf.head()

# COMMAND ----------

from dataclasses import dataclass

@dataclass
class VectorDocument:
    text: str
    metadata: dict

documents = []

for _, row in facts_pdf.iterrows():
    text = f"{row['name']} | {row['region']} | {row['facility_type_id']} | {row['fact_type']} | {row['fact_text']}"
    metadata = {
        "unique_id": row["unique_id"],
        "name": row["name"],
        "region": row["region"],
        "fact_type": row["fact_type"],
        "facility_type_id": row["facility_type_id"],
    }
    documents.append(VectorDocument(text=text, metadata=metadata))

print("Documents:", len(documents))
documents[:2]

# COMMAND ----------

import hashlib
import json
import math
from pathlib import Path

class FactVectorIndex:
    def __init__(self, dimension=256):
        self.dimension = dimension
        self.documents = []
        self.vectors = []
        self.index = None
        import faiss
        import numpy as np
        self.faiss = faiss
        self.np = np

    def _tokenize(self, text):
        return [t for t in "".join(ch.lower() if ch.isalnum() else " " for ch in text).split() if t]

    def _embed_text(self, text):
        vector = [0.0] * self.dimension
        for token in self._tokenize(text):
            token_hash = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
            vector[token_hash % self.dimension] += 1.0
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0:
            return vector
        return [v / norm for v in vector]

    def build(self, documents):
        self.documents = documents
        self.vectors = [self._embed_text(doc.text) for doc in documents]
        matrix = self.np.array(self.vectors, dtype="float32")
        # vectors are cosine-normalized, so inner product behaves like cosine similarity
        self.index = self.faiss.IndexFlatIP(self.dimension)
        self.index.add(matrix)

    def search(self, query, top_k=5):
        q = self.np.array([self._embed_text(query)], dtype="float32")
        scores, indices = self.index.search(q, top_k)
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx == -1:
                continue
            results.append({
                "score": float(score),
                "text": self.documents[idx].text,
                "metadata": self.documents[idx].metadata,
            })
        return results

    def save(self, path):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        payload = {
            "dimension": self.dimension,
            "documents": [{"text": d.text, "metadata": d.metadata} for d in self.documents],
            "vectors": self.vectors,
        }
        (path / "vector_index.json").write_text(json.dumps(payload), encoding="utf-8")


# COMMAND ----------

index = FactVectorIndex(dimension=256)
index.build(documents)
print("Built index with", len(index.documents), "documents")

# COMMAND ----------

results = index.search("cardiology hospital in accra", top_k=5)
results

# COMMAND ----------

results = index.search("cataract surgery clinic ashanti", top_k=5)
results

# COMMAND ----------

index.save("/Volumes/workspace/default/vf_health_data/vector_index")
print("Saved vector index")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/vf_health_data/vector_index"))

# COMMAND ----------

