import hashlib
import math

import faiss
import numpy as np
import pandas as pd
import streamlit as st
from databricks import sql

from components import (
    render_anomaly_cards,
    render_badges,
    render_evidence_table,
    render_planning_view,
    render_summary_card,
    sidebar_filters,
)
from map_views import build_region_metric_map


# =========================
# CONFIG
# =========================
HOST = "dbc-b1ebec05-46c3.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/c4dabe06b0ec1750"
TOKEN = "dapi86030567b0e15a0dcd5c706ef17e6607"

FACILITY_TABLE = "workspace.default.gold_facility_master"
REGION_TABLE = "workspace.default.gold_region_summary"
FACT_TABLE = "workspace.default.gold_facility_facts_long"


# =========================
# PAGE SETUP
# =========================
st.set_page_config(page_title="Virtue Health Planning Copilot", layout="wide")
st.title("Virtue Health Planning Copilot")
st.caption("Planner-facing healthcare coverage, retrieval, and anomaly assistant.")


# =========================
# DATA ACCESS
# =========================
@st.cache_data
def run_sql_query(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    ) as conn:
        return pd.read_sql(query, conn)


@st.cache_data
def load_data():
    facility_pdf = run_sql_query(f"SELECT * FROM {FACILITY_TABLE}")
    region_pdf = run_sql_query(f"SELECT * FROM {REGION_TABLE}")
    facts_pdf = run_sql_query(f"SELECT * FROM {FACT_TABLE}")
    return facility_pdf, region_pdf, facts_pdf


# =========================
# VECTOR SEARCH
# =========================
def tokenize(text: str):
    return [t for t in "".join(ch.lower() if ch.isalnum() else " " for ch in str(text)).split() if t]


def embed_text(text: str, dimension: int = 256):
    vector = [0.0] * dimension
    for token in tokenize(text):
        token_hash = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16)
        vector[token_hash % dimension] += 1.0
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]


@st.cache_resource
def build_vector_index(_facts_pdf: pd.DataFrame):
    documents = []
    for _, row in _facts_pdf.iterrows():
        text = f"{row.get('name')} | {row.get('region')} | {row.get('facility_type_id')} | {row.get('fact_type')} | {row.get('fact_text')}"
        documents.append(
            {
                "text": text,
                "metadata": {
                    "unique_id": row.get("unique_id"),
                    "name": row.get("name"),
                    "region": row.get("region"),
                    "fact_type": row.get("fact_type"),
                    "facility_type_id": row.get("facility_type_id"),
                },
            }
        )

    vectors = np.array([embed_text(doc["text"]) for doc in documents], dtype="float32")
    index = faiss.IndexFlatIP(256)
    index.add(vectors)
    return index, documents


def vector_search(query: str, index, documents, top_k: int = 8):
    q = np.array([embed_text(query)], dtype="float32")
    scores, indices = index.search(q, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        results.append(
            {
                "score": float(score),
                "text": documents[idx]["text"],
                "metadata": documents[idx]["metadata"],
            }
        )
    return results


# =========================
# QUERY LOGIC
# =========================
def classify_query(query: str) -> str:
    q = query.lower()
    if "how many" in q or "count" in q or "most" in q:
        return "count_ranking"
    if "which regions" in q or "prioritize" in q or "underserved" in q or "cold spots" in q:
        return "planner"
    if "overstating" in q or "unrealistic" in q or "suspicious" in q or "anomaly" in q:
        return "anomaly"
    if "what services" in q or "offer" in q:
        return "facility_lookup"
    return "service_search"


def run_query(query: str, facility_pdf: pd.DataFrame, region_pdf: pd.DataFrame, index, documents):
    family = classify_query(query)

    if family == "count_ranking":
        if "cardiology" in query.lower():
            result = facility_pdf[
                facility_pdf["specialties"].astype(str).str.contains("cardio", case=False, na=False)
            ]
            return {
                "query_type": family,
                "confidence": 0.84,
                "retrieval_path": "sql+filter",
                "summary": f"Found {len(result)} facilities matching cardiology-related specialties.",
                "risk_concern": None,
                "suggested_action": "Review regional concentration and whether coverage is uneven.",
                "evidence": result[["name", "region", "city"]].head(10).to_dict(orient="records"),
                "anomaly_cards": [],
                "planning_view": [],
            }

        grouped = (
            facility_pdf.groupby("region", dropna=False)
            .size()
            .reset_index(name="facility_count")
            .sort_values("facility_count", ascending=False)
        )
        return {
            "query_type": family,
            "confidence": 0.75,
            "retrieval_path": "sql+groupby",
            "summary": "Count/ranking query completed.",
            "risk_concern": None,
            "suggested_action": None,
            "evidence": grouped.head(10).to_dict(orient="records"),
            "anomaly_cards": [],
            "planning_view": [],
        }

    if family == "facility_lookup":
        results = vector_search(query, index, documents, top_k=8)
        return {
            "query_type": family,
            "confidence": 0.78,
            "retrieval_path": "faiss",
            "summary": "Facility lookup completed using semantic retrieval.",
            "risk_concern": None,
            "suggested_action": "Verify returned services against supporting evidence rows.",
            "evidence": results,
            "anomaly_cards": [],
            "planning_view": [],
        }

    if family == "planner":
        ranked = region_pdf.sort_values("facility_count", ascending=True).head(10)
        planning_rows = []
        for _, row in ranked.iterrows():
            planning_rows.append(
                {
                    "region": row.get("region"),
                    "summary": f"{row.get('region')} appears relatively underserved by facility count.",
                    "evidence": f"facility_count={row.get('facility_count')}, avg_support_score={row.get('avg_support_score')}",
                    "suggested_action": "Prioritize outreach, facility verification, or targeted support.",
                }
            )
        return {
            "query_type": family,
            "confidence": 0.80,
            "retrieval_path": "sql+region_summary",
            "summary": "Planner path completed using region summary table.",
            "risk_concern": "Low facility density may indicate underserved areas.",
            "suggested_action": "Review the lowest-coverage regions first.",
            "evidence": ranked.to_dict(orient="records"),
            "anomaly_cards": [],
            "planning_view": planning_rows,
        }

    if family == "anomaly":
        suspicious = facility_pdf.sort_values("anomaly_score", ascending=False).head(10)
        anomaly_rows = suspicious[["name", "region", "anomaly_score", "support_score", "evidence_count"]].to_dict(
            orient="records"
        )
        return {
            "query_type": family,
            "confidence": 0.73,
            "retrieval_path": "sql+anomaly_score",
            "summary": "Anomaly path completed using anomaly score ranking.",
            "risk_concern": "High-scoring facilities may require manual verification.",
            "suggested_action": "Inspect top-ranked facilities before making planning decisions.",
            "evidence": anomaly_rows,
            "anomaly_cards": anomaly_rows,
            "planning_view": [],
        }

    results = vector_search(query, index, documents, top_k=5)
    return {
        "query_type": family,
        "confidence": 0.70,
        "retrieval_path": "faiss",
        "summary": "Service search completed using semantic retrieval.",
        "risk_concern": None,
        "suggested_action": None,
        "evidence": results,
        "anomaly_cards": [],
        "planning_view": [],
    }


# =========================
# LOAD DATA
# =========================
facility_pdf, region_pdf, facts_pdf = load_data()
index, documents = build_vector_index(facts_pdf)

region_options = [r for r in region_pdf["region"].dropna().astype(str).tolist() if r]
ui_filters = sidebar_filters(region_options=region_options)


# =========================
# RUN QUERY
# =========================
query = ui_filters["query"].strip()
if ui_filters["region"] and ui_filters["region"].lower() not in query.lower():
    query = f"{query} in {ui_filters['region']}"

if st.button("Run Query", type="primary"):
    response = run_query(query, facility_pdf, region_pdf, index, documents)

    render_badges(response["query_type"], response["confidence"], response["retrieval_path"])
    render_summary_card(
        response["summary"],
        response.get("risk_concern"),
        response.get("suggested_action"),
    )

    figure = build_region_metric_map(region_pdf.to_dict(orient="records"), metric="facility_count")
    if figure is not None:
        st.plotly_chart(figure, use_container_width=True)

    render_evidence_table(response["evidence"])
    render_anomaly_cards(response["anomaly_cards"])
    render_planning_view(response["planning_view"])

    with st.expander("Debug response"):
        st.json(response)