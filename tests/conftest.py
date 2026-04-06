"""Shared fixtures for local unit tests."""

from __future__ import annotations

import pytest

from src.agents.graph import VirtueHealthGraph
from src.analytics.anomaly_scoring import score_anomalies
from src.analytics.region_metrics import build_region_summary
from src.processing.feature_builder import build_feature_rows, build_long_fact_rows
from src.processing.normalizers import normalize_records
from src.processing.specialty_mapper import SpecialtyMapper
from src.retrieval.hybrid_router import HybridRetrievalEngine
from src.retrieval.sql_retriever import SQLRetriever
from src.retrieval.vector_retriever import VectorRetriever
from src.utils.constants import DEMO_FACILITIES


@pytest.fixture
def demo_pipeline() -> dict[str, object]:
    mapper = SpecialtyMapper()
    normalized = normalize_records(DEMO_FACILITIES, specialty_mapper=mapper)
    featured = build_feature_rows(normalized, mapper=mapper)
    scored = score_anomalies(featured)
    region_summary = build_region_summary(scored)
    fact_rows = build_long_fact_rows(scored, mapper=mapper)

    sql_retriever = SQLRetriever(facility_rows=scored, region_rows=region_summary)
    vector_retriever = VectorRetriever.from_fact_rows(fact_rows, use_faiss_if_available=False)
    graph = VirtueHealthGraph(
        retrieval_engine=HybridRetrievalEngine(sql_retriever, vector_retriever),
        mapper=mapper,
        known_regions=[row["region"] for row in region_summary],
        known_names=[row.get("name") for row in scored if row.get("name")],
        known_facility_types=sorted({row.get("facilityTypeId") for row in scored if row.get("facilityTypeId")}),
        known_operator_types=sorted({row.get("operatorTypeId") for row in scored if row.get("operatorTypeId")}),
        region_rows=region_summary,
    )
    return {
        "mapper": mapper,
        "normalized": normalized,
        "featured": featured,
        "scored": scored,
        "region_summary": region_summary,
        "fact_rows": fact_rows,
        "graph": graph,
        "retrieval_engine": HybridRetrievalEngine(sql_retriever, vector_retriever),
    }
