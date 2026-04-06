"""Tests for query family classification and retrieval routing."""

from __future__ import annotations

from src.agents.classifier import classify_query


def test_classifier_recognizes_core_query_families() -> None:
    assert classify_query("Which facilities may be overstating capabilities?").family == "anomaly detection"
    assert classify_query("Which regions have facilities for cardiology?").family == "service search"
    assert classify_query("What regions need NGO or infrastructure attention?").family == "planner recommendation"


def test_hybrid_router_chooses_expected_paths(demo_pipeline: dict[str, object]) -> None:
    retrieval_engine = demo_pipeline["retrieval_engine"]
    assert retrieval_engine.route("count/ranking") == "structured"
    assert retrieval_engine.route("facility lookup") == "hybrid"
