"""End-to-end tests for must-have planner questions."""

from __future__ import annotations


def test_service_search_query_returns_direct_evidence(demo_pipeline: dict[str, object]) -> None:
    graph = demo_pipeline["graph"]
    response = graph.run("Which regions have facilities for cardiology?")

    assert response.query_type == "service search"
    assert any(row.get("region") == "Greater Accra" for row in response.evidence if isinstance(row, dict))


def test_facility_lookup_query_surfaces_services(demo_pipeline: dict[str, object]) -> None:
    graph = demo_pipeline["graph"]
    response = graph.run("What services does Tema General Hospital appear to offer?")

    assert response.query_type == "facility lookup"
    assert "Tema General Hospital appears to offer" in response.summary
    assert any(row.get("name") == "Tema General Hospital" for row in response.evidence if isinstance(row, dict))


def test_anomaly_query_returns_flagged_facility(demo_pipeline: dict[str, object]) -> None:
    graph = demo_pipeline["graph"]
    response = graph.run("Which facilities may be overstating capabilities?")

    assert response.query_type == "anomaly detection"
    assert any(card.get("name") == "Tamale Community Clinic" for card in response.anomaly_cards)


def test_planner_query_returns_attention_regions(demo_pipeline: dict[str, object]) -> None:
    graph = demo_pipeline["graph"]
    response = graph.run("What regions need NGO or infrastructure attention?")

    assert response.query_type == "planner recommendation"
    assert any(row.get("region") == "Northern" for row in response.planning_view)
