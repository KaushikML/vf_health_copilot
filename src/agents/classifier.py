"""Rule-based query family classification for the planning copilot."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QueryClassification:
    """Classification output for a natural-language query."""

    family: str
    confidence: float
    rationale: str


def classify_query(query: str) -> QueryClassification:
    """Classify a planner query into a supported family."""

    lowered = query.lower().strip()

    if any(term in lowered for term in ["travel time", "drive time", "live web", "current news", "population census"]):
        return QueryClassification(
            family="external-data-needed",
            confidence=0.92,
            rationale="The query asks for external data not available in the local evidence base.",
        )

    if any(term in lowered for term in ["overstating", "suspicious", "suspicious claims", "weak", "anomaly"]):
        return QueryClassification(
            family="anomaly detection",
            confidence=0.90,
            rationale="The query explicitly asks for suspicious or weak claims.",
        )

    if any(term in lowered for term in ["need ngo", "infrastructure attention", "where should planners focus", "attention"]):
        return QueryClassification(
            family="planner recommendation",
            confidence=0.86,
            rationale="The query asks for planning prioritization or NGO attention.",
        )

    if any(term in lowered for term in ["care desert", "underserved", "lack", "missing", "gap analysis"]):
        return QueryClassification(
            family="region gap analysis",
            confidence=0.82,
            rationale="The query asks where care or service coverage is missing.",
        )

    if any(term in lowered for term in ["how many", "count", "most", "rank", "ranking", "top regions"]):
        return QueryClassification(
            family="count/ranking",
            confidence=0.88,
            rationale="The query asks for counts or ranked results.",
        )

    if "ngo" in lowered:
        return QueryClassification(
            family="ngo analysis",
            confidence=0.80,
            rationale="The query is focused on NGO presence or NGO-supported activity.",
        )

    if lowered.startswith("what services does") or lowered.startswith("tell me about"):
        return QueryClassification(
            family="facility lookup",
            confidence=0.90,
            rationale="The query is centered on a specific facility.",
        )

    if any(term in lowered for term in ["which regions have", "where can", "facilities for", "specialty", "procedure"]):
        return QueryClassification(
            family="service search",
            confidence=0.84,
            rationale="The query is asking where a specialty or procedure exists.",
        )

    return QueryClassification(
        family="unsupported",
        confidence=0.55,
        rationale="The query does not match a supported planning workflow with enough confidence.",
    )
