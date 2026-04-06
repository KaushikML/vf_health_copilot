"""Helpers for planner-focused response generation."""

from __future__ import annotations

from typing import Any

from src.analytics.planner_metrics import build_planner_recommendations


def summarize_planner_view(region_rows: list[dict[str, Any]], specialty: str | None = None) -> dict[str, Any]:
    """Create planner recommendations from region summary rows."""

    recommendations = build_planner_recommendations(region_rows, specialty=specialty)
    if not recommendations:
        return {
            "summary": "No planner recommendation could be produced from the available region summary.",
            "evidence": [],
            "risk_concern": "Region summary data is missing or incomplete.",
            "suggested_action": "Rebuild the Gold region summary before using planner outputs.",
            "confidence": 0.50,
            "planning_view": [],
        }

    top = recommendations[0]
    return {
        "summary": top["summary"],
        "evidence": recommendations,
        "risk_concern": top["risk_concern"],
        "suggested_action": top["suggested_action"],
        "confidence": 0.78,
        "planning_view": recommendations,
    }
