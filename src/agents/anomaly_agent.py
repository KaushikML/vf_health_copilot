"""Helpers for anomaly-focused response generation."""

from __future__ import annotations

from typing import Any


def summarize_anomalies(rows: list[dict[str, Any]], top_n: int = 5) -> dict[str, Any]:
    """Create a grounded anomaly response from scored facility rows."""

    top_rows = rows[:top_n]
    if not top_rows:
        return {
            "summary": "No facilities crossed the anomaly threshold based on current direct evidence.",
            "evidence": [],
            "risk_concern": "This does not prove records are accurate; it only means current rules did not flag them.",
            "suggested_action": "Review rule thresholds and add more evidence signals if stronger screening is needed.",
            "confidence": 0.60,
            "anomaly_cards": [],
        }

    facilities = ", ".join(f"{row.get('name')} ({row.get('region')})" for row in top_rows[:3])
    evidence = [
        {
            "unique_id": row.get("unique_id"),
            "name": row.get("name"),
            "region": row.get("region"),
            "anomaly_score": row.get("anomaly_score"),
            "findings": row.get("anomaly_findings", []),
        }
        for row in top_rows
    ]
    return {
        "summary": f"Potentially weak or overstated facility claims were found in {facilities}.",
        "evidence": evidence,
        "risk_concern": "Flagged facilities should be treated as records requiring validation, not as confirmed fraud.",
        "suggested_action": "Prioritize manual review of high-score facilities and compare claimed services with supporting evidence fields.",
        "confidence": 0.82,
        "anomaly_cards": evidence,
    }
