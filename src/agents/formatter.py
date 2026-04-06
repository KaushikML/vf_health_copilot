"""Final response formatting for UI and notebook consumers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class CopilotResponse:
    """Final response returned by the planning copilot."""

    summary: str
    evidence: list[dict[str, Any]] = field(default_factory=list)
    risk_concern: str = ""
    suggested_action: str = ""
    confidence: float = 0.0
    query_type: str = ""
    retrieval_path: str = ""
    sql_used: list[str] = field(default_factory=list)
    anomaly_cards: list[dict[str, Any]] = field(default_factory=list)
    planning_view: list[dict[str, Any]] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def format_response(
    classification: Any,
    filters: dict[str, Any],
    retrieval_result: dict[str, Any],
    reasoned: dict[str, Any],
) -> CopilotResponse:
    """Convert a reasoned payload into the standard response object."""

    return CopilotResponse(
        summary=reasoned.get("summary", ""),
        evidence=reasoned.get("evidence", []),
        risk_concern=reasoned.get("risk_concern", ""),
        suggested_action=reasoned.get("suggested_action", ""),
        confidence=float(reasoned.get("confidence", classification.confidence)),
        query_type=classification.family,
        retrieval_path=retrieval_result.get("retrieval_path", ""),
        sql_used=[retrieval_result.get("structured", {}).get("sql", "")] if retrieval_result.get("structured") else [],
        anomaly_cards=reasoned.get("anomaly_cards", []),
        planning_view=reasoned.get("planning_view", []),
        filters=filters,
    )
