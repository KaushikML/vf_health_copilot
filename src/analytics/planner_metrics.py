"""Planner-oriented scoring and recommendation helpers."""

from __future__ import annotations

from typing import Any


def build_planner_recommendations(
    region_summary: list[dict[str, Any]],
    specialty: str | None = None,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Build grounded planner recommendations from region summary metrics."""

    recommendations: list[dict[str, Any]] = []
    label = specialty or "broad service coverage"

    for row in sorted(region_summary, key=lambda item: item["medical_desert_risk_score"], reverse=True)[:top_n]:
        facility_count = row["facility_count"]
        specialty_count = row["specialty_count"]
        anomaly_density = row["anomaly_density"]
        region = row["region"]

        summary = f"{region} appears underserved for {label}."
        evidence = (
            f"{facility_count} facility records, {specialty_count} specialty signals, "
            f"anomaly density {anomaly_density:.2f}, NGO presence {row['ngo_count']}."
        )
        risk = "Coverage is thin and available claims may need validation before planning decisions."
        suggested_action = (
            "Prioritize record validation, NGO outreach mapping, and infrastructure review for this region."
        )
        recommendations.append(
            {
                "region": region,
                "summary": summary,
                "evidence": evidence,
                "risk_concern": risk,
                "suggested_action": suggested_action,
                "medical_desert_risk_score": row["medical_desert_risk_score"],
            }
        )

    return recommendations
