"""Region-level aggregation for coverage, anomalies, and planner views."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.utils.constants import REGION_CENTROIDS


def build_region_summary(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate facility and NGO metrics by region."""

    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "region": None,
            "facility_count": 0,
            "ngo_count": 0,
            "specialties": set(),
            "procedures": set(),
            "anomaly_count": 0,
            "anomaly_density": 0.0,
            "avg_support_score": 0.0,
            "support_score_sum": 0.0,
            "medical_desert_risk_score": 0.0,
        }
    )

    for record in records:
        region = record.get("region") or "Unknown"
        bucket = grouped[region]
        bucket["region"] = region
        bucket["facility_count"] += 1 if record.get("is_facility") else 0
        bucket["ngo_count"] += 1 if record.get("is_ngo") else 0
        bucket["specialties"].update(record.get("specialties_norm", []))
        bucket["procedures"].update(record.get("procedure_norm", []))
        bucket["anomaly_count"] += 1 if float(record.get("anomaly_score", 0.0)) >= 0.35 else 0
        bucket["support_score_sum"] += float(record.get("support_score", 0.0))

    summary_rows: list[dict[str, Any]] = []
    for region, bucket in grouped.items():
        facility_count = bucket["facility_count"]
        specialty_count = len(bucket["specialties"])
        procedure_support_count = len(bucket["procedures"])
        anomaly_density = round(bucket["anomaly_count"] / max(facility_count, 1), 3)
        avg_support = round(bucket["support_score_sum"] / max(facility_count + bucket["ngo_count"], 1), 3)

        facility_gap = 1.0 if facility_count == 0 else min(1.0, 2.0 / facility_count)
        specialty_gap = 1.0 if specialty_count == 0 else min(1.0, 2.0 / specialty_count)
        procedure_gap = 1.0 if procedure_support_count == 0 else min(1.0, 2.0 / procedure_support_count)
        ngo_buffer = 0.10 if bucket["ngo_count"] > 0 else 0.0
        desert_risk = round(
            max(
                0.0,
                min(
                    1.0,
                    (0.40 * facility_gap)
                    + (0.30 * specialty_gap)
                    + (0.15 * procedure_gap)
                    + (0.15 * anomaly_density)
                    - ngo_buffer,
                ),
            ),
            3,
        )

        centroid = REGION_CENTROIDS.get(region, {"lat": None, "lon": None})
        summary_rows.append(
            {
                "region": region,
                "facility_count": facility_count,
                "ngo_count": bucket["ngo_count"],
                "specialty_count": specialty_count,
                "procedure_support_count": procedure_support_count,
                "anomaly_density": anomaly_density,
                "avg_support_score": avg_support,
                "medical_desert_risk_score": desert_risk,
                "lat": centroid["lat"],
                "lon": centroid["lon"],
            }
        )

    return sorted(summary_rows, key=lambda row: row["medical_desert_risk_score"], reverse=True)
