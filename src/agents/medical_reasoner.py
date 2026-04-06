"""Grounded response synthesis using retrieved evidence only."""

from __future__ import annotations

from typing import Any

from src.agents.anomaly_agent import summarize_anomalies
from src.agents.planner_agent import summarize_planner_view
from src.utils.constants import REGION_CENTROIDS


class MedicalReasoner:
    """Build planner-readable answers while staying close to the evidence."""

    def __init__(self, region_rows: list[dict[str, Any]] | None = None) -> None:
        self.region_rows = region_rows or []

    @staticmethod
    def _service_terms(row: dict[str, Any]) -> list[str]:
        values: list[str] = []
        for key in ["specialties_norm", "procedure_norm", "equipment_norm", "capability_norm"]:
            values.extend(row.get(key, []))
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            lowered = value.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(value)
        return deduped

    def reason(
        self,
        query: str,
        classification: Any,
        filters: dict[str, Any],
        retrieval_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Return a grounded planner response payload."""

        family = classification.family
        structured_rows = retrieval_result["structured"]["rows"]
        vector_rows = retrieval_result["vector"]

        if family == "anomaly detection":
            return summarize_anomalies(structured_rows)

        if family == "planner recommendation":
            return summarize_planner_view(structured_rows, specialty=filters.get("specialty"))

        if family == "region gap analysis":
            observed_regions = {row.get("region") for row in structured_rows if row.get("region")}
            all_regions = set(self.region_rows and [row["region"] for row in self.region_rows] or REGION_CENTROIDS.keys())
            missing = sorted(region for region in all_regions if region not in observed_regions)
            target = filters.get("specialty") or filters.get("procedure") or "the requested service"
            summary = (
                f"No direct evidence of {target} was found in {', '.join(missing[:5])}."
                if missing
                else f"Direct evidence for {target} exists in the currently indexed regions."
            )
            return {
                "summary": summary,
                "evidence": structured_rows,
                "risk_concern": "Absence in the dataset is not proof of true absence on the ground.",
                "suggested_action": "Validate low-coverage regions before using the result as a planning decision.",
                "confidence": 0.74,
                "planning_view": [],
            }

        if family == "count/ranking":
            target = filters.get("specialty") or filters.get("procedure") or "matched facilities"
            if not structured_rows:
                summary = f"No direct evidence was found for {target} in the current dataset slice."
            else:
                top = structured_rows[0]
                summary = f"{top['region']} has the highest visible count for {target} in the current records."
            return {
                "summary": summary,
                "evidence": structured_rows,
                "risk_concern": "Counts depend on current record completeness and should not be treated as population coverage.",
                "suggested_action": "Use the count as a directional planning signal and review row-level evidence for the top regions.",
                "confidence": 0.80,
            }

        if family == "ngo analysis":
            if not structured_rows:
                summary = "No NGO-tagged records matched the current filters."
            else:
                regions = sorted({row.get("region") for row in structured_rows if row.get("region")})
                summary = f"NGO-linked records are present in {', '.join(regions[:5])}."
            return {
                "summary": summary,
                "evidence": structured_rows,
                "risk_concern": "NGO presence reflects tagged records only and may undercount informal support activity.",
                "suggested_action": "Cross-check NGO-linked rows before using them for partnership planning.",
                "confidence": 0.75,
            }

        if family == "facility lookup":
            if not structured_rows:
                return {
                    "summary": "No direct facility record matched the requested name.",
                    "evidence": vector_rows,
                    "risk_concern": "Facility naming may differ across the source data.",
                    "suggested_action": "Try a shorter or alternate facility name, then review duplicate candidates if needed.",
                    "confidence": 0.55,
                }
            row = structured_rows[0]
            services = self._service_terms(row)
            service_text = ", ".join(services[:8]) if services else "no explicit service claims"
            summary = f"{row.get('name')} appears to offer {service_text} based on direct record evidence."
            risk = (
                "Some claims in this facility record are weakly supported and should be validated."
                if float(row.get("anomaly_score", 0.0)) >= 0.35
                else "This summary only reflects recorded evidence and may miss unstructured services not captured cleanly."
            )
            return {
                "summary": summary,
                "evidence": [row, *vector_rows[:5]],
                "risk_concern": risk,
                "suggested_action": "Use the evidence rows below before attributing additional specialty or procedure support to the facility.",
                "confidence": 0.83,
            }

        if family == "service search":
            target = filters.get("specialty") or filters.get("procedure") or "the requested service"
            if not structured_rows:
                summary = f"No direct evidence was found for {target} in the current facility records."
            else:
                regions = sorted({row.get('region') for row in structured_rows if row.get('region')})
                summary = f"Direct evidence for {target} appears in {', '.join(regions[:5])}."
            return {
                "summary": summary,
                "evidence": structured_rows[:10] + vector_rows[:5],
                "risk_concern": "Presence in the dataset does not guarantee service quality, capacity, or current availability.",
                "suggested_action": "Review the facility-level evidence and anomaly signals before using the result for referral planning.",
                "confidence": 0.81,
            }

        return {
            "summary": "The query falls outside the current supported planning workflows.",
            "evidence": [],
            "risk_concern": "Unsupported questions can lead to over-interpretation if answered without the right evidence.",
            "suggested_action": "Try a facility lookup, service search, count/ranking, anomaly, gap, planner, or NGO question.",
            "confidence": 0.52,
        }
