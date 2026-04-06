"""Deterministic retrieval helpers that mirror future Spark SQL behavior."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from src.utils.constants import DEFAULT_TABLE_NAMES, REGION_CENTROIDS


@dataclass
class StructuredQuery:
    """Structured retrieval request."""

    family: str
    filters: dict[str, Any] = field(default_factory=dict)
    limit: int = 25


class SQLRetriever:
    """A lightweight structured retriever with SQL-like trace output."""

    def __init__(
        self,
        facility_rows: list[dict[str, Any]],
        region_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.facility_rows = facility_rows
        self.region_rows = region_rows or []

    @staticmethod
    def _contains(values: list[str], target: str | None) -> bool:
        if not target:
            return True
        lowered_target = target.lower()
        return any(value.lower() == lowered_target for value in values)

    def _row_matches(self, row: dict[str, Any], filters: dict[str, Any]) -> bool:
        if filters.get("region") and row.get("region") != filters["region"]:
            return False
        if filters.get("facility_type") and row.get("facilityTypeId") != filters["facility_type"]:
            return False
        if filters.get("operator_type") and row.get("operatorTypeId") != filters["operator_type"]:
            return False
        if filters.get("facility_name"):
            facility_name = str(row.get("name") or "").lower()
            if filters["facility_name"].lower() not in facility_name:
                return False
        if filters.get("specialty") and not self._contains(row.get("specialties_norm", []), filters["specialty"]):
            return False
        if filters.get("procedure") and not self._contains(row.get("procedure_norm", []), filters["procedure"]):
            return False
        return True

    def filter_rows(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        """Return facility rows that match the given filters."""

        return [row for row in self.facility_rows if self._row_matches(row, filters)]

    def _sql_trace(self, family: str, filters: dict[str, Any]) -> str:
        clauses = ["1 = 1"]
        if filters.get("region"):
            clauses.append(f"region = '{filters['region']}'")
        if filters.get("facility_type"):
            clauses.append(f"facilityTypeId = '{filters['facility_type']}'")
        if filters.get("operator_type"):
            clauses.append(f"operatorTypeId = '{filters['operator_type']}'")
        if filters.get("facility_name"):
            clauses.append(f"name ILIKE '%{filters['facility_name']}%'")
        if filters.get("specialty"):
            clauses.append(f"ARRAY_CONTAINS(specialties_norm, '{filters['specialty']}')")
        if filters.get("procedure"):
            clauses.append(f"ARRAY_CONTAINS(procedure_norm, '{filters['procedure']}')")
        where_clause = " AND ".join(clauses)
        return (
            f"-- {family}\n"
            f"SELECT * FROM {DEFAULT_TABLE_NAMES['gold_facility_master']}\n"
            f"WHERE {where_clause};"
        )

    def _count_by_region(self, rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        counts = Counter(row.get("region") or "Unknown" for row in rows if row.get("is_facility"))
        ranked = [{"region": region, "facility_count": count} for region, count in counts.most_common(limit)]
        for row in ranked:
            centroid = REGION_CENTROIDS.get(row["region"], {"lat": None, "lon": None})
            row["lat"] = centroid["lat"]
            row["lon"] = centroid["lon"]
        return ranked

    def _anomaly_rows(self, limit: int) -> list[dict[str, Any]]:
        ranked = sorted(
            (row for row in self.facility_rows if float(row.get("anomaly_score", 0.0)) > 0),
            key=lambda row: float(row.get("anomaly_score", 0.0)),
            reverse=True,
        )
        return ranked[:limit]

    def _ngo_rows(self, filters: dict[str, Any], limit: int) -> list[dict[str, Any]]:
        rows = [row for row in self.facility_rows if row.get("is_ngo")]
        rows = [row for row in rows if self._row_matches(row, filters)]
        return rows[:limit]

    def run(self, query: StructuredQuery) -> dict[str, Any]:
        """Execute a structured retrieval request."""

        family = query.family
        filters = query.filters
        base_rows = self.filter_rows(filters)

        if family == "count/ranking":
            rows = self._count_by_region(base_rows, query.limit)
        elif family == "anomaly detection":
            rows = self._anomaly_rows(query.limit)
        elif family == "planner recommendation":
            rows = sorted(
                self.region_rows,
                key=lambda row: float(row.get("medical_desert_risk_score", 0.0)),
                reverse=True,
            )[: query.limit]
        elif family == "region gap analysis":
            rows = self._count_by_region(base_rows, query.limit)
        elif family == "ngo analysis":
            rows = self._ngo_rows(filters, query.limit)
        else:
            rows = base_rows[: query.limit]

        return {
            "family": family,
            "rows": rows,
            "sql": self._sql_trace(family, filters),
            "filters": filters,
        }
