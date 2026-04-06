"""Extract conservative structured filters from planner queries."""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict

from src.processing.specialty_mapper import SpecialtyMapper
from src.utils.constants import REGION_CENTROIDS


@dataclass
class QueryFilters:
    """Structured filters extracted from a query."""

    region: str | None = None
    specialty: str | None = None
    procedure: str | None = None
    facility_type: str | None = None
    operator_type: str | None = None
    facility_name: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


def _extract_facility_name(query: str, known_names: list[str] | None = None) -> str | None:
    query_stripped = query.strip().rstrip("?")
    if known_names:
        for name in sorted(known_names, key=len, reverse=True):
            if name.lower() in query.lower():
                return name

    patterns = [
        r"what services does (?P<name>.+?) appear to offer$",
        r"tell me about (?P<name>.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, query_stripped, flags=re.IGNORECASE)
        if match:
            return match.group("name").strip(" \"'")
    return None


def build_filters(
    query: str,
    mapper: SpecialtyMapper | None = None,
    known_regions: list[str] | None = None,
    known_facility_types: list[str] | None = None,
    known_operator_types: list[str] | None = None,
    known_names: list[str] | None = None,
) -> QueryFilters:
    """Extract direct structured filters from a natural-language query."""

    mapper = mapper or SpecialtyMapper()
    lowered = query.lower()

    regions = known_regions or list(REGION_CENTROIDS.keys())
    region = next((candidate for candidate in sorted(regions, key=len, reverse=True) if candidate.lower() in lowered), None)

    facility_types = known_facility_types or ["hospital", "clinic", "diagnostic centre", "ngo", "health post"]
    facility_type = next(
        (candidate for candidate in sorted(facility_types, key=len, reverse=True) if candidate.lower() in lowered),
        None,
    )

    operator_types = known_operator_types or ["public", "private", "faith-based", "ngo"]
    operator_type = next(
        (candidate for candidate in sorted(operator_types, key=len, reverse=True) if candidate.lower() in lowered),
        None,
    )

    extracted_terms = mapper.extract_from_text(query, sections=["specialties", "procedures"])
    specialty = extracted_terms.get("specialties", [None])[0]
    procedure = extracted_terms.get("procedures", [None])[0]
    facility_name = _extract_facility_name(query, known_names=known_names)

    return QueryFilters(
        region=region,
        specialty=specialty,
        procedure=procedure,
        facility_type=facility_type,
        operator_type=operator_type,
        facility_name=facility_name,
    )
