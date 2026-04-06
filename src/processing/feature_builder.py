"""Feature engineering for medallion outputs and anomaly scoring."""

from __future__ import annotations

from typing import Any

from src.processing.freeform_parser import parse_freeform_text
from src.processing.normalizers import parse_list_like
from src.processing.specialty_mapper import SpecialtyMapper


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _list_value(record: dict[str, Any], key: str) -> list[str]:
    if isinstance(record.get(key), list):
        return record[key]
    return parse_list_like(record.get(key))


def build_feature_row(record: dict[str, Any], mapper: SpecialtyMapper | None = None) -> dict[str, Any]:
    """Derive planning and anomaly features from a normalized record."""

    mapper = mapper or SpecialtyMapper()
    enriched = dict(record)

    description = enriched.get("description")
    freeform = parse_freeform_text(description, mapper=mapper)

    specialties = _list_value(enriched, "specialties_norm") or freeform.specialties
    procedures = _list_value(enriched, "procedure_norm") or freeform.procedures
    equipment = _list_value(enriched, "equipment_norm") or freeform.equipment
    capabilities = _list_value(enriched, "capability_norm") or freeform.capabilities
    phones = _list_value(enriched, "phone_numbers_norm")
    websites = _list_value(enriched, "websites_norm")

    doctor_count = _safe_float(enriched.get("doctor_count"))
    capacity = _safe_float(enriched.get("capacity"))
    area = _safe_float(enriched.get("area"))

    enriched["specialties_norm"] = specialties
    enriched["procedure_norm"] = procedures
    enriched["equipment_norm"] = equipment
    enriched["capability_norm"] = capabilities
    enriched["phone_numbers_norm"] = phones
    enriched["websites_norm"] = websites

    facility_type = str(enriched.get("facilityTypeId") or "").lower()
    operator_type = str(enriched.get("operatorTypeId") or "").lower()
    is_ngo = "ngo" in {facility_type, operator_type} or "ngo" in str(enriched.get("name") or "").lower()
    is_facility = not is_ngo and facility_type in {"hospital", "clinic", "diagnostic centre", "health post"}

    service_breadth_score = len(specialties) + len(procedures) + (0.5 * len(capabilities))
    infrastructure_depth_score = len(equipment) + len(capabilities) + (doctor_count / 10.0) + (capacity / 100.0)
    facility_size_proxy = (doctor_count / 5.0) + (capacity / 50.0) + area

    evidence_count = (
        len(specialties)
        + len(procedures)
        + len(equipment)
        + len(capabilities)
        + len(phones)
        + len(websites)
        + (1 if description else 0)
    )
    supporting_evidence_score = min(1.0, evidence_count / 8.0)
    support_ratio = min(1.0, infrastructure_depth_score / max(service_breadth_score, 1.0))
    specialty_plausibility_score = min(
        1.0,
        (infrastructure_depth_score + supporting_evidence_score) / max(len(specialties) + (0.5 * len(procedures)), 1.0),
    )
    support_score = round((0.55 * supporting_evidence_score) + (0.45 * support_ratio), 3)

    enriched.update(
        {
            "is_facility": is_facility,
            "is_ngo": is_ngo,
            "has_specialty": bool(specialties),
            "has_procedure": bool(procedures),
            "has_equipment": bool(equipment),
            "has_capability": bool(capabilities),
            "facility_size_proxy": round(facility_size_proxy, 3),
            "service_breadth_score": round(service_breadth_score, 3),
            "infrastructure_depth_score": round(infrastructure_depth_score, 3),
            "supporting_evidence_score": round(supporting_evidence_score, 3),
            "support_ratio": round(support_ratio, 3),
            "specialty_plausibility_score": round(specialty_plausibility_score, 3),
            "support_score": support_score,
            "evidence_count": evidence_count,
        }
    )
    return enriched


def build_feature_rows(records: list[dict[str, Any]], mapper: SpecialtyMapper | None = None) -> list[dict[str, Any]]:
    """Build feature rows for a list of normalized records."""

    return [build_feature_row(record, mapper=mapper) for record in records]


def build_long_fact_rows(records: list[dict[str, Any]], mapper: SpecialtyMapper | None = None) -> list[dict[str, Any]]:
    """Explode normalized records into long-form facts for retrieval."""

    mapper = mapper or SpecialtyMapper()
    fact_rows: list[dict[str, Any]] = []

    for record in records:
        base = {
            "unique_id": record.get("unique_id"),
            "name": record.get("name"),
            "region": record.get("region"),
            "facilityTypeId": record.get("facilityTypeId"),
            "operatorTypeId": record.get("operatorTypeId"),
        }
        for source_key, fact_type in [
            ("specialties_norm", "specialty"),
            ("procedure_norm", "procedure"),
            ("equipment_norm", "equipment"),
            ("capability_norm", "capability"),
        ]:
            for item in _list_value(record, source_key):
                fact_rows.append({**base, "fact_type": fact_type, "fact_text": item})

        description = record.get("description")
        parsed = parse_freeform_text(description, mapper=mapper)
        if parsed.evidence_sentences:
            for sentence in parsed.evidence_sentences:
                fact_rows.append({**base, "fact_type": "description", "fact_text": sentence["sentence"]})
        elif description:
            fact_rows.append({**base, "fact_type": "description", "fact_text": description})

    return fact_rows
