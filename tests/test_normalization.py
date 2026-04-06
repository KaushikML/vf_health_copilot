"""Normalization tests for structured and list-like fields."""

from __future__ import annotations

from src.processing.normalizers import normalize_record, parse_list_like
from src.processing.specialty_mapper import SpecialtyMapper


def test_parse_list_like_handles_malformed_arrays() -> None:
    assert parse_list_like("[Cardiology, Obstetrics]") == ["Cardiology", "Obstetrics"]
    assert parse_list_like('["X-Ray", "Ultrasound"]') == ["X-Ray", "Ultrasound"]
    assert parse_list_like("null") == []


def test_normalize_record_canonicalizes_fields() -> None:
    mapper = SpecialtyMapper()
    record = {
        "name": " Tema General Hospital ",
        "facilityTypeId": "General Hospital",
        "operatorTypeId": "Government",
        "region": "greater accra region",
        "country": "ghana",
        "specialties": '["Cardiac"]',
        "procedure": "[]",
        "equipment": '["Ultrasound"]',
        "capability": '["Emergency services"]',
        "phone_numbers": '["+233-555-0101"]',
        "websites": "[]",
        "affiliationTypeIds": "[]",
        "description": "  Cardiac clinic with emergency services.  ",
    }

    normalized = normalize_record(record, specialty_mapper=mapper)

    assert normalized["facilityTypeId"] == "hospital"
    assert normalized["operatorTypeId"] == "public"
    assert normalized["region"] == "Greater Accra"
    assert normalized["country"] == "Ghana"
    assert normalized["specialties_norm"] == ["cardiology"]
    assert normalized["capability_norm"] == ["emergency care"]
