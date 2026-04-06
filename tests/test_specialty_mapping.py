"""Tests for config-driven specialty and evidence mapping."""

from __future__ import annotations

from src.processing.specialty_mapper import SpecialtyMapper


def test_specialty_mapper_canonicalizes_aliases() -> None:
    mapper = SpecialtyMapper()
    assert mapper.canonicalize("Cardiac", "specialties") == "cardiology"
    assert mapper.canonicalize("c-section", "procedures") == "caesarean section"


def test_specialty_mapper_extracts_direct_text_evidence() -> None:
    mapper = SpecialtyMapper()
    matches = mapper.extract_from_text("The facility has ultrasound support and emergency care.")
    assert matches["equipment"] == ["ultrasound"]
    assert matches["capabilities"] == ["emergency care"]
