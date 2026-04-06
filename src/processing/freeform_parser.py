"""Conservative free-form parsing for direct-evidence extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from src.processing.specialty_mapper import SpecialtyMapper


@dataclass
class ParsedFreeformEvidence:
    """Structured evidence extracted from a description field."""

    specialties: list[str] = field(default_factory=list)
    procedures: list[str] = field(default_factory=list)
    equipment: list[str] = field(default_factory=list)
    capabilities: list[str] = field(default_factory=list)
    evidence_sentences: list[dict[str, Any]] = field(default_factory=list)


def split_sentences(text: str | None) -> list[str]:
    """Split free-form text into simple sentence-like spans."""

    if not text:
        return []
    pieces = re.split(r"[.;\n]+", text)
    return [piece.strip() for piece in pieces if piece.strip()]


def parse_freeform_text(text: str | None, mapper: SpecialtyMapper | None = None) -> ParsedFreeformEvidence:
    """Parse descriptions conservatively using direct keyword matches only."""

    if not text:
        return ParsedFreeformEvidence()

    mapper = mapper or SpecialtyMapper()
    parsed = ParsedFreeformEvidence()
    for sentence in split_sentences(text):
        matches = mapper.extract_from_text(sentence)
        if not matches:
            continue
        if "specialties" in matches:
            parsed.specialties.extend(matches["specialties"])
        if "procedures" in matches:
            parsed.procedures.extend(matches["procedures"])
        if "equipment" in matches:
            parsed.equipment.extend(matches["equipment"])
        if "capabilities" in matches:
            parsed.capabilities.extend(matches["capabilities"])
        parsed.evidence_sentences.append({"sentence": sentence, "matches": matches})

    parsed.specialties = mapper.map_terms(parsed.specialties, "specialties")
    parsed.procedures = mapper.map_terms(parsed.procedures, "procedures")
    parsed.equipment = mapper.map_terms(parsed.equipment, "equipment")
    parsed.capabilities = mapper.map_terms(parsed.capabilities, "capabilities")
    return parsed
