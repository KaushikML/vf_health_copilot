"""Config-driven canonicalization for specialties and related vocabularies."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from src.utils.constants import CONFIGS_DIR

DEFAULT_MAPPING: dict[str, dict[str, list[str]]] = {
    "specialties": {
        "cardiology": ["cardiology", "cardiac", "heart care"],
        "obstetrics": ["obstetrics", "maternal care", "maternity"],
        "emergency surgery": ["emergency surgery", "trauma surgery", "surgical emergency"],
        "radiology": ["radiology", "diagnostic imaging", "imaging"],
        "dialysis": ["dialysis", "renal replacement"],
        "oncology": ["oncology", "cancer care"],
    },
    "procedures": {
        "caesarean section": ["caesarean section", "c section", "c-section"],
        "appendectomy": ["appendectomy", "appendix surgery"],
        "catheterization": ["catheterization", "cath lab"],
        "dialysis procedure": ["dialysis", "hemodialysis"],
    },
    "equipment": {
        "x-ray": ["x-ray", "xray"],
        "ultrasound": ["ultrasound", "sonography"],
        "ventilator": ["ventilator", "ventilators"],
        "operating theatre": ["operating theatre", "operating room", "theatre"],
        "dialysis machine": ["dialysis machine", "hemodialysis machine"],
    },
    "capabilities": {
        "emergency care": ["emergency care", "emergency services", "emergency response"],
        "inpatient surgery": ["inpatient surgery", "surgical admissions"],
        "intensive care": ["intensive care", "icu"],
        "blood bank": ["blood bank", "transfusion support"],
    },
    "facility_types": {
        "hospital": ["hospital", "general hospital", "district hospital", "regional hospital"],
        "clinic": ["clinic", "medical clinic"],
        "diagnostic centre": ["diagnostic centre", "diagnostic center", "imaging centre", "imaging center"],
        "ngo": ["ngo", "non governmental organization", "non-governmental organization"],
        "health post": ["health post", "outreach post"],
    },
    "operator_types": {
        "public": ["public", "government", "ministry"],
        "private": ["private", "for profit"],
        "faith-based": ["faith-based", "mission", "church"],
        "ngo": ["ngo", "nonprofit", "non profit"],
    },
}


@dataclass
class SpecialtyMapper:
    """Canonicalize raw terms to direct-evidence vocabularies."""

    mapping: dict[str, dict[str, list[str]]] = field(default_factory=lambda: DEFAULT_MAPPING.copy())

    @classmethod
    def from_yaml(cls, path: str | Path | None = None) -> "SpecialtyMapper":
        """Load mappings from YAML, falling back to the internal default mapping."""

        resolved_path = Path(path) if path else CONFIGS_DIR / "specialty_mapping.yaml"
        try:
            import yaml
        except Exception as exc:  # pragma: no cover - dependency install issue
            raise ImportError("PyYAML is required to load specialty mappings from YAML.") from exc

        with resolved_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        return cls(mapping=loaded)

    def _lookup(self, section: str) -> dict[str, str]:
        """Return alias-to-canonical lookup for a mapping section."""

        lookup: dict[str, str] = {}
        for canonical, aliases in self.mapping.get(section, {}).items():
            lookup[canonical.lower()] = canonical
            for alias in aliases:
                lookup[alias.lower()] = canonical
        return lookup

    def canonicalize(self, value: str | None, section: str) -> str | None:
        """Canonicalize a single term if there is a direct alias match."""

        if not value:
            return None
        lookup = self._lookup(section)
        lowered = value.strip().lower()
        return lookup.get(lowered, value.strip())

    def map_terms(self, values: list[str], section: str) -> list[str]:
        """Map and deduplicate a list of terms."""

        canonical_terms: list[str] = []
        seen: set[str] = set()
        for value in values:
            canonical = self.canonicalize(value, section)
            if canonical is None:
                continue
            lowered = canonical.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            canonical_terms.append(canonical)
        return canonical_terms

    def extract_from_text(self, text: str | None, sections: list[str] | None = None) -> dict[str, list[str]]:
        """Extract direct term mentions from text without free inference."""

        if not text:
            return {}

        lowered = text.lower()
        section_names = sections or ["specialties", "procedures", "equipment", "capabilities"]
        matches: dict[str, list[str]] = {}
        for section in section_names:
            section_matches: list[str] = []
            for canonical, aliases in self.mapping.get(section, {}).items():
                candidate_aliases = sorted({canonical, *aliases}, key=len, reverse=True)
                for alias in candidate_aliases:
                    pattern = rf"(?<!\w){re.escape(alias.lower())}(?!\w)"
                    if re.search(pattern, lowered):
                        section_matches.append(canonical)
                        break
            if section_matches:
                matches[section] = self.map_terms(section_matches, section)
        return matches
