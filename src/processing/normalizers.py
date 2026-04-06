"""Normalization helpers for structured and list-like facility data."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Mapping
from typing import Any

from src.utils.constants import COUNTRY_ALIASES, GHANA_REGION_ALIASES, LIST_LIKE_COLUMNS, NULL_LIKE_TOKENS

FACILITY_TYPE_ALIASES = {
    "general hospital": "hospital",
    "district hospital": "hospital",
    "regional hospital": "hospital",
    "hospital": "hospital",
    "clinic": "clinic",
    "medical clinic": "clinic",
    "diagnostic center": "diagnostic centre",
    "diagnostic centre": "diagnostic centre",
    "imaging center": "diagnostic centre",
    "imaging centre": "diagnostic centre",
    "ngo": "ngo",
    "non-governmental organization": "ngo",
    "non governmental organization": "ngo",
    "health post": "health post",
}

OPERATOR_TYPE_ALIASES = {
    "government": "public",
    "public": "public",
    "private": "private",
    "for profit": "private",
    "faith based": "faith-based",
    "faith-based": "faith-based",
    "mission": "faith-based",
    "ngo": "ngo",
    "nonprofit": "ngo",
    "non profit": "ngo",
}


def is_null_like(value: Any) -> bool:
    """Return True when a value should be treated as missing."""

    if value is None:
        return True
    if isinstance(value, float) and str(value).lower() == "nan":
        return True
    if isinstance(value, str):
        return value.strip().lower() in NULL_LIKE_TOKENS
    if isinstance(value, (list, tuple, set, dict)) and len(value) == 0:
        return True
    return False


def normalize_whitespace(value: Any) -> str | None:
    """Trim and collapse whitespace for free text."""

    if is_null_like(value):
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def _split_loose_list(value: str) -> list[str]:
    """Split loosely formatted list-like strings."""

    stripped = value.strip().strip("[](){}")
    if not stripped:
        return []
    parts = re.split(r"\s*[;,|]\s*|\s*,\s*", stripped)
    return [part.strip(" '\"") for part in parts if part.strip(" '\"")]


def parse_list_like(value: Any) -> list[str]:
    """Parse list-like values from malformed strings, JSON, or Python literals."""

    if is_null_like(value):
        return []

    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if is_null_like(stripped):
            return []

        parsed: Any | None = None
        if stripped.startswith(("[", "(")) and stripped.endswith(("]", ")")):
            for loader in (json.loads, ast.literal_eval):
                try:
                    parsed = loader(stripped)
                    break
                except Exception:
                    continue
        items = parsed if isinstance(parsed, (list, tuple)) else _split_loose_list(stripped)
    else:
        items = [value]

    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = normalize_whitespace(item)
        if text is None:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


def canonicalize_region(value: Any) -> str | None:
    """Canonicalize Ghana region values conservatively."""

    text = normalize_whitespace(value)
    if text is None:
        return None
    lowered = text.lower()
    return GHANA_REGION_ALIASES.get(lowered, text.title() if text.islower() else text)


def canonicalize_country(value: Any) -> str | None:
    """Canonicalize country names conservatively."""

    text = normalize_whitespace(value)
    if text is None:
        return None
    lowered = text.lower()
    return COUNTRY_ALIASES.get(lowered, text.title() if text.islower() else text)


def canonicalize_facility_type(value: Any) -> str | None:
    """Canonicalize facility types."""

    text = normalize_whitespace(value)
    if text is None:
        return None
    lowered = text.lower()
    return FACILITY_TYPE_ALIASES.get(lowered, lowered)


def canonicalize_operator_type(value: Any) -> str | None:
    """Canonicalize operator types."""

    text = normalize_whitespace(value)
    if text is None:
        return None
    lowered = text.lower()
    return OPERATOR_TYPE_ALIASES.get(lowered, lowered)


def normalize_record(record: Mapping[str, Any], specialty_mapper: Any | None = None) -> dict[str, Any]:
    """Normalize a single raw organization record while preserving raw fields."""

    normalized = dict(record)
    normalized["region_raw"] = record.get("region")
    normalized["country_raw"] = record.get("country")
    normalized["facilityTypeId_raw"] = record.get("facilityTypeId")
    normalized["operatorTypeId_raw"] = record.get("operatorTypeId")

    normalized["region"] = canonicalize_region(record.get("region"))
    normalized["country"] = canonicalize_country(record.get("country"))
    normalized["facilityTypeId"] = canonicalize_facility_type(record.get("facilityTypeId"))
    normalized["operatorTypeId"] = canonicalize_operator_type(record.get("operatorTypeId"))
    normalized["name"] = normalize_whitespace(record.get("name"))
    normalized["city"] = normalize_whitespace(record.get("city"))
    normalized["description"] = normalize_whitespace(record.get("description"))

    mapping_sections = {
        "specialties": "specialties",
        "procedure": "procedures",
        "equipment": "equipment",
        "capability": "capabilities",
    }

    for column in LIST_LIKE_COLUMNS:
        raw_value = record.get(column)
        normalized[f"{column}_raw"] = raw_value
        parsed = parse_list_like(raw_value)
        if specialty_mapper and column in mapping_sections:
            parsed = specialty_mapper.map_terms(parsed, mapping_sections[column])
        normalized[f"{column}_norm"] = parsed

    return normalized


def normalize_records(records: list[Mapping[str, Any]], specialty_mapper: Any | None = None) -> list[dict[str, Any]]:
    """Normalize a list of records."""

    return [normalize_record(record, specialty_mapper=specialty_mapper) for record in records]
