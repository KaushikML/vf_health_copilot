"""Duplicate detection helpers with confidence scoring but no auto-merge."""

from __future__ import annotations

import hashlib
import itertools
import re
from dataclasses import dataclass
from typing import Any

from src.processing.normalizers import normalize_whitespace, parse_list_like


@dataclass
class DuplicatePair:
    """Potential duplicate pair between two organizations."""

    left_id: str
    right_id: str
    confidence: float
    reasons: list[str]
    group_id: str


def _normalize_phone_numbers(record: dict[str, Any]) -> set[str]:
    phones = parse_list_like(record.get("phone_numbers_norm") or record.get("phone_numbers"))
    return {re.sub(r"\D", "", phone) for phone in phones if re.sub(r"\D", "", phone)}


def _normalize_websites(record: dict[str, Any]) -> set[str]:
    websites = parse_list_like(record.get("websites_norm") or record.get("websites"))
    return {website.lower().rstrip("/") for website in websites if website}


def _address_signature(record: dict[str, Any]) -> str:
    parts = [
        normalize_whitespace(record.get("address")),
        normalize_whitespace(record.get("city")),
        normalize_whitespace(record.get("region")),
    ]
    usable = [part.lower() for part in parts if part]
    return " | ".join(usable)


def _name_signature(record: dict[str, Any]) -> str:
    name = normalize_whitespace(record.get("name"))
    return re.sub(r"[^a-z0-9]+", "", name.lower()) if name else ""


def _component_groups(pairs: list[DuplicatePair]) -> dict[str, list[str]]:
    """Collapse duplicate pairs into connected components."""

    adjacency: dict[str, set[str]] = {}
    for pair in pairs:
        adjacency.setdefault(pair.left_id, set()).add(pair.right_id)
        adjacency.setdefault(pair.right_id, set()).add(pair.left_id)

    visited: set[str] = set()
    groups: dict[str, list[str]] = {}
    for node in adjacency:
        if node in visited:
            continue
        stack = [node]
        members: list[str] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            members.append(current)
            stack.extend(adjacency.get(current, set()) - visited)
        group_hash = hashlib.md5("|".join(sorted(members)).encode("utf-8")).hexdigest()[:10]
        groups[group_hash] = sorted(members)
    return groups


def detect_duplicate_pairs(records: list[dict[str, Any]], min_confidence: float = 0.5) -> list[DuplicatePair]:
    """Return duplicate candidates with reasons and grouping metadata."""

    working = [dict(record) for record in records]
    provisional_pairs: list[DuplicatePair] = []

    for left, right in itertools.combinations(working, 2):
        left_id = str(left.get("unique_id") or left.get("name"))
        right_id = str(right.get("unique_id") or right.get("name"))
        reasons: list[str] = []
        score = 0.0

        if left.get("unique_id") and left.get("unique_id") == right.get("unique_id"):
            reasons.append("same_unique_id")
            score += 0.95
        if _name_signature(left) and _name_signature(left) == _name_signature(right):
            reasons.append("same_name")
            score += 0.25
        if _normalize_websites(left) & _normalize_websites(right):
            reasons.append("same_website")
            score += 0.40
        if _normalize_phone_numbers(left) & _normalize_phone_numbers(right):
            reasons.append("same_phone")
            score += 0.35
        if _address_signature(left) and _address_signature(left) == _address_signature(right):
            reasons.append("same_address_city_region")
            score += 0.30

        confidence = min(score, 0.99)
        if confidence >= min_confidence:
            provisional_pairs.append(
                DuplicatePair(
                    left_id=left_id,
                    right_id=right_id,
                    confidence=round(confidence, 3),
                    reasons=reasons,
                    group_id="",
                )
            )

    groups = _component_groups(provisional_pairs)
    finalized_pairs: list[DuplicatePair] = []
    for pair in provisional_pairs:
        group_id = next(
            (
                existing_group_id
                for existing_group_id, members in groups.items()
                if pair.left_id in members and pair.right_id in members
            ),
            "",
        )
        finalized_pairs.append(
            DuplicatePair(
                left_id=pair.left_id,
                right_id=pair.right_id,
                confidence=pair.confidence,
                reasons=pair.reasons,
                group_id=group_id,
            )
        )
    return finalized_pairs
