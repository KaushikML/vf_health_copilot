"""Transparent anomaly scoring based on evidence-support mismatches."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.utils.constants import CONFIGS_DIR

DEFAULT_RULES = {
    "weights": {
        "procedure_without_support": 0.35,
        "over_broad_small_facility": 0.30,
        "specialty_infrastructure_mismatch": 0.20,
        "evidence_thinness": 0.15,
    },
    "thresholds": {
        "over_broad_service_breadth": 5,
        "small_facility_size_proxy": 2.5,
        "low_infrastructure_depth": 1.5,
        "low_support_ratio": 0.4,
        "thin_evidence_count": 2,
        "high_anomaly_score": 0.65,
        "medium_anomaly_score": 0.35,
    },
    "severity_labels": {"high": "high", "medium": "medium", "low": "low"},
}


@dataclass
class AnomalyFinding:
    """A single transparent anomaly rule hit."""

    anomaly_type: str
    reason: str
    evidence_found: list[str]
    evidence_missing: list[str]
    severity: str
    confidence: float


def load_anomaly_rules(path: str | Path | None = None) -> dict[str, Any]:
    """Load anomaly rules from YAML or fall back to defaults."""

    if path is None:
        path = CONFIGS_DIR / "anomaly_rules.yaml"
    try:
        import yaml
    except Exception:
        return DEFAULT_RULES

    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or DEFAULT_RULES
    except FileNotFoundError:
        return DEFAULT_RULES


def _severity_from_score(score: float, rules: dict[str, Any]) -> str:
    thresholds = rules["thresholds"]
    if score >= thresholds["high_anomaly_score"]:
        return rules["severity_labels"]["high"]
    if score >= thresholds["medium_anomaly_score"]:
        return rules["severity_labels"]["medium"]
    return rules["severity_labels"]["low"]


def score_record(record: dict[str, Any], rules: dict[str, Any] | None = None) -> dict[str, Any]:
    """Score a single facility or NGO record for suspicious service claims."""

    rules = rules or DEFAULT_RULES
    weights = rules["weights"]
    thresholds = rules["thresholds"]

    procedures = list(record.get("procedure_norm", []))
    specialties = list(record.get("specialties_norm", []))
    equipment = list(record.get("equipment_norm", []))
    capabilities = list(record.get("capability_norm", []))

    findings: list[AnomalyFinding] = []

    if procedures and not equipment and not capabilities:
        findings.append(
            AnomalyFinding(
                anomaly_type="procedure-without-support",
                reason="Procedure claims exist without direct equipment or capability support.",
                evidence_found=procedures,
                evidence_missing=["equipment", "capability"],
                severity="high",
                confidence=0.85 if len(procedures) > 1 else 0.70,
            )
        )

    if (
        float(record.get("service_breadth_score", 0.0)) >= thresholds["over_broad_service_breadth"]
        and float(record.get("facility_size_proxy", 0.0)) <= thresholds["small_facility_size_proxy"]
    ):
        findings.append(
            AnomalyFinding(
                anomaly_type="over-broad-small-facility",
                reason="A small facility appears to claim unusually broad service coverage.",
                evidence_found=specialties + procedures,
                evidence_missing=["staffing depth", "capacity evidence"],
                severity="high",
                confidence=0.80,
            )
        )

    if specialties and float(record.get("infrastructure_depth_score", 0.0)) <= thresholds["low_infrastructure_depth"]:
        findings.append(
            AnomalyFinding(
                anomaly_type="specialty-infrastructure-mismatch",
                reason="Specialty claims are weakly supported by infrastructure signals.",
                evidence_found=specialties,
                evidence_missing=["equipment", "capability", "facility scale"],
                severity="medium",
                confidence=0.68,
            )
        )

    if (
        float(record.get("support_ratio", 0.0)) <= thresholds["low_support_ratio"]
        and int(record.get("evidence_count", 0)) <= thresholds["thin_evidence_count"]
        and float(record.get("service_breadth_score", 0.0)) > 1.0
    ):
        findings.append(
            AnomalyFinding(
                anomaly_type="evidence-thinness",
                reason="The record contains multiple service claims but limited supporting evidence.",
                evidence_found=specialties + procedures + capabilities,
                evidence_missing=["additional direct evidence"],
                severity="medium",
                confidence=0.60,
            )
        )

    anomaly_score = round(
        min(
            1.0,
            sum(weights[finding.anomaly_type.replace("-", "_")] * finding.confidence for finding in findings),
        ),
        3,
    )
    enriched = dict(record)
    enriched["anomaly_score"] = anomaly_score
    enriched["anomaly_severity"] = _severity_from_score(anomaly_score, rules)
    enriched["anomaly_findings"] = [asdict(finding) for finding in findings]
    return enriched


def score_anomalies(records: list[dict[str, Any]], rules: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Apply anomaly scoring to a list of records."""

    resolved_rules = rules or load_anomaly_rules()
    return [score_record(record, rules=resolved_rules) for record in records]
