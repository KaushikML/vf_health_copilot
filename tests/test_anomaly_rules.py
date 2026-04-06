"""Tests for transparent anomaly scoring."""

from __future__ import annotations


def test_anomaly_scoring_flags_unsupported_procedure_claims(demo_pipeline: dict[str, object]) -> None:
    scored = demo_pipeline["scored"]
    tamale_row = next(row for row in scored if row["name"] == "Tamale Community Clinic")

    assert tamale_row["anomaly_score"] > 0
    anomaly_types = {finding["anomaly_type"] for finding in tamale_row["anomaly_findings"]}
    assert "procedure-without-support" in anomaly_types
    assert "specialty-infrastructure-mismatch" in anomaly_types
