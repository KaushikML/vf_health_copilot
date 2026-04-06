"""Safe MLflow hooks that degrade to no-op behavior when unavailable."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)

try:
    import mlflow
except Exception:  # pragma: no cover - import protection is intentional
    mlflow = None


@dataclass
class TracePayload:
    """Trace payload for user query execution."""

    user_query: str
    classified_intent: str
    extracted_filters: dict[str, Any] = field(default_factory=dict)
    retrieval_path: str = ""
    sql_used: list[str] = field(default_factory=list)
    retrieved_fact_rows: list[dict[str, Any]] = field(default_factory=list)
    anomaly_rules_triggered: list[dict[str, Any]] = field(default_factory=list)
    final_answer_text: str = ""


class SafeMLflowTracker:
    """A minimal tracker wrapper that never makes MLflow mandatory."""

    def __init__(self, enabled: bool = False, experiment_name: str | None = None) -> None:
        self.enabled = enabled and mlflow is not None
        self.experiment_name = experiment_name
        self.last_trace: dict[str, Any] | None = None

        if self.enabled and experiment_name:
            try:
                mlflow.set_experiment(experiment_name)
            except Exception as exc:  # pragma: no cover - environment specific
                LOGGER.warning("MLflow experiment setup failed: %s", exc)
                self.enabled = False

    def start_run(self, run_name: str | None = None) -> None:
        """Start an MLflow run when available."""

        if not self.enabled:
            return
        try:
            mlflow.start_run(run_name=run_name)
        except Exception as exc:  # pragma: no cover - environment specific
            LOGGER.warning("MLflow start_run failed: %s", exc)
            self.enabled = False

    def end_run(self) -> None:
        """End the active MLflow run if one exists."""

        if not self.enabled:
            return
        try:
            mlflow.end_run()
        except Exception as exc:  # pragma: no cover - environment specific
            LOGGER.warning("MLflow end_run failed: %s", exc)

    def log_trace(self, payload: TracePayload) -> None:
        """Log a trace payload or keep it in memory when MLflow is disabled."""

        self.last_trace = asdict(payload)
        if not self.enabled:
            return

        try:
            mlflow.log_param("classified_intent", payload.classified_intent)
            mlflow.log_param("retrieval_path", payload.retrieval_path)
            mlflow.log_dict(self.last_trace, "trace_payload.json")
            mlflow.log_text(payload.final_answer_text or "", "final_answer.txt")
        except Exception as exc:  # pragma: no cover - environment specific
            LOGGER.warning("MLflow trace logging failed: %s", exc)

    def log_json_artifact(self, payload: dict[str, Any], artifact_file: str) -> None:
        """Write an arbitrary JSON artifact when MLflow is enabled."""

        if not self.enabled:
            return
        try:
            mlflow.log_text(json.dumps(payload, indent=2), artifact_file)
        except Exception as exc:  # pragma: no cover - environment specific
            LOGGER.warning("MLflow artifact logging failed: %s", exc)
