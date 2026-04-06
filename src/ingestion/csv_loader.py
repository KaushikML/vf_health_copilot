"""CSV loading utilities for local and Databricks-compatible ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from src.utils.logging_utils import get_logger

LOGGER = get_logger(__name__)


@dataclass
class CSVLoadConfig:
    """Configuration for reading a CSV with pandas or Spark."""

    path: str | Path
    engine: Literal["auto", "pandas", "spark"] = "auto"
    delimiter: str = ","
    encoding: str = "utf-8"
    infer_schema: bool = True
    header: bool = True
    pandas_kwargs: dict[str, Any] = field(default_factory=dict)
    spark_options: dict[str, str] = field(default_factory=dict)


def _resolve_spark_session(spark: Any | None = None) -> Any | None:
    """Return the provided SparkSession or try the active session."""

    if spark is not None:
        return spark

    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:
        return None


def _load_with_pandas(config: CSVLoadConfig) -> Any:
    """Read CSV with pandas."""

    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover - dependency install issue
        raise ImportError("pandas is required for pandas CSV loading.") from exc

    read_kwargs = {
        "sep": config.delimiter,
        "encoding": config.encoding,
        "header": 0 if config.header else None,
        **config.pandas_kwargs,
    }
    LOGGER.info("Loading CSV with pandas from %s", config.path)
    return pd.read_csv(config.path, **read_kwargs)


def _load_with_spark(config: CSVLoadConfig, spark: Any) -> Any:
    """Read CSV with Spark."""

    reader = spark.read.option("header", str(config.header).lower()).option(
        "inferSchema", str(config.infer_schema).lower()
    )
    reader = reader.option("sep", config.delimiter).option("encoding", config.encoding)
    for key, value in config.spark_options.items():
        reader = reader.option(key, value)
    LOGGER.info("Loading CSV with Spark from %s", config.path)
    return reader.csv(str(config.path))


def load_csv(config: CSVLoadConfig, spark: Any | None = None) -> Any:
    """Load a CSV with pandas or Spark depending on the configured engine."""

    resolved_spark = _resolve_spark_session(spark)
    engine = config.engine

    if engine == "auto":
        engine = "spark" if resolved_spark is not None else "pandas"

    if engine == "spark":
        if resolved_spark is None:
            raise ValueError("Spark engine requested but no SparkSession is available.")
        return _load_with_spark(config, resolved_spark)

    if engine == "pandas":
        return _load_with_pandas(config)

    raise ValueError(f"Unsupported engine: {engine}")
