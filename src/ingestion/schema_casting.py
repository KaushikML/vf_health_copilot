"""Schema casting helpers that work with plain records, pandas, or Spark."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class ColumnSchema:
    """A simple schema hint for a column."""

    name: str
    dtype: str
    nullable: bool = True
    default: Any | None = None


def _coerce_value(value: Any, dtype: str, default: Any | None = None) -> Any:
    """Coerce a scalar value to a target type."""

    if value is None or value == "":
        return default

    if dtype == "string":
        return str(value)
    if dtype == "int":
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default
    if dtype == "float":
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    if dtype == "bool":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "yes", "y"}
    if dtype == "list":
        from src.processing.normalizers import parse_list_like

        return parse_list_like(value)
    return value


def cast_record(record: dict[str, Any], schema: Iterable[ColumnSchema]) -> dict[str, Any]:
    """Cast a single record according to schema hints."""

    updated = dict(record)
    for column in schema:
        updated[column.name] = _coerce_value(updated.get(column.name), column.dtype, column.default)
    return updated


def cast_records(records: Iterable[dict[str, Any]], schema: Iterable[ColumnSchema]) -> list[dict[str, Any]]:
    """Cast a list of records according to schema hints."""

    schema_list = list(schema)
    return [cast_record(record, schema_list) for record in records]


def cast_pandas_frame(frame: Any, schema: Iterable[ColumnSchema]) -> Any:
    """Apply schema hints to a pandas DataFrame without requiring pandas at import time."""

    schema_list = list(schema)
    for column in schema_list:
        if column.name not in frame.columns:
            continue
        if column.dtype == "string":
            frame[column.name] = frame[column.name].astype("string")
        elif column.dtype == "int":
            frame[column.name] = frame[column.name].apply(
                lambda value: _coerce_value(value, "int", column.default)
            )
        elif column.dtype == "float":
            frame[column.name] = frame[column.name].apply(
                lambda value: _coerce_value(value, "float", column.default)
            )
        elif column.dtype == "bool":
            frame[column.name] = frame[column.name].apply(
                lambda value: _coerce_value(value, "bool", column.default)
            )
        elif column.dtype == "list":
            frame[column.name] = frame[column.name].apply(
                lambda value: _coerce_value(value, "list", column.default)
            )
    return frame


def cast_spark_frame(frame: Any, schema: Iterable[ColumnSchema]) -> Any:
    """Apply basic Spark casts when Spark DataFrames are used."""

    try:
        from pyspark.sql import functions as F
    except Exception as exc:  # pragma: no cover - dependency install issue
        raise ImportError("pyspark is required for Spark schema casting.") from exc

    updated = frame
    for column in schema:
        if column.name not in updated.columns:
            continue
        if column.dtype in {"string", "int", "float", "bool"}:
            spark_type = {
                "string": "string",
                "int": "int",
                "float": "double",
                "bool": "boolean",
            }[column.dtype]
            updated = updated.withColumn(column.name, F.col(column.name).cast(spark_type))
    return updated
