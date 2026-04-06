# Databricks notebook source
import zipfile
from pathlib import Path

zip_path = "/Volumes/workspace/default/vf_health_data/vf-health-code.zip"
extract_path = "/tmp/vf-health-copilot"

Path(extract_path).mkdir(parents=True, exist_ok=True)

with zipfile.ZipFile(zip_path, "r") as zf:
    zf.extractall(extract_path)

print("Extracted to:", extract_path)

# COMMAND ----------

import sys
sys.path.insert(0, "/tmp/vf-health-copilot/vf-health-copilot")

# COMMAND ----------

import os
print(os.listdir("/tmp/vf-health-copilot"))

# COMMAND ----------

print(os.listdir("/tmp/vf-health-copilot/vf-health-copilot"))

# COMMAND ----------

# DBTITLE 1,Cell 5
# Databricks notebook source
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def _project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:  # pragma: no cover - Databricks notebook runtime
        return Path("/tmp/vf-health-copilot/vf-health-copilot")


PROJECT_ROOT = _project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import yaml

from src.ingestion.csv_loader import CSVLoadConfig, load_csv
from src.ingestion.schema_casting import ColumnSchema, cast_pandas_frame, cast_records, cast_spark_frame
from src.utils.constants import DEFAULT_TABLE_NAMES


def load_app_config() -> dict[str, Any]:
    with (PROJECT_ROOT / "configs" / "app_config.yaml").open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_spark() -> Any | None:
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:
        return None


def default_schema() -> list[ColumnSchema]:
    return [
        ColumnSchema("unique_id", "string"),
        ColumnSchema("name", "string"),
        ColumnSchema("facilityTypeId", "string"),
        ColumnSchema("operatorTypeId", "string"),
        ColumnSchema("region", "string"),
        ColumnSchema("city", "string"),
        ColumnSchema("country", "string"),
        ColumnSchema("specialties", "string"),
        ColumnSchema("procedure", "string"),
        ColumnSchema("equipment", "string"),
        ColumnSchema("capability", "string"),
        ColumnSchema("description", "string"),
        ColumnSchema("doctor_count", "float", default=0.0),
        ColumnSchema("capacity", "float", default=0.0),
        ColumnSchema("area", "float", default=0.0),
        ColumnSchema("phone_numbers", "string"),
        ColumnSchema("websites", "string"),
        ColumnSchema("affiliationTypeIds", "string"),
    ]


def ingest_raw_data(raw_csv_path: str | None = None, spark: Any | None = None) -> Any:
    if raw_csv_path:
        path = raw_csv_path
        config = {"ingestion": {"csv_engine": "auto", "delimiter": ",", "encoding": "utf-8", "infer_schema": True}}
    else:
        config = load_app_config()
        path = config["paths"]["raw_csv_path"]
    
    if not path:
        raise ValueError("TODO: provide a raw CSV path through config or notebook parameters.")
    spark = spark or get_spark()

    loader_config = CSVLoadConfig(
        path=path,
        engine=config["ingestion"]["csv_engine"],
        delimiter=config["ingestion"]["delimiter"],
        encoding=config["ingestion"]["encoding"],
        infer_schema=config["ingestion"]["infer_schema"],
    )
    frame = load_csv(loader_config, spark=spark)
    return frame


def write_local_bronze(frame: Any) -> Path:
    output_dir = Path("/tmp/vf-health-copilot/vf-health-copilot/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{DEFAULT_TABLE_NAMES['bronze_vf_ghana_raw']}.json"

    if hasattr(frame, "to_dict"):
        target.write_text(json.dumps(frame.to_dict(orient="records"), indent=2), encoding="utf-8")
    else:
        records = [row.asDict(recursive=True) for row in frame.collect()]
        target.write_text(json.dumps(records, indent=2), encoding="utf-8")
    return target


if __name__ == "__main__":
    bronze_frame = ingest_raw_data(
        raw_csv_path="/Volumes/workspace/default/vf_health_data/Virtue Foundation Ghana v0.3 - Sheet1.csv"
    )
    bronze_path = write_local_bronze(bronze_frame)
    print(f"Wrote local Bronze extract to {bronze_path}")

# COMMAND ----------

display(bronze_frame.limit(10) if hasattr(bronze_frame, "limit") else bronze_frame.head(10))

# COMMAND ----------

if hasattr(bronze_frame, "count"):
    print("Row count:", bronze_frame.count())
else:
    print("Row count:", len(bronze_frame))

# COMMAND ----------

bronze_table_name = "workspace.default.bronze_vf_ghana_raw"
print(bronze_table_name)

# COMMAND ----------

def clean_column_names(df):
    import re
    
    def clean(col):
        col = col.strip()
        col = col.lower()
        col = re.sub(r"[^\w]+", "_", col)   # replace special chars with _
        col = re.sub(r"_+", "_", col)       # remove duplicate _
        return col.strip("_")

    return df.toDF(*[clean(c) for c in df.columns])

# Apply cleaning
if hasattr(bronze_frame, "columns"):
    bronze_frame = clean_column_names(bronze_frame)

print("Cleaned columns:", bronze_frame.columns)

# COMMAND ----------

bronze_frame.write.mode("overwrite").format("delta").saveAsTable(bronze_table_name)

# COMMAND ----------

display(spark.table("workspace.default.bronze_vf_ghana_raw"))

# COMMAND ----------

