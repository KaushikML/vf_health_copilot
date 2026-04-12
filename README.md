# Virtue Health Planning Copilot

A Databricks-powered AI planning system that helps NGOs identify healthcare gaps, detect suspicious facility claims, and make data-driven infrastructure decisions using Ghana facility data.

---

## Overview

Virtue Health Planning Copilot is a hybrid AI system built on Databricks that enables non-technical planners to:

- Understand healthcare coverage across regions
- Identify underserved areas
- Detect suspicious or overstated facility claims
- Explore services offered by healthcare facilities
- Make evidence-based planning decisions

The system combines structured analytics, semantic retrieval with FAISS, and rule-based intelligence to deliver transparent and actionable insights.

---

## Problem

NGO planners often lack tools to:

- Identify where healthcare services exist
- Detect gaps in infrastructure and care
- Validate facility claims
- Make data-driven investment decisions

This leads to inefficient resource allocation and missed critical healthcare needs.

---

## Solution

This project builds a Databricks-based planning system that:

- Processes structured and semi-structured healthcare data
- Supports natural language-style planner queries
- Retrieves evidence from facility facts
- Detects anomalies in facility claims
- Highlights underserved regions
- Provides planner-oriented recommendations

---

## Architecture

### Core Components

- **Data Pipeline** → PySpark notebooks (Bronze → Silver → Gold)
- **Storage** → Delta Tables (Unity Catalog)
- **Retrieval** → SQL + FAISS (hybrid)
- **Logic** → Query classification + anomaly rules
- **Frontend** → Streamlit (Databricks Apps)

---

## Databricks Components Used

- Databricks Notebooks
- Delta Lake
- Unity Catalog
- SQL Warehouse
- Databricks Apps

---

## Features

- Natural language query interface
- Hybrid retrieval (SQL + FAISS)
- Facility-level evidence tracing
- Region-level planning insights
- Rule-based anomaly detection
- Interactive map visualization
- Planner recommendations

---

## Example Queries

- How many hospitals have cardiology?
- What services does Korle Bu Teaching Hospital offer?
- Which regions should VF prioritize for emergency care investment?
- Which facilities may be overstating capabilities?

---

## Project Structure

```text
vf-health-copilot/
├── app/
│   ├── app.py
│   ├── components.py
│   ├── map_views.py
│   ├── requirements.txt
│   └── app.yaml
├── notebooks/
│   ├── 01_ingest_raw_data.py
│   ├── 02_clean_normalize.py
│   ├── 03_build_gold_tables.py
│   ├── 04_build_vector_index.py
│   └── 05_agent_query_testing.py
├── src/
├── configs/
├── tests/
├── databricks.yml
├── requirements.txt
├── README.md
└── LICENSE
```

---

## Pipeline Flow

1. Raw Ghana dataset is ingested
2. Cleaned and normalized (Silver layer)
3. Gold tables created:
   - `gold_facility_master`
   - `gold_region_summary`
   - `gold_facility_facts_long`
4. FAISS index built for semantic search
5. Streamlit app queries these tables

---

## Streamlit App Configuration

The app connects directly to Databricks using:

```python
HOST = "<YOUR-DATABRICKS-HOST>"
HTTP_PATH = "<YOUR-SQL-WAREHOUSE-HTTP-PATH>"
TOKEN = "<YOUR-DATABRICKS-TOKEN>"
```

Replace these in `app/app.py`.

---

## How to Get These Values

### HOST

From your browser URL:

```text
https://dbc-xxxx.cloud.databricks.com
```

Use:

```python
HOST = "dbc-xxxx.cloud.databricks.com"
```

---

### HTTP_PATH

Go to:

SQL Warehouses → Select Warehouse → Connection Details → Copy HTTP Path

Example:

```python
HTTP_PATH = "/sql/1.0/warehouses/xxxxxxxx"
```

---

### TOKEN

Go to:

User Settings → Developer → Access Tokens → Generate Token

Example:

```python
TOKEN = "dapiXXXXXXXXXXXX"
```

---

## Important Note on Secrets

- Do NOT upload real tokens to GitHub
- Keep placeholders in the repo
- Paste real values only inside Databricks App editor

---

## Running the Pipeline

Run notebooks in this order:

1. Ingest
2. Clean
3. Gold
4. Vector Index
5. Query System

---

## Deploying the App

1. Open Databricks Apps
2. Create or open your app
3. Replace template files with your `app/` folder files
4. Fill in HOST, HTTP_PATH, TOKEN
5. Click Deploy
6. Open your app

---

## Demo Flow

1. Run a count query
2. Run a facility query
3. Run planner query
4. Run anomaly query
5. Show notebooks
6. Show repository

---

## Tech Stack

- Python
- PySpark
- Databricks
- Delta Lake
- FAISS
- Streamlit
- Pandas, NumPy
- Plotly

---

## Key Innovations

- Hybrid retrieval (SQL + FAISS)
- Planner-first outputs
- Transparent anomaly detection
- Evidence-backed responses

---

## Notes

- Built for Databricks execution
- Uses SQL + vector retrieval
- Some notebooks include setup commands

---

## Demo

https://drive.google.com/file/d/15jOKU1fY5x3sIp5gZ_0sxnBmztslMV1m/view?usp=sharing

---

## License

MIT License
