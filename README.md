# DataPulse AI

> **Intelligent ETL pipeline with AI-powered data quality scoring** — ingest CSV/JSON, profile datasets, validate quality across 4 dimensions, and get Claude AI-generated business insights.

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Claude AI](https://img.shields.io/badge/Anthropic_Claude-AI_Insights-8B5CF6)](https://anthropic.com)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white)](https://docker.com)

---

## What It Does

| Feature | Detail |
|---|---|
| **Multi-format Ingestion** | CSV and JSON ingestion with automatic schema detection |
| **Dataset Profiling** | Row count, column types, null rates, duplicate detection |
| **4-Dimension Quality Scoring** | Completeness, Uniqueness, Consistency, Validity — scored 0-100 |
| **Data Transformation** | Drop nulls, deduplicate, rename, drop columns, normalize strings |
| **Claude AI Insights** | Business-readable quality summary with prioritized action items |
| **REST API** | FastAPI with interactive Swagger docs |
| **Docker Ready** | One-command deployment |

---

## Quality Dimensions

```
Completeness  — % of non-null values across all cells
Uniqueness    — % of non-duplicate rows
Consistency   — column type uniformity and structural integrity
Validity      — dataset size and schema soundness
```

## Quick Start

```bash
# Docker
export ANTHROPIC_API_KEY=sk-ant-...
docker build -t datapulse-ai . && docker run -p 8000:8000 -e ANTHROPIC_API_KEY datapulse-ai

# Local
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## API Reference

```bash
# Ingest and profile a CSV
curl -X POST http://localhost:8000/api/v1/ingest -F "file=@data.csv"

# Transform data
curl -X POST http://localhost:8000/api/v1/transform \
  -H "Content-Type: application/json" \
  -d '{"data": [...], "rules": {"drop_nulls": true, "drop_duplicates": true}}'

# Demo
curl http://localhost:8000/api/v1/demo
```

---

## Built By

Discovery Analytics Inc.
