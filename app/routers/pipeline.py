"""DataPulse AI — FastAPI router."""
import os
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from app.pipelines.etl_pipeline import ETLPipeline
from app.validators.quality_validator import DataQualityValidator

router = APIRouter()
pipeline = ETLPipeline()
validator = DataQualityValidator()


@router.post("/ingest")
async def ingest(file: UploadFile = File(...), ai: bool = Query(default=False)):
    content = (await file.read()).decode("utf-8", errors="replace")
    filename = file.filename or ""

    if filename.endswith(".csv"):
        rows, columns = pipeline.ingest_csv(content)
    elif filename.endswith(".json"):
        rows, columns = pipeline.ingest_json(content)
    else:
        raise HTTPException(400, "Supported formats: CSV, JSON")

    profile = pipeline.profile(rows, columns)
    quality = validator.validate(profile)

    ai_summary = None
    if ai and os.environ.get("ANTHROPIC_API_KEY"):
        try:
            from app.services.ai_insights import AIInsightsEngine
            engine = AIInsightsEngine()
            ai_summary = engine.summarize(profile, quality)
        except Exception:
            pass

    return {
        "filename": filename,
        "profile": {
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "columns": profile.columns,
            "dtypes": profile.dtypes,
            "null_counts": profile.null_counts,
            "duplicate_rows": profile.duplicate_rows,
            "sample": profile.sample_rows,
        },
        "quality": {
            "overall_score": quality.overall_score,
            "grade": quality.grade,
            "passed": quality.passed,
            "dimensions": [{"name": d.name, "score": d.score, "issues": d.issues} for d in quality.dimensions],
            "recommendations": quality.recommendations,
        },
        "ai_summary": ai_summary,
    }


@router.post("/transform")
async def transform(payload: dict):
    rows = payload.get("data", [])
    rules = payload.get("rules", {})
    if not rows:
        raise HTTPException(400, "No data provided.")
    result = pipeline.transform(rows, rules)
    return {
        "rows_processed": result.rows_processed,
        "rows_dropped": result.rows_dropped,
        "transformations_applied": result.transformations_applied,
        "data": result.data[:100],
    }


@router.get("/demo")
async def demo():
    csv_data = """name,age,email,salary
Alice,30,alice@example.com,75000
Bob,,bob@example.com,82000
Charlie,25,charlie@example.com,
Alice,30,alice@example.com,75000
Eve,28,eve@example.com,91000"""
    rows, columns = pipeline.ingest_csv(csv_data)
    profile = pipeline.profile(rows, columns)
    quality = validator.validate(profile)
    return {"profile": {"rows": profile.row_count, "columns": profile.columns, "nulls": profile.null_counts},
            "quality_score": quality.overall_score, "grade": quality.grade,
            "issues": quality.critical_issues, "recommendations": quality.recommendations}
