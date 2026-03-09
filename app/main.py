"""DataPulse AI — FastAPI entry point."""
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.pipeline import router

app = FastAPI(
    title="DataPulse AI",
    description="Intelligent ETL pipeline with AI-powered data quality scoring and insights.",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(router, prefix="/api/v1", tags=["pipeline"])

@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0", "ai_enabled": bool(os.environ.get("ANTHROPIC_API_KEY"))}

@app.get("/")
def root():
    return {"name": "DataPulse AI", "docs": "/docs",
            "endpoints": {"ingest": "POST /api/v1/ingest", "transform": "POST /api/v1/transform", "demo": "GET /api/v1/demo"}}
