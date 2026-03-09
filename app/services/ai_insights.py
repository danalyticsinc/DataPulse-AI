"""
DataPulse AI — AI Insights Engine
Uses Claude AI to generate business-readable data quality summaries and recommendations.
"""
import os
import anthropic
from app.validators.quality_validator import QualityReport
from app.pipelines.etl_pipeline import DatasetProfile


class AIInsightsEngine:

    MODEL = "claude-opus-4-6"

    def __init__(self):
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set.")
        self.client = anthropic.Anthropic(api_key=api_key)

    def summarize(self, profile: DatasetProfile, quality: QualityReport) -> str:
        prompt = f"""You are a data quality analyst. Summarize this dataset report for a business stakeholder in 3-4 sentences. Be direct and actionable.

Dataset:
- Rows: {profile.row_count}, Columns: {profile.column_count}
- Duplicate rows: {profile.duplicate_rows}
- Null counts: {profile.null_counts}
- Column types: {profile.dtypes}

Quality Scores:
- Overall: {quality.overall_score}/100 (Grade: {quality.grade})
- Dimensions: {[(d.name, d.score) for d in quality.dimensions]}
- Issues: {quality.critical_issues}

Write a concise executive summary with the top 2 actions needed."""

        message = self.client.messages.create(
            model=self.MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
