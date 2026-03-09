"""
DataPulse AI — Data Quality Validator
Scores dataset quality across completeness, consistency, validity, and uniqueness.
"""
from dataclasses import dataclass, field
from app.pipelines.etl_pipeline import DatasetProfile


@dataclass
class QualityDimension:
    name: str
    score: float       # 0-100
    issues: list[str] = field(default_factory=list)
    passed: bool = True


@dataclass
class QualityReport:
    overall_score: float
    grade: str
    dimensions: list[QualityDimension]
    critical_issues: list[str]
    recommendations: list[str]

    @property
    def passed(self) -> bool:
        return self.overall_score >= 70


class DataQualityValidator:
    """Scores a dataset on 4 quality dimensions."""

    def validate(self, profile: DatasetProfile) -> QualityReport:
        dimensions = [
            self._completeness(profile),
            self._uniqueness(profile),
            self._consistency(profile),
            self._validity(profile),
        ]

        overall = sum(d.score for d in dimensions) / len(dimensions)
        grade = "A" if overall >= 90 else "B" if overall >= 75 else "C" if overall >= 60 else "F"

        critical = [i for d in dimensions for i in d.issues if "critical" in i.lower() or d.score < 50]
        recommendations = self._generate_recommendations(dimensions, profile)

        return QualityReport(
            overall_score=round(overall, 1),
            grade=grade,
            dimensions=dimensions,
            critical_issues=critical,
            recommendations=recommendations,
        )

    def _completeness(self, profile: DatasetProfile) -> QualityDimension:
        issues = []
        if profile.row_count == 0:
            return QualityDimension("Completeness", 0, ["Dataset is empty"], False)

        total_cells = profile.row_count * profile.column_count
        null_cells = sum(profile.null_counts.values())
        completeness_pct = ((total_cells - null_cells) / total_cells * 100) if total_cells else 100

        for col, nulls in profile.null_counts.items():
            null_pct = nulls / profile.row_count * 100
            if null_pct > 50:
                issues.append(f"CRITICAL: Column '{col}' is {null_pct:.0f}% null")
            elif null_pct > 20:
                issues.append(f"Column '{col}' has {null_pct:.0f}% missing values")

        return QualityDimension(
            name="Completeness",
            score=round(completeness_pct, 1),
            issues=issues,
            passed=completeness_pct >= 80,
        )

    def _uniqueness(self, profile: DatasetProfile) -> QualityDimension:
        issues = []
        if profile.row_count == 0:
            return QualityDimension("Uniqueness", 100, [])

        dup_pct = profile.duplicate_rows / profile.row_count * 100
        score = max(0, 100 - dup_pct * 2)

        if dup_pct > 20:
            issues.append(f"CRITICAL: {dup_pct:.0f}% duplicate rows detected")
        elif dup_pct > 5:
            issues.append(f"{profile.duplicate_rows} duplicate rows ({dup_pct:.1f}%)")

        return QualityDimension("Uniqueness", round(score, 1), issues, dup_pct < 10)

    def _consistency(self, profile: DatasetProfile) -> QualityDimension:
        issues = []
        score = 100

        mixed_type_cols = []
        for col, dtype in profile.dtypes.items():
            if dtype == "unknown" and profile.null_counts.get(col, 0) < profile.row_count:
                mixed_type_cols.append(col)

        if mixed_type_cols:
            score -= len(mixed_type_cols) * 10
            issues.append(f"Mixed or unknown types in: {', '.join(mixed_type_cols)}")

        return QualityDimension("Consistency", max(0, score), issues, score >= 70)

    def _validity(self, profile: DatasetProfile) -> QualityDimension:
        issues = []
        score = 100

        if profile.row_count < 10:
            score -= 20
            issues.append("Very small dataset — statistical validity limited")

        if profile.column_count == 0:
            score = 0
            issues.append("No columns detected")

        return QualityDimension("Validity", max(0, score), issues, score >= 70)

    def _generate_recommendations(self, dimensions: list[QualityDimension], profile: DatasetProfile) -> list[str]:
        recs = []
        for d in dimensions:
            if d.name == "Completeness" and d.score < 80:
                recs.append("Impute or drop columns with high null rates before analysis")
            if d.name == "Uniqueness" and d.score < 90:
                recs.append("Run deduplication before loading to target store")
            if d.name == "Consistency" and d.score < 80:
                recs.append("Enforce column type constraints at ingestion layer")
        if profile.row_count > 10000:
            recs.append("Large dataset — consider partitioned loading for performance")
        return recs
