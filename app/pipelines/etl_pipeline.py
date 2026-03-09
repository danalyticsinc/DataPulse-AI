"""
DataPulse AI — ETL Pipeline Engine
Handles CSV/JSON/Excel ingestion, transformation, and quality validation.
"""
import io
import json
from dataclasses import dataclass, field
from typing import Any, Optional
import csv


@dataclass
class DatasetProfile:
    row_count: int
    column_count: int
    columns: list[str]
    dtypes: dict[str, str]
    null_counts: dict[str, int]
    duplicate_rows: int
    sample_rows: list[dict]


@dataclass
class TransformResult:
    success: bool
    rows_processed: int
    rows_dropped: int
    transformations_applied: list[str]
    errors: list[str] = field(default_factory=list)
    data: list[dict] = field(default_factory=list)


class ETLPipeline:
    """Lightweight ETL pipeline — no pandas dependency, pure Python."""

    def ingest_csv(self, content: str) -> tuple[list[dict], list[str]]:
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        columns = reader.fieldnames or []
        return rows, list(columns)

    def ingest_json(self, content: str) -> tuple[list[dict], list[str]]:
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        columns = list(data[0].keys()) if data else []
        return data, columns

    def profile(self, rows: list[dict], columns: list[str]) -> DatasetProfile:
        if not rows:
            return DatasetProfile(0, len(columns), columns, {}, {}, 0, [])

        null_counts = {col: sum(1 for r in rows if not r.get(col)) for col in columns}
        dtypes = {}
        for col in columns:
            values = [r.get(col) for r in rows if r.get(col)]
            if not values:
                dtypes[col] = "unknown"
                continue
            try:
                [float(v) for v in values]
                dtypes[col] = "numeric"
            except (ValueError, TypeError):
                dtypes[col] = "string"

        seen = set()
        dups = 0
        for row in rows:
            key = tuple(sorted(row.items()))
            if key in seen:
                dups += 1
            seen.add(key)

        return DatasetProfile(
            row_count=len(rows),
            column_count=len(columns),
            columns=columns,
            dtypes=dtypes,
            null_counts=null_counts,
            duplicate_rows=dups,
            sample_rows=rows[:5],
        )

    def transform(self, rows: list[dict], rules: dict) -> TransformResult:
        applied = []
        errors = []
        original_count = len(rows)
        result = list(rows)

        if rules.get("drop_nulls"):
            cols = rules["drop_nulls"] if isinstance(rules["drop_nulls"], list) else result[0].keys()
            before = len(result)
            result = [r for r in result if all(r.get(c) for c in cols)]
            applied.append(f"Dropped {before - len(result)} rows with nulls")

        if rules.get("drop_duplicates"):
            seen = set()
            deduped = []
            for row in result:
                key = tuple(sorted(row.items()))
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            applied.append(f"Removed {len(result) - len(deduped)} duplicate rows")
            result = deduped

        if rules.get("rename"):
            for old, new in rules["rename"].items():
                for row in result:
                    if old in row:
                        row[new] = row.pop(old)
            applied.append(f"Renamed columns: {rules['rename']}")

        if rules.get("drop_columns"):
            for col in rules["drop_columns"]:
                for row in result:
                    row.pop(col, None)
            applied.append(f"Dropped columns: {rules['drop_columns']}")

        if rules.get("lowercase_strings"):
            for row in result:
                for k, v in row.items():
                    if isinstance(v, str):
                        row[k] = v.strip().lower()
            applied.append("Lowercased and stripped string values")

        return TransformResult(
            success=True,
            rows_processed=len(result),
            rows_dropped=original_count - len(result),
            transformations_applied=applied,
            errors=errors,
            data=result,
        )
