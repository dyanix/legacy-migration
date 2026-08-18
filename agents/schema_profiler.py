"""
schema_profiler.py

Introspects the source legacy database and produces a structured JSON
"schema profile" -- the ground truth document that every downstream AI
agent (ai_mapper, rule_generator, doc_generator) reads from, and the
baseline that Great Expectations validates against later.

For each table, captures:
  - row count
  - column metadata (type, nullable, primary key, default)
  - foreign key relationships
For each column, captures:
  - null rate
  - cardinality (distinct value count)
  - value distribution (full counts if low-cardinality / a status-code-like
    column, otherwise a sample of distinct values)

Usage:
    python agents/schema_profiler.py
    python agents/schema_profiler.py --output audit/schema_profile.json
"""

import argparse
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

load_dotenv()

SOURCE_DB_URL = os.getenv(
    "SOURCE_DB_URL",
    "mysql+pymysql://legacy_user:legacy_pass@localhost:3307/legacy_db",
)

# Columns with <= this many distinct values get their FULL value distribution
# captured (this is exactly what catches things like pat_st_cd's junk codes).
LOW_CARDINALITY_THRESHOLD = 25
# For high-cardinality columns, just grab a small sample of distinct values.
SAMPLE_SIZE = 10

# ----------------------------------------------------------------------------
# PII / PHI detection (bonus feature).
# This is deliberately conservative and pattern-based -- not a substitute for
# a real classifier, but enough to auto-flag columns for DECISIONS.md and to
# stop an AI mapper from casually proposing to log/export them unredacted.
# Two detection methods, either of which flags a column:
#   1. Column name matches a known-sensitive naming pattern.
#   2. Sample values match a known-sensitive value pattern (email, SSN, phone).
# ----------------------------------------------------------------------------
import re

PII_NAME_PATTERNS = {
    "person_name": re.compile(r"(first|last|fst|lst|full)?_?nm$|^name$|full_?name", re.I),
    "date_of_birth": re.compile(r"\bdob\b|date_of_birth|birth_?dt", re.I),
    "ssn": re.compile(r"\bssn\b|social_?security", re.I),
    "email": re.compile(r"e[-_]?mail", re.I),
    "phone": re.compile(r"phone|tel_?num|mobile", re.I),
    "address": re.compile(r"\baddr\b|street|\bzip\b|postal", re.I),
    "medical_record_number": re.compile(r"\bmrn\b|medical_record", re.I),
    "provider_identifier": re.compile(r"(^|_)npi(_|$)", re.I),
    "diagnosis_phi": re.compile(r"^dx_|diagnos", re.I),
    "service_date_phi": re.compile(r"admit_?dt|discharge_?dt|enc_?dt", re.I),
}

# Order matters: date_like is checked first and short-circuits phone/ssn checks
# below (an ISO date like 1990-01-01 would otherwise false-positive as a phone
# number). Column-name-based date_of_birth detection already covers real DOBs.
PII_VALUE_PATTERNS = {
    "date_like": re.compile(r"^\d{4}-\d{2}-\d{2}"),
    "email": re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.I),
    "ssn": re.compile(r"^\d{3}-\d{2}-\d{4}$"),
    "phone": re.compile(r"^\+?\d{0,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$"),
}
# Categories detected by value pattern but not worth surfacing as PII on their own.
PII_VALUE_SUPPRESS = {"date_like"}


def detect_pii(column_name: str, sample_values: list) -> dict:
    """Returns {is_pii, categories, methods} for a single column."""
    categories = set()
    methods = []

    for category, pattern in PII_NAME_PATTERNS.items():
        if pattern.search(column_name):
            categories.add(category)
            methods.append(f"name_pattern:{category}")

    for value in (sample_values or [])[:SAMPLE_SIZE]:
        if not isinstance(value, str):
            continue
        for category, pattern in PII_VALUE_PATTERNS.items():
            if pattern.match(value.strip()):
                if category not in PII_VALUE_SUPPRESS:
                    categories.add(category)
                    methods.append(f"value_pattern:{category}")
                break  # first matching pattern wins for this value

    return {
        "is_pii": bool(categories),
        "pii_categories": sorted(categories),
        "pii_detection_methods": methods,
    }


def mask_db_url(url: str) -> str:
    """Never write credentials into the profile output."""
    if "@" in url:
        scheme_and_creds, rest = url.split("@", 1)
        scheme = scheme_and_creds.split("://")[0]
        return f"{scheme}://***:***@{rest}"
    return url


def profile_column(conn, table_name: str, column_name: str, total_rows: int) -> dict:
    quoted_table = f"`{table_name}`"
    quoted_col = f"`{column_name}`"

    null_count = conn.execute(
        text(f"SELECT COUNT(*) FROM {quoted_table} WHERE {quoted_col} IS NULL")
    ).scalar()

    distinct_count = conn.execute(
        text(f"SELECT COUNT(DISTINCT {quoted_col}) FROM {quoted_table}")
    ).scalar()

    null_rate = round(null_count / total_rows, 4) if total_rows else 0.0

    profile = {
        "null_count": null_count,
        "null_rate": null_rate,
        "cardinality": distinct_count,
    }

    if distinct_count is not None and 0 < distinct_count <= LOW_CARDINALITY_THRESHOLD:
        rows = conn.execute(
            text(
                f"SELECT {quoted_col} AS val, COUNT(*) AS cnt "
                f"FROM {quoted_table} "
                f"WHERE {quoted_col} IS NOT NULL "
                f"GROUP BY {quoted_col} "
                f"ORDER BY cnt DESC"
            )
        ).fetchall()
        # repr() preserves visibility of stray whitespace / case issues,
        # e.g. ' D' vs 'D' vs 'd' -- exactly the mess ai_mapper needs to see.
        profile["value_counts"] = {repr(r.val)[1:-1]: r.cnt for r in rows}
        profile["sample_values"] = list(profile["value_counts"].keys())
    else:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT {quoted_col} AS val FROM {quoted_table} "
                f"WHERE {quoted_col} IS NOT NULL LIMIT {SAMPLE_SIZE}"
            )
        ).fetchall()
        profile["sample_values"] = [str(r.val) for r in rows]

    return profile


def profile_table(engine, inspector, table_name: str) -> dict:
    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()

        pk_constraint = inspector.get_pk_constraint(table_name)
        pk_cols = set(pk_constraint.get("constrained_columns") or [])

        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            fks.append(
                {
                    "column": fk["constrained_columns"][0] if fk["constrained_columns"] else None,
                    "references_table": fk["referred_table"],
                    "references_column": fk["referred_columns"][0] if fk["referred_columns"] else None,
                }
            )

        columns = []
        for col in inspector.get_columns(table_name):
            col_name = col["name"]
            stats = profile_column(conn, table_name, col_name, total_rows)
            pii = detect_pii(col_name, stats.get("sample_values", []))
            columns.append(
                {
                    "name": col_name,
                    "type": str(col["type"]),
                    "nullable": col["nullable"],
                    "primary_key": col_name in pk_cols,
                    "default": str(col["default"]) if col.get("default") is not None else None,
                    **stats,
                    **pii,
                }
            )

        return {
            "row_count": total_rows,
            "columns": columns,
            "foreign_keys": fks,
        }


def build_fk_graph(tables_profile: dict) -> list:
    """Flat edge list -- convenient for the ai_mapper prompt and for doc_generator's lineage section."""
    edges = []
    for table_name, profile in tables_profile.items():
        for fk in profile["foreign_keys"]:
            edges.append(
                {
                    "from_table": table_name,
                    "from_column": fk["column"],
                    "to_table": fk["references_table"],
                    "to_column": fk["references_column"],
                }
            )
    return edges


def build_pii_summary(tables_profile: dict) -> dict:
    """Table -> list of flagged columns with their categories. Feeds DECISIONS.md directly."""
    summary = {}
    for table_name, profile in tables_profile.items():
        flagged = [
            {"column": c["name"], "categories": c["pii_categories"]}
            for c in profile["columns"]
            if c["is_pii"]
        ]
        if flagged:
            summary[table_name] = flagged
    return summary


def run_profiler(db_url: str = SOURCE_DB_URL) -> dict:
    engine = create_engine(db_url)
    inspector = inspect(engine)

    table_names = inspector.get_table_names()
    if not table_names:
        raise RuntimeError(
            "No tables found in the source database. "
            "Did docker compose up run the schema init, and did db/02_seed.py complete?"
        )

    print(f"Profiling {len(table_names)} tables: {table_names}")

    tables_profile = {}
    for t in table_names:
        print(f"  -> profiling `{t}`...")
        tables_profile[t] = profile_table(engine, inspector, t)

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_db_url": mask_db_url(db_url),
        "table_count": len(table_names),
        "tables": tables_profile,
        "fk_graph": build_fk_graph(tables_profile),
        "pii_summary": build_pii_summary(tables_profile),
    }
    return result


def main():
    parser = argparse.ArgumentParser(description="Profile the legacy source database schema.")
    parser.add_argument(
        "--output",
        default="audit/schema_profile.json",
        help="Path to write the JSON profile (default: audit/schema_profile.json)",
    )
    args = parser.parse_args()

    profile = run_profiler()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(profile, f, indent=2, default=str)

    print(f"\nSchema profile written to {args.output}")
    print(f"Tables profiled: {profile['table_count']}")
    for t, p in profile["tables"].items():
        flagged = [
            c["name"]
            for c in p["columns"]
            if c["cardinality"] and c["cardinality"] <= LOW_CARDINALITY_THRESHOLD and c["null_rate"] < 1.0
        ]
        print(f"  {t}: {p['row_count']} rows, low-cardinality columns worth reviewing: {flagged}")

    if profile["pii_summary"]:
        print("\nPII/PHI flagged (feed this into DECISIONS.md):")
        for t, cols in profile["pii_summary"].items():
            for c in cols:
                print(f"  {t}.{c['column']}: {c['categories']}")
    else:
        print("\nNo PII/PHI patterns detected.")


if __name__ == "__main__":
    main()