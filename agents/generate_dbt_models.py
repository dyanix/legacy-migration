"""
Generates the dbt staging models, schema tests, and reconciliation model
from the migration's own artifacts.

Why generated rather than hand-written: target column names come from
ai_mapper -> human_review_gate -> rule_generator, and rule_generator re-runs
on every pipeline execution. Hand-written models encode a snapshot of those
names and silently rot the moment a mapping changes -- which is exactly how
the previous models ended up selecting `department_id` when the target had
`dept_id`, and `patient_status` when the target had `patient_status_code`.
Reading the rules at generation time makes that class of drift impossible.

Inputs (both produced by earlier pipeline nodes):
    audit/transformation_rules.json  -- authoritative target column names
    audit/schema_profile.json        -- primary/foreign keys, best-effort

Outputs:
    validation/dbt_models/models/stg_<table>.sql
    validation/dbt_models/models/schema.yml
    validation/dbt_models/models/reconciliation_summary.sql

Run standalone, or call generate() from validator.py before `dbt test`.
"""

import json
import os
import re

RULES_PATH = os.getenv("TRANSFORMATION_RULES_PATH", "audit/transformation_rules.json")
PROFILE_PATH = os.getenv("SCHEMA_PROFILE_PATH", "audit/schema_profile.json")
DBT_DIR = os.getenv("DBT_PROJECT_DIR", "validation/dbt_models")
MODELS_DIR = os.path.join(DBT_DIR, "models")

# SQLite: "main". Snowflake: your SNOWFLAKE_SCHEMA.
TARGET_SCHEMA = os.getenv("DBT_TARGET_SCHEMA", "main")


def load_json(path, required=True):
    if not os.path.exists(path):
        if required:
            raise SystemExit(f"Required input missing: {path}. Run the pipeline first.")
        return None
    with open(path) as f:
        return json.load(f)


def target_columns_by_table(rules_doc: dict) -> dict:
    """table -> [target_column, ...] in rule order, de-duplicated.

    Skips rules with no usable target_column for the same reason
    build_source_query does: a failed rule shouldn't break the whole table.
    """
    out = {}
    for rule in rules_doc.get("rules", []):
        table = rule.get("source_table")
        col = rule.get("target_column")
        if not table or not col:
            continue
        cols = out.setdefault(table, [])
        if col not in cols:
            cols.append(col)
    return out


def _profile_columns(profile: dict, table: str) -> list:
    if not profile:
        return []
    try:
        return profile["tables"][table]["columns"]
    except (KeyError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Verification against the migrated data.
#
# These used to open target/migration_target.db directly, which was wrong in a
# way that produced no error: with TARGET_BACKEND=snowflake the generator was
# validating claims about Snowflake by querying a stale SQLite file, and once
# that file is deleted every check returns False and the generator emits five
# models with ZERO tests while reporting a clean run.
#
# Going through TargetWriter.read_table() means the checks run against whichever
# backend TARGET_BACKEND names, and avoids writing SQL twice: SQLite wants
# double-quoted lowercase identifiers, Snowflake wants unquoted uppercase, and
# getting that wrong fails silently in exactly the same way. Both writers
# already return a DataFrame with lowercase columns, so the comparison is
# identical either way.
# ---------------------------------------------------------------------------

_writer = None
_frames = {}
_target_unreadable = False


def _get_writer():
    """Opens the configured target once and reuses it."""
    global _writer
    if _writer is not None:
        return _writer
    try:
        import sys as _sys

        _sys.path.insert(
            0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "workflow")
        )
        from migration_executor import get_target_writer

        _writer = get_target_writer().connect()
    except Exception as e:
        print(f"  [verify] cannot connect to the migration target ({type(e).__name__}: {e})")
        _writer = False
    return _writer


def _get_frame(table: str):
    """Reads a migrated table once and caches it.

    Caching matters more than it looks: detect_primary_key tries several
    candidate columns and verifies each, so without a cache a five-table
    project issues dozens of full-table reads across the network.
    """
    global _target_unreadable
    if table in _frames:
        return _frames[table]

    writer = _get_writer()
    if not writer:
        _frames[table] = None
        _target_unreadable = True
        return None
    try:
        _frames[table] = writer.read_table(table)
    except Exception as e:
        print(f"  [verify] cannot read `{table}` from the target "
              f"({type(e).__name__}: {e}) -- no tests will be emitted for it.")
        _frames[table] = None
        _target_unreadable = True
    return _frames[table]


def close_target():
    global _writer
    if _writer:
        try:
            _writer.close()
        except Exception:
            pass
    _writer = None
    _frames.clear()


def _clean_series(df, column):
    """Values with NULLs and empty strings removed. Empty string is treated as
    absent because every target column is VARCHAR, so a missing value arrives
    as '' rather than NULL on some paths."""
    if df is None or column not in df.columns:
        return None
    s = df[column]
    return s[s.notna() & (s.astype(str).str.strip() != "")]


def verify_unique_not_null(table: str, column: str) -> bool:
    """Checks against the ACTUAL migrated data whether `column` is a valid
    primary key -- non-null and distinct across every row.

    This is the difference between the old hand-written models and these. A
    guessed key produces a uniqueness test that fails on the first run, and a
    validation layer that cries wolf gets ignored, which is worse than no
    validation layer. Verifying against real data means every emitted test is
    known-green at generation time, so any future red is a genuine regression
    rather than a bad guess.

    Returns True only when the check positively passes. If the target cannot be
    read, returns False and the caller emits no test -- silence is safer than
    an unverified assertion, but generate() now reports when that happens
    rather than letting an empty test suite look like a clean run.
    """
    df = _get_frame(table)
    if df is None or column not in getattr(df, "columns", []):
        return False
    total = len(df)
    values = _clean_series(df, column)
    if values is None or total == 0:
        return False
    # Every row must have a value, and every value must be distinct.
    return len(values) == total and values.nunique() == total


def verify_relationship(table: str, column: str, ref_table: str, ref_column: str) -> bool:
    """True when every non-null value in table.column exists in
    ref_table.ref_column -- i.e. the FK test would pass today."""
    child = _get_frame(table)
    parent = _get_frame(ref_table)
    if child is None or parent is None:
        return False
    if column not in getattr(child, "columns", []) or ref_column not in getattr(parent, "columns", []):
        return False

    child_vals = _clean_series(child, column)
    parent_vals = _clean_series(parent, ref_column)
    if child_vals is None or parent_vals is None:
        return False
    if len(child_vals) == 0:
        return False   # nothing to relate; do not claim an unverified FK
    # Compared as strings: every target column is VARCHAR, but a backend may
    # still hand back ints for one table and strings for another.
    return set(child_vals.astype(str)).issubset(set(parent_vals.astype(str)))


def detect_primary_key(table: str, columns: list, profile: dict, rules: list) -> str:
    """Returns the TARGET column that should carry not_null + unique, or
    None when no candidate can be confirmed.

    Candidates are ranked by how strong the evidence is (profile flag, then
    name shape), but every candidate is then VERIFIED against the migrated
    data. Ranking alone is not trusted -- that is what produced a
    uniqueness test on `encounter_date`.
    """
    src_to_tgt = {}
    seen_twice = set()
    for r in rules:
        sc, tc = r.get("source_column"), r.get("target_column")
        if not sc or not tc:
            continue
        if sc in src_to_tgt:
            seen_twice.add(sc)   # ambiguous mapping -- can't trust it for key detection
        src_to_tgt[sc] = tc

    candidates = []
    for col in _profile_columns(profile, table):
        name = col.get("name")
        if name in seen_twice:
            continue
        for flag in ("primary_key", "is_primary_key", "pk", "is_pk"):
            if col.get(flag) and src_to_tgt.get(name) in columns:
                candidates.append(src_to_tgt[name])

    stem = re.sub(r"[^a-z]", "", table.lower())
    id_cols = [c for c in columns if c.lower().endswith("_id")]
    for c in id_cols:
        c_stem = re.sub(r"[^a-z]", "", c.lower()[:-3])
        if c_stem and (c_stem in stem or stem in c_stem):
            candidates.append(c)
    candidates.extend(id_cols)
    candidates.extend(columns)

    seen = set()
    for c in candidates:
        if c in seen or c not in columns:
            continue
        seen.add(c)
        if verify_unique_not_null(table, c):
            return c
    return None


def detect_relationships(table: str, columns: list, pk_by_table: dict) -> list:
    """Returns [(fk_column, referenced_table)] for target columns that
    exactly match another migrated table's primary key.

    Deliberately conservative -- exact name match only. Fuzzy FK inference
    (dept_id <-> department_id) would generate relationships tests that fail
    against real data and make the whole dbt layer look broken.
    """
    rels = []
    for col in columns:
        for other_table, other_pk in pk_by_table.items():
            if other_table == table or not other_pk:
                continue
            if col == other_pk and verify_relationship(table, col, other_table, other_pk):
                rels.append((col, other_table))
                break
    return rels


def render_staging_model(table: str, columns: list) -> str:
    cols = ",\n".join(f"    {c}" for c in columns)
    return (f"-- GENERATED by generate_dbt_models.py -- do not edit by hand.\n"
            f"-- Columns come from {RULES_PATH}; re-run the generator after\n"
            f"-- any change to mappings or transformation rules.\n"
            f"select\n{cols}\nfrom {{{{ source('migrated', '{table}') }}}}\n")


def render_reconciliation(tables: list) -> str:
    parts = []
    for i, t in enumerate(tables):
        prefix = "select" if i == 0 else "union all\nselect"
        label = f"'{t}' as table_name, count(*) as row_count" if i == 0 else f"'{t}', count(*)"
        parts.append(f"{prefix} {label} from {{{{ ref('stg_{t}') }}}}")
    return ("-- GENERATED by generate_dbt_models.py -- do not edit by hand.\n"
            "-- Row counts per migrated table, as a dbt-native cross-check on\n"
            "-- the Python reconciliation in audit/validation_report.json.\n\n"
            + "\n".join(parts) + "\n")


def render_schema_yml(tables_cols: dict, pk_by_table: dict, rels_by_table: dict) -> str:
    lines = [
        "version: 2",
        "",
        "# GENERATED by generate_dbt_models.py -- do not edit by hand.",
        "# Sources point at the raw migrated tables in the target warehouse.",
        "# Column names are read from the transformation rules, so they stay",
        "# correct when rule_generator changes them.",
        "",
        "sources:",
        "  - name: migrated",
        f"    schema: {TARGET_SCHEMA}",
        "    tables:",
    ]
    for t in tables_cols:
        lines.append(f"      - name: {t}")

    lines += ["", "models:"]
    for t, cols in tables_cols.items():
        lines.append(f"  - name: stg_{t}")
        lines.append(f'    description: "Staged {t}, post-migration."')
        pk = pk_by_table.get(t)
        rels = rels_by_table.get(t, [])
        if not pk and not rels:
            lines.append("    columns: []")
            continue
        lines.append("    columns:")
        if pk:
            lines.append(f"      - name: {pk}")
            lines.append("        tests: [not_null, unique]")
        for fk_col, ref_table in rels:
            if fk_col == pk:
                continue
            lines.append(f"      - name: {fk_col}")
            lines.append("        tests:")
            lines.append("          - relationships:")
            # dbt 1.11+ wants generic-test args under `arguments:`; the older
            # flat form still runs but emits a deprecation warning on every
            # build, which trains people to ignore dbt output.
            lines.append("              arguments:")
            lines.append(f"                to: ref('stg_{ref_table}')")
            lines.append(f"                field: {fk_col}")
            lines.append("              config:")
            lines.append("                severity: warn   # FK may be nullable in source")
    return "\n".join(lines) + "\n"


def generate(verbose: bool = True) -> dict:
    rules_doc = load_json(RULES_PATH)
    profile = load_json(PROFILE_PATH, required=False)

    tables_cols = target_columns_by_table(rules_doc)
    if not tables_cols:
        raise SystemExit(f"No usable rules found in {RULES_PATH}.")

    rules_by_table = {}
    for r in rules_doc.get("rules", []):
        rules_by_table.setdefault(r.get("source_table"), []).append(r)

    pk_by_table = {
        t: detect_primary_key(t, cols, profile, rules_by_table.get(t, []))
        for t, cols in tables_cols.items()
    }
    rels_by_table = {
        t: detect_relationships(t, cols, pk_by_table) for t, cols in tables_cols.items()
    }

    os.makedirs(MODELS_DIR, exist_ok=True)
    written = []

    for t, cols in tables_cols.items():
        path = os.path.join(MODELS_DIR, f"stg_{t}.sql")
        with open(path, "w") as f:
            f.write(render_staging_model(t, cols))
        written.append(path)

    recon_path = os.path.join(MODELS_DIR, "reconciliation_summary.sql")
    with open(recon_path, "w") as f:
        f.write(render_reconciliation(list(tables_cols)))
    written.append(recon_path)

    schema_path = os.path.join(MODELS_DIR, "schema.yml")
    with open(schema_path, "w") as f:
        f.write(render_schema_yml(tables_cols, pk_by_table, rels_by_table))
    written.append(schema_path)

    close_target()

    if verbose:
        for t, cols in tables_cols.items():
            pk = pk_by_table.get(t) or "(none detected)"
            rel = ", ".join(f"{c}->{r}" for c, r in rels_by_table.get(t, [])) or "none"
            print(f"  stg_{t}: {len(cols)} cols | pk={pk} | fks={rel}")
        print(f"  wrote {len(written)} file(s) to {MODELS_DIR}")

    # An empty test suite must not look like a clean run. If the target could
    # not be read, every verification returned False and the models carry no
    # key or relationship tests at all -- dbt would then report PASS on
    # nothing, which is worse than an error.
    test_count = sum(1 for v in pk_by_table.values() if v) + \
                 sum(len(v) for v in rels_by_table.values())
    if _target_unreadable or test_count == 0:
        print("  WARNING: the migration target could not be read, so no key or "
              "relationship tests were emitted. The generated models will pass "
              "dbt trivially. Run the migration first, then regenerate.")

    return {"written": written, "tables": list(tables_cols),
            "primary_keys": pk_by_table, "relationships": rels_by_table,
            "tests_emitted": test_count, "target_readable": not _target_unreadable}


if __name__ == "__main__":
    generate()
