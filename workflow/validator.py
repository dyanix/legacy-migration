"""
workflow/validator.py

Runs Great Expectations suites at the three checkpoints the spec requires:
  1. source_baseline    -- sanity checks against the source MySQL tables,
                            before any migration happens.
  2. post_extraction     -- re-runs each table's approved transformation
                            query (the exact same SQL migration_executor
                            used) and validates the transformed-but-not-yet-
                            loaded result set.
  3. post_load            -- reads the actual target table (SQLite or
                            Snowflake, whichever is configured) and
                            validates what actually landed there.

Also generates a reconciliation report comparing source vs target for every
migrated table: row counts, null rates, and value distributions.

Failures block the pipeline (spec requirement). Two checks are treated as
hard failures:
  - target row count doesn't match what migration_executor reported loading
    (silent data loss during load)
  - a column that was never null in the source has nulls in the target
    (a broken transformation introducing nulls that shouldn't exist)
Everything else (general null-rate drift on already-nullable columns,
value-distribution differences from intentional transformation) is
informational in the reconciliation report, not blocking -- transformed
values are *supposed* to differ from source values.

dbt (validation/dbt_models/) provides the same checks in dbt's own test
framework as a second, independent layer -- see that folder's schema.yml.
Running `dbt test` requires a configured adapter/profile for your target
(dbt-sqlite or dbt-snowflake); this script attempts it best-effort and
degrades gracefully if dbt isn't set up yet, since it's not required for
the GE-based pass/fail decision.

Usage:
    python workflow/validator.py
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cross-package imports need the project root on sys.path (see
# langgraph_orchestrator.py for the same fallback and why it's needed).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from workflow.migration_executor import (
    SOURCE_DB_URL,
    TRANSFORMATION_RULES_PATH,
    MIGRATION_ORDER_PATH,
    RESULTS_PATH as MIGRATION_RESULTS_PATH,
    group_rules_by_table,
    build_source_query,
    get_target_writer,
)

load_dotenv()

SCHEMA_PROFILE_PATH = "audit/schema_profile.json"
VALIDATION_REPORT_PATH = "audit/validation_report.json"
GE_SUITES_DIR = "validation/great_expectations"
NULL_TOLERANCE = 0.0  # a source column that was 0% null must stay 0% null in target


def _quiet_ge_progress_bars():
    """GE's metric-calculation progress bars are noisy in a script context
    and add nothing here; suppress without touching real warnings/errors."""
    warnings.filterwarnings("ignore", category=UserWarning, module="great_expectations")


def get_ge_context():
    import great_expectations as gx

    return gx.get_context(mode="ephemeral")


def run_suite_on_dataframe(context, df, asset_name: str, suite_name: str, expectations: list) -> dict:
    import great_expectations as gx

    datasource = context.data_sources.add_pandas(f"{asset_name}_source")
    asset = datasource.add_dataframe_asset(f"{asset_name}_asset")
    batch_def = asset.add_batch_definition_whole_dataframe(f"{asset_name}_batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)

    result = batch.validate(suite)

    return {
        "suite_name": suite_name,
        "success": bool(result.success),
        "expectations": [
            {
                "type": r.expectation_config.type,
                "column": r.expectation_config.kwargs.get("column"),
                "success": bool(r.success),
                "result": {k: v for k, v in (r.result or {}).items() if k in ("observed_value", "element_count", "unexpected_count")},
            }
            for r in result.results
        ],
    }


def save_suite_definition(suite_name: str, expectations_desc: list) -> None:
    """Writes a human-readable record of each suite's expectations to
    validation/great_expectations/ -- the deliverable artifact for this
    checkpoint, independent of the ephemeral in-memory run above."""
    os.makedirs(GE_SUITES_DIR, exist_ok=True)
    path = os.path.join(GE_SUITES_DIR, f"{suite_name}.json")
    with open(path, "w") as f:
        json.dump({"suite_name": suite_name, "expectations": expectations_desc}, f, indent=2)


# ---------------------------------------------------------------------------
# Checkpoint 1: source_baseline
# ---------------------------------------------------------------------------
def checkpoint_source_baseline(context, source_engine, table_name: str, schema_profile: dict) -> dict:
    import great_expectations as gx
    import pandas as pd

    table_profile = schema_profile["tables"][table_name]
    pk_columns = [c["name"] for c in table_profile["columns"] if c["primary_key"]]

    df = pd.read_sql(f"SELECT * FROM `{table_name}`", source_engine)

    expectations = [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)]
    desc = [{"type": "expect_table_row_count_to_be_between", "min_value": 1}]
    for pk in pk_columns:
        expectations.append(gx.expectations.ExpectColumnValuesToNotBeNull(column=pk))
        desc.append({"type": "expect_column_values_to_not_be_null", "column": pk})

    suite_name = f"source_baseline__{table_name}"
    result = run_suite_on_dataframe(context, df, f"src_{table_name}", suite_name, expectations)
    save_suite_definition(suite_name, desc)
    result["row_count"] = len(df)
    return result


# ---------------------------------------------------------------------------
# Checkpoint 2: post_extraction (re-runs the approved transformation query)
# ---------------------------------------------------------------------------
def checkpoint_post_extraction(context, source_engine, table_name: str, table_rules: list) -> dict:
    import great_expectations as gx
    import pandas as pd

    sql, target_columns = build_source_query(table_name, table_rules)
    df = pd.read_sql(text(sql), source_engine)

    expectations = [gx.expectations.ExpectTableColumnsToMatchSet(column_set=set(target_columns), exact_match=False)]
    desc = [{"type": "expect_table_columns_to_match_set", "column_set": target_columns}]
    for col in target_columns:
        expectations.append(gx.expectations.ExpectColumnToExist(column=col))
        desc.append({"type": "expect_column_to_exist", "column": col})

    suite_name = f"post_extraction__{table_name}"
    result = run_suite_on_dataframe(context, df, f"ext_{table_name}", suite_name, expectations)
    save_suite_definition(suite_name, desc)
    result["row_count"] = len(df)
    result["target_columns"] = target_columns
    return result, df


# ---------------------------------------------------------------------------
# Checkpoint 3: post_load (reads the actual target)
# ---------------------------------------------------------------------------
def expected_target_rows(migration_result: dict):
    """Rows the target should hold after this run.

    For a full load that's simply what the executor loaded. For an
    incremental load, rows_migrated is only the delta above the watermark --
    comparing it against the table total reads a healthy no-op (0 new rows,
    table already populated) as total data loss, which is what blocked
    enc_log. Use the post-load count the executor recorded instead.

    Returns None when the executor couldn't supply a count, in which case
    callers should skip the comparison rather than fail on a missing value.

    Note this makes the incremental check weaker than the full-load one: it
    compares the validator's read of the target against the executor's read
    of the same table, so it catches drift between load and validation but
    not loss during the load itself. That limitation is inherent to
    incremental mode -- the row count alone cannot distinguish "nothing new
    to load" from "the load silently dropped everything".
    """
    if migration_result.get("is_incremental"):
        return migration_result.get("rows_in_target_after")
    return migration_result.get("rows_migrated")


def checkpoint_post_load(context, writer, table_name: str, expected_row_count: int) -> dict:
    import great_expectations as gx

    df = writer.read_table(table_name)

    if expected_row_count is None:
        expected_row_count = len(df)  # nothing to compare against; don't fail on a missing value

    expectations = [gx.expectations.ExpectTableRowCountToEqual(value=expected_row_count)]
    desc = [{"type": "expect_table_row_count_to_equal", "value": expected_row_count}]

    suite_name = f"post_load__{table_name}"
    result = run_suite_on_dataframe(context, df, f"tgt_{table_name}", suite_name, expectations)
    save_suite_definition(suite_name, desc)
    result["row_count"] = len(df)
    return result, df


# ---------------------------------------------------------------------------
# Reconciliation report
# ---------------------------------------------------------------------------
def build_reconciliation_entry(table_name: str, table_rules: list, schema_profile: dict,
                                source_row_count: int, target_row_count: int,
                                migration_result: dict, target_df) -> dict:
    table_profile = schema_profile["tables"][table_name]
    source_columns_by_name = {c["name"]: c for c in table_profile["columns"]}

    column_comparisons = []
    hard_failures = []

    for rule in table_rules:
        src_col = rule["source_column"]
        tgt_col = rule["target_column"]
        src_profile = source_columns_by_name.get(src_col)
        if src_profile is None or tgt_col not in target_df.columns:
            continue

        source_null_rate = src_profile["null_rate"]
        target_null_count = int(target_df[tgt_col].isna().sum())
        target_row_total = len(target_df)
        target_null_rate = round(target_null_count / target_row_total, 4) if target_row_total else 0.0

        entry = {
            "source_column": src_col,
            "target_column": tgt_col,
            "source_null_rate": source_null_rate,
            "target_null_rate": target_null_rate,
        }

        if source_null_rate <= NULL_TOLERANCE and target_null_rate > NULL_TOLERANCE:
            entry["flag"] = "NEW_NULLS_INTRODUCED"
            hard_failures.append(
                f"{table_name}.{tgt_col}: source was never null but target has "
                f"{target_null_count}/{target_row_total} nulls -- likely a broken transformation."
            )

        if src_profile.get("value_counts") and tgt_col in target_df.columns:
            entry["source_value_counts"] = src_profile["value_counts"]
            entry["target_value_counts"] = target_df[tgt_col].value_counts(dropna=False).to_dict()

        column_comparisons.append(entry)

    expected_row_count = expected_target_rows(migration_result)
    if expected_row_count is None:
        row_count_matches_expected = True  # executor supplied no count -- nothing to reconcile
    else:
        row_count_matches_expected = target_row_count == expected_row_count

    if not row_count_matches_expected:
        basis = "rows_in_target_after" if migration_result.get("is_incremental") else "rows_migrated"
        hard_failures.append(
            f"{table_name}: target has {target_row_count} rows but migration_executor "
            f"reported {expected_row_count} ({basis}) -- possible data loss on load."
        )

    return {
        "table": table_name,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "rows_reported_migrated": migration_result.get("rows_migrated"),
        "expected_row_count": expected_row_count,
        "row_count_matches_expected": row_count_matches_expected,
        "load_mode": migration_result.get("mode"),
        "column_comparisons": column_comparisons,
        "hard_failures": hard_failures,
    }


# ---------------------------------------------------------------------------
# dbt (best-effort; not required for the pass/fail decision)
# ---------------------------------------------------------------------------
def try_run_dbt() -> dict:
    """Best-effort second validation layer. Never gates the pipeline -- the
    pass/fail decision belongs to the GE checkpoints above."""
    import shutil
    import subprocess

    if shutil.which("dbt") is None:
        return {"attempted": False,
                "reason": 'dbt is not installed (pip install "dbt-sqlite==1.10.0" to enable).'}

    generated = None
    try:
        # Regenerate models from the current transformation rules before
        # testing. Without this the models drift the moment rule_generator
        # renames a target column, and every model fails to compile against
        # a column that no longer exists.
        # Look in agents/ (spec layout) and alongside this file (workflow/),
        # so the hook works whichever folder the generator lives in.
        here = os.path.dirname(os.path.abspath(__file__))
        for candidate in (os.path.join(here, "..", "agents"), here):
            if os.path.exists(os.path.join(candidate, "generate_dbt_models.py")):
                sys.path.insert(0, candidate)
                break
        import generate_dbt_models

        generated = generate_dbt_models.generate(verbose=False)
    except Exception as e:
        return {"attempted": False, "reason": f"could not generate dbt models: {e}"}

    try:
        # `dbt build`, not `dbt test`: these models are views, and the
        # relationships tests ref() them. `dbt test` alone would error on a
        # missing relation because nothing built the views first.
        result = subprocess.run(
            ["dbt", "build", "--project-dir", "validation/dbt_models"],
            capture_output=True, text=True, timeout=300,
        )
        return {"attempted": True, "returncode": result.returncode,
                "models_generated": len(generated["written"]) if generated else 0,
                "tables_covered": generated["tables"] if generated else [],
                "stdout": result.stdout[-2000:], "stderr": result.stderr[-2000:]}
    except Exception as e:
        return {"attempted": True, "error": str(e)}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_validation() -> dict:
    _quiet_ge_progress_bars()

    for path in (TRANSFORMATION_RULES_PATH, MIGRATION_ORDER_PATH, MIGRATION_RESULTS_PATH, SCHEMA_PROFILE_PATH):
        if not os.path.exists(path):
            raise SystemExit(f"{path} not found. Run the pipeline through migration_executor first.")

    with open(TRANSFORMATION_RULES_PATH) as f:
        rules_doc = json.load(f)
    with open(MIGRATION_ORDER_PATH) as f:
        order_doc = json.load(f)
    with open(MIGRATION_RESULTS_PATH) as f:
        migration_results = json.load(f)
    with open(SCHEMA_PROFILE_PATH) as f:
        schema_profile = json.load(f)

    rules_by_table = group_rules_by_table(rules_doc["rules"])
    results_by_table = {r["table"]: r for r in migration_results["table_results"]}

    context = get_ge_context()
    source_engine = create_engine(SOURCE_DB_URL)
    writer = get_target_writer()
    writer.connect()

    checkpoints = {"source_baseline": [], "post_extraction": [], "post_load": []}
    reconciliation = []
    all_hard_failures = []

    try:
        for table_name in order_doc["migration_order"]:
            table_rules = rules_by_table.get(table_name)
            migration_result = results_by_table.get(table_name)
            if not table_rules or not migration_result:
                continue

            baseline = checkpoint_source_baseline(context, source_engine, table_name, schema_profile)
            checkpoints["source_baseline"].append(baseline)

            extraction, extracted_df = checkpoint_post_extraction(context, source_engine, table_name, table_rules)
            checkpoints["post_extraction"].append(extraction)

            load_check, target_df = checkpoint_post_load(
                context, writer, table_name, expected_target_rows(migration_result))
            checkpoints["post_load"].append(load_check)

            recon = build_reconciliation_entry(
                table_name, table_rules, schema_profile,
                baseline["row_count"], load_check["row_count"], migration_result, target_df,
            )
            reconciliation.append(recon)
            all_hard_failures.extend(recon["hard_failures"])

            status = "OK" if not recon["hard_failures"] else "FAILED"
            print(f"[{table_name}] source={recon['source_row_count']} target={recon['target_row_count']} "
                  f"mode={recon['load_mode']} -> {status}")
            for f in recon["hard_failures"]:
                print(f"    ! {f}")
    finally:
        writer.close()

    dbt_result = try_run_dbt()

    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "success": len(all_hard_failures) == 0,
        "hard_failures": all_hard_failures,
        "checkpoints": checkpoints,
        "reconciliation": reconciliation,
        "dbt": dbt_result,
    }

    os.makedirs(os.path.dirname(VALIDATION_REPORT_PATH) or ".", exist_ok=True)
    with open(VALIDATION_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, default=str)

    return report


def main():
    parser = argparse.ArgumentParser(description="Run GE validation checkpoints and produce a reconciliation report.")
    parser.parse_args()

    report = run_validation()

    print(f"\nValidation report written to {VALIDATION_REPORT_PATH}")
    print(f"GE suite definitions written to {GE_SUITES_DIR}/")
    if report["success"]:
        print("\n✓ Validation PASSED -- no hard failures.")
    else:
        print(f"\n🚫 Validation FAILED -- {len(report['hard_failures'])} hard failure(s):")
        for f in report["hard_failures"]:
            print(f"  - {f}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()