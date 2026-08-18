"""
workflow/langgraph_orchestrator.py

Wires the full migration pipeline together as a LangGraph state machine:

    schema_profiler -> ai_mapper -> human_review_gate -> rule_generator
        -> dependency_resolver -> migration_executor -> validator -> doc_generator

Design notes
------------
human_review_gate is implemented as a *graceful halt*, not an in-process
LangGraph interrupt(). Why: the actual review happens in a separate,
long-running Flask app (review_ui/app.py) that a human interacts with in
a browser -- it isn't part of this process's call stack. So instead of
blocking this process, the gate node checks review status; if anything
gated is still unresolved, it logs clear instructions and the graph run
ends with status=PAUSED_FOR_HUMAN_REVIEW. Re-running this script later
picks up exactly where it left off, because every stage is file-based and
idempotent (a stage whose output file already exists is skipped, not
redone) -- this matters most for ai_mapper, since re-running it would
mean paying for LLM calls a second time for no reason.

Every node appends an entry to audit/migration_audit_log.json as it runs,
building the full-traceability audit trail required by the spec.

Stages 6-8 (migration_executor, validator, doc_generator) are wired into
the graph now with their exact node contracts, but are intentionally
stubbed -- they're built in the next steps of this project. Running the
orchestrator today executes stages 1-5 for real and then reports which
stages remain, rather than crashing or silently skipping them.

Usage (run from the project root):
    python -m workflow.langgraph_orchestrator
    python workflow/langgraph_orchestrator.py   # also works (sys.path fix below)
"""

import hashlib
import json
import operator
import os
import sys
from datetime import datetime, timezone
from typing import Annotated, TypedDict

# Cross-package imports (agents.*, review_ui.*, workflow.*) require the
# project root on sys.path. `python -m workflow.langgraph_orchestrator`
# from the project root already guarantees this; this fallback makes
# `python workflow/langgraph_orchestrator.py` work too.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph

from agents.schema_profiler import run_profiler
from agents.ai_mapper import run_mapper
from agents.rule_generator import generate_rules
from agents.doc_generator import generate as generate_data_dictionary

# Optional observability. Import defensively so a missing langfuse install
# cannot stop a migration -- tracing is instrumentation, not a dependency.
try:
    from workflow.tracing import flush as flush_traces
    from workflow.tracing import status as tracing_status
except Exception:  # pragma: no cover
    def flush_traces():
        return None

    def tracing_status():
        return {"tracing_enabled": False, "reason": "workflow/tracing.py not importable"}
from review_ui.app import is_gated, is_resolved, load_mappings
from workflow.dependency_resolver import resolve_migration_order, CircularDependencyError
from workflow.migration_executor import run_migration
from workflow.validator import run_validation

load_dotenv()

SCHEMA_PROFILE_PATH = "audit/schema_profile.json"
AI_MAPPINGS_PATH = "audit/ai_mappings.json"
REVIEWED_MAPPINGS_PATH = "audit/reviewed_mappings.json"
TRANSFORMATION_RULES_PATH = "audit/transformation_rules.json"
MIGRATION_ORDER_PATH = "audit/migration_order.json"
AUDIT_LOG_PATH = "audit/migration_audit_log.json"


# ---------------------------------------------------------------------------
# Audit log -- append-only, one entry per node execution. This is the file
# required at audit/migration_audit_log.json per the submission structure.
# ---------------------------------------------------------------------------
def _entry_hash(entry: dict, prev_hash: str) -> str:
    """SHA-256 over the entry's content plus the previous entry's hash.

    This is what makes the log immutable in the sense that matters: not
    that the bytes cannot be edited -- any file can be edited -- but that
    editing them is *detectable*. Changing a past decision, or deleting an
    entry, breaks every hash after it, and verify_audit_log() reports
    exactly where. Without the chain, a rewritten audit log is
    indistinguishable from an honest one, which defeats the purpose of
    keeping it for a regulated migration.
    """
    body = {k: v for k, v in entry.items() if k not in ("entry_hash",)}
    payload = json.dumps(body, sort_keys=True, default=str) + (prev_hash or "")
    return hashlib.sha256(payload.encode()).hexdigest()


def append_audit_log(node: str, status: str, detail: str, payload: dict = None) -> None:
    """Appends one tamper-evident entry. `payload` carries structured detail
    (AI suggestions, human decisions) alongside the human-readable message."""
    os.makedirs(os.path.dirname(AUDIT_LOG_PATH) or ".", exist_ok=True)
    entries = []
    if os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH) as f:
            try:
                entries = json.load(f)
            except json.JSONDecodeError:
                entries = []

    prev_hash = entries[-1].get("entry_hash") if entries else None
    entry = {
        "entry_id": len(entries) + 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node": node,
        "status": status,
        "detail": detail,
        "prev_hash": prev_hash,
    }
    if payload:
        entry["payload"] = payload
    entry["entry_hash"] = _entry_hash(entry, prev_hash)

    entries.append(entry)
    with open(AUDIT_LOG_PATH, "w") as f:
        json.dump(entries, f, indent=2)


def verify_audit_log(path: str = AUDIT_LOG_PATH) -> dict:
    """Walks the hash chain and reports the first entry that doesn't verify.
    Run standalone: python -c "from workflow.langgraph_orchestrator import
    verify_audit_log; print(verify_audit_log())"
    """
    if not os.path.exists(path):
        return {"ok": False, "reason": f"{path} not found."}
    with open(path) as f:
        try:
            entries = json.load(f)
        except json.JSONDecodeError as e:
            return {"ok": False, "reason": f"not valid JSON: {e}"}

    prev_hash = None
    for i, entry in enumerate(entries, start=1):
        if entry.get("prev_hash") != prev_hash:
            return {"ok": False, "entries": len(entries), "broken_at": i,
                    "reason": f"entry {i} does not chain to entry {i - 1} (an entry was altered or removed)."}
        expected = _entry_hash(entry, prev_hash)
        if entry.get("entry_hash") != expected:
            return {"ok": False, "entries": len(entries), "broken_at": i,
                    "reason": f"entry {i} content does not match its hash (it was edited after being written)."}
        prev_hash = entry["entry_hash"]
    return {"ok": True, "entries": len(entries),
            "reason": "hash chain intact; no entry has been altered or removed."}


def log_ai_suggestions(path: str, node: str) -> int:
    """Records one audit entry per AI-suggested mapping, so the log itself
    answers "what did the model propose, with what confidence, from which
    prompt" -- rather than only "ai_mapper ran". The spec requires every AI
    suggestion be traceable to a prompt ID; that traceability has to live in
    the audit log, not only in the mappings file the log describes.
    """
    if not os.path.exists(path):
        return 0
    try:
        with open(path) as f:
            doc = json.load(f)
    except json.JSONDecodeError:
        return 0
    records = doc if isinstance(doc, list) else doc.get("mappings", [])
    count = 0
    for m in records:
        if not isinstance(m, dict):
            continue
        append_audit_log(
            node, "ai_suggestion",
            f"{m.get('source_table')}.{m.get('source_column')} -> {m.get('target_column')} "
            f"(confidence {m.get('confidence')})",
            payload={
                "source_table": m.get("source_table"),
                "source_column": m.get("source_column"),
                "target_column": m.get("target_column"),
                "confidence": m.get("confidence"),
                "prompt_id": m.get("prompt_id"),
                "inferred_meaning": m.get("inferred_meaning"),
                "reasoning": m.get("reasoning"),
            },
        )
        count += 1
    return count


def log_human_decisions(path: str, node: str) -> int:
    """Records one audit entry per human decision -- approve, reject, or
    edit with an override_note. Auto-approved mappings are logged too, with
    the threshold that cleared them, so the log shows why no human was
    asked rather than staying silent about it."""
    if not os.path.exists(path):
        return 0
    try:
        with open(path) as f:
            doc = json.load(f)
    except json.JSONDecodeError:
        return 0
    records = doc if isinstance(doc, list) else doc.get("mappings", [])
    count = 0
    for m in records:
        if not isinstance(m, dict):
            continue
        reviewed = bool(m.get("human_reviewed"))
        decision = m.get("review_decision") or ("human_reviewed" if reviewed else "auto_approved")
        append_audit_log(
            node, "human_decision" if reviewed else "auto_approved",
            f"{m.get('source_table')}.{m.get('source_column')} -> {m.get('target_column')}: {decision}",
            payload={
                "source_table": m.get("source_table"),
                "source_column": m.get("source_column"),
                "target_column": m.get("target_column"),
                "prompt_id": m.get("prompt_id"),
                "confidence": m.get("confidence"),
                "confidence_threshold": os.getenv("CONFIDENCE_THRESHOLD", "0.80"),
                "human_reviewed": reviewed,
                "review_decision": m.get("review_decision"),
                "override_note": m.get("override_note"),
                "reviewed_at": m.get("reviewed_at"),
            },
        )
        count += 1
    return count


# ---------------------------------------------------------------------------
# Graph state
# ---------------------------------------------------------------------------
class MigrationState(TypedDict, total=False):
    status: str
    paused_reason: str
    log: Annotated[list, operator.add]


def _skip_or_run(path: str, node_name: str, run_fn, *, skip_msg: str = None):
    """Idempotency helper: if `path` already exists, skip `run_fn` (most
    important for ai_mapper, where re-running means paying for LLM calls
    again). Returns (ran: bool, message: str)."""
    if os.path.exists(path):
        msg = skip_msg or f"{path} already exists -- skipping, using existing output."
        return False, msg
    run_fn()
    return True, f"Generated {path}."


# ---------------------------------------------------------------------------
# Node 1: schema_profiler
# ---------------------------------------------------------------------------
def node_schema_profiler(state: MigrationState) -> dict:
    def _run():
        profile = run_profiler()
        os.makedirs(os.path.dirname(SCHEMA_PROFILE_PATH) or ".", exist_ok=True)
        with open(SCHEMA_PROFILE_PATH, "w") as f:
            json.dump(profile, f, indent=2, default=str)

    ran, msg = _skip_or_run(SCHEMA_PROFILE_PATH, "schema_profiler", _run)
    append_audit_log("schema_profiler", "ran" if ran else "skipped", msg)
    return {"status": "SCHEMA_PROFILED", "log": [f"[schema_profiler] {msg}"]}


# ---------------------------------------------------------------------------
# Node 2: ai_mapper
# ---------------------------------------------------------------------------
def node_ai_mapper(state: MigrationState) -> dict:
    def _run():
        mappings = run_mapper(SCHEMA_PROFILE_PATH)
        os.makedirs(os.path.dirname(AI_MAPPINGS_PATH) or ".", exist_ok=True)
        with open(AI_MAPPINGS_PATH, "w") as f:
            json.dump(mappings, f, indent=2)

    ran, msg = _skip_or_run(
        AI_MAPPINGS_PATH,
        "ai_mapper",
        _run,
        skip_msg=f"{AI_MAPPINGS_PATH} already exists -- skipping to avoid re-billing LLM calls.",
    )
    append_audit_log("ai_mapper", "ran" if ran else "skipped", msg)
    # Log every suggestion, including on the skip path: the audit trail must
    # describe the mappings actually in use, not only the run that created them.
    n = log_ai_suggestions(AI_MAPPINGS_PATH, "ai_mapper")
    return {"status": "MAPPINGS_GENERATED",
            "log": [f"[ai_mapper] {msg} ({n} suggestion(s) recorded in the audit log)"]}


# ---------------------------------------------------------------------------
# Node 3: human_review_gate (graceful halt, see module docstring)
# ---------------------------------------------------------------------------
def node_human_review_gate(state: MigrationState) -> dict:
    mappings = load_mappings()
    gated = [m for m in mappings if is_gated(m)]
    pending = [m for m in gated if not is_resolved(m)]

    if pending:
        pending_list = [f"{m['source_table']}.{m['source_column']}" for m in pending]
        msg = (
            f"{len(pending)} mapping(s) below the confidence threshold still need human review: "
            f"{pending_list}. Run `python review_ui/app.py`, open http://localhost:5050, "
            f"resolve each pending item, click Finish Review & Export, then re-run this orchestrator."
        )
        append_audit_log("human_review_gate", "paused", msg)
        return {
            "status": "PAUSED_FOR_HUMAN_REVIEW",
            "paused_reason": msg,
            "log": [f"[human_review_gate] {msg}"],
        }

    def _run():
        with open(REVIEWED_MAPPINGS_PATH, "w") as f:
            json.dump(mappings, f, indent=2)

    ran, msg = _skip_or_run(REVIEWED_MAPPINGS_PATH, "human_review_gate", _run)
    append_audit_log("human_review_gate", "ran" if ran else "skipped", msg)
    n = log_human_decisions(REVIEWED_MAPPINGS_PATH, "human_review_gate")
    return {"status": "REVIEW_COMPLETE",
            "log": [f"[human_review_gate] {msg} ({n} decision(s) recorded in the audit log)"]}


def route_after_gate(state: MigrationState) -> str:
    return END if state.get("status") == "PAUSED_FOR_HUMAN_REVIEW" else "rule_generator"


# ---------------------------------------------------------------------------
# Node 4: rule_generator
# ---------------------------------------------------------------------------
def node_rule_generator(state: MigrationState) -> dict:
    with open(REVIEWED_MAPPINGS_PATH) as f:
        reviewed_mappings = json.load(f)

    rules, violations = generate_rules(reviewed_mappings)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rule_count": len(rules),
        "pipeline_ready": len(violations) == 0,
        "blocking_violation_count": len(violations),
        "rules": rules,
    }
    os.makedirs(os.path.dirname(TRANSFORMATION_RULES_PATH) or ".", exist_ok=True)
    with open(TRANSFORMATION_RULES_PATH, "w") as f:
        json.dump(output, f, indent=2)

    if violations:
        msg = f"{len(violations)} rule(s) violate the confidence gate -- pipeline blocked."
        append_audit_log("rule_generator", "blocked", msg)
        return {"status": "BLOCKED", "log": [f"[rule_generator] {msg}"]}

    msg = f"{len(rules)} transformation rules generated, all cleared the confidence gate."
    append_audit_log("rule_generator", "ran", msg)
    return {"status": "RULES_GENERATED", "log": [f"[rule_generator] {msg}"]}


def route_after_rules(state: MigrationState) -> str:
    return END if state.get("status") == "BLOCKED" else "dependency_resolver"


# ---------------------------------------------------------------------------
# Node 5: dependency_resolver
# ---------------------------------------------------------------------------
def node_dependency_resolver(state: MigrationState) -> dict:
    with open(SCHEMA_PROFILE_PATH) as f:
        schema_profile = json.load(f)

    try:
        result = resolve_migration_order(schema_profile)
    except CircularDependencyError as e:
        append_audit_log("dependency_resolver", "blocked", str(e))
        return {"status": "BLOCKED", "log": [f"[dependency_resolver] {e}"]}

    os.makedirs(os.path.dirname(MIGRATION_ORDER_PATH) or ".", exist_ok=True)
    with open(MIGRATION_ORDER_PATH, "w") as f:
        json.dump(result, f, indent=2)

    msg = f"Load order computed: {result['migration_order']}"
    append_audit_log("dependency_resolver", "ran", msg)
    return {"status": "ORDER_RESOLVED", "log": [f"[dependency_resolver] {msg}"]}


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Node 6: migration_executor -- real implementation (see workflow/migration_executor.py)
# ---------------------------------------------------------------------------
def node_migration_executor(state: MigrationState) -> dict:
    try:
        summary = run_migration()
    except SystemExit as e:
        # Missing prerequisites (no rules file, pipeline not ready, etc.)
        msg = str(e)
        append_audit_log("migration_executor", "blocked", msg)
        return {"status": "BLOCKED", "log": [f"[migration_executor] {msg}"]}
    except Exception as e:
        # Everything else -- connection failures after retries are exhausted,
        # target-side errors, etc. Must not crash the orchestrator: fail this
        # node cleanly, with the real error preserved in the audit trail, so
        # a human can fix credentials/connectivity and simply re-run.
        msg = f"migration_executor failed: {type(e).__name__}: {e}"
        append_audit_log("migration_executor", "failed", msg)
        return {"status": "MIGRATION_FAILED", "log": [f"[migration_executor] {msg}"]}

    msg = (
        f"Migrated {summary['tables_migrated']} table(s), "
        f"{summary['total_rows_migrated']} total rows, target={summary['target_backend']}."
    )
    append_audit_log("migration_executor", "ran", msg)
    return {"status": "MIGRATION_COMPLETE", "log": [f"[migration_executor] {msg}"]}


# ---------------------------------------------------------------------------
# Node 7: validator -- real implementation (see workflow/validator.py)
# ---------------------------------------------------------------------------
def node_validator(state: MigrationState) -> dict:
    try:
        report = run_validation()
    except SystemExit as e:
        msg = str(e)
        append_audit_log("validator", "blocked", msg)
        return {"status": "BLOCKED", "log": [f"[validator] {msg}"]}
    except Exception as e:
        msg = f"validator failed: {type(e).__name__}: {e}"
        append_audit_log("validator", "failed", msg)
        return {"status": "VALIDATION_FAILED", "log": [f"[validator] {msg}"]}

    if not report["success"]:
        msg = f"{len(report['hard_failures'])} hard failure(s) -- pipeline blocked: {report['hard_failures']}"
        append_audit_log("validator", "blocked", msg)
        return {"status": "BLOCKED", "log": [f"[validator] {msg}"]}

    msg = f"Validation passed for {len(report['reconciliation'])} table(s). See audit/validation_report.json."
    append_audit_log("validator", "ran", msg)
    return {"status": "VALIDATION_COMPLETE", "log": [f"[validator] {msg}"]}


# ---------------------------------------------------------------------------
# Node 8: doc_generator -- real implementation (see agents/doc_generator.py)
# ---------------------------------------------------------------------------
def node_doc_generator(state: MigrationState) -> dict:
    """Generates the target data dictionary from the approved mappings and
    transformation rules. Runs only on the success branch: a blocked
    migration must not publish documentation describing data that failed
    validation, since that document is the artifact people would trust.

    Unlike the other nodes this deliberately does NOT use _skip_or_run.
    The dictionary is derived entirely from upstream artifacts, so
    regenerating it is cheap and involves no billable LLM calls beyond the
    optional column descriptions -- whereas a stale dictionary left in place
    because the file already existed would silently misdescribe the schema
    after any mapping change.
    """
    try:
        result = generate_data_dictionary(verbose=False)
    except SystemExit as e:
        msg = str(e)
        append_audit_log("doc_generator", "failed", msg)
        return {"status": "DOCUMENTATION_FAILED", "log": [f"[doc_generator] {msg}"]}
    except Exception as e:
        msg = f"doc_generator failed: {type(e).__name__}: {e}"
        append_audit_log("doc_generator", "failed", msg)
        return {"status": "DOCUMENTATION_FAILED", "log": [f"[doc_generator] {msg}"]}

    msg = (f"Documented {result['table_count']} table(s), {result['column_count']} column(s) "
           f"({result['descriptions_source']} descriptions); "
           f"{result['sensitive_columns']} sensitive column(s), "
           f"{result['human_overrides']} human override(s). "
           f"See {result['markdown_path']}.")
    append_audit_log("doc_generator", "ran", msg)
    return {"status": "DOCUMENTATION_COMPLETE", "log": [f"[doc_generator] {msg}"]}


def route_after_migration(state: MigrationState) -> str:
    return "validator" if state.get("status") == "MIGRATION_COMPLETE" else END


def route_after_validation(state: MigrationState) -> str:
    # Only a clean validation proceeds to documentation. A BLOCKED or
    # VALIDATION_FAILED run ends here, so the data dictionary is never
    # written for a migration whose data was rejected.
    return "doc_generator" if state.get("status") == "VALIDATION_COMPLETE" else END


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------
def build_graph():
    graph = StateGraph(MigrationState)

    graph.add_node("schema_profiler", node_schema_profiler)
    graph.add_node("ai_mapper", node_ai_mapper)
    graph.add_node("human_review_gate", node_human_review_gate)
    graph.add_node("rule_generator", node_rule_generator)
    graph.add_node("dependency_resolver", node_dependency_resolver)
    graph.add_node("migration_executor", node_migration_executor)
    graph.add_node("validator", node_validator)
    graph.add_node("doc_generator", node_doc_generator)

    graph.add_edge(START, "schema_profiler")
    graph.add_edge("schema_profiler", "ai_mapper")
    graph.add_edge("ai_mapper", "human_review_gate")
    graph.add_conditional_edges(
        "human_review_gate", route_after_gate, {"rule_generator": "rule_generator", END: END}
    )
    graph.add_conditional_edges(
        "rule_generator", route_after_rules, {"dependency_resolver": "dependency_resolver", END: END}
    )
    graph.add_edge("dependency_resolver", "migration_executor")
    graph.add_conditional_edges(
        "migration_executor", route_after_migration, {"validator": "validator", END: END}
    )
    graph.add_conditional_edges(
        "validator", route_after_validation, {"doc_generator": "doc_generator", END: END}
    )
    graph.add_edge("doc_generator", END)
    # doc_generator remains wired for when it's real:

    return graph.compile()


def main():
    app = build_graph()
    try:
        final_state = app.invoke({"status": "STARTED", "log": []})
    finally:
        # Always flush, including on failure -- a run that errored is exactly
        # the one whose traces you want. The SDK batches events on a
        # background thread, so a short-lived script can exit before anything
        # is sent, which looks identical to tracing never having worked.
        flush_traces()

    print("\n" + "=" * 70)
    print("ORCHESTRATOR RUN LOG")
    print("=" * 70)
    for line in final_state.get("log", []):
        print(line)
    print("=" * 70)
    print(f"Final status: {final_state.get('status')}")

    ts = tracing_status()
    if ts.get("tracing_enabled"):
        print(f"LangFuse: traced under session {ts['session_id']} at {ts['host']}")
    else:
        print(f"LangFuse: not tracing ({ts.get('reason')})")
    if final_state.get("paused_reason"):
        print(f"\n{final_state['paused_reason']}")


if __name__ == "__main__":
    main()