"""
review_ui/app.py

Implements the human_review_gate node as a small local web app instead of a
CLI (bonus: visual mapping review UI).

Behavior:
  - Loads audit/ai_mappings.json (produced by agents/ai_mapper.py).
  - Mappings with confidence >= CONFIDENCE_THRESHOLD auto-pass
    (human_reviewed stays False, override_note stays null -- they were
    never ambiguous enough to need a human).
  - Mappings with confidence < CONFIDENCE_THRESHOLD are blocked: a human
    must Approve or Override each one, and must supply a non-null note,
    before the "Finish Review & Export" action becomes available.
  - Every edit is saved back to audit/ai_mappings.json immediately, so
    progress survives a server restart.
  - "Finish Review & Export" writes audit/reviewed_mappings.json -- the
    file agents/rule_generator.py consumes next -- and is only enabled
    once zero low-confidence mappings remain unreviewed.

Usage:
    python review_ui/app.py
    # then open http://localhost:5050
"""

import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for

load_dotenv()

MAPPINGS_PATH = os.getenv("AI_MAPPINGS_PATH", "audit/ai_mappings.json")
REVIEWED_PATH = os.getenv("REVIEWED_MAPPINGS_PATH", "audit/reviewed_mappings.json")
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", 0.80))

app = Flask(__name__)


def load_mappings():
    if not os.path.exists(MAPPINGS_PATH):
        return []
    with open(MAPPINGS_PATH) as f:
        data = json.load(f)
    # Backfill review fields for mappings generated before this step existed.
    for m in data:
        m.setdefault("human_reviewed", False)
        m.setdefault("override_note", None)
    return data


def save_mappings(mappings):
    os.makedirs(os.path.dirname(MAPPINGS_PATH) or ".", exist_ok=True)
    with open(MAPPINGS_PATH, "w") as f:
        json.dump(mappings, f, indent=2)


def is_gated(mapping):
    return mapping.get("confidence", 0) < CONFIDENCE_THRESHOLD


def is_resolved(mapping):
    """A gated mapping is resolved once a human has reviewed it AND left a note."""
    if not is_gated(mapping):
        return True
    return bool(mapping.get("human_reviewed")) and bool(mapping.get("override_note"))


def snapshot_ai_original(mapping):
    """Records what the model originally proposed, the first time a human
    touches this mapping.

    Without this, a revision overwrites the AI's proposal and the record
    can no longer show what was changed -- only what it ended up as. The
    data dictionary reads original_target_column to render its "AI proposed
    X -> final Y" line, and the audit trail is meaningless if the "before"
    is gone.
    """
    if not mapping.get("ai_original"):
        mapping["ai_original"] = {
            "target_column": mapping.get("target_column"),
            "transformation_rule": mapping.get("transformation_rule"),
            "null_handling": mapping.get("null_handling"),
            "confidence": mapping.get("confidence"),
        }


def append_revision(mapping):
    """Moves the CURRENT decision into revision_history before it is
    replaced.

    A reviewer changing their mind is normal and should be supported; a
    reviewer silently replacing a prior decision is exactly what an audit
    log exists to prevent. So revising appends rather than overwrites, and
    every superseded decision keeps its note and timestamp.
    """
    if not mapping.get("human_reviewed"):
        return
    mapping.setdefault("revision_history", []).append({
        "superseded_at": datetime.now(timezone.utc).isoformat(),
        "decision": mapping.get("review_decision"),
        "note": mapping.get("override_note"),
        "target_column": mapping.get("target_column"),
        "transformation_rule": mapping.get("transformation_rule"),
        "null_handling": mapping.get("null_handling"),
        "reviewed_at": mapping.get("reviewed_at"),
    })


@app.route("/")
def index():
    mappings = load_mappings()
    gated = [m for m in mappings if is_gated(m)]
    auto_passed = [m for m in mappings if not is_gated(m)]
    pending = [m for m in gated if not is_resolved(m)]
    resolved = [m for m in gated if is_resolved(m)]

    revised = [m for m in mappings if m.get("revision_history")]

    return render_template(
        "index.html",
        pending=pending,
        resolved=resolved,
        auto_passed=auto_passed,
        revised=revised,
        threshold=CONFIDENCE_THRESHOLD,
        can_finish=(len(pending) == 0 and len(mappings) > 0),
        export_exists=os.path.exists(REVIEWED_PATH),
        total=len(mappings),
    )


@app.route("/review/<prompt_id>", methods=["POST"])
def review(prompt_id):
    mappings = load_mappings()
    decision = request.form.get("decision")  # "approve" or "override"
    note = request.form.get("override_note", "").strip()
    edited_target_column = request.form.get("target_column", "").strip()
    edited_transformation_rule = request.form.get("transformation_rule", "").strip()
    edited_null_handling = request.form.get("null_handling", "").strip()

    if not note:
        return jsonify({"error": "A review note is required before saving."}), 400

    for m in mappings:
        if m["prompt_id"] == prompt_id:
            snapshot_ai_original(m)
            append_revision(m)   # no-op on a first review

            if decision == "override":
                m["target_column"] = edited_target_column or m["target_column"]
                m["transformation_rule"] = edited_transformation_rule or m["transformation_rule"]
                m["null_handling"] = edited_null_handling or m["null_handling"]

            # Surface a renamed target so doc_generator can report the change.
            ai_target = (m.get("ai_original") or {}).get("target_column")
            if ai_target and ai_target != m.get("target_column"):
                m["original_target_column"] = ai_target
            else:
                m.pop("original_target_column", None)

            m["human_reviewed"] = True
            m["override_note"] = note
            m["review_decision"] = decision
            m["reviewed_at"] = datetime.now(timezone.utc).isoformat()
            m["revision_count"] = len(m.get("revision_history", []))
            break
    else:
        return jsonify({"error": f"No mapping found with prompt_id {prompt_id}"}), 404

    save_mappings(mappings)
    return redirect(url_for("index"))


@app.route("/finish", methods=["POST"])
def finish():
    mappings = load_mappings()
    pending = [m for m in mappings if is_gated(m) and not is_resolved(m)]
    if pending:
        return jsonify(
            {
                "error": f"{len(pending)} low-confidence mapping(s) still need review before finishing.",
                "pending": [f"{m['source_table']}.{m['source_column']}" for m in pending],
            }
        ), 400

    os.makedirs(os.path.dirname(REVIEWED_PATH) or ".", exist_ok=True)
    with open(REVIEWED_PATH, "w") as f:
        json.dump(mappings, f, indent=2)

    return jsonify({"message": f"Review complete. {len(mappings)} mappings exported to {REVIEWED_PATH}."})


@app.route("/status")
def status():
    """Machine-readable status -- lets workflow/langgraph_orchestrator.py poll this
    gate instead of scraping HTML."""
    mappings = load_mappings()
    gated = [m for m in mappings if is_gated(m)]
    pending = [m for m in gated if not is_resolved(m)]
    return jsonify(
        {
            "total_mappings": len(mappings),
            "gated_count": len(gated),
            "pending_count": len(pending),
            "ready_to_finish": len(pending) == 0 and len(mappings) > 0,
            "reviewed_export_exists": os.path.exists(REVIEWED_PATH),
        }
    )


if __name__ == "__main__":
    print(f"Loading mappings from {MAPPINGS_PATH}")
    print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
    print("Starting human review UI on http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)