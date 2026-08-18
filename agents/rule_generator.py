"""
rule_generator.py

Converts human_review_gate's output (audit/reviewed_mappings.json) into
final transformation rules conforming EXACTLY to the Transformation Rule
Schema defined in the spec:

    {
      "source_column": "pat_st_cd",
      "target_column": "patient_status",
      "logic": "CASE WHEN src = 'A' THEN 'Active' ... END",
      "null_handling": "Map to NULL; flag in reconciliation report.",
      "edge_cases": ["Unknown codes default to NULL", "Trim whitespace before mapping"],
      "confidence": 0.84,
      "prompt_id": "prompt-uuid-abc123",
      "human_reviewed": false,
      "override_note": null
    }

One field is added beyond the spec's schema: "source_table". The spec's
example is single-table; this project has 5 tables with overlapping
column names, so source_table is required for migration_executor and
dbt to unambiguously target the right table. It's additive, not a
substitution for any required field.

Hard rule enforced here (spec section 07, "Blocks Pipeline"):
    Any rule with confidence < CONFIDENCE_THRESHOLD must have
    human_reviewed = true AND a non-null override_note before it is
    passed to migration_executor. Rules that don't meet this are
    unapproved and block the pipeline -- this script will refuse to
    mark the pipeline ready if any exist.

Usage:
    python agents/rule_generator.py
    python agents/rule_generator.py --input audit/reviewed_mappings.json --output audit/transformation_rules.json
"""

import argparse
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", 0.80))


def mapping_to_rule(mapping: dict) -> dict:
    """Maps ai_mapper/human_review_gate field names onto the exact spec schema."""
    return {
        "source_column": mapping["source_column"],
        "target_column": mapping["target_column"],
        "logic": mapping["transformation_rule"],
        "null_handling": mapping["null_handling"],
        "edge_cases": mapping.get("edge_cases", []),
        "confidence": mapping["confidence"],
        "prompt_id": mapping["prompt_id"],
        "human_reviewed": mapping.get("human_reviewed", False),
        "override_note": mapping.get("override_note"),
        # additive, not part of the spec schema -- see module docstring
        "source_table": mapping["source_table"],
    }


def check_blocking_violations(rules: list) -> list:
    """Returns rules that violate the confidence-gate rule: confidence < threshold
    but not (human_reviewed AND override_note is non-null/non-empty)."""
    violations = []
    for rule in rules:
        if rule["confidence"] < CONFIDENCE_THRESHOLD:
            reviewed = bool(rule.get("human_reviewed"))
            noted = bool(rule.get("override_note"))
            if not (reviewed and noted):
                violations.append(rule)
    return violations


def generate_rules(reviewed_mappings: list) -> tuple:
    rules = [mapping_to_rule(m) for m in reviewed_mappings]
    violations = check_blocking_violations(rules)
    return rules, violations


def main():
    parser = argparse.ArgumentParser(
        description="Generate transformation rules from reviewed mappings, enforcing the confidence gate."
    )
    parser.add_argument("--input", default="audit/reviewed_mappings.json")
    parser.add_argument("--output", default="audit/transformation_rules.json")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise SystemExit(
            f"{args.input} not found. Run agents/ai_mapper.py then complete review_ui/app.py first."
        )

    with open(args.input) as f:
        reviewed_mappings = json.load(f)

    rules, violations = generate_rules(reviewed_mappings)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "confidence_threshold": CONFIDENCE_THRESHOLD,
        "rule_count": len(rules),
        "pipeline_ready": len(violations) == 0,
        "blocking_violation_count": len(violations),
        "rules": rules,
    }
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    print(f"{len(rules)} transformation rules written to {args.output}")

    if violations:
        print(f"\n🚫 PIPELINE BLOCKED — {len(violations)} rule(s) violate the confidence gate:")
        for v in violations:
            print(
                f"  {v['source_table']}.{v['source_column']}: confidence={v['confidence']} "
                f"human_reviewed={v['human_reviewed']} override_note={v['override_note']!r}"
            )
        print(
            "\nThese rules have confidence below the threshold but were never reviewed "
            "(or reviewed without a note). Run review_ui/app.py and resolve them, then re-run this script."
        )
        raise SystemExit(1)
    else:
        print(f"\n✓ Pipeline ready. All {len(rules)} rules cleared the confidence gate.")


if __name__ == "__main__":
    main()
