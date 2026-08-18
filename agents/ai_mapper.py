"""
ai_mapper.py

LangChain agent (Claude) that reads the schema profile produced by
schema_profiler.py and generates, for every source column, a proposed
target-schema mapping with a confidence score and reasoning trace.

This mirrors the prompting pattern from the project spec exactly:
schema context + sample values + null rate + related columns (FKs) ->
forced structured JSON output with inferred_meaning, transformation_rule,
null_handling, edge_cases, confidence, reasoning.

Every mapping is stored alongside the prompt_id that produced it (full
traceability requirement) and, in a later step, traced to LangFuse.

Usage:
    python agents/ai_mapper.py
    python agents/ai_mapper.py --profile audit/schema_profile.json --output audit/ai_mappings.json
"""

import argparse
import json
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from pydantic import BaseModel, Field, field_validator

# Optional observability. Imported defensively for the same reason the rest
# of this module defends against truncated responses: a missing langfuse
# install, an unset key, or an unreachable collector must never stop a
# migration. trace_config() returns {} when tracing isn't configured, which
# LangChain treats as "no config".
try:
    import sys as _sys

    _sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "workflow")
    )
    from tracing import trace_config
except Exception:  # pragma: no cover
    def trace_config(node, **metadata):
        return {}

load_dotenv()

LLM_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-5")
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("ANTHROPIC_API_KEY")

SYSTEM_PROMPT = (
    "You are a data migration assistant. Given the following column metadata, "
    "infer the semantic meaning, generate a transformation rule, and return a "
    "confidence score with reasoning.\n\n"
    "Return JSON with fields: inferred_meaning, target_column, transformation_rule, "
    "null_handling, edge_cases, confidence (0.0-1.0), reasoning.\n\n"
    "Do not guess on values with no clear pattern -- flag those for human review "
    "by assigning a low confidence score (< 0.80) and explaining the ambiguity in "
    "reasoning. Never fabricate a mapping you aren't reasonably confident about."
)


class ColumnMapping(BaseModel):
    inferred_meaning: str = Field(description="Plain-English semantic meaning of this column")
    target_column: str = Field(description="Proposed snake_case target column name")
    transformation_rule: str = Field(
        description="SQL-like or CASE-style logic to transform source values to target values"
    )
    null_handling: str = Field(description="How NULLs / missing values should be handled")
    edge_cases: list[str] = Field(default_factory=list, description="Known edge cases or ambiguities")
    # confidence and reasoning default rather than requiring presence: if a
    # response is ever truncated before these trailing fields arrive (they're
    # emitted last), we'd otherwise discard target_column/transformation_rule/
    # etc. that DID arrive intact. Defaulting confidence to 0.0 guarantees a
    # truncated response is conservative -- it's forced into human review
    # rather than silently dropped or, worse, silently trusted.
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Confidence in this mapping, 0.0-1.0")
    reasoning: str = Field(
        default="[No reasoning returned -- response may have been truncated. Review manually.]",
        description="Why this mapping and confidence score were chosen",
    )

    @field_validator("edge_cases", mode="before")
    @classmethod
    def coerce_edge_cases(cls, v):
        """The LLM occasionally returns a bare string, null, or a list of
        non-string items (e.g. {"case": "...", "note": "..."} dicts) instead
        of list[str]. Normalize all of these instead of hard-failing the
        whole mapping over a formatting quirk."""
        if v is None:
            return []
        if isinstance(v, str):
            return [v] if v.strip() else []
        if isinstance(v, list):
            return [item if isinstance(item, str) else json.dumps(item) for item in v]
        return [str(v)]

    @field_validator("confidence", mode="before")
    @classmethod
    def coerce_confidence(cls, v):
        """The LLM occasionally returns confidence as a string ('0.8') or
        as a percentage (80). Normalize to a 0.0-1.0 float."""
        if v is None:
            return 0.0
        if isinstance(v, str):
            v = float(v.strip().rstrip("%"))
        if isinstance(v, (int, float)) and v > 1.0:
            v = v / 100.0
        return v


def build_column_prompt(table_name: str, column: dict, fk_graph: list, all_tables: dict) -> str:
    related = []
    for edge in fk_graph:
        if edge["from_table"] == table_name and edge["from_column"] == column["name"]:
            related.append(f"{edge['from_column']} (FK -> {edge['to_table']}.{edge['to_column']})")
        if edge["to_table"] == table_name and edge["to_column"] == column["name"]:
            related.append(f"{edge['from_table']}.{edge['from_column']} (FK -> this column)")

    # Also surface sibling columns in the same table for context (PKs, timestamps etc.)
    siblings = [
        c["name"] for c in all_tables[table_name]["columns"]
        if c["name"] != column["name"]
    ]

    sample_values = column.get("sample_values", [])
    value_counts = column.get("value_counts")

    prompt = (
        f"Column: {column['name']}\n"
        f"Type: {column['type']}\n"
        f"Sample values: {json.dumps(sample_values)}\n"
    )
    if value_counts:
        prompt += f"Full value distribution: {json.dumps(value_counts)}\n"
    prompt += (
        f"Null rate: {column['null_rate']}\n"
        f"Cardinality: {column['cardinality']}\n"
        f"Primary key: {column['primary_key']}\n"
        f"Table: {table_name}\n"
        f"Other columns in table: {siblings}\n"
        f"Related columns (FKs): {related if related else 'none'}\n"
    )
    return prompt


def map_column(llm, table_name: str, column: dict, fk_graph: list, all_tables: dict) -> dict:
    prompt_id = f"prompt-{uuid.uuid4()}"
    user_prompt = build_column_prompt(table_name, column, fk_graph, all_tables)

    structured_llm = llm.with_structured_output(ColumnMapping)
    # prompt_id is attached to the trace so a mapping decision can be walked
    # back to the exact call that produced it: the same id appears in
    # ai_mappings.json, in reviewed_mappings.json, and in every audit log
    # entry for this suggestion. That chain is what makes "every AI
    # suggestion traceable to a prompt ID" true in practice rather than
    # just being a field name.
    result: ColumnMapping = structured_llm.invoke(
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        config=trace_config(
            "ai_mapper",
            prompt_id=prompt_id,
            source_table=table_name,
            source_column=column["name"],
            column_type=column.get("type"),
            null_rate=column.get("null_rate"),
        ),
    )

    return {
        "source_table": table_name,
        "source_column": column["name"],
        "prompt_id": prompt_id,
        "prompt_text": user_prompt,
        "inferred_meaning": result.inferred_meaning,
        "target_column": result.target_column,
        "transformation_rule": result.transformation_rule,
        "null_handling": result.null_handling,
        "edge_cases": result.edge_cases,
        "confidence": result.confidence,
        "reasoning": result.reasoning,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def run_mapper(profile_path: str) -> list:
    if not LLM_API_KEY:
        raise RuntimeError(
            "LLM_API_KEY (or ANTHROPIC_API_KEY) is not set. Add it to your .env file."
        )

    with open(profile_path) as f:
        profile = json.load(f)

    # max_tokens capped explicitly so no provider can silently request the
    # model's max possible output (this bit us once already via a different
    # provider). 2048 rather than 1024: ambiguous, low-confidence columns
    # need the longest reasoning + edge_cases explanations -- exactly the
    # ones we most need to survive intact for the human reviewer -- and
    # 1024 was truncating those specific responses mid-JSON. Raising the
    # cap costs nothing extra unless a response actually needs it; you're
    # billed for tokens generated, not the ceiling.
    # NOTE: temperature is intentionally omitted. Claude Sonnet 5 and other
    # recent models reject the temperature parameter outright (HTTP 400:
    # "temperature is deprecated for this model") if it's present at all,
    # regardless of value -- Anthropic moved these models to fixed internal
    # sampling. Do not add temperature=0 back in for "determinism"; it will
    # break the request on these models.
    llm = ChatAnthropic(model=LLM_MODEL, api_key=LLM_API_KEY, max_tokens=2048)

    mappings = []
    for table_name, table_profile in profile["tables"].items():
        print(f"Mapping table `{table_name}` ({len(table_profile['columns'])} columns)...")
        for column in table_profile["columns"]:
            print(f"  -> {column['name']}...", end=" ")
            try:
                mapping = map_column(llm, table_name, column, profile["fk_graph"], profile["tables"])
                print(f"confidence={mapping['confidence']}")
            except Exception as e:
                print(f"FAILED ({e})")
                mapping = {
                    "source_table": table_name,
                    "source_column": column["name"],
                    "prompt_id": f"prompt-{uuid.uuid4()}",
                    "prompt_text": None,
                    "inferred_meaning": None,
                    "target_column": None,
                    "transformation_rule": None,
                    "null_handling": None,
                    "edge_cases": [],
                    "confidence": 0.0,
                    "reasoning": f"LLM call failed: {e}",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "error": True,
                }
            mappings.append(mapping)

    return mappings


def main():
    parser = argparse.ArgumentParser(description="Generate AI-proposed column mappings from a schema profile.")
    parser.add_argument("--profile", default="audit/schema_profile.json")
    parser.add_argument("--output", default="audit/ai_mappings.json")
    args = parser.parse_args()

    mappings = run_mapper(args.profile)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(mappings, f, indent=2)

    low_confidence = [m for m in mappings if m["confidence"] < float(os.getenv("CONFIDENCE_THRESHOLD", 0.80))]
    print(f"\n{len(mappings)} mappings written to {args.output}")
    print(f"{len(low_confidence)} mappings below confidence threshold -- will route to human_review_gate:")
    for m in low_confidence:
        print(f"  {m['source_table']}.{m['source_column']}: confidence={m['confidence']} -- {m.get('reasoning', '')[:80]}")


if __name__ == "__main__":
    main()