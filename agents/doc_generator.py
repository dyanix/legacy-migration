"""
doc_generator -- node 7 of 7.

Generates the target data dictionary from the artifacts the earlier nodes
produced. Per the spec this is "the canonical documentation for the migrated
schema", covering column definitions, data lineage, transformation logic
applied, and any human overrides recorded.

Inputs (all written by earlier pipeline nodes):
    audit/transformation_rules.json  -- lineage + transformation logic
    audit/reviewed_mappings.json     -- human decisions and overrides
    audit/schema_profile.json        -- source column detail + PII/PHI flags
    audit/validation_report.json     -- per-table migration outcome

Outputs:
    docs/data_dictionary.md          -- human-readable canonical doc
    audit/data_dictionary.json       -- same content, machine-readable

Design note on the LLM: the spec says LangChain generates the dictionary.
Facts here are too consequential to paraphrase -- a column description that
misstates a transformation is worse than no description -- so the structure,
lineage, logic and overrides are assembled deterministically from the JSON
artifacts, and the model is used only to write prose descriptions of what
each column means. If no API key is configured the module degrades to
rule-derived descriptions and still produces a complete dictionary, so the
pipeline never blocks on model availability.
"""

import json
import os
from datetime import datetime, timezone

# The orchestrator calls load_dotenv() before importing nodes, but this
# module also runs standalone (python agents/doc_generator.py). Without
# this, ANTHROPIC_API_KEY is unset on the standalone path and description
# generation silently falls back to rule-derived text.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Tracing is optional: doc_generator runs standalone as well as inside the
# orchestrator, and a missing workflow package must not break it.
try:
    import sys as _sys

    _sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "workflow"))
    from tracing import trace_config
except Exception:  # pragma: no cover
    def trace_config(node, **metadata):
        return {}

RULES_PATH = os.getenv("TRANSFORMATION_RULES_PATH", "audit/transformation_rules.json")
REVIEWED_PATH = os.getenv("REVIEWED_MAPPINGS_PATH", "audit/reviewed_mappings.json")
PROFILE_PATH = os.getenv("SCHEMA_PROFILE_PATH", "audit/schema_profile.json")
VALIDATION_PATH = os.getenv("VALIDATION_REPORT_PATH", "audit/validation_report.json")

MARKDOWN_OUT = os.getenv("DATA_DICTIONARY_PATH", "docs/data_dictionary.md")
JSON_OUT = os.getenv("DATA_DICTIONARY_JSON_PATH", "audit/data_dictionary.json")

USE_LLM = os.getenv("DOC_GENERATOR_USE_LLM", "true").lower() not in ("false", "0", "no")

# Mirror agents/ai_mapper.py exactly: same env var names, same explicit
# api_key= argument. ai_mapper is the node that definitively makes a working
# LLM call, so it is the authority on credentials. Relying on ChatAnthropic
# picking the key up from the ambient environment silently disagreed with
# ai_mapper's explicit api_key= and produced an auth failure that this
# module then swallowed as "no descriptions available".
LLM_API_KEY = os.getenv("LLM_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
LLM_MODEL = (os.getenv("DOC_GENERATOR_MODEL")
             or os.getenv("LLM_MODEL")
             or "claude-sonnet-5")   # matches agents/ai_mapper.py default


# ---------------------------------------------------------------------------
# Input loading. Every reader is defensive: doc_generator runs last, so a
# missing or differently-shaped upstream artifact should degrade the document
# rather than crash the final node of a migration that already succeeded.
# ---------------------------------------------------------------------------

def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def _as_list(doc, *keys):
    """Pulls a list out of a doc that might be the list itself, or wrap it
    under any of several plausible keys."""
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict):
        for k in keys:
            if isinstance(doc.get(k), list):
                return doc[k]
    return []


def index_reviewed(reviewed_doc) -> dict:
    """(table, source_column) -> review record."""
    out = {}
    for m in _as_list(reviewed_doc, "mappings", "reviewed_mappings", "reviewed", "rules"):
        key = (m.get("source_table"), m.get("source_column"))
        if key != (None, None):
            out[key] = m
    return out


def index_profile(profile_doc) -> dict:
    """(table, source_column) -> profiled column record."""
    out = {}
    tables = (profile_doc or {}).get("tables", {})
    if isinstance(tables, dict):
        items = tables.items()
    else:
        items = [(t.get("name"), t) for t in tables if isinstance(t, dict)]
    for tname, tval in items:
        for col in (tval or {}).get("columns", []) or []:
            if isinstance(col, dict) and col.get("name"):
                out[(tname, col["name"])] = col
    return out


def index_validation(validation_doc) -> dict:
    out = {}
    for entry in _as_list(validation_doc, "reconciliation"):
        if entry.get("table"):
            out[entry["table"]] = entry
    return out


def column_pii(profile_col: dict) -> list:
    """Returns PII/PHI category labels for a column, tolerating the several
    shapes a profiler might use. Surfacing these in the dictionary matters
    more than usual here: this is clinical data, and the people reading this
    document are the ones deciding who gets access to which column."""
    if not profile_col:
        return []
    cats = profile_col.get("pii_categories") or profile_col.get("categories")
    if isinstance(cats, list) and cats:
        return [str(c) for c in cats]
    if isinstance(cats, str):
        return [cats]
    for flag in ("pii", "sensitive", "is_pii", "phi"):
        if profile_col.get(flag) is True:
            return ["sensitive"]
    return []


# ---------------------------------------------------------------------------
# Descriptions
# ---------------------------------------------------------------------------

def _response_text(content) -> str:
    """Extracts the assistant's text from a LangChain response.

    Recent Claude models return `content` as a LIST of typed blocks
    (thinking, text, ...) rather than a plain string. The previous code did
    `str(content)` on that list, which produces Python's repr of the whole
    structure -- signature blobs, thinking block and all -- and json.loads
    cannot parse it. The JSON was present and well-formed inside the last
    block's "text" field the entire time.

    The failure was silent: json.loads raised, the caller returned {}, and
    the document simply had no table overviews. It surfaced only as a
    changed word in a log line. Hence this helper is shared by both LLM
    call sites rather than fixed at one of them.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text" and block.get("text"):
                    parts.append(block["text"])
            elif isinstance(block, str):
                parts.append(block)
        # Joined with "" rather than a newline: if the model splits a JSON
        # string value across two blocks, an inserted newline lands INSIDE
        # that string and makes the document unparseable.
        return "".join(parts)
    return str(content)


def fallback_description(target_col: str, logic: str) -> str:
    """Rule-derived description used when no model is available. Deliberately
    describes the transformation rather than inventing business meaning."""
    words = target_col.replace("_", " ").strip()
    lg = (logic or "").upper()
    if "CASE" in lg:
        return f"{words}, derived by mapping source codes to explicit values."
    if "CAST" in lg and "DATE" in lg:
        return f"{words}, parsed from the source value into a date/time type."
    if "CAST" in lg:
        return f"{words}, type-cast from the source column."
    if "UPPER" in lg or "LOWER" in lg or "TRIM" in lg:
        return f"{words}, normalised from the source column."
    return f"{words}, carried over from the source column."


def llm_descriptions(columns: list, callbacks=None) -> dict:
    """Asks the model for one-line business descriptions, keyed by
    "table.column". Returns {} on any failure -- a missing description
    degrades the document, a crash loses the whole node.

    `callbacks` is passed through to LangChain so a LangFuse handler can be
    attached without touching this function.
    """
    if not USE_LLM or not LLM_API_KEY:
        print(f"[doc_generator] LLM skipped for column descriptions: "
              f"USE_LLM={USE_LLM}, api_key_set={bool(LLM_API_KEY)}")
        return {}
    try:
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import HumanMessage, SystemMessage
    except ImportError as e:
        print(f"[doc_generator] LLM skipped: langchain import failed ({e})")
        return {}

    payload = [{"key": f"{c['table']}.{c['target_column']}",
                "source_column": c["source_column"],
                "transformation": c["logic"]} for c in columns]

    system = (
        "You document migrated database schemas for a healthcare data platform. "
        "For each column return one plain-English sentence describing what the column holds. "
        "Base it only on the column name and transformation given -- never invent business "
        "rules, units, code meanings, or clinical semantics that are not present. "
        "If a column's meaning is unclear, say so plainly. "
        "Return ONLY a JSON object mapping each key to its sentence, no prose, no markdown."
    )
    try:
        # temperature intentionally omitted -- recent Claude models reject the
        # parameter outright (HTTP 400) regardless of value, as documented in
        # agents/ai_mapper.py. Passing it would break this call the moment
        # LLM_MODEL is bumped, and the except block below would hide it as
        # "no descriptions available".
        llm = ChatAnthropic(model=LLM_MODEL, api_key=LLM_API_KEY, max_tokens=4000)
        resp = llm.invoke(
            [SystemMessage(content=system),
             HumanMessage(content=json.dumps(payload, indent=2))],
            config=_merged_config("doc_generator.column_descriptions", callbacks,
                                  columns=len(payload)),
        )
        text = _response_text(resp.content)
        text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        # strict=False tolerates raw newlines inside string values. The
        # model writes multi-sentence paragraphs and sometimes emits a real
        # line break rather than \n, which strict JSON rejects outright.
        parsed = json.loads(text, strict=False)
        if not isinstance(parsed, dict):
            # Also a silent exit before: a JSON array parses fine but is
            # not the shape we asked for, and returning {} looked identical
            # to the model having nothing to say.
            print(f"[doc_generator] LLM returned {type(parsed).__name__}, expected object; ignoring.")
            return {}
        return parsed
    except Exception as e:
        print(f"[doc_generator] LLM response could not be used: {type(e).__name__}: {e}")
        return {}


def _merged_config(node: str, callbacks, **metadata) -> dict:
    """Combines LangFuse tracing with any callbacks the caller passed in,
    rather than letting one silently replace the other."""
    cfg = trace_config(node, **metadata)
    if callbacks:
        cfg["callbacks"] = list(cfg.get("callbacks", [])) + list(callbacks)
    return cfg or None


def llm_table_overviews(tables: dict, validation: dict, callbacks=None) -> dict:
    """Asks the model for a short prose overview of each migrated table.

    This is where LangChain earns its place in this node. Column-level
    descriptions are NOT regenerated here -- ai_mapper already produced an
    inferred_meaning for each and a human approved it, so asking a model to
    restate them costs money and risks contradicting an approved mapping.
    Table-level narrative exists in no upstream artifact.

    One call PER TABLE, returning plain prose rather than one call returning
    JSON for all five. The JSON version failed three times in a row on
    different malformations -- a literal newline inside a string, then an
    unescaped delimiter -- because asking a model to emit multi-sentence
    English prose *inside* a JSON string makes every apostrophe, quote and
    line break a potential parse error, and one bad character loses all five
    paragraphs. Plain text has no parser to fail. The cost is five small
    calls instead of one; the benefit is that a failure on one table cannot
    take the others down, and each table gets its own LangFuse trace.

    Returns {} on total failure -- documentation degrades, the pipeline does
    not.
    """
    if not USE_LLM or not LLM_API_KEY:
        print(f"[doc_generator] LLM skipped for table overviews: "
              f"USE_LLM={USE_LLM}, api_key_set={bool(LLM_API_KEY)}")
        return {}
    try:
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import HumanMessage, SystemMessage
    except ImportError as e:
        print(f"[doc_generator] LLM skipped: langchain import failed ({e})")
        return {}

    system = (
        "You write data dictionaries for a healthcare data platform. Given one "
        "table's columns and row count, write 2-3 sentences covering: what the "
        "table holds, how it relates to the other tables named, and any caveat a "
        "consumer should know (sensitive columns, human-reviewed mappings, "
        "documented edge cases). "
        "Use ONLY the facts given. Never invent clinical semantics, code meanings, "
        "units, retention rules, or regulatory claims. Do not restate every column. "
        "Return ONLY the paragraph as plain text -- no JSON, no markdown, no "
        "heading, no preamble."
    )

    try:
        llm = ChatAnthropic(model=LLM_MODEL, api_key=LLM_API_KEY, max_tokens=1000)
    except Exception as e:
        print(f"[doc_generator] LLM unavailable: {type(e).__name__}: {e}")
        return {}

    overviews = {}
    for table, cols in tables.items():
        v = validation.get(table, {})
        payload = {
            "table": table,
            "other_tables": [t for t in tables if t != table],
            "row_count": v.get("target_row_count"),
            "load_mode": v.get("load_mode"),
            "columns": [
                {"name": c["target_column"], "meaning": c.get("inferred_meaning"),
                 "sensitive": c["pii_categories"] or None,
                 "human_reviewed": c.get("human_reviewed") or None,
                 "edge_cases": c.get("edge_cases") or None}
                for c in cols
            ],
        }
        try:
            resp = llm.invoke(
                [SystemMessage(content=system),
                 HumanMessage(content=json.dumps(payload, indent=2, default=str))],
                config=_merged_config("doc_generator.table_overview", callbacks,
                                      table=table, columns=len(cols)),
            )
            text = _response_text(resp.content).strip()
            # Strip a stray fence or heading if the model adds one anyway.
            text = text.removeprefix("```").removesuffix("```").strip()
            if text.startswith("#"):
                text = text.split("\n", 1)[-1].strip()
            if text:
                overviews[table] = text
        except Exception as e:
            # Per-table so one failure cannot lose the other four.
            print(f"[doc_generator] overview failed for {table}: {type(e).__name__}: {e}")

    return overviews


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_dictionary(callbacks=None) -> dict:
    rules_doc = load_json(RULES_PATH)
    if not rules_doc:
        raise SystemExit(f"Cannot build data dictionary: {RULES_PATH} not found. Run the pipeline first.")

    reviewed = index_reviewed(load_json(REVIEWED_PATH, {}))
    profile = index_profile(load_json(PROFILE_PATH, {}))
    validation = index_validation(load_json(VALIDATION_PATH, {}))

    tables = {}
    flat_columns = []

    for rule in _as_list(rules_doc, "rules"):
        table = rule.get("source_table")
        target_col = rule.get("target_column")
        source_col = rule.get("source_column")
        if not table or not target_col:
            continue

        review = reviewed.get((table, source_col), {})
        prof_col = profile.get((table, source_col), {})

        entry = {
            "target_column": target_col,
            "source_column": source_col,
            "source_data_type": prof_col.get("data_type"),
            "nullable": prof_col.get("nullable"),
            "primary_key": bool(prof_col.get("primary_key")),
            "transformation": rule.get("logic"),
            "confidence": rule.get("confidence", review.get("confidence")),
            "rule_id": rule.get("rule_id"),
            # Prompt traceability: the spec requires every AI suggestion be
            # traceable to a prompt ID, and ai_mapper records one per mapping.
            "prompt_id": rule.get("prompt_id") or review.get("prompt_id"),
            "ai_reasoning": review.get("reasoning"),
            "null_handling": review.get("null_handling"),
            "edge_cases": review.get("edge_cases"),
            # Review fields, using ai_mapper/review_ui's actual key names.
            "human_reviewed": bool(review.get("human_reviewed")),
            "review_decision": review.get("review_decision"),
            "reviewed_at": review.get("reviewed_at"),
            "override_rationale": review.get("override_note"),
            "reviewer": review.get("reviewer"),
            "original_target_column": review.get("original_target_column"),
            "pii_categories": column_pii(prof_col),
            "source_null_rate": prof_col.get("null_rate"),
            # inferred_meaning is ai_mapper's own description of the column,
            # already produced and already human-approved. Preferred over a
            # fresh LLM call: it costs nothing, and it cannot contradict the
            # mapping a reviewer signed off on.
            "inferred_meaning": review.get("inferred_meaning"),
        }
        tables.setdefault(table, []).append(entry)
        flat_columns.append({"table": table, "target_column": target_col,
                             "source_column": source_col, "logic": rule.get("logic")})

    # Only ask the model about columns that have no approved meaning already.
    needs_llm = [c for c in flat_columns
                 if not (tables[c["table"]] and any(
                     e["target_column"] == c["target_column"] and e.get("inferred_meaning")
                     for e in tables[c["table"]]))]
    descriptions = llm_descriptions(needs_llm, callbacks=callbacks) if needs_llm else {}

    sources = set()
    for table, cols in tables.items():
        for c in cols:
            key = f"{table}.{c['target_column']}"
            if c.get("inferred_meaning"):
                c["description"] = c["inferred_meaning"]
                sources.add("approved mappings")
            elif descriptions.get(key):
                c["description"] = descriptions[key]
                sources.add("llm")
            else:
                c["description"] = fallback_description(c["target_column"], c["transformation"])
                sources.add("rule-derived")
    llm_used = ", ".join(sorted(sources)) if sources else "rule-derived"

    overviews = llm_table_overviews(tables, validation, callbacks=callbacks)
    if overviews:
        sources.add("llm table overviews")
        llm_used = ", ".join(sorted(sources))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "table_overviews": overviews,
        "descriptions_source": llm_used,
        "table_count": len(tables),
        "column_count": sum(len(v) for v in tables.values()),
        "tables": tables,
        "validation": validation,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _esc(v) -> str:
    """Markdown table cells break on unescaped pipes -- CASE expressions and
    regex transformations contain them regularly."""
    if v is None or v == "":
        return "-"
    return str(v).replace("|", "\\|").replace("\n", " ").strip()


def render_markdown(d: dict) -> str:
    L = []
    L.append("# Target Data Dictionary")
    L.append("")
    L.append(f"Generated {d['generated_at']} by `doc_generator`. "
             f"{d['table_count']} tables, {d['column_count']} columns. "
             f"Column descriptions are {d['descriptions_source']}.")
    L.append("")
    L.append("Do not edit by hand -- this file is regenerated on every pipeline run "
             "from the approved mappings and transformation rules.")
    L.append("")

    # Migration summary
    if d.get("validation"):
        L.append("## Migration summary")
        L.append("")
        L.append("| Table | Source rows | Target rows | Load mode | Status |")
        L.append("|---|---|---|---|---|")
        for t, v in d["validation"].items():
            status = "OK" if not v.get("hard_failures") else "FAILED"
            L.append(f"| `{t}` | {_esc(v.get('source_row_count'))} | {_esc(v.get('target_row_count'))} "
                     f"| {_esc(v.get('load_mode'))} | {status} |")
        L.append("")

    # PII inventory -- collected first so it reads before the detail tables.
    pii_rows = [(t, c) for t, cols in d["tables"].items() for c in cols if c["pii_categories"]]
    if pii_rows:
        L.append("## Sensitive column inventory")
        L.append("")
        L.append(f"{len(pii_rows)} columns were flagged as PII/PHI by the schema profiler. "
                 "Access to these should be restricted in the target warehouse.")
        L.append("")
        L.append("| Table | Column | Categories |")
        L.append("|---|---|---|")
        for t, c in pii_rows:
            L.append(f"| `{t}` | `{c['target_column']}` | {_esc(', '.join(c['pii_categories']))} |")
        L.append("")

    # Human overrides -- the audit-relevant section.
    overrides = [(t, c) for t, cols in d["tables"].items() for c in cols
                 if c.get("human_reviewed") or c.get("override_rationale")
                 or c.get("original_target_column")]
    L.append("## Human review overrides")
    L.append("")
    if overrides:
        L.append(f"{len(overrides)} mapping(s) were changed or explicitly approved by a human reviewer "
                 "rather than auto-approved by confidence score.")
        L.append("")
        for t, c in overrides:
            L.append(f"### `{t}.{c['target_column']}`")
            L.append("")
            if c.get("original_target_column"):
                L.append(f"- AI proposed: `{c['original_target_column']}` -> final: `{c['target_column']}`")
            L.append(f"- Confidence: {_esc(c.get('confidence'))}")
            L.append(f"- Decision: {_esc(c.get('review_decision'))} by {_esc(c.get('reviewer'))} "
                     f"at {_esc(c.get('reviewed_at'))}")
            if c.get("override_rationale"):
                L.append(f"- Rationale: {_esc(c['override_rationale'])}")
            if c.get("prompt_id"):
                L.append(f"- Prompt ID: `{c['prompt_id']}`")
            L.append("")
    else:
        L.append("No human overrides recorded -- every mapping cleared the confidence gate.")
        L.append("")

    # Per-table detail
    L.append("## Tables")
    L.append("")
    for table, cols in d["tables"].items():
        L.append(f"### `{table}`")
        L.append("")
        overview = (d.get("table_overviews") or {}).get(table)
        if overview:
            L.append(str(overview).strip())
            L.append("")
        L.append("| Target column | Source column | Type | Transformation | Confidence | Sensitive | Description |")
        L.append("|---|---|---|---|---|---|---|")
        for c in cols:
            name = f"`{c['target_column']}`" + (" **(PK)**" if c["primary_key"] else "")
            L.append(
                f"| {name} | `{_esc(c['source_column'])}` | {_esc(c['source_data_type'])} "
                f"| `{_esc(c['transformation'])}` | {_esc(c['confidence'])} "
                f"| {_esc(', '.join(c['pii_categories'])) if c['pii_categories'] else '-'} "
                f"| {_esc(c['description'])} |"
            )
        L.append("")

    # AI provenance -- the spec requires every AI suggestion be traceable to
    # a prompt ID. Kept as its own table rather than extra columns on the
    # per-table tables, which are already wide enough to wrap badly.
    traceable = [(t, c) for t, cols in d["tables"].items() for c in cols if c.get("prompt_id")]
    if traceable:
        L.append("## AI provenance")
        L.append("")
        L.append(f"{len(traceable)} of {d['column_count']} column mappings trace to a recorded "
                 "prompt ID. Full prompt text and model reasoning are in "
                 "`audit/reviewed_mappings.json`.")
        L.append("")
        L.append("| Table | Column | Prompt ID | Confidence | Review |")
        L.append("|---|---|---|---|---|")
        for t, c in traceable:
            review = c.get("review_decision") or ("human reviewed" if c.get("human_reviewed") else "auto-approved")
            L.append(f"| `{t}` | `{c['target_column']}` | `{_esc(c['prompt_id'])}` "
                     f"| {_esc(c.get('confidence'))} | {_esc(review)} |")
        L.append("")

    L.append("## Lineage")
    L.append("")
    L.append("Every target column traces to exactly one source column via the transformation shown above. "
             "Full machine-readable lineage is in `audit/data_dictionary.json`; the authoritative rules "
             "are in `audit/transformation_rules.json`.")
    L.append("")
    return "\n".join(L)


def generate(callbacks=None, verbose: bool = True) -> dict:
    d = build_dictionary(callbacks=callbacks)

    os.makedirs(os.path.dirname(MARKDOWN_OUT) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(JSON_OUT) or ".", exist_ok=True)

    with open(MARKDOWN_OUT, "w") as f:
        f.write(render_markdown(d))
    with open(JSON_OUT, "w") as f:
        json.dump(d, f, indent=2)

    pii_count = sum(1 for cols in d["tables"].values() for c in cols if c["pii_categories"])
    overrides = sum(1 for cols in d["tables"].values() for c in cols
                    if c.get("human_reviewed") or c.get("override_rationale")
                    or c.get("original_target_column"))
    if verbose:
        print(f"[doc_generator] {d['table_count']} tables, {d['column_count']} columns documented "
              f"({d['descriptions_source']} descriptions).")
        print(f"[doc_generator] {pii_count} sensitive column(s), {overrides} human override(s).")
        print(f"[doc_generator] Wrote {MARKDOWN_OUT} and {JSON_OUT}.")

    return {"markdown_path": MARKDOWN_OUT, "json_path": JSON_OUT,
            "table_count": d["table_count"], "column_count": d["column_count"],
            "sensitive_columns": pii_count, "human_overrides": overrides,
            "descriptions_source": d["descriptions_source"]}


if __name__ == "__main__":
    generate()