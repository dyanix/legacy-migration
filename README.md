# Legacy Migration Platform

An AI-assisted data migration pipeline that moves a legacy MySQL schema into a
target warehouse, with a human-in-the-loop confidence gate, two independent
validation layers, and a tamper-evident audit trail.

---

## Project Overview

A hospital group's legacy MySQL database must move to a modern warehouse. A
prior attempt applied a status-code mapping inconsistently and silently
corrupted records, so the requirement is not just to move the data — it is that
**every transformation rule is documented, reviewed, and traceable before any
data moves**.

This platform runs as a LangGraph workflow of eight nodes. An LLM proposes a
mapping for every source column with a confidence score and reasoning; anything
below the confidence threshold is blocked until a human approves or edits it
with a written rationale; approved rules are compiled, executed with retry
logic, validated at three checkpoints, and documented into a data dictionary.

**What it produces for a 5-table, 28-column healthcare schema:**

| Output | Location |
|---|---|
| Schema profile — null rates, cardinality, FK graph, PII/PHI flags | `audit/schema_profile.json` |
| AI mappings with confidence + reasoning | `audit/ai_mappings.json` |
| Human-reviewed mappings with override notes | `audit/reviewed_mappings.json` |
| Transformation rules (spec schema) | `audit/transformation_rules.json` |
| Reconciliation + validation report | `audit/validation_report.json` |
| Target data dictionary | `docs/data_dictionary.md` |
| Tamper-evident audit log | `audit/migration_audit_log.json` |

Design rationale for the choices below lives in **[DECISIONS.md](DECISIONS.md)**.

---

## Architecture Description

```
Legacy MySQL (Docker)
        │
        ▼
┌─ LangGraph orchestrator ────────────────────────────────────┐
│                                                             │
│  1. schema_profiler      introspects source, flags PII/PHI  │
│  2. ai_mapper            LangChain + Claude → mappings      │
│  3. human_review_gate    confidence < 0.80 blocks here      │
│         ├── auto-approve (≥ 0.80)                           │
│         └── human review (Flask UI, note required)          │
│  4. rule_generator       compiles rules, enforces the gate  │
│  5. dependency_resolver  FK-derived load order   [bonus]    │
│  6. migration_executor   ETL, retry, table filter           │
│  7. validator            3 GE checkpoints + recon + dbt     │
│  8. doc_generator        data dictionary + lineage          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
        │                              │
        ▼                              ▼
  Migration target              audit/migration_audit_log.json
  (Snowflake)                   (hash-chained, all 8 nodes)
```

See `docs/architecture.png` for the full diagram.

**Node responsibilities**

- **schema_profiler** — per column: null rate, cardinality, value distribution
  (full counts when ≤ 25 distinct, which is what surfaces junk status codes),
  plus pattern-based PII/PHI flagging.
- **ai_mapper** — one LLM call per column with schema context, sample values,
  null rate and FK relationships; returns structured JSON with
  `inferred_meaning`, `transformation_rule`, `null_handling`, `edge_cases`,
  `confidence`, `reasoning`, and a `prompt_id` for traceability.
- **human_review_gate** — Flask UI at `localhost:5050`. Mappings below the
  threshold require a decision *and* a non-empty note. Decisions are revisable
  after export; revisions append to `revision_history` rather than overwriting.
- **rule_generator** — compiles to the spec's transformation-rule schema and
  refuses to mark the pipeline ready if any low-confidence rule lacks review.
- **dependency_resolver** *(bonus)* — topologically sorts tables from the FK
  graph so parents load before children.
- **migration_executor** — extract → transform → load with exponential backoff,
  incremental watermarks, and a preflight that dry-runs every table's query
  before opening a connection.
- **validator** — Great Expectations at `source_baseline`, `post_extraction`,
  and `post_load`; a reconciliation report; and dbt tests as an independent
  second layer. Only GE and reconciliation can block.
- **doc_generator** — data dictionary with column definitions, lineage,
  transformation logic, human overrides, and a sensitive-column inventory.

**Bonus features:** Flask review UI, multi-table dependency resolution,
incremental migration via watermarks, automated PII/PHI detection.

---

## Setup Instructions

**Prerequisites:** Python 3.10+, Docker, an Anthropic API key.

```bash
# 1. Source database
docker compose up -d
docker compose exec legacy_mysql python3 /tmp/02_seed.py   # if not auto-seeded

# 2. Python environment
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Configuration
cp .env.example .env
# then edit .env and set LLM_API_KEY

# 4. Verify
python -c "
from dotenv import load_dotenv; load_dotenv()
import os; from sqlalchemy import create_engine, text
with create_engine(os.getenv('SOURCE_DB_URL')).connect() as c:
    print('source tables:', [r[0] for r in c.execute(text('SHOW TABLES'))])
print('LLM key set:', bool(os.getenv('LLM_API_KEY')))
"
```

**Optional — dbt second validation layer.**

```bash
pip install dbt-snowflake
```

Then create `~/.dbt/profiles.yml` — it lives outside the repo, per dbt
convention, because it holds credentials. Format is in
`validation/dbt_models/README.md`.

Verify before relying on it:

```bash
cd validation/dbt_models && dbt debug; cd ../..
```

Look for `Registered adapter: snowflake` and `All checks passed!`.

**Optional — LangFuse tracing.** Get keys from
[cloud.langfuse.com](https://cloud.langfuse.com), add them to `.env`, then:

```bash
python -c "from workflow.tracing import status; print(status())"
```

---

## Environment Variables

All are documented in `.env.example`. `.env` is gitignored and must never be
committed.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SOURCE_DB_URL` | yes | — | Legacy MySQL connection. Port 3307 avoids clashing with a local MySQL. |
| `LLM_API_KEY` | yes | — | Anthropic key. `ANTHROPIC_API_KEY` accepted as a fallback name. Pipeline cannot run from scratch without it. |
| `LLM_MODEL` | recommended | `claude-sonnet-5` | Set explicitly — `ai_mapper` and `doc_generator` carry separate fallback defaults, so leaving it unset can run the two nodes on different models. |
| `CONFIDENCE_THRESHOLD` | no | `0.80` | Mappings below this route to human review. See DECISIONS.md §1. |
| `SNOWFLAKE_ACCOUNT` | yes | — | Account identifier in `ORG-ACCOUNT` form, e.g. `PNPKZBK-WO58094`. **Not** the full `https://...snowflakecomputing.com` URL — passing the URL is the most common connection failure. Get it with `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`. |
| `SNOWFLAKE_USER` | yes | — | Login name. Confirm with `SELECT CURRENT_USER();` — the UI display name is not always the login name. |
| `SNOWFLAKE_PASSWORD` | yes | — | Quote it in `.env` if it contains `#`, or dotenv truncates at that character. |
| `SNOWFLAKE_ROLE` | yes | — | See DECISIONS.md §2 on least privilege. |
| `SNOWFLAKE_WAREHOUSE` | yes | — | e.g. `COMPUTE_WH`. |
| `SNOWFLAKE_DATABASE` | yes | — | e.g. `MIGRATION_TARGET`. |
| `SNOWFLAKE_SCHEMA` | no | `PUBLIC` | Schema holding the migrated tables. |
| `DBT_TARGET_SCHEMA` | yes | `main` | Schema the generated dbt sources point at. **Must equal `SNOWFLAKE_SCHEMA`.** The orchestrator regenerates dbt models on every run, so setting this only on the command line is not enough — it must be in `.env`, or dbt fails with `Schema 'MIGRATION_TARGET.MAIN' does not exist`. |
| `LANGFUSE_PUBLIC_KEY` | no | — | Tracing. Absent → tracing is a silent no-op. |
| `LANGFUSE_SECRET_KEY` | no | — | Tracing. |
| `LANGFUSE_HOST` | no | `https://cloud.langfuse.com` | Self-hosted collectors also work. |
| `LANGFUSE_ENABLED` | no | `true` | Set `false` to disable without removing keys. |
| `MIGRATE_TABLES` | no | all | Comma-separated subset, e.g. `dept_master,prv_tbl`. |
| `SNOWFLAKE_*` | only for snowflake | — | `ACCOUNT`, `USER`, `PASSWORD`, `ROLE`, `WAREHOUSE`, `DATABASE`, `SCHEMA`. |

---

## First-time Snowflake setup

Run once in a Snowflake worksheet:

```sql
USE ROLE ACCOUNTADMIN;

ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS MIGRATION_TARGET;
CREATE SCHEMA IF NOT EXISTS MIGRATION_TARGET.PUBLIC;

SELECT CURRENT_ORGANIZATION_NAME() AS org, CURRENT_ACCOUNT_NAME() AS acct;
```

`SNOWFLAKE_ACCOUNT` is those two values joined with a hyphen.

`AUTO_SUSPEND = 60` matters: an idle warehouse still bills, and this workload
runs in bursts.

Verify the connection before loading anything — a failed 52,000-row load is a
slow way to discover a bad credential:

```bash
python -c "
from dotenv import load_dotenv; load_dotenv()
import sys; sys.path.insert(0,'workflow')
from migration_executor import get_target_writer
w = get_target_writer().connect()
cur = w.conn.cursor()
cur.execute('SELECT CURRENT_ACCOUNT(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()')
print('connected:', cur.fetchone()); w.close()
"
```

---

## How to Run the Migration Pipeline

```bash
python workflow/langgraph_orchestrator.py
```

Nodes **skip when their output already exists**, so re-running resumes rather
than repeating — this specifically avoids re-billing LLM calls. To force a node
to re-run, delete its artifact (see [Rollback](#how-to-rollback-a-failed-migration)).

### First run — the pipeline will pause

On a clean run, `ai_mapper` produces mappings and the gate halts:

```
[human_review_gate] 13 mapping(s) below the confidence threshold still need
human review: [...]. Run `python review_ui/app.py`, open http://localhost:5050,
resolve each pending item, click Finish Review & Export, then re-run.
```

**This pause is the point of the platform, not an error.** To resolve:

```bash
python review_ui/app.py     # then open http://localhost:5050
```

For each pending mapping: review the model's reasoning and edge cases, edit the
target column / transformation rule / null handling if needed, **write a review
note (required)**, then **Approve as shown** or **Save edits & override**.

When nothing is pending, click **Finish Review & Export** — this writes
`audit/reviewed_mappings.json`, which `rule_generator` consumes. Editing a
mapping without exporting has no effect on the pipeline.

Then re-run the orchestrator. Expected final state:

```
[validator] Validation passed for 5 table(s).
[doc_generator] Documented 5 table(s), 32 column(s) ...
Final status: DOCUMENTATION_COMPLETE
```

### Revising a decision after export

Restart the review UI, expand **Revise this decision** on any resolved mapping
(or **Correct this mapping** on an auto-approved one), save, then
**Re-export with revisions** and re-run. The superseded decision is retained in
`revision_history`.

### Running individual stages

```bash
python agents/schema_profiler.py
python agents/ai_mapper.py
python agents/rule_generator.py
python workflow/migration_executor.py
python workflow/validator.py
python agents/doc_generator.py
```

### Migrating a subset of tables

```bash
python workflow/migration_executor.py --tables dept_master,prv_tbl
```

Dependency order is preserved regardless of argument order. If a selected table
has a parent that is excluded, the executor warns rather than silently pulling
the parent in — foreign keys will only resolve if the parent was migrated
previously.

### Terminal statuses

| Status | Meaning |
|---|---|
| `DOCUMENTATION_COMPLETE` | Full success. |
| `PAUSED_FOR_HUMAN_REVIEW` | Gate is waiting. Not a failure. |
| `BLOCKED` | Validation found a hard failure. Nothing downstream ran. |
| `MIGRATION_FAILED` | Executor error. See the run log. |
| `VALIDATION_FAILED` | Validator itself errored. |

---

## How to Rollback a Failed Migration

**There is no transactional rollback across tables.** The executor loads table
by table, so a failure partway through leaves earlier tables already written.
Recovery is by deterministic rebuild, not by undo. The procedures below are what
to actually do.

### Before you migrate — take a snapshot

The only true rollback is a restore, so make one possible. Snowflake zero-copy
clones are instant and cost nothing until the source diverges:

```sql
CREATE SCHEMA IF NOT EXISTS MIGRATION_TARGET.BACKUP_20260812
  CLONE MIGRATION_TARGET.PUBLIC;
```

To restore:

```sql
CREATE OR REPLACE SCHEMA MIGRATION_TARGET.PUBLIC
  CLONE MIGRATION_TARGET.BACKUP_20260812;
```

Snowflake also keeps Time Travel by default, so a table can be recovered even
without a prior snapshot:

```sql
CREATE OR REPLACE TABLE ENC_LOG CLONE ENC_LOG AT(OFFSET => -3600);
```

Retention is 1 day on standard editions — enough to undo a bad run, not a
substitute for a snapshot.

### Scenario 1 — validation blocked the run

Nothing to roll back at the pipeline level: `doc_generator` never ran, and the
target holds data that failed validation. Read the failure, fix the rule, rerun.

```bash
python -c "
import json; r=json.load(open('audit/validation_report.json'))
print('success:', r['success'])
for f in r['hard_failures']: print(' -', f)
"
```

If it is a transformation problem, revise the rule in the review UI, re-export,
and re-run. A full reload then overwrites the bad data — see Scenario 3.

### Scenario 2 — the executor failed partway

Some tables loaded, others did not. Establish what actually landed:

```bash
python -c "
import json; r=json.load(open('audit/migration_results.json'))
for t in r.get('tables', []):
    print(f\"{t['table']:18s} {t['mode']:12s} loaded={t['rows_migrated']:<7} target={t.get('rows_in_target_after')}\"  )
"
```

Then rebuild only the affected tables:

```bash
python workflow/migration_executor.py --tables enc_log,dx_codes --full-reload
```

A full load drops and recreates the table before loading, so a rebuild is
idempotent — no duplicate rows from a retry.

### Scenario 3 — restore the target to a clean state

```bash
# Rebuild everything from source using the current rules
python workflow/migration_executor.py --full-reload

# Or restore a snapshot
cp target/backup_YYYYMMDD_HHMMSS.db target/migration_target.db
```

### Scenario 4 — reset stale incremental state

Watermarks drive incremental loads. If a table reports `0 rows extracted` with a
stale-data warning, its watermark is ahead of the data you want reloaded:

```bash
rm audit/watermarks.json                                   # all tables full-load next run
python workflow/migration_executor.py --full-reload        # equivalent, one-shot
```

**Always run `--full-reload` after changing any mapping or rule.** Incremental
mode only loads rows newer than the watermark, so an existing table keeps data
transformed by the *previous* rules. Row counts will still match and validation
will still pass — the staleness is invisible to counts.

### Scenario 5 — redo an AI or review stage

Each node skips when its output exists, so rollback means deleting artifacts.
Delete downstream ones too, or they will be reused.

| To redo | Delete |
|---|---|
| Schema profiling | `audit/schema_profile.json` |
| AI mapping *(re-bills LLM calls)* | `audit/ai_mappings.json` |
| Human review | `audit/reviewed_mappings.json` |
| Rules | `audit/transformation_rules.json` |
| Everything | `rm audit/*.json target/*.db` |

**Do not delete `audit/migration_audit_log.json` to "clean up".** It is the
append-only record of every run, and removing it destroys the audit trail —
including the evidence of whatever went wrong.

### After any rollback

```bash
python workflow/langgraph_orchestrator.py
python -c "from workflow.langgraph_orchestrator import verify_audit_log; print(verify_audit_log())"
```

---

## How to Inspect AI Decisions

Every AI suggestion is traceable end to end by its `prompt_id`.

### The audit log

`audit/migration_audit_log.json` records every node execution, every AI
suggestion, and every human decision.

```bash
# Summary
python -c "
import json, collections
e = json.load(open('audit/migration_audit_log.json'))
print('entries:', len(e))
print('by status:', dict(collections.Counter(x['status'] for x in e)))
"

# Verify nothing has been tampered with
python -c "from workflow.langgraph_orchestrator import verify_audit_log; print(verify_audit_log())"

# Every human decision, with rationale
python -c "
import json
for x in json.load(open('audit/migration_audit_log.json')):
    if x['status'] == 'human_decision':
        p = x['payload']
        print(f\"{p['source_table']}.{p['source_column']} -> {p['target_column']}\")
        print(f\"   {p['review_decision']} | confidence {p['confidence']} | prompt {p['prompt_id']}\")
        print(f\"   {p['override_note']}\n\")
"
```

Each entry carries `prev_hash` and `entry_hash`, forming a SHA-256 chain.
Editing or deleting an entry breaks every hash after it and `verify_audit_log()`
reports where.

### Tracing one column end to end

```bash
python -c "
import json
COL = 'created_ts'
m = [x for x in json.load(open('audit/reviewed_mappings.json')) if x['source_column'] == COL][0]
print('prompt_id     :', m['prompt_id'])
print('AI proposed   :', (m.get('ai_original') or {}).get('transformation_rule', m['transformation_rule']))
print('confidence    :', m['confidence'])
print('AI reasoning  :', m['reasoning'][:200])
print('human review  :', m.get('review_decision'), '|', m.get('reviewed_at'))
print('final rule    :', m['transformation_rule'])
print('rationale     :', m.get('override_note'))
print('revisions     :', len(m.get('revision_history', [])))
"
```

The same `prompt_id` appears in `ai_mappings.json`, `reviewed_mappings.json`,
`transformation_rules.json`, the audit log, the data dictionary's **AI
provenance** table, and LangFuse trace metadata.

### LangFuse

Every LLM call in `ai_mapper` and `doc_generator` is traced. Each run prints its
session ID:

```
LangFuse: traced under session migration-bef5bf6fd4f3 at https://cloud.langfuse.com
```

Open [cloud.langfuse.com](https://cloud.langfuse.com) → **Traces**, filter by
that session. Each trace shows the exact prompt sent, the raw response before
parsing, token counts, cost, latency, and metadata including `prompt_id`,
`source_table`, and `source_column`.

The raw response matters: it is the only place to see what the model actually
returned *before* structured-output parsing — which is how a confidence-parsing
failure was diagnosed (DECISIONS.md §3.4).

Tracing degrades to a no-op without keys; the pipeline runs identically.
Screenshots for evaluators without account access are in `docs/`.

### The data dictionary

`docs/data_dictionary.md` — per table: column definitions, source → target
lineage, transformation logic applied, human overrides with rationale, an AI
provenance table mapping every column to its prompt ID, and a sensitive-column
inventory.

---

## Known Limitations

Full detail in [DECISIONS.md §6–7](DECISIONS.md).

**Data handling**

- `schema_profiler` serialises datetime sample values via Python `repr()`, so
  the profile shows `datetime.datetime(...)` rather than the stored value. This
  caused an AI mapping to be built against a non-existent defect
  (DECISIONS.md §3.3). Affects `admit_dt`, `discharge_dt`, `enc_dt`, `updated_ts`.
- Incremental loads use `>` not `>=`, so rows sharing the maximum watermark
  timestamp are skipped permanently.
- Incremental loads have no upsert: a row updated in source is inserted as a
  second copy rather than replacing the original.
- `patient_records` can never load incrementally — its only watermark candidate,
  `updated_ts`, is 100% NULL in source.

**Validation**

- Values are not compared between source and target, only row counts and null
  rates. A rule that maps every value to a constant would pass. Transformation
  correctness is caught at the review gate, not by the validator.
- dbt materialises its views into the migration target, so
  `migration_target.db` contains 5 migrated tables plus 6 dbt views.

**Snowflake**

- The migration runs as `ACCOUNTADMIN`. Convenient, and wrong for production —
  see DECISIONS.md §2.
- dbt materialises its views into `MIGRATION_TARGET.PUBLIC` alongside the
  migrated tables: 5 tables plus 6 views in one schema. Fixable by pointing
  dbt's `schema:` at a dedicated schema in `profiles.yml`.
- Every validation run wakes the warehouse. `AUTO_SUSPEND = 60` on an XSMALL
  keeps this small, but a run is no longer free.
- All target columns are created `VARCHAR`. The transformation rules carry no
  target types, so nothing types them. More visible on Snowflake than it was on
  a file-based target, and more of a limitation.

**Operational**

- No transactional rollback across tables; recovery is by rebuild.
- No schema evolution — new source columns require re-running `ai_mapper` and
  the review gate.
- No scheduling; runs are manual.
- `rule_generator` output requires sanitisation at execution time (trailing
  comments, embedded aliases, `SELECT` fragments). The executor repairs and logs
  all three, but approved text can differ from executed text.

**Security**

- Development-grade only: no TLS on the source connection, no encryption at
  rest, database credentials as literals in `docker-compose.yml`.
- `audit/schema_profile.json` and `audit/ai_mappings.json` contain sample values
  from every column. Committed here only because the data is synthetic; with
  real patient data both would contain PHI.
- Column sample values are transmitted to a third-party LLM. With real PHI this
  would require a BAA, a locally-hosted model, or metadata-only profiling.

See DECISIONS.md §2 for the full regulatory position.
