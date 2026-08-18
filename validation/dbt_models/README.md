# dbt validation models

The second, independent validation layer alongside the Great Expectations
checkpoints in `workflow/validator.py`. Covers row counts, `not_null`,
`unique`, and FK integrity (`relationships`) across every migrated table.

## These models are generated, not hand-written

Run `python agents/generate_dbt_models.py` (or let `validator.py` call it)
to regenerate `models/` from `audit/transformation_rules.json`.

**Do not edit `models/*.sql` or `models/schema.yml` by hand.** Target column
names are decided by `ai_mapper` -> `human_review_gate` -> `rule_generator`,
and `rule_generator` re-runs on every pipeline execution. Hand-written
models encode a snapshot of those names and silently rot the moment a
mapping changes. The previous hand-written set did exactly this: it selected
`department_id` when the target had `dept_id`, and `patient_status` when the
target had `patient_status_code`, so every model failed to compile.

The generator also verifies each candidate key against the migrated data
before emitting a test — a `unique` test on a column that isn't unique makes
the whole layer look broken and trains people to ignore it.

## Version pins matter

`dbt-sqlite` is a community adapter and lags `dbt-core`. The newest
`dbt-core` will install, then fail at import:

```
ImportError: cannot import name 'Credentials' from 'dbt.adapters.base'
```

Use the pinned pair below, which is tested working:

```bash
pip install "dbt-sqlite==1.10.0"   # pulls a compatible dbt-core (1.11.x)
```

For Snowflake, `dbt-snowflake` tracks `dbt-core` closely and needs no pin:

```bash
pip install dbt-core dbt-snowflake
```

## Create `~/.dbt/profiles.yml`

Kept outside the repo per dbt convention — it holds credentials.

SQLite (default target). `schemas_and_paths` must be an **absolute** path:

```yaml
legacy_migration_validation:
  target: dev
  outputs:
    dev:
      type: sqlite
      threads: 1
      database: 'database'
      schema: 'main'
      schemas_and_paths:
        main: '/absolute/path/to/target/migration_target.db'
      schema_directory: '/absolute/path/to/target'
```

Snowflake:

```yaml
legacy_migration_validation:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: '{{ env_var("SNOWFLAKE_ACCOUNT") }}'
      user: '{{ env_var("SNOWFLAKE_USER") }}'
      password: '{{ env_var("SNOWFLAKE_PASSWORD") }}'
      role: '{{ env_var("SNOWFLAKE_ROLE") }}'
      database: '{{ env_var("SNOWFLAKE_DATABASE") }}'
      warehouse: '{{ env_var("SNOWFLAKE_WAREHOUSE") }}'
      schema: '{{ env_var("SNOWFLAKE_SCHEMA") }}'
      threads: 4
```

When targeting Snowflake, regenerate with the right schema so the source
block matches:

```bash
DBT_TARGET_SCHEMA="$SNOWFLAKE_SCHEMA" python agents/generate_dbt_models.py
```

## Run

```bash
python agents/generate_dbt_models.py
cd validation/dbt_models
dbt build          # NOT `dbt test`
```

`dbt test` alone does not build models. These are materialised as views, and
the `relationships` tests reference them via `ref()` — without `dbt run`
first, those tests error on a missing relation. `dbt build` does both in
dependency order.

Expected on a clean migration: `PASS=20 WARN=0 ERROR=0`.

## How failures present

- `unique` / `not_null` breach -> **ERROR**, and dbt exits non-zero
- FK breach -> **WARN** by default, because some FKs are legitimately
  nullable in the source (an encounter with no provider on record). Change
  `severity: warn` to `error` in the generator if your data shouldn't have
  any.

## Relationship to the pass/fail decision

`validator.py` runs dbt best-effort and records the result in
`audit/validation_report.json`, but the pipeline's pass/fail decision is
enforced by the Great Expectations checkpoints. dbt is a second opinion, not
the gate — so a missing dbt install never blocks a migration.
