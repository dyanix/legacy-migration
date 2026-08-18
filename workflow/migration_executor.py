"""
workflow/migration_executor.py

Executes extraction -> transformation -> load using the approved rule set
from audit/transformation_rules.json, in the dependency-safe order from
audit/migration_order.json.

Target is pluggable via TARGET_BACKEND in .env:
    TARGET_BACKEND=sqlite      (default -- zero setup, works today, no
                                 signup/credit card, writes to
                                 target/migration_target.db)
    TARGET_BACKEND=snowflake   (writes to Snowflake -- needs SNOWFLAKE_*
                                 vars in .env; see .env.example)

Swapping targets later is a one-line .env change. Nothing else in this
file, or in validator/doc_generator downstream, needs to change -- both
writers implement the same TargetWriter interface and both populate the
same audit/migration_results.json shape.

Features:
  - Python-based retry with exponential backoff on transient failures
    (spec requirement).
  - Incremental loading (bonus): if a table has an obvious watermark
    column (created_ts/updated_ts/enc_dt/admit_dt -- first match wins),
    only rows newer than the last successful run are extracted. Falls
    back to a full load if no watermark column is found or this is the
    table's first run. Watermarks persist in audit/watermarks.json.

Usage:
    python workflow/migration_executor.py
    python workflow/migration_executor.py --full-reload   # ignore watermarks
"""

import argparse
import decimal
import json
import os
import re
import sys
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time  # aliased: plain `time` is the stdlib module used by with_retry

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

SOURCE_DB_URL = os.getenv(
    "SOURCE_DB_URL",
    "mysql+pymysql://legacy_user:legacy_pass@localhost:3307/legacy_db",
)
TARGET_BACKEND = os.getenv("TARGET_BACKEND", "sqlite")
SQLITE_TARGET_PATH = os.getenv("SQLITE_TARGET_PATH", "target/migration_target.db")

TRANSFORMATION_RULES_PATH = "audit/transformation_rules.json"
MIGRATION_ORDER_PATH = "audit/migration_order.json"
WATERMARKS_PATH = "audit/watermarks.json"
RESULTS_PATH = "audit/migration_results.json"

# Candidate watermark columns, checked in this priority order, per table's
# rule set (source_column names, since we filter against the source table).
WATERMARK_CANDIDATES = ["updated_ts", "created_ts", "enc_dt", "admit_dt", "discharge_dt"]

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.0


# ---------------------------------------------------------------------------
# Retry with exponential backoff (spec requirement)
# ---------------------------------------------------------------------------
def _non_retryable_errors():
    """Exception types that a retry can never fix. A malformed statement or
    an unbindable parameter fails identically every attempt, so retrying
    only delays the report and buries the real error under duplicate
    tracebacks -- both the CAST-type and parameter-binding failures in this
    pipeline burned the full 1s/2s/4s schedule before surfacing."""
    import sqlite3

    from sqlalchemy import exc as sa_exc

    return (
        sa_exc.ProgrammingError,   # SQL syntax / unknown column
        sa_exc.ArgumentError,
        sqlite3.ProgrammingError,
        sqlite3.InterfaceError,    # unbindable parameter type
        ValueError,                # our own fail-fast validation
        TypeError,
    )


def with_retry(fn, *, max_retries=MAX_RETRIES, base_backoff=BASE_BACKOFF_SECONDS, on_retry=None):
    """Calls fn() with exponential backoff on failure: 1s, 2s, 4s, ...
    Re-raises the last exception if all attempts are exhausted, and re-raises
    immediately (no backoff) for deterministic errors that retrying cannot
    resolve -- see _non_retryable_errors."""
    non_retryable = _non_retryable_errors()
    attempt = 0
    while True:
        try:
            return fn()
        except non_retryable:
            raise
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise
            wait = base_backoff * (2 ** (attempt - 1))
            if on_retry:
                on_retry(attempt, wait, e)
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Target writer interface -- swap backends without touching migration logic
# ---------------------------------------------------------------------------
class TargetWriter(ABC):
    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        ...

    @abstractmethod
    def count_rows(self, table_name: str) -> int:
        """Total rows currently in the target table. Distinct from
        read_table: this is a cheap COUNT(*) used for reconciliation, not a
        full DataFrame materialisation. Needed because rows_migrated counts
        only what THIS run loaded, which in incremental mode says nothing
        about how full the table is."""
        ...

    @abstractmethod
    def read_table(self, table_name: str):
        """Returns the table's full contents as a pandas DataFrame. Used by
        validator.py for post-load Great Expectations checks and the
        reconciliation report -- lives here rather than in validator.py so
        both writers share one place that knows how to talk to their
        backend."""
        ...

    @abstractmethod
    def create_or_replace_table(self, table_name: str, columns: list):
        ...

    @abstractmethod
    def load_rows(self, table_name: str, columns: list, rows: list):
        ...

    @abstractmethod
    def close(self):
        ...


class SQLiteTargetWriter(TargetWriter):
    """Default dev/demo target. Zero setup -- no account, no credit card,
    no network. Good enough to prove the whole pipeline works end-to-end;
    swap to SnowflakeTargetWriter later with no other code changes."""

    def __init__(self, path: str = SQLITE_TARGET_PATH):
        self.path = path
        self.conn = None

    @staticmethod
    def _coerce(value):
        """sqlite3 can only bind None, int, float, str and bytes. The MySQL
        driver legitimately returns other types -- DECIMAL columns arrive as
        decimal.Decimal and TIME columns as datetime.timedelta -- and binding
        either raises InterfaceError('unsupported type'), which on older
        Pythons doesn't even name the offending type.

        Converting to str is lossless here and consistent with the schema
        this writer already builds: create_or_replace_table declares every
        column TEXT, so these values were always going to be stored as text.
        NULLs are preserved as None rather than the string 'None'.

        Note this is a SQLite-target concern only. SnowflakeTargetWriter is
        deliberately left alone -- the Snowflake connector binds Decimal and
        the datetime types natively, and stringifying them there would throw
        away the typing that the real target is meant to preserve.
        """
        if value is None or isinstance(value, (int, float, str, bytes)):
            return value  # bool is an int subclass -- sqlite3 stores it as 0/1
        if isinstance(value, decimal.Decimal):
            return str(value)
        if isinstance(value, (datetime, date, dt_time)):
            return value.isoformat()
        if isinstance(value, timedelta):
            return str(value)
        return str(value)

    def connect(self):
        import sqlite3

        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        return self

    def table_exists(self, table_name: str) -> bool:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        return cur.fetchone() is not None

    def count_rows(self, table_name: str) -> int:
        try:
            cur = self.conn.cursor()
            return cur.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        except Exception:
            return None  # table may not exist yet -- not worth failing the run over

    def read_table(self, table_name: str):
        import pandas as pd

        return pd.read_sql(f'SELECT * FROM "{table_name}"', self.conn)

    def create_or_replace_table(self, table_name: str, columns: list):
        cur = self.conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
        self.conn.commit()

    def load_rows(self, table_name: str, columns: list, rows: list):
        if not rows:
            return
        cur = self.conn.cursor()
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(f'"{c}"' for c in columns)
        payload = [tuple(self._coerce(row.get(c)) for c in columns) for row in rows]
        try:
            cur.executemany(
                f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
                payload,
            )
        except Exception as e:
            # sqlite3 reports a positional index and (on older Pythons) no
            # type name. Re-raise naming the column and the value that broke,
            # so the failure is diagnosable without bisecting the row.
            raise type(e)(f"{e} -- while loading `{table_name}`; "
                          f"{self._describe_bad_binding(columns, payload)}") from e
        self.conn.commit()

    @staticmethod
    def _describe_bad_binding(columns: list, payload: list) -> str:
        for row_idx, row in enumerate(payload):
            for col_idx, value in enumerate(row):
                if not (value is None or isinstance(value, (int, float, str, bytes))):
                    return (f"column '{columns[col_idx]}' (position {col_idx + 1}) "
                            f"in row {row_idx} holds an unbindable "
                            f"{type(value).__name__}: {value!r}")
        return "no unbindable value found -- check column count vs row keys."

    def close(self):
        if self.conn:
            self.conn.close()


class SnowflakeTargetWriter(TargetWriter):
    """Real Snowflake target. Needs SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE in .env. Imports are lazy so
    `snowflake-connector-python` is only required if you select this backend.

    IDENTIFIER CASING -- the thing that breaks Snowflake migrations.
    Snowflake folds unquoted identifiers to UPPERCASE but preserves the case
    of quoted ones, so `"dept_id"` and `dept_id` are two different columns.
    An earlier version of this class created tables with an uppercased,
    quoted name but lowercase quoted columns, which meant dbt's unquoted
    `select dept_id` resolved to DEPT_ID and could not find `"dept_id"`.

    This version uses UNQUOTED UPPERCASE everywhere -- the Snowflake
    convention -- so dbt, the Snowflake UI, and ad-hoc SQL all resolve
    identifiers the same way. read_table() then lowercases the DataFrame's
    columns on the way out, because validator.py and doc_generator address
    columns by the lowercase target_column names from the rules.
    """

    def __init__(self):
        self.conn = None
        self._database = None
        self._schema = None

    def connect(self):
        import snowflake.connector

        self._database = os.environ["SNOWFLAKE_DATABASE"]
        self._schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=self._database,
            schema=self._schema,
            role=os.environ.get("SNOWFLAKE_ROLE"),
            # Explicit rather than relying on the driver default: a silent
            # rollback at close would look exactly like a migration that
            # loaded zero rows.
            autocommit=True,
        )
        return self

    def table_exists(self, table_name: str) -> bool:
        cur = self.conn.cursor()
        # Filtered by schema: INFORMATION_SCHEMA.TABLES spans every schema in
        # the database, so an unfiltered match reports True for a same-named
        # table in an unrelated schema.
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s",
            (table_name.upper(), (self._schema or "PUBLIC").upper()),
        )
        return cur.fetchone() is not None

    def count_rows(self, table_name: str) -> int:
        try:
            cur = self.conn.cursor()
            # .upper() was missing here previously, so every count failed and
            # returned None. That is not loud: rows_in_target_after becomes
            # None, the validator skips its reconciliation comparison, and the
            # migration reports success without ever checking row counts.
            cur.execute(f"SELECT COUNT(*) FROM {table_name.upper()}")
            return cur.fetchone()[0]
        except Exception:
            return None

    def read_table(self, table_name: str):
        # Needs `pip install "snowflake-connector-python[pandas]"` for
        # fetch_pandas_all (pulls in pyarrow).
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name.upper()}")
        df = cur.fetch_pandas_all()
        # Snowflake returns UPPERCASE column names; callers address columns by
        # the lowercase target_column values from transformation_rules.json.
        df.columns = [str(c).lower() for c in df.columns]
        return df

    def create_or_replace_table(self, table_name: str, columns: list):
        cur = self.conn.cursor()
        # Every column VARCHAR, matching the SQLite backend. Typed columns
        # would be better in a real warehouse, but the transformation rules
        # do not carry target types -- see DECISIONS.md. Keeping both
        # backends identical means validation results are comparable.
        col_defs = ", ".join(f"{c.upper()} VARCHAR" for c in columns)
        cur.execute(f"CREATE OR REPLACE TABLE {table_name.upper()} ({col_defs})")

    def load_rows(self, table_name: str, columns: list, rows: list):
        if not rows:
            return

        # write_pandas stages the batch and COPYs it. executemany issues
        # INSERTs over the network and is orders of magnitude slower -- the
        # difference between seconds and many minutes on a 25k-row table.
        try:
            import pandas as pd
            from snowflake.connector.pandas_tools import write_pandas

            df = pd.DataFrame([{c: row.get(c) for c in columns} for row in rows])
            df.columns = [str(c).upper() for c in df.columns]
            success, _, nrows, _ = write_pandas(
                self.conn,
                df,
                table_name=table_name.upper(),
                database=self._database,
                schema=self._schema,
                quote_identifiers=True,   # df columns are already uppercase
            )
            if success:
                return
            print(f"  [{table_name}] write_pandas reported failure; falling back to INSERT.")
        except Exception as e:
            print(f"  [{table_name}] write_pandas unavailable ({type(e).__name__}: {e}); "
                  f"falling back to INSERT.")

        cur = self.conn.cursor()
        placeholders = ", ".join("%s" for _ in columns)
        col_list = ", ".join(c.upper() for c in columns)
        cur.executemany(
            f"INSERT INTO {table_name.upper()} ({col_list}) VALUES ({placeholders})",
            [tuple(row.get(c) for c in columns) for row in rows],
        )
        self.conn.commit()

    def close(self):
        if self.conn:
            self.conn.close()


def get_target_writer() -> TargetWriter:
    if TARGET_BACKEND == "snowflake":
        return SnowflakeTargetWriter()
    return SQLiteTargetWriter()


# ---------------------------------------------------------------------------
# Rule grouping + query building (pure functions -- no I/O, easy to test)
# ---------------------------------------------------------------------------
def group_rules_by_table(rules: list) -> dict:
    grouped = {}
    for rule in rules:
        grouped.setdefault(rule["source_table"], []).append(rule)
    return grouped


def pick_watermark_column(table_rules: list) -> str:
    """Returns the first candidate watermark column that this table
    actually has a rule for, or None if incremental loading isn't
    possible for this table (falls back to full load)."""
    source_columns = {r["source_column"] for r in table_rules}
    for candidate in WATERMARK_CANDIDATES:
        if candidate in source_columns:
            return candidate
    return None


def sanitize_sql_expression(logic: str) -> str:
    """Strips SQL line comments (-- ...) and block comments (/* ... */) from
    an LLM-generated `logic` expression, and collapses embedded newlines.

    Why this matters: rule_generator's `logic` field sometimes contains an
    inline rationale comment on the same line as the expression, e.g.
    "CAST(dept_id AS INTEGER) -- direct passthrough, no transformation
    needed". In SQL, `--` comments run to the end of the line. Since we
    build a single-line SELECT statement, an un-stripped trailing comment
    swallows everything after it -- the closing paren, the column alias,
    the comma, and every remaining column -- producing a syntax error
    (this happened in testing: see the dept_master failure this fixes).
    The real transformation logic is always the code BEFORE the comment
    marker, so stripping the comment doesn't change execution semantics,
    it only removes documentation that isn't safe to inline here.

    Caveat: this uses a simple heuristic (first `--` to end of string) and
    doesn't parse SQL string literals, so a `--` appearing inside a quoted
    string value would also be stripped. Not a concern for this project's
    short alphanumeric status codes, but worth knowing if reused elsewhere.
    """
    if not logic:
        return logic
    logic = re.sub(r"/\*.*?\*/", " ", logic, flags=re.DOTALL)
    logic = logic.replace("\n", " ").replace("\r", " ")
    logic = re.sub(r"--.*$", "", logic)
    return logic.strip()


# MySQL's CAST() only accepts: BINARY, CHAR, DATE, DATETIME, DECIMAL, DOUBLE,
# FLOAT, JSON, NCHAR, REAL, SIGNED [INTEGER], TIME, UNSIGNED [INTEGER], YEAR.
# It rejects ANSI-standard names that are valid in most other dialects --
# these are the ones Claude generates most often.
MYSQL_CAST_TYPE_FIXES = {
    "INTEGER": "SIGNED",
    "INT": "SIGNED",
    "BIGINT": "SIGNED",
    "SMALLINT": "SIGNED",
    "TINYINT": "SIGNED",
    "VARCHAR": "CHAR",
    "TEXT": "CHAR",
    "STRING": "CHAR",
    "BOOLEAN": "UNSIGNED",
    "BOOL": "UNSIGNED",
    # Temporal: MySQL CAST has no TIMESTAMP target -- DATETIME is the
    # equivalent. TIMESTAMP is what most dialects (and Snowflake, this
    # project's eventual target) call it, so it shows up often.
    "TIMESTAMP": "DATETIME",
    "DATETIME2": "DATETIME",
    "SMALLDATETIME": "DATETIME",
    # Numeric / string aliases from other dialects.
    "NUMERIC": "DECIMAL",
    "MONEY": "DECIMAL",
    "VARCHAR2": "CHAR",
    "NVARCHAR": "NCHAR",
    "NVARCHAR2": "NCHAR",
    "CLOB": "CHAR",
    "LONGTEXT": "CHAR",
    "MEDIUMTEXT": "CHAR",
    "TINYTEXT": "CHAR",
    "BIT": "UNSIGNED",
}

# The complete set of target types MySQL's CAST() actually accepts. Used to
# catch anything that is neither already-valid nor in the fix map above, so
# an unknown type surfaces as a named rule at query-build time instead of a
# raw 1064 syntax error mid-migration.
MYSQL_VALID_CAST_TYPES = {
    "BINARY", "CHAR", "DATE", "DATETIME", "DECIMAL", "DOUBLE", "FLOAT",
    "JSON", "NCHAR", "REAL", "SIGNED", "TIME", "UNSIGNED", "YEAR",
}

# Types that accept a length/precision qualifier, e.g. DATETIME(6).
MYSQL_CAST_TYPES_WITH_LENGTH = {"CHAR", "NCHAR", "BINARY", "DATETIME", "TIME", "DECIMAL", "FLOAT"}


# Types that appear in the fix map above are exact-match fast paths. For
# names NOT listed there, resolve_mysql_cast_type falls back to these
# family patterns, so a dialect name nobody anticipated (TIMESTAMP_NTZ,
# NUMBER, INT8, LONGVARCHAR) still resolves without editing the map.
#
# Patterns are ANCHORED deliberately. A naive substring test for "INT"
# would rewrite POINT and INTERVAL to SIGNED -- silently corrupting a
# spatial or duration column rather than failing. Order matters: the first
# match wins.
MYSQL_CAST_TYPE_FAMILIES = [
    (r"^(TINY|SMALL|MEDIUM|BIG)?INT(EGER)?\d*$", "SIGNED"),
    (r"^(TIMESTAMP|DATETIME|SMALLDATETIME)", "DATETIME"),
    (r"^TIME(\(|$)", "TIME"),
    (r"^DATE(\(|$)", "DATE"),
    (r"(DECIMAL|NUMERIC|NUMBER|MONEY)", "DECIMAL"),
    (r"(FLOAT|DOUBLE|REAL)", "DOUBLE"),
    (r"(CHAR|TEXT|STRING|CLOB)", "CHAR"),
    (r"^(BOOL|BIT)", "UNSIGNED"),
    (r"(BLOB|BINARY|BYTEA)", "BINARY"),
    (r"JSON", "JSON"),
]

# Types we refuse to guess at. These have no honest MySQL CAST equivalent,
# so inferring one would quietly produce wrong data instead of stopping.
# Better to fail and let a human decide what the column should become.
MYSQL_CAST_TYPES_NEVER_INFER = {
    "GEOMETRY", "GEOGRAPHY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT",
    "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION",
    "VARIANT", "OBJECT", "ARRAY", "STRUCT", "MAP", "INTERVAL", "XML",
    "UUID", "ENUM", "SET", "HSTORE", "VECTOR",
}


def resolve_mysql_cast_type(type_name: str) -> str:
    """Maps a CAST target type to its MySQL equivalent, or returns None if
    it can't be resolved honestly.

    Three tiers, in order:
      1. Already a valid MySQL target      -> unchanged
      2. Exact match in MYSQL_CAST_TYPE_FIXES -> mapped
      3. Family pattern match              -> inferred

    Anything in MYSQL_CAST_TYPES_NEVER_INFER, or matching nothing, returns
    None so the caller can fail with a named error. The alternative -- a
    catch-all like "unknown types become CHAR" -- would let a numeric or
    spatial column land as text and pass every downstream check, which is
    the worse outcome for a clinical dataset.
    """
    if not type_name:
        return None
    base = re.sub(r"\(.*\)", "", type_name).strip().upper()
    base = re.sub(r"\s+", " ", base)
    if base in ("SIGNED INTEGER",):
        return "SIGNED"
    if base in ("UNSIGNED INTEGER",):
        return "UNSIGNED"
    if base == "DOUBLE PRECISION":
        return "DOUBLE"
    if base in MYSQL_VALID_CAST_TYPES:
        return base
    if base in MYSQL_CAST_TYPE_FIXES:
        return MYSQL_CAST_TYPE_FIXES[base]
    if base in MYSQL_CAST_TYPES_NEVER_INFER:
        return None
    for pattern, target in MYSQL_CAST_TYPE_FAMILIES:
        if re.search(pattern, base):
            return target
    return None


def _iter_cast_spans(logic: str):
    """Yields (start, end, type_text) for each CAST(...) in `logic`, where
    type_text is everything after that CAST's own top-level AS.

    Both callers below previously used a flat regex, and they used two
    slightly different ones -- `[A-Za-z]+` vs `[A-Za-z0-9_]*` -- so a name
    like TIMESTAMP_NTZ was invisible to the rewriter but visible to the
    validator, and got reported as invalid after silently not being fixed.
    Scanning real paren spans keeps one definition of "the type" and
    confines the search to actual CAST calls, so an aliased subquery such
    as `(SELECT x AS foo FROM t)` is never mistaken for a cast.
    """
    for match in re.finditer(r"\bCAST\s*\(", logic, flags=re.IGNORECASE):
        start = match.start()
        i = match.end()
        depth = 1
        quote = None
        as_pos = None
        while i < len(logic) and depth > 0:
            ch = logic[i]
            if quote:
                if ch == "\\" and quote in ("'", '"'):
                    i += 2
                    continue
                if ch == quote:
                    quote = None
                i += 1
                continue
            if ch in ("'", '"', "`"):
                quote = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            elif depth == 1 and ch in ("a", "A") and re.match(r"AS\b", logic[i:], re.IGNORECASE):
                if not (i > 0 and (logic[i - 1].isalnum() or logic[i - 1] == "_")):
                    as_pos = i
            i += 1
        if depth == 0 and as_pos is not None:
            yield as_pos, i, logic[as_pos + 2:i].strip()


def normalize_mysql_cast_types(logic: str) -> str:
    """Rewrites CAST(... AS <type>) target types that are invalid in MySQL
    to their closest MySQL equivalent (e.g. INTEGER -> SIGNED). Types that
    resolve_mysql_cast_type refuses to guess at are left untouched, so
    find_invalid_cast_types can report them by name.

    Rewriting already-approved rules here -- rather than requiring
    regeneration -- preserves the human review already done: the *logic*
    a reviewer approved doesn't change, only its dialect-specific spelling
    does, right before it hits the database.
    """
    if not logic:
        return logic
    edits = []
    for as_pos, close_pos, type_text in _iter_cast_spans(logic):
        resolved = resolve_mysql_cast_type(type_text)
        if not resolved:
            continue
        length_match = re.search(r"\(\s*(\d+(?:\s*,\s*\d+)?)\s*\)", type_text)
        if resolved in MYSQL_CAST_TYPES_WITH_LENGTH and length_match:
            replacement = f"AS {resolved}({length_match.group(1)})"
        else:
            replacement = f"AS {resolved}"
        edits.append((as_pos, close_pos, replacement))

    for as_pos, close_pos, replacement in reversed(edits):  # right-to-left keeps offsets valid
        logic = logic[:as_pos] + replacement + logic[close_pos:]
    return logic


def find_invalid_cast_types(logic: str) -> list:
    """Returns any CAST() target types in `logic` that MySQL will reject.
    Run AFTER normalize_mysql_cast_types, so anything it reports is a type
    that is neither already valid, nor in the fix map, nor inferable from
    its family -- i.e. something that genuinely needs a human decision.

    This exists because an unknown type previously passed through silently
    and only surfaced as a raw 1064 syntax error from the server, pointing
    at a character offset in a 900-character generated SELECT. Catching it
    here names the offending rule and type instead.

    Failing fast also matters because a syntax error is not transient: the
    retry wrapper would otherwise burn its full 1s/2s/4s backoff schedule
    re-sending a statement that can never succeed.
    """
    if not logic:
        return []
    bad = []
    for _, _, type_text in _iter_cast_spans(logic):
        if resolve_mysql_cast_type(type_text) is None:
            base = re.sub(r"\(.*\)", "", type_text).strip().upper()
            if base:
                bad.append(base)
    return bad


def split_trailing_alias(logic: str) -> tuple:
    """Splits a `logic` expression into (expression, trailing_alias). Returns
    (logic, None) when there is no trailing alias.

    Why this matters: rule_generator's `logic` field sometimes includes the
    column alias inside the expression itself, e.g.
    "TRIM(dept_nm) AS department_name". build_source_query then wraps the
    expression in parens and appends its own `AS \\`target_column\\``,
    producing the doubly-aliased and syntactically invalid
    "(TRIM(dept_nm) AS department_name) AS `department_name`" -- MySQL
    errors at the inner AS (this happened in testing: see the dept_master
    failure this fixes). The alias is redundant with target_column anyway,
    so removing it doesn't change execution semantics.

    A plain regex can't be used here: `AS` is also legal *inside* an
    expression, most commonly in CAST(x AS SIGNED). So we only treat `AS`
    as an alias marker when it appears at paren-depth zero and outside any
    quoted span -- which distinguishes the stray alias in
    "TRIM(dept_nm) AS department_name" from the necessary one in
    "CAST(dept_id AS SIGNED)".

    The returned alias is reported by the caller rather than used, so a
    rule whose embedded alias disagrees with its approved target_column
    surfaces as a warning instead of silently resolving one way.
    """
    if not logic:
        return logic, None

    depth = 0
    quote = None
    i, n = 0, len(logic)
    while i < n:
        ch = logic[i]

        if quote:
            # Inside a quoted span: honour backslash and doubled-quote escapes.
            if ch == "\\" and quote in ("'", '"'):
                i += 2
                continue
            if ch == quote:
                if i + 1 < n and logic[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and ch in ("a", "A") and re.match(r"AS\b", logic[i:], re.IGNORECASE):
            # Guard against matching the tail of an identifier like `alias`.
            if i > 0 and (logic[i - 1].isalnum() or logic[i - 1] == "_"):
                i += 1
                continue
            expression = logic[:i].strip()
            alias = logic[i + 2:].strip().strip("`\"'").strip()
            if not expression:
                return logic, None  # leading AS -- malformed, leave it alone
            return expression, (alias or None)

        i += 1

    return logic, None


def find_invalid_cast_types(logic: str) -> list:
    """Returns any CAST() target types in `logic` that MySQL will reject.
    Run AFTER normalize_mysql_cast_types, so anything it reports is a type
    that is neither already valid nor covered by the fix map.

    This exists because an unknown type previously passed through silently
    and only surfaced as a raw 1064 syntax error from the server, pointing
    at a character offset in a 900-character generated SELECT. Catching it
    here names the offending rule and type instead.

    Failing fast also matters because a syntax error is not transient: the
    retry wrapper would otherwise burn its full 1s/2s/4s backoff schedule
    re-sending a statement that can never succeed.
    """
    if not logic:
        return []
    found = []
    for match in re.finditer(r"\bAS\s+([A-Za-z][A-Za-z0-9_]*)\s*(?:\(\s*\d+\s*(?:,\s*\d+\s*)?\))?\s*\)",
                             logic, flags=re.IGNORECASE):
        type_name = match.group(1).upper()
        if type_name not in MYSQL_VALID_CAST_TYPES:
            found.append(type_name)
    return found


def build_source_query(table_name: str, table_rules: list, watermark_col: str = None,
                       watermark_value=None, verbose: bool = True) -> tuple:
    """Builds a SELECT that applies each rule's `logic` expression, aliased
    to its target_column, directly against the source table. Returns
    (sql, target_columns). Rules that failed and have no usable logic are
    skipped with a warning rather than breaking the whole table's query.

    If watermark_col is set, it's ALSO selected under a reserved internal
    alias (__watermark_tracking_value) regardless of whether any rule maps
    it to a target_column, and regardless of what that target_column is
    named. This keeps incremental tracking correct even when the watermark
    source column gets renamed by its transformation rule (e.g.
    updated_ts -> updated_at) -- we track the raw source value directly
    rather than trying to reverse-engineer it from the transformed row.
    """
    def _say(msg):
        if verbose:
            print(msg)

    select_parts = []
    target_columns = []
    for rule in table_rules:
        logic = sanitize_sql_expression(rule.get("logic"))
        target_col = rule.get("target_column")

        logic, embedded_alias = split_trailing_alias(logic)
        if embedded_alias:
            if target_col and embedded_alias.lower() != str(target_col).lower():
                _say(f"  [{table_name}] rule for {rule.get('source_column')} embedded alias "
                      f"'{embedded_alias}' but target_column is '{target_col}'; "
                      f"using target_column.")
            else:
                _say(f"  [{table_name}] stripped redundant embedded alias "
                      f"'{embedded_alias}' from rule for {rule.get('source_column')}.")

        logic = normalize_mysql_cast_types(logic) if logic else logic

        bad_types = find_invalid_cast_types(logic)
        if bad_types:
            raise ValueError(
                f"[{table_name}] rule for source column '{rule.get('source_column')}' "
                f"-> '{target_col}' uses CAST target type(s) {sorted(set(bad_types))}, "
                f"which MySQL does not accept. Valid targets: "
                f"{', '.join(sorted(MYSQL_VALID_CAST_TYPES))}. "
                f"Either add a mapping to MYSQL_CAST_TYPE_FIXES or correct the rule "
                f"in {TRANSFORMATION_RULES_PATH}.\n  logic: {logic}"
            )

        if not logic or not target_col:
            _say(f"  [{table_name}] skipping rule for {rule.get('source_column')}: "
                  f"empty logic or target_column after sanitizing.")
            continue
        select_parts.append(f"({logic}) AS `{target_col}`")
        target_columns.append(target_col)

    if watermark_col:
        select_parts.append(f"`{watermark_col}` AS `__watermark_tracking_value`")

    sql = f"SELECT {', '.join(select_parts)} FROM `{table_name}`"
    if watermark_col and watermark_value is not None:
        sql += f" WHERE `{watermark_col}` > :watermark_value"

    return sql, target_columns


# ---------------------------------------------------------------------------
# Watermarks (incremental loading, bonus feature)
# ---------------------------------------------------------------------------
def load_watermarks() -> dict:
    if os.path.exists(WATERMARKS_PATH):
        with open(WATERMARKS_PATH) as f:
            return json.load(f)
    return {}


def save_watermarks(watermarks: dict) -> None:
    os.makedirs(os.path.dirname(WATERMARKS_PATH) or ".", exist_ok=True)
    with open(WATERMARKS_PATH, "w") as f:
        json.dump(watermarks, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Per-table migration
# ---------------------------------------------------------------------------
def migrate_table(source_engine, writer: TargetWriter, table_name: str, table_rules: list,
                   watermarks: dict, full_reload: bool, log: list) -> dict:
    watermark_col = None if full_reload else pick_watermark_column(table_rules)
    watermark_value = watermarks.get(table_name) if watermark_col else None
    is_incremental = watermark_col is not None and watermark_value is not None

    if is_incremental and not writer.table_exists(table_name):
        # audit/watermarks.json claims this table was already fully loaded,
        # but it doesn't actually exist in the target -- e.g. the target
        # file/database was reset without also clearing watermarks.json.
        # Trusting the stale watermark here would silently load an empty
        # or partial table with zero rows and no error. Force a full load
        # instead, and drop the stale watermark so it doesn't linger.
        msg = (f"  [{table_name}] watermark exists but table is missing from the target "
               f"(likely a reset target without a matching watermarks.json reset) -- "
               f"forcing a full load instead of trusting the stale watermark.")
        print(msg)
        log.append(msg)
        is_incremental = False
        watermark_value = None
        watermarks.pop(table_name, None)

    sql, target_columns = build_source_query(table_name, table_rules, watermark_col, watermark_value)

    def _extract():
        with source_engine.connect() as conn:
            params = {"watermark_value": watermark_value} if is_incremental else {}
            result = conn.execute(text(sql), params)
            col_names = list(target_columns)
            if watermark_col:
                col_names.append("__watermark_tracking_value")
            return [dict(zip(col_names, row)) for row in result.fetchall()]

    def _load(rows):
        if not is_incremental:
            writer.create_or_replace_table(table_name, target_columns)
        writer.load_rows(table_name, target_columns, rows)

    retries_used = {"extract": 0, "load": 0}

    def _log_retry(phase):
        def _cb(attempt, wait, exc):
            retries_used[phase] += 1
            msg = f"  [{table_name}] {phase} attempt {attempt} failed ({exc}); retrying in {wait}s"
            print(msg)
            log.append(msg)
        return _cb

    rows = with_retry(_extract, on_retry=_log_retry("extract"))
    with_retry(lambda: _load(rows), on_retry=_log_retry("load"))

    if watermark_col and rows:
        values = [r["__watermark_tracking_value"] for r in rows if r.get("__watermark_tracking_value") is not None]
        if values:
            watermarks[table_name] = str(max(values))

    mode = "incremental" if is_incremental else "full"
    rows_in_target = writer.count_rows(table_name)

    # In incremental mode rows_migrated counts only what this run added, so a
    # downstream check comparing it against the target's total row count will
    # read a healthy no-op (0 new rows, table already full) as catastrophic
    # data loss. Reporting both numbers, plus the flag distinguishing them,
    # lets validator.py compare like with like.
    msg = (f"[{table_name}] {mode} load: {len(rows)} rows extracted and loaded"
           + (f"; target now holds {rows_in_target}." if rows_in_target is not None else "."))
    print(msg)
    log.append(msg)

    if is_incremental and not rows and rows_in_target:
        note = (f"  [{table_name}] no new rows since watermark {watermark_value!r} on "
                f"`{watermark_col}`. The {rows_in_target} rows already in the target were "
                f"written by an EARLIER run and were not re-transformed with the current "
                f"rules -- use --full-reload to rebuild this table.")
        print(note)
        log.append(note)

    return {
        "table": table_name,
        "mode": mode,
        "rows_migrated": len(rows),
        "rows_in_target_after": rows_in_target,
        "is_incremental": is_incremental,
        "retries_used": retries_used,
        "watermark_column": watermark_col,
        "watermark_value_after": watermarks.get(table_name),
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def preflight_check(rules_by_table: dict, migration_order: list, full_reload: bool) -> list:
    """Dry-runs query construction for every table BEFORE any table loads,
    and returns the list of problems found.

    Why this exists: build_source_query is a pure function, so every rule
    defect it can detect is detectable without touching either database.
    Discovering them one run at a time -- the earlier pattern here -- meant
    each fix required a full rerun, and by the time a later table failed,
    earlier tables had already been written to the target. That leaves the
    warehouse holding a partial migration whose completeness isn't obvious
    from the data itself.

    Checking all tables up front turns five sequential failures into one
    report, and guarantees nothing is written until every table can at
    least produce a valid statement.
    """
    problems = []
    for table_name in migration_order:
        table_rules = rules_by_table.get(table_name, [])
        if not table_rules:
            continue
        watermark_col = None if full_reload else pick_watermark_column(table_rules)
        try:
            build_source_query(table_name, table_rules, watermark_col, None, verbose=False)
        except Exception as e:
            problems.append(str(e))
    return problems


def apply_table_filter(migration_order: list, tables: str = None, rules_by_table: dict = None) -> list:
    """Narrows the migration order to a caller-selected subset, preserving
    the dependency ordering the resolver computed.

    Accepts a comma-separated string from --tables or the MIGRATE_TABLES
    environment variable. Returns the full order when neither is set.

    Two things this deliberately does NOT do. It does not reorder: the
    subset keeps the resolver's sequence, so parents still load before
    children. And it does not silently pull in dependencies -- if you
    select a child table whose parent is excluded, that is a legitimate
    thing to want (reloading one table against an already-migrated parent),
    so it warns rather than expanding the selection behind your back.

    Unknown table names are a hard error. Silently migrating nothing
    because of a typo is the worst outcome here: it looks like success.
    """
    selected = tables if tables is not None else os.getenv("MIGRATE_TABLES")
    if not selected or not str(selected).strip():
        return list(migration_order)

    wanted = [t.strip() for t in str(selected).split(",") if t.strip()]
    unknown = [t for t in wanted if t not in migration_order]
    if unknown:
        raise SystemExit(
            f"--tables includes name(s) not in {MIGRATION_ORDER_PATH}: {unknown}. "
            f"Available: {list(migration_order)}"
        )

    filtered = [t for t in migration_order if t in wanted]

    # Warn about parents left out of the selection. Uses the FK-derived
    # ordering as the dependency signal: anything ordered before a selected
    # table may be a parent of it.
    excluded_earlier = [t for t in migration_order
                        if t not in wanted and migration_order.index(t) < max(
                            migration_order.index(w) for w in filtered)]
    if excluded_earlier:
        print(f"  [table filter] migrating {len(filtered)} of {len(migration_order)} table(s): {filtered}")
        print(f"  [table filter] NOTE: {excluded_earlier} load earlier in dependency order and are "
              f"excluded. Foreign keys into them will only resolve if they were migrated previously.")
    else:
        print(f"  [table filter] migrating {len(filtered)} of {len(migration_order)} table(s): {filtered}")

    return filtered


def run_migration(full_reload: bool = False, tables: str = None) -> dict:
    if not os.path.exists(TRANSFORMATION_RULES_PATH):
        raise SystemExit(f"{TRANSFORMATION_RULES_PATH} not found. Run the pipeline through rule_generator first.")
    if not os.path.exists(MIGRATION_ORDER_PATH):
        raise SystemExit(f"{MIGRATION_ORDER_PATH} not found. Run workflow/dependency_resolver.py first.")

    with open(TRANSFORMATION_RULES_PATH) as f:
        rules_doc = json.load(f)
    if not rules_doc.get("pipeline_ready"):
        raise SystemExit(
            f"Pipeline is not ready ({rules_doc.get('blocking_violation_count')} blocking violation(s)). "
            f"Resolve them via review_ui/app.py and re-run rule_generator.py first."
        )

    with open(MIGRATION_ORDER_PATH) as f:
        order_doc = json.load(f)

    rules_by_table = group_rules_by_table(rules_doc["rules"])
    watermarks = {} if full_reload else load_watermarks()

    migration_order = apply_table_filter(order_doc["migration_order"], tables, rules_by_table)

    problems = preflight_check(rules_by_table, migration_order, full_reload)
    if problems:
        raise SystemExit(
            f"Preflight failed: {len(problems)} table(s) have rules that cannot produce a valid "
            f"statement. Nothing was written to the target.\n\n"
            + "\n\n".join(problems)
        )

    source_engine = create_engine(SOURCE_DB_URL)
    writer = get_target_writer()
    writer.connect()

    log = []
    table_results = []
    try:
        for table_name in migration_order:
            table_rules = rules_by_table.get(table_name, [])
            if not table_rules:
                msg = f"[{table_name}] no rules found -- skipping."
                print(msg)
                log.append(msg)
                continue
            result = migrate_table(source_engine, writer, table_name, table_rules, watermarks, full_reload, log)
            table_results.append(result)
    finally:
        writer.close()
        save_watermarks(watermarks)

    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "target_backend": TARGET_BACKEND,
        "full_reload": full_reload,
        "tables_migrated": len(table_results),
        "total_rows_migrated": sum(r["rows_migrated"] for r in table_results),
        "table_results": table_results,
        "log": log,
    }

    os.makedirs(os.path.dirname(RESULTS_PATH) or ".", exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Extract, transform, and load approved rules to the target warehouse.")
    parser.add_argument("--full-reload", action="store_true", help="Ignore watermarks; reload every table in full.")
    parser.add_argument("--tables", default=None,
                        help="Comma-separated subset of tables to migrate, e.g. --tables dept_master,prv_tbl. "
                             "Defaults to every table in the resolved migration order (or MIGRATE_TABLES).")
    args = parser.parse_args()

    summary = run_migration(full_reload=args.full_reload, tables=args.tables)

    print(f"\nMigration complete. Target: {summary['target_backend']}")
    print(f"Tables migrated: {summary['tables_migrated']}, total rows: {summary['total_rows_migrated']}")
    print(f"Results written to {RESULTS_PATH}")


if __name__ == "__main__":
    main()