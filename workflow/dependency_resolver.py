"""
workflow/dependency_resolver.py

Bonus feature: multi-table dependency resolution.

migration_executor needs to load tables in an order that respects foreign
key dependencies -- a parent table (e.g. dept_master) must be fully loaded
into Snowflake before a child table that references it (e.g. prv_tbl) is
loaded, or the FK-equivalent constraint/join in the target will reference
rows that don't exist yet.

This module takes the fk_graph produced by schema_profiler.py and performs
a topological sort (Kahn's algorithm) to produce a safe load order. It
also detects circular foreign keys -- rare, but real legacy databases do
occasionally have them (e.g. a self-referencing "manager_id" or two tables
that reference each other) -- and raises a clear, actionable error rather
than silently producing a broken order.

Usage:
    python workflow/dependency_resolver.py
    python workflow/dependency_resolver.py --profile audit/schema_profile.json --output audit/migration_order.json
"""

import argparse
import json
import os
from collections import defaultdict, deque


class CircularDependencyError(Exception):
    """Raised when the FK graph contains a cycle that makes a strict load
    order impossible without special handling (e.g. deferred constraints)."""

    def __init__(self, remaining_tables: list, dependency_graph: dict):
        self.remaining_tables = remaining_tables
        self.dependency_graph = dependency_graph
        cycle_desc = "; ".join(
            f"{t} depends on {sorted(dependency_graph[t])}" for t in remaining_tables
        )
        super().__init__(
            f"Circular foreign key dependency detected among tables: {remaining_tables}. "
            f"Cannot compute a strict load order. Details: {cycle_desc}. "
            f"Resolve by loading the cycle's tables together in a single transaction "
            f"with FK constraints deferred, or break the cycle at the source schema level."
        )


def build_dependency_graph(fk_graph: list, all_tables: list) -> dict:
    """Returns {table: set(tables it depends on)}. A table "depends on" the
    table(s) its foreign keys point to -- those must load first."""
    graph = {t: set() for t in all_tables}
    for edge in fk_graph:
        child = edge["from_table"]
        parent = edge["to_table"]
        if child == parent:
            # Self-referencing FK (e.g. employee.manager_id -> employee.id).
            # Not a cross-table ordering problem -- the table still loads
            # once, self-references are resolved with a second-pass UPDATE
            # in migration_executor, not by table ordering. Skip it here.
            continue
        if child in graph and parent in graph:
            graph[child].add(parent)
    return graph


def topological_sort(dependency_graph: dict) -> list:
    """Kahn's algorithm. Deterministic: among tables with no remaining
    dependencies, always picks alphabetically first, so re-runs produce
    the same order (useful for reproducible audit logs and diffs)."""
    # in_degree here = number of unresolved dependencies for each table
    in_degree = {t: len(deps) for t, deps in dependency_graph.items()}
    # reverse_graph[p] = set of tables that depend on p, i.e. tables to
    # "release" once p is loaded
    reverse_graph = defaultdict(set)
    for t, deps in dependency_graph.items():
        for dep in deps:
            reverse_graph[dep].add(t)

    ready = deque(sorted(t for t, deg in in_degree.items() if deg == 0))
    order = []

    while ready:
        table = ready.popleft()
        order.append(table)
        for dependent in sorted(reverse_graph.get(table, [])):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                ready.append(dependent)
        ready = deque(sorted(ready))  # keep deterministic after each release

    if len(order) != len(dependency_graph):
        remaining = sorted(set(dependency_graph) - set(order))
        raise CircularDependencyError(remaining, dependency_graph)

    return order


def resolve_migration_order(schema_profile: dict) -> dict:
    all_tables = list(schema_profile["tables"].keys())
    fk_graph = schema_profile.get("fk_graph", [])

    dependency_graph = build_dependency_graph(fk_graph, all_tables)
    order = topological_sort(dependency_graph)

    return {
        "migration_order": order,
        "dependency_graph": {t: sorted(deps) for t, deps in dependency_graph.items()},
        "table_count": len(order),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Compute a safe table load order from the schema's FK graph."
    )
    parser.add_argument("--profile", default="audit/schema_profile.json")
    parser.add_argument("--output", default="audit/migration_order.json")
    args = parser.parse_args()

    if not os.path.exists(args.profile):
        raise SystemExit(f"{args.profile} not found. Run agents/schema_profiler.py first.")

    with open(args.profile) as f:
        schema_profile = json.load(f)

    result = resolve_migration_order(schema_profile)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Migration order ({result['table_count']} tables) written to {args.output}:")
    for i, table in enumerate(result["migration_order"], 1):
        deps = result["dependency_graph"][table]
        dep_note = f" (depends on: {deps})" if deps else " (no dependencies)"
        print(f"  {i}. {table}{dep_note}")


if __name__ == "__main__":
    main()