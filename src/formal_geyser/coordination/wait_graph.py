"""Which tasks wait on which: the graph a new task subscription must keep free of cycles.

An edge A -> B says that work on task A waits on task B: a worker of A waits with a subscription
to B (or, in a wait recorded before 2026-11-07, with a requirement naming B), or A's contract
requires B before it can be claimed. The graph is read from those records whenever it is needed,
so it never drifts from them. Tasks that are done or retired wait on nothing.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict, deque

EDGE_SQL = (
    # Waits: their subscriptions, and the task requirements of waits recorded before subscriptions.
    "SELECT k.task_id AS source,json_extract(s.value,'$.task_id') AS target,k.preferred_worker AS worker "
    "FROM continuations k, json_each(k.predicate_json,'$.subscriptions') s WHERE k.state='waiting' "
    "UNION ALL SELECT k.task_id,json_extract(r.value,'$.task_id'),k.preferred_worker "
    "FROM continuations k, json_each(k.predicate_json,'$.requirements') r WHERE k.state='waiting' "
    "AND json_extract(r.value,'$.task_id') IS NOT NULL "
    # Contracts of open tasks: a required task comes first.
    "UNION ALL SELECT t.id,json_extract(r.value,'$.task_id'),NULL FROM tasks t JOIN task_revisions v "
    "ON v.task_id=t.id AND v.revision=t.current_revision, json_each(v.contract_json,'$.requirements') r "
    "WHERE t.state NOT IN ('done','retired') AND json_extract(r.value,'$.task_id') IS NOT NULL")


def wait_edges(c: sqlite3.Connection, *, replacing: tuple[str, str] | None = None) -> dict[str, set[str]]:
    """The graph's edges. `replacing` (worker, task): that worker's waits in that task, which the
    wait being recorded replaces, are left out."""
    edges: dict[str, set[str]] = defaultdict(set)
    for source, target, worker in c.execute(EDGE_SQL).fetchall():
        if target is None or (replacing is not None and (worker, source) == replacing):
            continue
        edges[source].add(target)
    return edges


def path_between(edges: dict[str, set[str]], start: str, goal: str) -> list[str] | None:
    """A shortest path of edges from `start` to `goal`, both included; None if there is none."""
    previous: dict[str, str | None] = {start: None}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        if node == goal:
            path = []
            current: str | None = node
            while current is not None:
                path.append(current)
                current = previous[current]
            return path[::-1]
        for following in sorted(edges.get(node, ())):
            if following not in previous:
                previous[following] = node
                queue.append(following)
    return None
