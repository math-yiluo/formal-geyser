"""How tasks relate, for the Task center (DESIGN.md, "Task center").

Two kinds of relation, read only:

- **Publication.** Where each work task came from: the task whose attempt published it
  (``task_origins``), or for a repair task the task whose evidence it repairs. Any other task is a
  root: the operator or the system published it. Reviews hang under the task they review and are
  counted there, not listed.
- **Dependencies.** What an open task needs before it can go on, each with whether it is met: a
  requirement of its current contract binds the task itself; a waiting worker's subscription,
  stored requirement or request to the operator binds that worker. A link's ``source`` is what is
  needed (a task, or ``operator``) and its ``target`` the task that needs it. Unmet links that form a
  cycle are a deadlock; a task that reaches the operator through unmet links waits on the operator.
  A wait on the worker's own submission or on a peer's answer involves no other task: a note.

Nothing here grants authority, wakes a worker or changes a record, apart from the one-time
backfill of ``task_origins`` at startup.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Any

from .errors import HubError
from .task_browser import NO_REVIEWS, TERMINAL, _executions, _running_ids, review_counts
from .worker_names import _display_name

log = logging.getLogger(__name__)

OPERATOR = "operator"
"""The source of a link that only the operator's answer to a request can meet."""
ORIGIN_BACKFILL = "task_origins.publish_receipts"
CHILD_LIMIT = 200
REVIEW_LIMIT = 100


@dataclass(frozen=True)
class Link:
    """``target`` needs ``source``: another task, or the operator."""
    source: str
    target: str
    kind: str
    """``requirement`` from the target's contract; ``subscription`` or ``wait`` from a waiting
    worker of the target."""
    met: bool
    agent: str | None = None
    """For a subscription or wait: the worker that waits."""
    detail: str = ""


@dataclass(frozen=True)
class Note:
    """A wait or need of a task that involves no other task."""
    task: str
    agent: str | None
    kind: str
    """``validation``, ``publication`` (a candidate's outcome), ``declaration``, ``answer`` (a peer's
    answer to a question), or ``requirement`` for one that names no task."""
    met: bool
    detail: str = ""


# ------------------------------------------------------------------------ publication
def backfill_task_origins(c, now: int) -> int:
    """Record, once, where each task published before ``task_origins`` existed came from: the task
    of the attempt whose ``task.publish`` call returned it. A call received after the task existed
    returned an existing task (deduplication), so it never names an origin. Every other task gets an
    empty origin, as one the operator published. Runs in the caller's transaction."""
    if c.execute("SELECT 1 FROM ledger_backfills WHERE name=?", (ORIGIN_BACKFILL,)).fetchone():
        return 0
    # A display record never stops startup: if the fill fails it is undone, logged, and tried
    # again at the next start. (In the Linux pilot it reads 195,053 receipts in under 20 s.)
    c.execute("SAVEPOINT task_origin_backfill")
    try:
        before = c.execute("SELECT COUNT(*) FROM task_origins").fetchone()[0]
        c.execute(
            "INSERT OR IGNORE INTO task_origins(task_id,source_task_id,worker_id,attempt_id,recorded_at) "
            "SELECT t.id,a.task_id,a.worker_id,a.id,? FROM native_call_receipts r "
            # CASE reads only valid JSON, wherever the planner evaluates it.
            "JOIN attempts a ON a.id=r.attempt_id JOIN tasks t ON t.id=CASE WHEN json_valid(r.response_json) "
            "THEN json_extract(r.response_json,'$.id') END "
            "WHERE CASE WHEN json_valid(r.command_json) THEN json_extract(r.command_json,'$.name') END='task.publish' "
            "AND r.created_at<=t.created_at "
            "AND a.task_id<>t.id ORDER BY r.created_at,r.rowid", (now,))
        c.execute("INSERT OR IGNORE INTO task_origins(task_id,source_task_id,worker_id,attempt_id,recorded_at) "
                  "SELECT id,NULL,NULL,NULL,? FROM tasks", (now,))
        c.execute("INSERT INTO ledger_backfills VALUES(?,?)", (ORIGIN_BACKFILL, now))
        added = c.execute("SELECT COUNT(*) FROM task_origins").fetchone()[0] - before
    except sqlite3.Error:
        c.execute("ROLLBACK TO task_origin_backfill")
        c.execute("RELEASE task_origin_backfill")
        log.exception("Filling task_origins from publish receipts failed; it is tried again at the next start")
        return 0
    c.execute("RELEASE task_origin_backfill")
    log.info("Recorded the origins of %d tasks from their publish receipts", added)
    return added


def _work_rows(c, where: str = "", args: Iterable[Any] = ()) -> list[dict]:
    """Work tasks (no reviews): id, title, state, creation, and the task they hang under."""
    return [dict(row) for row in c.execute(
        "SELECT t.id,t.state,t.created_at,t.current_revision,json_extract(v.contract_json,'$.title') AS title,"
        "o.source_task_id,i.task_id AS repair_of FROM tasks t "
        "JOIN task_revisions v ON v.task_id=t.id AND v.revision=t.current_revision "
        "LEFT JOIN task_origins o ON o.task_id=t.id "
        "LEFT JOIN completion_evidence_invalidations i ON i.repair_task_id=t.id "
        "WHERE coalesce(json_extract(v.contract_json,'$.kind'),'work') NOT IN ('review','integration')"
        + where + " ORDER BY t.created_at,t.id", tuple(args))]


def _hosts(c) -> dict[str, str]:
    """The task each review task reviews."""
    return {row[0]: row[1] for row in c.execute(
        "SELECT task_id,host_task_id FROM task_collaboration WHERE host_task_id IS NOT NULL")}


def _parent(row: dict, hosts: dict[str, str]) -> tuple[str | None, str | None]:
    """The task ``row`` hangs under, and how: published from it, or repairing it. A task published
    in a review's attempt hangs under the reviewed task."""
    if row["source_task_id"]:
        return hosts.get(row["source_task_id"], row["source_task_id"]), "published"
    if row["repair_of"]:
        return row["repair_of"], "repair"
    return None, None


def _executors(c, task_ids: list[str]) -> dict[str, list[dict]]:
    """The current executors of open tasks, with what each is doing."""
    found = _executions(c, task_ids, set()) if task_ids else {}
    return {task_id: [{"worker_id": m["worker_id"], "display_name": m["display_name"],
                       "execution_status": m["execution_status"]}
                      for m in members if m["executor"] and m["membership_state"] == "member"]
            for task_id, (members, _) in found.items()}


async def task_outline(hub) -> dict:
    """Every work task once, compactly, under the task it came from; Task 0 first. Reviews are
    counted on their task. Executors are read for open tasks only."""
    def read(c):
        hosts, running = _hosts(c), _running_ids(c)
        rows = _work_rows(c)
        reviews = review_counts(c)
        executors = _executors(c, [row["id"] for row in rows if row["state"] not in TERMINAL])
        root = c.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()
        items = []
        for row in rows:
            parent, relation = _parent(row, hosts)
            items.append({"id": row["id"], "state": row["state"], "revision": row["current_revision"],
                          "created_at": row["created_at"], "contract": {"title": row["title"], "kind": "work"},
                          "parent": parent, "relation": relation, "running": row["id"] in running,
                          "executors": executors.get(row["id"], []),
                          "reviews": reviews.get(row["id"], dict(NO_REVIEWS))})
        return {"root_task_id": root["root_task_id"] if root else None, "items": items,
                "review_count": c.execute("SELECT COUNT(*) FROM review_requests").fetchone()[0]}
    return await hub.db.read(read)


# ----------------------------------------------------------------------- dependencies
_CONTRACT_REQUIREMENTS = (
    "SELECT t.id AS task_id,r.value AS requirement FROM tasks t "
    "JOIN task_revisions v ON v.task_id=t.id AND v.revision=t.current_revision, "
    "json_each(v.contract_json,'$.requirements') r WHERE (t.state NOT IN ('done','retired') OR t.id=?)")
_ABOUT_TASK = (" AND (t.id=? OR json_extract(r.value,'$.task_id')=? OR json_extract(r.value,'$.artifact_id') "
               "IN (SELECT id FROM artifacts WHERE task_id=?))")


def _provider(c, requirement: dict) -> str | None:
    if requirement.get("task_id"):
        return requirement["task_id"]
    if requirement.get("artifact_id"):
        row = c.execute("SELECT task_id FROM artifacts WHERE id=?", (requirement["artifact_id"],)).fetchone()
        return row[0] if row else None
    return None


def _requirement_detail(requirement: dict) -> str:
    """What a requirement asks of its provider, which the link already names."""
    key = requirement.get("key", "requirement")
    if requirement.get("artifact_id"):
        return f"{key}: artifact {requirement['artifact_id'][:8]}"
    if requirement.get("task_revision"):
        return f"{key}: revision {requirement['task_revision']} completed"
    return key


class _Collector:
    def __init__(self, hub, c):
        self.hub, self.c = hub, c
        self.links: list[Link] = []
        self.notes: list[Note] = []

    def met(self, requirement: dict) -> bool:
        try:
            return bool(self.hub._requirement(self.c, requirement))
        except HubError:  # A named artifact or task that no longer exists never holds.
            return False

    def requirement(self, task_id: str, requirement: dict, *, agent: str | None, kind: str) -> None:
        provider, detail = _provider(self.c, requirement), _requirement_detail(requirement)
        met = self.met(requirement)
        if provider is None or provider == task_id:
            self.notes.append(Note(task_id, agent, "requirement", met, detail))
        else:
            self.links.append(Link(provider, task_id, kind, met, agent, detail))

    def contract(self, task_id: str | None) -> None:
        sql, args = _CONTRACT_REQUIREMENTS, [task_id]
        if task_id is not None:
            sql, args = sql + _ABOUT_TASK, [task_id, task_id, task_id, task_id]
        for row in self.c.execute(sql, args).fetchall():
            self.requirement(row["task_id"], json.loads(row["requirement"]), agent=None, kind="requirement")

    def waits(self, task_id: str | None) -> None:
        for row in self.c.execute(
                "SELECT task_id,preferred_worker,predicate_json FROM continuations WHERE state='waiting' "
                "ORDER BY created_at,id").fetchall():
            predicate = json.loads(row["predicate_json"])
            if task_id is not None and row["task_id"] != task_id and not _names_task(self.c, predicate, task_id):
                continue
            self.wait(row["task_id"], row["preferred_worker"], predicate)

    def wait(self, task_id: str, agent: str, predicate: dict) -> None:
        for subscription in predicate.get("subscriptions", []):
            if subscription.get("task_id") == task_id:
                continue
            self.links.append(Link(subscription["task_id"], task_id, "subscription", False, agent,
                                   subscription.get("need", "")))
        for requirement in predicate.get("requirements", []):
            self.requirement(task_id, requirement, agent=agent, kind="wait")
        if predicate.get("external_response_id"):
            request = self.c.execute("SELECT kind FROM external_requests WHERE id=?",
                                     (predicate["external_response_id"],)).fetchone()
            label = {"literature": "a literature request", "question": "a question"}.get(
                request["kind"] if request else "", "a request")
            self.links.append(Link(OPERATOR, task_id, "wait", bool(self.hub._external_wait_satisfied(self.c, predicate)),
                                   agent, f"your answer to {label} ({predicate['external_response_id'][:8]})"))
        for question in predicate.get("answers", []):
            asked = self.c.execute("SELECT recipient_id FROM peer_messages WHERE id=?", (question,)).fetchone()
            self.notes.append(Note(task_id, agent, "answer", bool(self.hub._answered(self.c, question)),
                                   f"an answer from {asked['recipient_id']}" if asked else "an answer"))
        for artifact_id in predicate.get("validations", []):
            self.notes.append(Note(task_id, agent, "validation", bool(self.hub._validation_finished(self.c, artifact_id)),
                                   f"the validation of {artifact_id}"))
        for candidate_id in predicate.get("publications", []):
            self.notes.append(Note(task_id, agent, "publication",
                                   self.hub._publication_outcome(self.c, candidate_id) is not None,
                                   f"the publication of {candidate_id}"))
        if predicate.get("completion_declaration_id"):
            self.notes.append(Note(task_id, agent, "declaration", False,
                                   f"the decision on {predicate['completion_declaration_id']}"))


def _names_task(c, predicate: dict, task_id: str) -> bool:
    if any(item.get("task_id") == task_id for item in predicate.get("subscriptions", [])):
        return True
    return any(_provider(c, requirement) == task_id for requirement in predicate.get("requirements", []))


def collect(hub, c, *, task_id: str | None = None) -> tuple[list[Link], list[Note]]:
    """Every dependency link and note of open tasks, or only those involving ``task_id`` (whose own
    contract requirements are read even when it is done)."""
    collector = _Collector(hub, c)
    collector.contract(task_id)
    collector.waits(task_id)
    return collector.links, collector.notes


def deadlocks(links: Iterable[Link]) -> list[list[str]]:
    """Groups of tasks that need each other through unmet links, so none can go on."""
    graph = _unmet_graph(links, include_operator=False)
    reach = {node: _reachable(graph, node) for node in list(graph)}
    groups: list[list[str]] = []
    seen: set[str] = set()
    for node in sorted(reach):
        if node in seen or node not in reach[node]:
            continue
        group = sorted(other for other in reach[node] if node in reach.get(other, ()))
        seen.update(group)
        groups.append(group)
    return groups


def waiting_on_operator(links: Iterable[Link]) -> list[str]:
    """Tasks that need an answer from the operator, directly or through other tasks."""
    return sorted(_reachable(_unmet_graph(links, include_operator=True), OPERATOR))


def _unmet_graph(links: Iterable[Link], *, include_operator: bool) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for link in links:
        if link.met or link.source == link.target or (link.source == OPERATOR and not include_operator):
            continue
        graph[link.source].add(link.target)
    return graph


def _reachable(graph: dict[str, set[str]], start: str) -> set[str]:
    found: set[str] = set()
    queue = deque(graph.get(start, ()))
    while queue:
        node = queue.popleft()
        if node not in found:
            found.add(node)
            queue.extend(graph.get(node, ()))
    return found


def _nodes(c, task_ids: Iterable[str]) -> list[dict]:
    ids = sorted(set(task_ids) - {OPERATOR})
    if not ids:
        return []
    running = _running_ids(c)
    rows = {row["id"]: dict(row) for row in c.execute(
        "SELECT t.id,t.state,json_extract(v.contract_json,'$.title') AS title,"
        "json_extract(v.contract_json,'$.kind') AS kind FROM tasks t "
        "JOIN task_revisions v ON v.task_id=t.id AND v.revision=t.current_revision "
        "WHERE t.id IN (SELECT value FROM json_each(?))", (json.dumps(ids),))}
    executors = _executors(c, [task_id for task_id in ids if rows.get(task_id, {}).get("state") not in TERMINAL])
    return [{"id": task_id, "title": rows[task_id]["title"], "state": rows[task_id]["state"],
             "kind": rows[task_id]["kind"] or "work", "running": task_id in running,
             "executors": executors.get(task_id, [])} for task_id in ids if task_id in rows]


def _names(c, worker_ids: Iterable[str | None]) -> dict[str, str]:
    """Display names of these workers."""
    ids = sorted({worker_id for worker_id in worker_ids if worker_id})
    if not ids:
        return {}
    return {row["id"]: _display_name(row) for row in c.execute(
        "SELECT id,worker_number,reviewer_number,kind FROM workers WHERE id IN (SELECT value FROM json_each(?))",
        (json.dumps(ids),))}


async def task_graph(hub, *, include_met: bool = False) -> dict:
    """The dependency view: open tasks joined by unmet links (and met ones on request), each
    deadlock, and the tasks that wait on the operator."""
    def read(c):
        links, notes = collect(hub, c)
        shown = links if include_met else [link for link in links if not link.met]
        nodes = _nodes(c, [end for link in shown for end in (link.source, link.target)])
        return {"tasks": nodes, "links": [asdict(link) for link in shown],
                "notes": [asdict(note) for note in notes if include_met or not note.met],
                "deadlocks": deadlocks(links), "waiting_on_operator": waiting_on_operator(links),
                "agents": _names(c, [*(link.agent for link in shown), *(note.agent for note in notes)]),
                "include_met": include_met,
                "counts": {"links": len(links), "unmet": sum(not link.met for link in links)}}
    return await hub.db.read(read)


async def task_relations(hub, task_id: str) -> dict:
    """Everything one task relates to: where it came from, what it published, its reviews, what it
    needs and what needs it."""
    def read(c):
        hub._get(c, "tasks", task_id)
        hosts = _hosts(c)
        host = hosts.get(task_id)
        origin = c.execute("SELECT o.source_task_id,o.worker_id,i.task_id AS repair_of FROM tasks t "
                           "LEFT JOIN task_origins o ON o.task_id=t.id "
                           "LEFT JOIN completion_evidence_invalidations i ON i.repair_task_id=t.id WHERE t.id=?",
                           (task_id,)).fetchone()
        parent, relation = _parent(dict(origin), hosts)
        # Published in its attempts or in those of its reviews (as the outline hangs them), or repairing it.
        children = _work_rows(c, " AND t.id IN (SELECT task_id FROM task_origins WHERE source_task_id=? OR "
                              "source_task_id IN (SELECT task_id FROM task_collaboration WHERE host_task_id=?) "
                              "UNION SELECT repair_task_id FROM completion_evidence_invalidations WHERE task_id=?)",
                              (task_id, task_id, task_id))
        reviews = [dict(row) for row in c.execute(
            "SELECT r.task_id AS id,r.state,r.candidate_id,r.created_at,p.verdict,p.reviewer_id FROM review_requests r "
            "LEFT JOIN peer_review_receipts p ON p.review_task_id=r.task_id WHERE r.host_task_id=? "
            "ORDER BY r.created_at DESC,r.task_id DESC LIMIT ?", (task_id, REVIEW_LIMIT))]
        links, notes = collect(hub, c, task_id=task_id)
        needs = [link for link in links if link.target == task_id]
        needed_by = [link for link in links if link.source == task_id]
        related = [link.source for link in needs] + [link.target for link in needed_by]
        nodes = {node["id"]: node for node in _nodes(c, [*related, *(t for t in (parent, host) if t)])}
        names = _names(c, [origin["worker_id"], *(row["reviewer_id"] for row in reviews),
                           *(link.agent for link in links), *(note.agent for note in notes)])
        return {
            "task_id": task_id, "parent": nodes.get(parent) if parent else None, "relation": relation,
            "publisher": {"worker_id": origin["worker_id"], "display_name": names.get(origin["worker_id"], origin["worker_id"])}
            if origin["worker_id"] else None,
            "host": nodes.get(host) if host else None,
            "children": {"total": len(children), "items": [
                {"id": row["id"], "title": row["title"], "state": row["state"],
                 "relation": "repair" if row["repair_of"] == task_id and row["source_task_id"] != task_id else "published"}
                for row in children[:CHILD_LIMIT]]},
            "reviews": {"counts": review_counts(c, [task_id]).get(task_id, dict(NO_REVIEWS)),
                        "items": [{**row, "reviewer_name": names.get(row["reviewer_id"])} for row in reviews]},
            "needs": [asdict(link) for link in needs], "needed_by": [asdict(link) for link in needed_by],
            "notes": [asdict(note) for note in notes if note.task == task_id],
            "tasks": nodes, "agents": names,
            "deadlocks": [group for group in deadlocks(collect(hub, c)[0]) if task_id in group]
            if any(not link.met for link in needs) and any(not link.met for link in needed_by) else [],
        }
    return await hub.db.read(read)
