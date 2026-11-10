"""Stable worker and reviewer labels without changing operational identities."""
from __future__ import annotations

import asyncio
import copy
import re
import threading
from typing import TYPE_CHECKING, Any

from .errors import HubError

WORKER_ALIAS = re.compile(r"Worker (0|[1-9][0-9]*)", re.IGNORECASE)
REVIEWER_ALIAS = re.compile(r"Reviewer ([1-9][0-9]*)", re.IGNORECASE)
WORKER_FIELDS = frozenset({
    "worker_id", "source_worker_id", "initial_worker_id",
    "author_id", "sender_id", "recipient_id", "preferred_worker", "feedback_worker_id",
    "declarer_id", "reviewer_id", "worker_low", "worker_high", "peer_id", "producer",
})
WORKER_LIST_FIELDS = frozenset({"coauthors", "coauthor_ids", "worker_ids", "publishers", "executors"})
# Authored content and exact evidence are not coordination presentation objects.
# Their typed-looking keys may be quotations or part of a hashed native receipt.
OPAQUE_FIELDS = frozenset({
    "data", "contract", "payload", "checkpoint", "proof", "details", "evidence",
    "receipt", "receipts", "candidate_binding", "binding", "raw_result", "profile",
    "semantic_checks", "integration_notes", "record",
})


IMMUTABLE_WORKER_NUMBER = ("CREATE TRIGGER IF NOT EXISTS immutable_worker_number "
                           "BEFORE UPDATE OF worker_number ON workers "
                           "WHEN OLD.worker_number IS NOT NULL AND NEW.worker_number IS NOT OLD.worker_number "
                           "BEGIN SELECT RAISE(ABORT,'Worker numbers are immutable'); END")
# A task carried from another instance where it had a worker keeps its own number, X, for its
# first executor here, Worker X (core_transfer). No other worker takes X; worker_id is the worker
# that took it. Tasks published here, and carried tasks that never had a worker, keep none.
KEPT_WORKER_NUMBERS = ("CREATE TABLE IF NOT EXISTS kept_worker_numbers(task_id TEXT PRIMARY KEY,"
                       "worker_number INTEGER NOT NULL UNIQUE CHECK(worker_number>=0),worker_id TEXT UNIQUE,"
                       "kept_at INTEGER NOT NULL)")


def initialize_agent_names(connection) -> None:
    """Initialize disjoint stable identities for ordinary workers and reviewers."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS worker_name_sequence("
                           "id INTEGER PRIMARY KEY CHECK(id=1),next_number INTEGER NOT NULL CHECK(next_number>=0))")
        connection.execute("INSERT OR IGNORE INTO worker_name_sequence VALUES(1,0)")
        connection.execute("CREATE TABLE IF NOT EXISTS reviewer_name_sequence("
                           "id INTEGER PRIMARY KEY CHECK(id=1),next_number INTEGER NOT NULL CHECK(next_number>=1))")
        connection.execute("INSERT OR IGNORE INTO reviewer_name_sequence VALUES(1,1)")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS workers_number_idx ON workers(worker_number)")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewers_number_idx ON workers(reviewer_number)")
        connection.execute(IMMUTABLE_WORKER_NUMBER)
        connection.execute(KEPT_WORKER_NUMBERS)
        connection.execute("CREATE TRIGGER IF NOT EXISTS immutable_reviewer_number "
                           "BEFORE UPDATE OF reviewer_number ON workers "
                           "WHEN OLD.reviewer_number IS NOT NULL AND NEW.reviewer_number IS NOT OLD.reviewer_number "
                           "BEGIN SELECT RAISE(ABORT,'Reviewer numbers are immutable'); END")
        for operation in ("INSERT", "UPDATE"):
            connection.execute(f"CREATE TRIGGER IF NOT EXISTS disjoint_agent_numbers_{operation.lower()} "
                               f"BEFORE {operation} ON workers WHEN "
                               "(NEW.kind='review' AND NEW.worker_number IS NOT NULL) OR "
                               "(NEW.kind<>'review' AND NEW.reviewer_number IS NOT NULL) "
                               "BEGIN SELECT RAISE(ABORT,'Worker and reviewer numbers are disjoint'); END")
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def allocate_worker_number(connection) -> int:
    """Allocate inside the transaction that creates an ordinary worker: the next number that no
    task keeps (kept_worker_number)."""
    number = next_worker_number(connection)
    connection.execute("UPDATE worker_name_sequence SET next_number=? WHERE id=1", (number + 1,))
    return number


def next_worker_number(connection) -> int:
    """The number the next ordinary worker gets."""
    return unkept_number(connection, int(connection.execute(
        "SELECT next_number FROM worker_name_sequence WHERE id=1").fetchone()[0]))


def unkept_number(connection, start: int) -> int:
    """The first number from `start` on that no task keeps for its first executor."""
    kept = {row[0] for row in connection.execute(
        "SELECT worker_number FROM kept_worker_numbers WHERE worker_number>=?", (start,))}
    while start in kept:
        start += 1
    return start


def kept_worker_number(connection, task_id: str) -> int | None:
    """The number Task X keeps for its first executor while it has none: X, when it was carried
    from an instance where it had a worker (core_transfer)."""
    row = connection.execute("SELECT worker_number FROM kept_worker_numbers WHERE task_id=? AND worker_id IS NULL",
                             (task_id,)).fetchone()
    return None if row is None else int(row[0])


def keep_worker_numbers(connection, numbers: dict[str, int], now: int) -> int:
    """Keep each task's number (task id -> its number) for its first executor; returns how many
    were not kept already. A number another worker already has cannot be kept."""
    taken = {row[0] for row in connection.execute("SELECT worker_number FROM workers WHERE worker_number IS NOT NULL")}
    added = 0
    for task_id, number in numbers.items():
        if connection.execute("SELECT 1 FROM kept_worker_numbers WHERE task_id=?", (task_id,)).fetchone():
            continue
        if number in taken:
            raise HubError("WORKER_NUMBER_TAKEN", f"Worker {number} exists already: renumber the instance's "
                           "workers first (keep_carried_worker_numbers)", 409)
        connection.execute("INSERT INTO kept_worker_numbers(task_id,worker_number,worker_id,kept_at) VALUES(?,?,NULL,?)",
                           (task_id, number, now))
        added += 1
    return added


def allocate_reviewer_number(connection) -> int:
    """Allocate from one inside the transaction that creates an ephemeral reviewer."""
    row = connection.execute("UPDATE reviewer_name_sequence SET next_number=next_number+1 "
                             "WHERE id=1 RETURNING next_number-1").fetchone()
    return int(row[0])


def is_reserved_identity(reference: Any) -> bool:
    """Whether a reference occupies either generated project-name namespace."""
    return isinstance(reference, str) and bool(
        reference.casefold() in {"integrator", "dispatcher", "waker"} or WORKER_ALIAS.fullmatch(reference)
        or REVIEWER_ALIAS.fullmatch(reference))


def worker_identity(record: dict) -> dict:
    """Decorate a copy; the stored worker ID remains the operational identity."""
    if record.get("kind") == "integrator":
        return {**record, "display_name": "Integrator"}
    if record.get("kind") == "dispatcher":
        # The system identity that owns assignment decisions; it never executes a task.
        return {**record, "display_name": "Dispatcher"}
    if record.get("kind") == "waker":
        # The system identity that owns wake decisions (waker.py); it never executes a task.
        return {**record, "display_name": "Waker"}
    if record.get("reviewer_number") is not None:
        return {**record, "display_name": f"Reviewer {record['reviewer_number']}"}
    if record.get("worker_number") is not None and record.get("kind") != "review":
        return {**record, "display_name": f"Worker {record['worker_number']}"}
    return dict(record)


def _display_name(row) -> str:
    return worker_identity(dict(row)).get("display_name", row["id"])


def present_worker_names(value: Any, names: dict[str, str]) -> Any:
    """Project typed coordination references for display with these names; never edit the input."""

    def label(item):
        return names.get(item, item) if isinstance(item, str) else copy.deepcopy(item)

    def present(item):
        if isinstance(item, list):
            return [present(child) for child in item]
        if not isinstance(item, dict):
            return copy.deepcopy(item)
        result = {}
        for key, child in item.items():
            if key in OPAQUE_FIELDS or key.endswith("_json"):
                result[key] = copy.deepcopy(child)
            elif key in WORKER_FIELDS:
                result[key] = label(child)
            elif key == "sender_worker_id" or (key == "object_id" and str(item.get("kind", "")).startswith("worker.")):
                result[key] = label(child)
            elif key in WORKER_LIST_FIELDS and isinstance(child, list):
                result[key] = [label(entry) for entry in child]
            else:
                result[key] = present(child)
        if (isinstance(item.get("worker_number"), int) or isinstance(item.get("reviewer_number"), int)) and item.get("id") in names:
            result["id"] = names[item["id"]]
            result["display_name"] = names[item["id"]]
        for stem, fields in (("sender", ("sender_worker_id", "sender_id")),
                             ("recipient", ("recipient_id",))):
            for field in fields:
                if isinstance(item.get(field), str) and item[field] in names:
                    result[stem + "_display_name"] = names[item[field]]
                    break
        return result

    return present(value)


class WorkerNames:
    if TYPE_CHECKING:
        db: Any

    def _resolve_worker_id(self, c, reference: str) -> str:
        if not isinstance(reference, str) or not reference:
            raise HubError("INVALID_WORKER", "Select an agent ID, Worker N name, or Reviewer N name", 422)
        if reference.casefold() in {"integrator", "dispatcher", "waker"}:
            kind = reference.casefold()
            row = c.execute("SELECT id FROM workers WHERE kind=?", (kind,)).fetchone()
            if row is None:
                raise HubError("NOT_FOUND", f"The {kind} has not been created yet", 404)
            return str(row["id"])
        direct = c.execute("SELECT id FROM workers WHERE id=?", (reference,)).fetchone()
        reviewer_alias = REVIEWER_ALIAS.fullmatch(reference)
        alias = reviewer_alias or WORKER_ALIAS.fullmatch(reference)
        number = int(alias[1]) if alias and len(alias[1]) <= 19 else None
        field = "reviewer_number" if reviewer_alias else "worker_number"
        kind = "review" if reviewer_alias else "ordinary"
        named = (c.execute(f"SELECT id FROM workers WHERE {field}=? AND kind=?", (number, kind)).fetchone()
                 if number is not None and number <= 9223372036854775807 else None)
        if direct is not None and named is not None and direct["id"] != named["id"]:
            raise HubError("AMBIGUOUS_WORKER", "This historical ID conflicts with an agent display name", 409)
        row = named if named is not None else direct
        if row is None:
            raise HubError("NOT_FOUND", "Worker or reviewer does not exist", 404)
        return str(row["id"])

    async def resolve_worker_id(self, reference: str) -> str:
        return await self.db.call(lambda c: self._resolve_worker_id(c, reference))

    def _worker_display_names(self, c, worker_ids=None) -> dict[str, str]:
        names = self._display_names(c)
        if worker_ids is None:
            return names
        selected = {self._resolve_worker_id(c, reference) for reference in worker_ids}
        return {worker_id: name for worker_id, name in names.items() if worker_id in selected}

    def _display_names(self, c) -> dict[str, str]:
        """Every agent's display name, by ID. A name is fixed when its worker is created (numbers
        are immutable), so only the workers added since the last call are read: every tool result
        is presented with these names, and reading all 1,858 of the Linux pilot's workers each
        time took 2-3 s a call, 60-70 calls in 5 minutes (and up to 30 s each for 3,100 workers
        while snapshots held the interpreter lock). The mapping returned is shared and never
        changed; a new one replaces it."""
        lock = self.__dict__.setdefault("_display_name_lock", threading.Lock())
        with lock:
            seen, last, names = self.__dict__.get("_display_name_cache", (0, None, {}))
            rows = c.execute("SELECT rowid,id,kind,worker_number,reviewer_number FROM workers WHERE rowid>? "
                             "ORDER BY rowid", (seen,)).fetchall()
            if rows:
                names = {**names, **{row["id"]: _display_name(row) for row in rows}}
                seen, last = rows[-1]["rowid"], rows[-1]["id"]
            count, at_seen = c.execute("SELECT count(*),(SELECT id FROM workers WHERE rowid=?) FROM workers",
                                       (seen,)).fetchone()
            if count != len(names) or at_seen != last:
                # A worker was removed (TaskHub never does), and its rowid may be reused: read all.
                rows = c.execute("SELECT rowid,id,kind,worker_number,reviewer_number FROM workers "
                                 "ORDER BY rowid").fetchall()
                names = {row["id"]: _display_name(row) for row in rows}
                seen, last = (rows[-1]["rowid"], rows[-1]["id"]) if rows else (0, None)
            # A transaction's own new workers may still be rolled back. The read connections, where
            # tool results are presented, always hold a read-only snapshot: committed workers only.
            if not c.in_transaction or c.execute("PRAGMA query_only").fetchone()[0]:
                self._display_name_cache = (seen, last, names)
            return names

    def _resolve_worker_refs(self, c, arguments: dict) -> dict:
        """Resolve declared top-level identity inputs, never bodies or raw receipts."""
        result = dict(arguments)
        for field in WORKER_FIELDS - {"producer"}:
            if field in result and result[field] is not None:
                result[field] = self._resolve_worker_id(c, result[field])
        for field in WORKER_LIST_FIELDS:
            if field in result:
                if not isinstance(result[field], list):
                    raise HubError("INVALID_WORKER", f"{field} must be a list of worker or reviewer names or IDs", 422)
                result[field] = [self._resolve_worker_id(c, reference) for reference in result[field]]
        return result

    async def resolve_worker_refs(self, arguments: dict) -> dict:
        return await self.db.call(lambda c: self._resolve_worker_refs(c, arguments))

    def _normalize_worker_fields(self, c, arguments: dict, *fields: str) -> None:
        """Internal dispatch copy only, after the handler's authority checks."""
        selected = {field: arguments[field] for field in fields if field in arguments}
        arguments.update(self._resolve_worker_refs(c, selected))

    def _present_worker_data(self, c, value: Any) -> Any:
        """Project typed coordination references for display; never edit the input."""
        return present_worker_names(value, self._worker_display_names(c))

    async def present_worker_data(self, value: Any) -> Any:
        """The same, off the coordination thread: every tool result is presented, so the
        names come from a reader connection and the copy is made in a worker thread."""
        names = await self.db.read(self._worker_display_names)
        return await asyncio.to_thread(present_worker_names, value, names)