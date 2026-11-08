"""Task numbers: each work task's short name, Task N, beside its id.

The id stays every record's identity: evidence, receipts, bindings, file paths and the Library's
history name it, and core transfer keeps it. The number is what people and workers read and
type. Work tasks are numbered in publication order, Task 0 (the project's root) first; a number is
never reused or changed. Reviews and the Integrator's tasks have none: they are named by the
task they serve. Wherever a task id is taken, `Task N` (or `N`) is taken too.
"""
from __future__ import annotations

import copy
import re
import threading
from typing import TYPE_CHECKING, Any

from .errors import HubError

TASK_ALIAS = re.compile(r"(?:task\s*)?(0|[1-9][0-9]{0,17})", re.IGNORECASE)
# Inputs and outputs whose value is one task's id.
TASK_FIELDS = frozenset({
    "task_id", "source_task_id", "host_task_id", "root_task_id", "parent_task_id",
    "origin_task_id", "repair_task_id", "current_task_id", "review_task_id",
})
TASK_LIST_FIELDS = frozenset({"task_ids"})
# Authored content and exact evidence keep their ids (worker_names.OPAQUE_FIELDS), and so do
# the decisions' own packets, whose schemas name tasks by id.
OPAQUE_FIELDS = frozenset({
    "data", "contract", "payload", "checkpoint", "proof", "details", "evidence",
    "receipt", "receipts", "candidate_binding", "binding", "raw_result", "profile",
    "semantic_checks", "integration_notes", "record", "paths",
})
INTERNAL_KINDS = ("review", "integration")
_WORK_ORDER = ("SELECT t.id FROM tasks t JOIN task_revisions r ON r.task_id=t.id AND r.revision=t.current_revision "
               "WHERE {unnumbered}coalesce(json_extract(r.contract_json,'$.kind'),'work') NOT IN ('review','integration') "
               "ORDER BY coalesce(t.id=(SELECT root_task_id FROM project_lifecycle WHERE id=1),0) DESC,"
               "t.created_at,t.rowid")


def task_name(number: int | None) -> str | None:
    return None if number is None else f"Task {number}"


def work_task_order(connection, *, unnumbered: bool = True) -> list[str]:
    """The (unnumbered) work tasks in the order they are numbered: the root first, then by
    publication. All of them is what an export gives an instance that numbers no task yet."""
    sql = _WORK_ORDER.format(unnumbered="t.task_number IS NULL AND " if unnumbered else "")
    return [row[0] for row in connection.execute(sql)]


def initialize_task_names(connection) -> None:
    """Number every work task that has no number yet, and keep numbers unique and fixed. On the
    first start after this change that is every work task the project has."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(tasks)")}
        if "task_number" not in columns:
            connection.execute("ALTER TABLE tasks ADD COLUMN task_number INTEGER CHECK(task_number>=0)")
        connection.execute("CREATE TABLE IF NOT EXISTS task_name_sequence("
                           "id INTEGER PRIMARY KEY CHECK(id=1),next_number INTEGER NOT NULL CHECK(next_number>=0))")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS tasks_number_idx ON tasks(task_number)")
        connection.execute("CREATE TRIGGER IF NOT EXISTS immutable_task_number "
                           "BEFORE UPDATE OF task_number ON tasks "
                           "WHEN OLD.task_number IS NOT NULL AND NEW.task_number IS NOT OLD.task_number "
                           "BEGIN SELECT RAISE(ABORT,'Task numbers are immutable'); END")
        highest = connection.execute("SELECT max(task_number) FROM tasks").fetchone()[0]
        connection.execute("INSERT OR IGNORE INTO task_name_sequence VALUES(1,?)",
                           (0 if highest is None else highest + 1,))
        for task_id in work_task_order(connection):
            connection.execute("UPDATE tasks SET task_number=? WHERE id=?", (allocate_task_number(connection), task_id))
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def allocate_task_number(connection, requested: int | None = None) -> int:
    """A new work task's number, inside the transaction that creates it: the next one, or
    `requested` (a task carried from another instance keeps its own) if it is free."""
    if requested is None:
        row = connection.execute("UPDATE task_name_sequence SET next_number=next_number+1 "
                                 "WHERE id=1 RETURNING next_number-1").fetchone()
        return int(row[0])
    if type(requested) is not int or requested < 0:
        raise HubError("INVALID_INPUT", "A task number is a nonnegative integer", 422)
    if connection.execute("SELECT 1 FROM tasks WHERE task_number=?", (requested,)).fetchone():
        raise HubError("TASK_EXISTS", f"Task {requested} already exists", 409)
    connection.execute("UPDATE task_name_sequence SET next_number=max(next_number,?) WHERE id=1", (requested + 1,))
    return requested


def present_task_references(value: Any, names: dict[str, str]) -> Any:
    """A model-facing copy in which each typed reference to a numbered task reads Task N. Task
    objects keep their exact id (a worker builds paths such as drafts/<task-id>/ from it) and
    say their name beside it; authored content, evidence and paths are copied unchanged."""
    def present(item, key=None):
        if isinstance(item, list):
            if key in TASK_LIST_FIELDS:
                return [names.get(child, child) if isinstance(child, str) else present(child) for child in item]
            return [present(child) for child in item]
        if isinstance(item, str):
            return names.get(item, item) if key in TASK_FIELDS or key == "reference" else item
        if not isinstance(item, dict):
            return copy.deepcopy(item)
        result = {}
        for child_key, child in item.items():
            if child_key in OPAQUE_FIELDS or child_key.endswith("_json"):
                result[child_key] = copy.deepcopy(child)
            else:
                result[child_key] = present(child, child_key)
        if isinstance(item.get("id"), str) and item["id"] in names and "name" not in item:
            result["name"] = names[item["id"]]
        return result
    return present(value)


class TaskNames:
    if TYPE_CHECKING:
        db: Any

    def _resolve_task_ref(self, c, reference: Any) -> Any:
        """The id that `reference` names: Task N or N for a numbered task; anything else as it
        is, for the handler to check as before."""
        if not isinstance(reference, str):
            return reference
        alias = TASK_ALIAS.fullmatch(reference.strip())
        if alias is None:
            return reference
        row = c.execute("SELECT id FROM tasks WHERE task_number=?", (int(alias[1]),)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", f"Task {int(alias[1])} does not exist", 404)
        return str(row["id"])

    def _resolve_task_refs(self, c, arguments: dict) -> dict:
        """Resolve task names in the declared task fields of a command or read: top-level ids
        and lists of them, a pause's subscriptions, and a contract's requirements."""
        result = dict(arguments)
        for field in TASK_FIELDS & result.keys():
            result[field] = self._resolve_task_ref(c, result[field])
        for field in TASK_LIST_FIELDS & result.keys():
            if isinstance(result[field], list):
                result[field] = [self._resolve_task_ref(c, item) for item in result[field]]
        predicate = result.get("predicate")
        if isinstance(predicate, dict) and isinstance(predicate.get("subscriptions"), list):
            result["predicate"] = {**predicate, "subscriptions": [
                {**item, "task_id": self._resolve_task_ref(c, item.get("task_id"))}
                if isinstance(item, dict) and "task_id" in item else item
                for item in predicate["subscriptions"]]}
        contract = result.get("contract")
        if isinstance(contract, dict) and isinstance(contract.get("requirements"), list):
            result["contract"] = {**contract, "requirements": [
                {**item, "task_id": self._resolve_task_ref(c, item["task_id"])}
                if isinstance(item, dict) and item.get("task_id") is not None else item
                for item in contract["requirements"]]}
        return result

    async def resolve_task_refs(self, arguments: dict) -> dict:
        return await self.db.read(lambda c: self._resolve_task_refs(c, arguments))

    async def resolve_task_ref(self, reference: str) -> str:
        return await self.db.read(lambda c: self._resolve_task_ref(c, reference))

    def _task_display_names(self, c) -> dict[str, str]:
        """Every numbered task's name, by id. A number is fixed when its task is created, so only
        the tasks added since the last call are read (as WorkerNames._display_names); the mapping
        returned is shared and never changed."""
        lock = self.__dict__.setdefault("_task_name_lock", threading.Lock())
        with lock:
            seen, names = self.__dict__.get("_task_name_cache", (0, {}))
            rows = c.execute("SELECT rowid,id,task_number FROM tasks WHERE rowid>? ORDER BY rowid", (seen,)).fetchall()
            if rows:
                names = {**names, **{row["id"]: f"Task {row['task_number']}"
                                     for row in rows if row["task_number"] is not None}}
                seen = rows[-1]["rowid"]
            if c.execute("SELECT count(*) FROM tasks WHERE task_number IS NOT NULL").fetchone()[0] != len(names):
                rows = c.execute("SELECT rowid,id,task_number FROM tasks ORDER BY rowid").fetchall()
                names = {row["id"]: f"Task {row['task_number']}" for row in rows if row["task_number"] is not None}
                seen = rows[-1]["rowid"] if rows else 0
            # A transaction's own new tasks may still be rolled back; read connections hold
            # committed ones only.
            if not c.in_transaction or c.execute("PRAGMA query_only").fetchone()[0]:
                self._task_name_cache = (seen, names)
            return names

    async def task_display_names(self) -> dict[str, str]:
        return await self.db.read(self._task_display_names)
