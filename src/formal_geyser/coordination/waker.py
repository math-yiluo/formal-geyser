"""The waker: the system identity, like the Dispatcher, that ends task subscriptions.

After each integration that publishes Lean (a candidate other than a public note), the waker
gets one decision, in a new conversation without tools (runtime/helpers.py): for each wait
subscribed to a task that published in it, whether what that task has published to the Library
since the subscription meets the wait's need. Its answer is one boolean per pair, nothing else.
A wake ends the wait (Hub._end_subscription_wait): its worker reconsiders, told only that its
subscribed task changed, and checks the Library itself. An answer outside the schema, or none in
time, asks again, WAKE_ATTEMPTS times in all; then the decision waits for the operator as an
uncertain helper, and the waits it judged go on waiting.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, get_args
from uuid import uuid4

from .errors import HubError
from .models import Effort, Role
from .storage import canonical

WAKE_ATTEMPTS = 5
WAKE_INSTRUCTION = (
    "Workers wait on other tasks. For each pair below, a waiting worker's need, in its own words, "
    "and what the task it waits on has published to the Library since it began waiting: decide "
    "whether the published results meet the need, so that the worker should wake and use them. "
    "Wake only when they do; a worker woken too early is woken in vain. Answer only with one "
    "boolean for each pair, under the pair's name.")
WAKER_PREFERENCES_SCHEMA = """
CREATE TABLE IF NOT EXISTS waker_preferences(
 id INTEGER PRIMARY KEY CHECK(id=1),model TEXT NOT NULL,effort TEXT NOT NULL,updated_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_waker ON workers(kind) WHERE kind='waker';
"""


def wake_schema(names: list[str]) -> dict:
    """The answer: one boolean for each pair offered, under its name."""
    return {"type": "object", "additionalProperties": False,
            "properties": {name: {"type": "boolean"} for name in names}, "required": list(names)}


def _pair_name(index: int) -> str:
    return f"pair_{index + 1}"


def _time(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000, UTC).isoformat(timespec="minutes")


class Waker:
    if TYPE_CHECKING:
        db: Any
        config: Any
        clock: Any
        _get: Any
        _helper: Any
        _event: Any
        _new_worker: Any
        _role: Any
        _dispatch_holds: Any
        _assignment_scope: Any
        _end_subscription_wait: Any
        _check_saved_helper_decision: Any
        artifact: Any

    def _waker(self, c) -> str:
        """The system identity that owns wake decisions; it never executes a task."""
        row = c.execute("SELECT id FROM workers WHERE kind='waker'").fetchone()
        if row is not None:
            return str(row["id"])
        worker_id = self._new_worker(c, kind="waker", backend="codex")
        c.execute("UPDATE workers SET work_intention='retired' WHERE id=?", (worker_id,))
        return str(worker_id)

    @staticmethod
    def _subscribers(c, task_ids) -> list[dict]:
        """One pair for each wait subscribed to one of `task_ids` and that task, in a stable order."""
        pairs = []
        for task_id in sorted(set(task_ids)):
            pairs += [{"continuation_id": row["id"], "task_id": task_id} for row in c.execute(
                "SELECT DISTINCT k.id,k.created_at FROM continuations k, json_each(k.predicate_json,'$.subscriptions') s "
                "WHERE k.state='waiting' AND json_extract(s.value,'$.task_id')=? ORDER BY k.created_at,k.id", (task_id,))]
        return pairs

    def _request_wake(self, c, task_ids, run_id, *, origin=None, attempt=1, pairs=None) -> str | None:
        """A wake decision for the waits subscribed to `task_ids`, the tasks an integration
        (`origin`) published from; none when no wait subscribes to them."""
        pairs = self._subscribers(c, task_ids) if pairs is None else pairs
        if not pairs:
            return None
        helper_id = uuid4().hex
        c.execute("INSERT INTO helper_intents(id,kind,source_worker_id,source_task_id,run_id,candidates_json,"
                  "state,created_at) VALUES(?,'wake',?,NULL,?,?,'prepared',?)",
                  (helper_id, self._waker(c), run_id,
                   canonical({"origin": origin, "attempt": attempt, "pairs": pairs}), self.clock()))
        self._event(c, "wake.requested", helper_id, {"origin": origin, "attempt": attempt, "pairs": len(pairs)})
        return helper_id

    def _wake_after_batch(self, c, batch_id, run_id) -> str | None:
        """After a published integration batch: a wake decision for the tasks it published Lean
        from. A batch of public notes alone asks none."""
        tasks = [row[0] for row in c.execute(
            "SELECT DISTINCT b.task_id FROM integration_batch_items i JOIN artifacts a ON a.id=i.candidate_id "
            "JOIN candidate_bindings b ON b.artifact_id=i.candidate_id "
            "WHERE i.batch_id=? AND i.state='published' AND a.kind<>'note.wiki'", (batch_id,))]
        return self._request_wake(c, tasks, run_id, origin=batch_id) if tasks else None

    def _waker_preferences(self, c) -> dict:
        """The waker's model and effort: the operator's recorded choice, else the project
        configuration; None while neither chose one."""
        row = c.execute("SELECT model,effort FROM waker_preferences WHERE id=1").fetchone()
        if row is not None:
            return {"model": row["model"], "effort": row["effort"], "source": "operator"}
        if self.config.waker.model is None:
            return {"model": None, "effort": None, "source": None}
        return {"model": self.config.waker.model, "effort": self.config.waker.effort, "source": "config"}

    async def waker_preferences(self) -> dict:
        return await self.db.read(self._waker_preferences)

    def _set_waker_model(self, c, p, actor) -> dict:
        self._role(actor, Role.OPERATOR)
        model, effort = p.get("model"), p.get("effort")
        if (not isinstance(model, str) or not model.strip() or len(model) > 200
                or any(ch.isspace() for ch in model)):
            raise HubError("INVALID_INPUT", "Expected a model name", 422)
        if effort not in get_args(Effort):
            raise HubError("INVALID_INPUT", "Expected effort low, medium, high, xhigh or max", 422)
        c.execute("INSERT INTO waker_preferences(id,model,effort,updated_at) VALUES(1,?,?,?) "
                  "ON CONFLICT(id) DO UPDATE SET model=excluded.model,effort=excluded.effort,"
                  "updated_at=excluded.updated_at", (model, effort, self.clock()))
        self._event(c, "runtime.waker_model_selected", "runtime", {"model": model, "effort": effort})
        return self._waker_preferences(c)

    async def due_wake_intents(self, run_id) -> list[dict]:
        """Wake decisions ready to run, or whose retained answer still needs its commit. None
        while the run gives no new work, the Codex account is held or no waker model is chosen."""
        def read(c):
            try:
                self._assignment_scope(c, run_id)
            except HubError:
                return []
            if "codex" in set(self._dispatch_holds(c)) or self._waker_preferences(c)["model"] is None:
                return []
            return [self._helper(c, row["id"]) for row in c.execute(
                "SELECT id FROM helper_intents WHERE kind='wake' AND run_id=? "
                "AND (state='prepared' OR (state='running' AND raw_result_json IS NOT NULL)) "
                "ORDER BY created_at,id", (run_id,))]
        return await self.db.read(read)

    async def wake_packet(self, intent) -> dict:
        """The decision's input: for each pair whose wait still waits, its name, the wait's need
        and what the task published to the Library since the wait began. Read when the waker
        runs, so it is current, and off the coordination writer."""
        def read(c):
            pairs = []
            for index, pair in enumerate(intent["candidates"]["pairs"]):
                wait = c.execute("SELECT predicate_json,created_at FROM continuations WHERE id=? AND state='waiting'",
                                 (pair["continuation_id"],)).fetchone()
                needs = [item["need"] for item in json.loads(wait["predicate_json"]).get("subscriptions", [])
                         if item["task_id"] == pair["task_id"]] if wait is not None else []
                if not needs:
                    continue  # Its wait ended meanwhile.
                title = c.execute("SELECT json_extract(v.contract_json,'$.title') FROM tasks t JOIN task_revisions v "
                                  "ON v.task_id=t.id AND v.revision=t.current_revision WHERE t.id=?",
                                  (pair["task_id"],)).fetchone()[0]
                published = [dict(row) for row in c.execute(
                    "SELECT p.candidate_id,a.kind,MIN(p.created_at) AS published_at FROM repository_publications p "
                    "JOIN candidate_bindings b ON b.artifact_id=p.candidate_id JOIN artifacts a ON a.id=p.candidate_id "
                    "WHERE b.task_id=? AND p.created_at>=? GROUP BY p.candidate_id ORDER BY published_at,p.candidate_id",
                    (pair["task_id"], wait["created_at"]))]
                pairs.append({"pair": _pair_name(index), "need": "\n".join(needs), "task_title": title,
                              "published": published})
            return pairs
        pairs = await self.db.read(read)
        for pair in pairs:
            entries = []
            for row in pair.pop("published"):
                data = (await self.artifact(row["candidate_id"])).get("data") or {}
                entries.append({"artifact_id": row["candidate_id"], "kind": row["kind"],
                                **{key: data[key] for key in ("declaration", "statement_declaration", "entry_module")
                                   if isinstance(data.get(key), str)},
                                "task_title": pair["task_title"], "published_at": _time(row["published_at"])})
            pair["published"] = entries
        return {"instruction": WAKE_INSTRUCTION, "pairs": pairs}

    def _complete_wake(self, c, intent_id, decision) -> dict:
        """Commit a wake decision: each pair answered true ends its wait. No decision (`None`)
        is an answer outside the schema or none in time: the waker is asked again, or, after
        WAKE_ATTEMPTS, the decision waits for the operator."""
        helper = self._helper(c, intent_id)
        if helper["kind"] != "wake":
            raise HubError("INVALID_INPUT", "Expected a wake decision", 422)
        if decision is not None:
            self._check_saved_helper_decision(helper, decision)
        if helper["state"] == "completed":
            return helper["result"]
        if helper["state"] not in {"prepared", "running"}:
            raise HubError("HELPER_RECONCILIATION_REQUIRED", "Reconcile the decision before committing it")
        offer = helper["candidates"]
        if decision is None:
            raw = json.loads(helper["raw_result_json"] or "{}")
            failure = "timed_out" if raw.get("timed_out") else "invalid_output"
            if offer["attempt"] >= WAKE_ATTEMPTS:
                reason = (f"The waker answered outside its schema, or not in time, {WAKE_ATTEMPTS} times; the "
                          "waits it judged keep waiting. Reconcile this helper to end the decision.")
                c.execute("UPDATE helper_intents SET state='uncertain',reason=? WHERE id=?", (reason, intent_id))
                self._event(c, "wake.held", intent_id, {"failure": failure, "attempts": offer["attempt"]})
                return {"outcome": "held", "failure": failure}
            pairs = [pair for pair in offer["pairs"] if c.execute(
                "SELECT 1 FROM continuations WHERE id=? AND state='waiting'", (pair["continuation_id"],)).fetchone()]
            again = self._request_wake(c, [], helper["run_id"], origin=offer.get("origin"),
                                       attempt=offer["attempt"] + 1, pairs=pairs)
            result: dict[str, Any] = {"outcome": failure, "next_helper_id": again}
        else:
            named = {_pair_name(index): pair for index, pair in enumerate(offer["pairs"])}
            woken = [named[name]["continuation_id"] for name, wake in sorted(decision.items())
                     if wake is True and name in named
                     and self._end_subscription_wait(c, named[name]["continuation_id"], named[name]["task_id"])]
            result = {"outcome": "decided", "woken": woken}
        c.execute("UPDATE helper_intents SET state='completed',result_json=?,ended_at=? WHERE id=?",
                  (canonical(result), self.clock(), intent_id))
        self._event(c, "wake.decided", intent_id, result)
        return result

    async def complete_wake(self, intent_id, decision) -> dict:
        return await self.db.call(lambda c: self._complete_wake(c, intent_id, decision), write=True)

    def _wake_again(self, c, helper) -> None:
        """A wake decision a pause stopped before it answered is asked again, as the same attempt."""
        offer = helper["candidates"]
        pairs = [pair for pair in offer["pairs"] if c.execute(
            "SELECT 1 FROM continuations WHERE id=? AND state='waiting'", (pair["continuation_id"],)).fetchone()]
        self._request_wake(c, [], helper["run_id"], origin=offer.get("origin"), attempt=offer["attempt"], pairs=pairs)
