"""Replacing a long, or long-idle, conversation by a new one that continues the same task.

Claude's prompt cache expires an hour after its last use, so a conversation resumed after a
longer idle period re-reads its whole context at full price. Codex's has no fixed expiry but is
not kept either: in the pilot, about nine in ten Codex conversations resumed within an hour
found it, three in ten after two to four hours (docs/claude-backend.md). A long context makes
every request expensive either way.
Rotation ends such a conversation at a safe point and continues the task in a new one that
starts from the agent's handoff file and the runtime's state packet.

Modes (global, and per worker): conservative rotates a conversation idle for its backend's
idle time (the project's `idle_minutes`, or the operator's per backend, where 0 turns idle
rotation off); aggressive also rotates once its context exceeds the configured size, after reminding
the agent to update its handoff file and letting the turn end. Codex compaction remains
the fallback inside a long turn.
"""
from __future__ import annotations

import asyncio
import json
import statistics
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .communications import NOT_WITHDRAWN
from .errors import HubError
from .models import BACKENDS, Actor, Role
from .rotation_review import memory_report
from .storage import canonical
from .transcripts import TranscriptStore
from .worker_names import worker_identity

ROTATION_MODES = ("conservative", "aggressive")
# The longest idle time the operator can set, in minutes (a week); 0 turns idle rotation off.
IDLE_MINUTES_MAX = 7 * 24 * 60
# The Integrator starts a new conversation for every batch and keeps it for the whole batch.
ROTATABLE_KINDS = frozenset({"ordinary", "review"})
# A context rotation needs this much growth since the conversation began, so a new
# conversation that starts large does not rotate again at once.
CONTEXT_GROWTH = 50_000
# A token's price in uncached input tokens, per backend. Codex: OpenAI's published Codex credit
# rates price a cached input token at a tenth and an output token at five; Test 2's weekly quota,
# at 0.4 and 34. Claude: Anthropic's list prices have the same official ratios (a cache read a
# tenth, an output token five); cache writes are counted as uncached input.
CREDIT_WEIGHTS: dict[str, dict[str, dict[str, float]]] = {
    "codex": {"official": {"cached": 0.1, "output": 5}, "measured": {"cached": 0.4, "output": 34}},
    "claude": {"official": {"cached": 0.1, "output": 5}}}

ROTATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS rotation_preferences(
 id INTEGER PRIMARY KEY CHECK(id=1),mode TEXT NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rotations(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL REFERENCES workers(id),task_id TEXT,thread_id TEXT NOT NULL,
 reason TEXT NOT NULL,mode TEXT NOT NULL,state TEXT NOT NULL,context_tokens INTEGER,idle_ms INTEGER,
 reminder_sha256 TEXT,repairs INTEGER NOT NULL DEFAULT 0,handoff_json TEXT,state_path TEXT,
 automatic INTEGER NOT NULL DEFAULT 0,new_thread_id TEXT,ended_reason TEXT,requested_at INTEGER NOT NULL,
 ready_at INTEGER,started_at INTEGER,completed_at INTEGER);
CREATE TABLE IF NOT EXISTS rotation_idle_preferences(
 backend TEXT PRIMARY KEY CHECK(backend IN ('codex','claude')),
 minutes INTEGER NOT NULL CHECK(minutes>=0 AND minutes<=10080),updated_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_live_rotation ON rotations(worker_id)
 WHERE state IN ('requested','ready','started');
CREATE TABLE IF NOT EXISTS review_reports(
 review_task_id TEXT PRIMARY KEY,reviewer_id TEXT NOT NULL REFERENCES workers(id),path TEXT NOT NULL,
 sha256 TEXT NOT NULL,created_at INTEGER NOT NULL);
"""


def token_counts(usage: Any) -> list[int] | None:
    """A usage record's input (cached included), cached input and output tokens."""
    usage = usage.get("total", usage) if isinstance(usage, dict) else None
    if not isinstance(usage, dict):
        return None
    counts = [usage.get("inputTokens"), usage.get("cachedInputTokens", 0), usage.get("outputTokens")]
    return counts if all(type(count) is int and count >= 0 for count in counts) else None


def credit_units(uncached: float, cached: float, output: float, backend: str = "codex") -> dict[str, int]:
    """The tokens' price in uncached input tokens, at each of the backend's `CREDIT_WEIGHTS`."""
    return {name: round(uncached + weights["cached"] * cached + weights["output"] * output)
            for name, weights in CREDIT_WEIGHTS[backend].items()}


class Rotation:
    if TYPE_CHECKING:
        config: Any
        db: Any
        clock: Any
        root: Any
        state_dir: Any
        _get: Any
        _event: Any
        _role: Any
        _normalize_worker_fields: Any
        _release_conversation: Any
        _switch_possible: Any
        _task: Any

    # -- settings -------------------------------------------------------------------

    def _rotation_settings(self, c) -> dict:
        row = c.execute("SELECT mode FROM rotation_preferences WHERE id=1").fetchone()
        config = self.config.rotation
        saved = {row["backend"]: row["minutes"] for row in c.execute("SELECT backend,minutes FROM rotation_idle_preferences")}
        return {"mode": row["mode"] if row else config.mode, "source": "operator" if row else "config",
                "idle_minutes": config.idle_minutes, "context_tokens": config.context_tokens,
                "idle_by_backend": {backend: {"minutes": saved.get(backend, config.idle_minutes),
                                              "source": "operator" if backend in saved else "config"}
                                    for backend in BACKENDS}}

    def _idle_minutes(self, c, backend: str) -> int:
        """How long a conversation on this backend may be idle before it rotates; 0: never."""
        row = c.execute("SELECT minutes FROM rotation_idle_preferences WHERE backend=?", (backend,)).fetchone()
        return row["minutes"] if row else self.config.rotation.idle_minutes

    async def rotation_settings(self) -> dict:
        return await self.db.read(self._rotation_settings)

    def _rotation_mode(self, c, worker) -> str:
        return worker["rotation_mode"] or self._rotation_settings(c)["mode"]

    def _rotation_command(self, c, name: str, p: dict, actor: Actor):
        if name == "runtime.set_rotation_mode":
            self._role(actor, Role.OPERATOR)
            if p.get("mode") not in ROTATION_MODES:
                raise HubError("INVALID_INPUT", "Expected the conservative or aggressive rotation mode", 422)
            c.execute("INSERT INTO rotation_preferences(id,mode,updated_at) VALUES(1,?,?) ON CONFLICT(id) "
                      "DO UPDATE SET mode=excluded.mode,updated_at=excluded.updated_at", (p["mode"], self.clock()))
            self._event(c, "runtime.rotation_mode_selected", "runtime", {"mode": p["mode"]})
            return self._rotation_settings(c)
        if name == "runtime.set_rotation_idle":
            # Applies to the next activation of each conversation on that backend.
            self._role(actor, Role.OPERATOR)
            backend, minutes = p.get("backend"), p.get("minutes")
            if backend not in BACKENDS:
                raise HubError("INVALID_INPUT", "Expected the codex or claude backend", 422)
            if minutes is not None and (not isinstance(minutes, int) or isinstance(minutes, bool)
                                        or not 0 <= minutes <= IDLE_MINUTES_MAX):
                raise HubError("INVALID_INPUT", f"Expected 0 to {IDLE_MINUTES_MAX} minutes (0: no idle rotation), "
                               "or null for the project's", 422)
            if minutes is None:
                c.execute("DELETE FROM rotation_idle_preferences WHERE backend=?", (backend,))
            else:
                c.execute("INSERT INTO rotation_idle_preferences(backend,minutes,updated_at) VALUES(?,?,?) ON CONFLICT(backend) "
                          "DO UPDATE SET minutes=excluded.minutes,updated_at=excluded.updated_at", (backend, minutes, self.clock()))
            self._event(c, "runtime.rotation_idle_selected", "runtime", {"backend": backend, "minutes": minutes})
            return self._rotation_settings(c)
        if name == "worker.set_rotation_mode":
            self._role(actor, Role.OPERATOR)
            self._normalize_worker_fields(c, p, "worker_id")
            worker = self._get(c, "workers", p["worker_id"])
            mode = p.get("mode")
            if mode is not None and mode not in ROTATION_MODES:
                raise HubError("INVALID_INPUT", "Expected conservative, aggressive, or null for the project mode", 422)
            if worker["kind"] not in ROTATABLE_KINDS:
                raise HubError("INVALID_INPUT", "Only workers and reviewers have conversations to rotate", 422)
            c.execute("UPDATE workers SET rotation_mode=? WHERE id=?", (mode, worker["id"]))
            self._event(c, "worker.rotation_mode_selected", worker["id"], {"mode": mode}, worker_id=worker["id"])
            return {"worker_id": worker["id"], "rotation_mode": mode,
                    "effective_mode": self._rotation_mode(c, self._get(c, "workers", worker["id"]))}
        return None

    # -- records --------------------------------------------------------------------

    @staticmethod
    def _rotation_row(row) -> dict:
        result = dict(row)
        result["handoff"] = json.loads(result.pop("handoff_json")) if result.get("handoff_json") else None
        result["automatic"] = bool(result["automatic"])
        return result

    @staticmethod
    def _rotation(c, rotation_id):
        row = c.execute("SELECT * FROM rotations WHERE id=?", (rotation_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "rotations object does not exist", 404)
        return dict(row)

    def _live_rotation(self, c, worker_id):
        return c.execute("SELECT * FROM rotations WHERE worker_id=? AND state IN ('requested','ready','started')",
                         (worker_id,)).fetchone()

    def _end_rotation(self, c, rotation_id, reason) -> None:
        c.execute("UPDATE rotations SET state='cancelled',ended_reason=?,completed_at=? WHERE id=? "
                  "AND state IN ('requested','ready','started')", (reason, self.clock(), rotation_id))

    def _cancel_rotations(self, c, worker_id, reason, *, keep=None) -> None:
        """A conversation released for another reason supersedes a pending rotation."""
        for row in c.execute("SELECT id FROM rotations WHERE worker_id=? AND state IN ('requested','ready','started')",
                             (worker_id,)).fetchall():
            if row["id"] != keep:
                self._end_rotation(c, row["id"], reason)

    def _thread_last_turn_at(self, c, worker) -> int | None:
        """When the worker's current thread last had a turn (its end, else its reservation)."""
        row = c.execute("SELECT MAX(u.updated_at) FROM turn_intents t JOIN turn_usage_totals u ON u.intent_id=t.id "
                        "WHERE t.worker_id=? AND t.thread_id=?", (worker["id"], worker["thread_id"])).fetchone()
        return row[0] if row else None

    def _rotation_blocked(self, c, worker) -> str | None:
        """Records bound to the old thread that must settle before it is replaced."""
        if c.execute("SELECT 1 FROM message_delivery_holds WHERE worker_id=? AND state IN ('sending','uncertain')",
                     (worker["id"],)).fetchone():
            return "delivery_unresolved"
        if c.execute("SELECT 1 FROM native_call_receipts WHERE worker_id=? AND thread_id=? "
                     "AND validation_state IN ('pending','running','uncertain')",
                     (worker["id"], worker["thread_id"])).fetchone():
            return "validation_pending"
        return None

    # -- the rotation lifecycle -------------------------------------------------------

    async def rotation_due(self, worker_id: str, attempt: dict, instructions_sha256: str | None = None) -> dict | None:
        """The rotation to carry out before this activation opens its conversation, if any.

        A rotation the previous turn prepared, one the changed standing instructions require
        (`instructions_sha256` differs from the conversation's), or one the idle time requires
        now. A parked interruption resumes in its own thread, and nothing rotates while records
        bound to the old thread are still unsettled.
        """
        def due(c):
            worker = self._get(c, "workers", worker_id)
            if not worker["thread_id"] or worker["kind"] not in ROTATABLE_KINDS or attempt.get("interruption_id"):
                return None
            if attempt.get("execution_mode") == "message_only" and worker["current_task_id"] != attempt["task_id"]:
                return None  # A message for a worker that left the task: there is no task to continue.
            live = self._live_rotation(c, worker_id)
            if live is not None and (live["thread_id"] != worker["thread_id"] or live["task_id"] != attempt["task_id"]):
                self._end_rotation(c, live["id"], "superseded")
                live = None
            if live is not None:
                blocked = self._rotation_blocked(c, worker)
                if blocked and live["reason"] == "backend" and live["state"] == "ready":
                    # Records bound to the old conversation keep the worker there for this turn;
                    # admission prepares the move again once they settle.
                    self._end_rotation(c, live["id"], "backend_" + blocked)
                    return None
                return self._rotation_row(live) if live["state"] == "ready" and not blocked else None
            if self._rotation_blocked(c, worker):
                return None
            # Codex keeps a conversation's instructions as they were when it began (or was last
            # compacted): one that began under other instructions continues in a new one.
            recorded = worker["instructions_sha256"]  # None: not recorded, as when bound by hand.
            if instructions_sha256 is not None and recorded is not None and recorded != instructions_sha256:
                return self._ready_rotation(c, worker, attempt, "instructions")
            minutes = self._idle_minutes(c, worker["backend"])
            last = self._thread_last_turn_at(c, worker)
            if not minutes or last is None or self.clock() - last < minutes * 60_000:
                return None
            return self._ready_rotation(c, worker, attempt, "idle", idle_ms=self.clock() - last)
        return await self.db.call(due, write=True)

    def _ready_rotation(self, c, worker, attempt, reason, *, idle_ms=None):
        rotation_id = uuid4().hex
        c.execute("INSERT INTO rotations(id,worker_id,task_id,thread_id,reason,mode,state,idle_ms,requested_at,ready_at) "
                  "VALUES(?,?,?,?,?,?,'ready',?,?,?)",
                  (rotation_id, worker["id"], attempt["task_id"], worker["thread_id"], reason,
                   self._rotation_mode(c, worker), idle_ms, self.clock(), self.clock()))
        self._event(c, "rotation.ready", rotation_id, {"reason": reason, "idle_ms": idle_ms},
                    attempt["task_id"], worker["id"])
        return self._rotation_row(self._rotation(c, rotation_id))

    async def request_rotation(self, worker_id: str, thread_id: str, context_tokens: int,
                               reminder_sha256: str | None) -> dict | None:
        """In aggressive mode, a context past the limit asks the agent to prepare its handoff."""
        def request(c):
            worker = self._get(c, "workers", worker_id)
            settings = self._rotation_settings(c)
            if (worker["thread_id"] != thread_id or worker["kind"] not in ROTATABLE_KINDS
                    or self._rotation_mode(c, worker) != "aggressive" or context_tokens <= settings["context_tokens"]
                    or self._live_rotation(c, worker_id) is not None):
                return None
            start = c.execute("SELECT u.first_input_tokens FROM turn_intents t JOIN turn_usage_totals u "
                              "ON u.intent_id=t.id WHERE t.worker_id=? AND t.thread_id=? "
                              "AND u.first_input_tokens IS NOT NULL ORDER BY t.created_at,t.rowid LIMIT 1",
                              (worker_id, thread_id)).fetchone()
            if start is not None and context_tokens - start[0] < CONTEXT_GROWTH:
                return None
            rotation_id = uuid4().hex
            c.execute("INSERT INTO rotations(id,worker_id,task_id,thread_id,reason,mode,state,context_tokens,"
                      "reminder_sha256,requested_at) VALUES(?,?,?,?,'context','aggressive','requested',?,?,?)",
                      (rotation_id, worker_id, worker["current_task_id"], thread_id, context_tokens,
                       reminder_sha256, self.clock()))
            self._event(c, "rotation.requested", rotation_id, {"reason": "context", "context_tokens": context_tokens},
                        worker["current_task_id"], worker_id)
            return self._rotation_row(self._rotation(c, rotation_id))
        return await self.db.call(request, write=True)

    async def requested_rotation(self, worker_id: str, thread_id: str | None = None) -> dict | None:
        """A rotation still waiting for the end of the reminded turn on `thread_id`, by default
        the worker's current thread."""
        def read(c):
            thread = thread_id or self._get(c, "workers", worker_id)["thread_id"]
            row = c.execute("SELECT * FROM rotations WHERE worker_id=? AND thread_id=? AND state='requested'",
                            (worker_id, thread)).fetchone()
            return self._rotation_row(row) if row else None
        return await self.db.read(read)

    async def decide_rotation(self, rotation_id: str, check: dict) -> dict:
        """After the reminded turn: a valid handoff file is ready; an invalid one gets one more
        turn to be fixed, then the state packet alone starts the new conversation."""
        def decide(c):
            row = self._rotation(c, rotation_id)
            if row["state"] != "requested":
                return self._rotation_row(row)
            now = self.clock()
            if check.get("valid") or row["repairs"] >= 1:
                automatic = not check.get("valid")
                c.execute("UPDATE rotations SET state='ready',handoff_json=?,automatic=?,ready_at=? WHERE id=?",
                          (canonical(check), int(automatic), now, rotation_id))
                self._event(c, "rotation.ready", rotation_id, {"reason": row["reason"], "automatic": automatic},
                            row["task_id"], row["worker_id"])
            else:
                c.execute("UPDATE rotations SET repairs=repairs+1,handoff_json=? WHERE id=?", (canonical(check), rotation_id))
                self._event(c, "rotation.handoff_repair_requested", rotation_id, {"problems": check.get("problems", [])},
                            row["task_id"], row["worker_id"])
            return self._rotation_row(self._rotation(c, rotation_id))
        return await self.db.call(decide, write=True)

    async def begin_rotation(self, rotation_id: str, *, handoff: dict, state_path: str) -> dict | None:
        """Release the old conversation; the activation opens a new one. A move to another
        backend whose account can no longer start a session (held since admission) keeps the old
        conversation for this activation instead of opening a new one where it was."""
        def begin(c):
            row = self._rotation(c, rotation_id)
            worker = self._get(c, "workers", row["worker_id"])
            if row["state"] != "ready" or worker["thread_id"] != row["thread_id"]:
                return None
            if row["reason"] == "backend":
                request = c.execute("SELECT backend FROM worker_backend_requests WHERE worker_id=?",
                                    (worker["id"],)).fetchone()
                if (request is None or request["backend"] == worker["backend"]
                        or not self._switch_possible(c, worker["id"], request["backend"], worker["kind"])):
                    self._end_rotation(c, rotation_id, "backend_unavailable")
                    return None
            automatic = bool(row["automatic"]) or not handoff.get("valid")
            c.execute("UPDATE rotations SET state='started',handoff_json=?,state_path=?,automatic=?,started_at=? WHERE id=?",
                      (canonical(handoff), state_path, int(automatic), self.clock(), rotation_id))
            self._release_conversation(c, worker["id"], "rotation", keep_rotation=rotation_id)
            return self._rotation_row(self._rotation(c, rotation_id))
        return await self.db.call(begin, write=True)

    def _complete_rotation(self, c, worker_id: str, thread_id: str) -> None:
        """The new conversation is bound: the rotation is complete."""
        row = c.execute("SELECT * FROM rotations WHERE worker_id=? AND state='started'", (worker_id,)).fetchone()
        if row is None or row["thread_id"] == thread_id:
            return
        c.execute("UPDATE rotations SET state='completed',new_thread_id=?,completed_at=? WHERE id=?",
                  (thread_id, self.clock(), row["id"]))
        self._event(c, "rotation.completed", row["id"], {"reason": row["reason"], "mode": row["mode"],
                    "automatic": bool(row["automatic"]), "new_thread_id": thread_id}, row["task_id"], worker_id)

    async def rotation_state(self, worker_id: str, task_id: str | None) -> dict:
        """TaskHub's records for the state packet: the task, its team, the worker's candidates."""
        def read(c):
            if task_id is None:
                return {"task": {"id": None, "title": "No current task", "revision": 0, "state": "idle"}}
            task = self._task(c, task_id)
            members = []
            for row in c.execute("SELECT m.worker_id,m.membership_state,m.executor,l.initial_worker_id FROM task_members m "
                                 "LEFT JOIN task_lifecycle l ON l.task_id=m.task_id WHERE m.task_id=? AND m.executor=1 "
                                 "ORDER BY m.joined_at", (task_id,)):
                name = worker_identity(self._get(c, "workers", row["worker_id"]))["display_name"]
                members.append({"name": name + (" (you)" if row["worker_id"] == worker_id else ""),
                                "role": "first executor" if row["worker_id"] == row["initial_worker_id"] else "collaborator",
                                "current": row["membership_state"] == "member"})
            candidates = []
            for row in c.execute("SELECT id,kind,status FROM artifacts WHERE task_id=? AND producer=? "
                                 "ORDER BY created_at", (task_id, worker_id)):
                evidence = c.execute("SELECT verdict FROM evidence WHERE artifact_id=? ORDER BY sequence DESC LIMIT 1",
                                     (row["id"],)).fetchone()
                review = c.execute("SELECT state FROM review_requests WHERE candidate_id=?", (row["id"],)).fetchone()
                candidates.append({"id": row["id"], "kind": row["kind"], "status": row["status"],
                                   "validation": evidence["verdict"] if evidence else None,
                                   "review": review["state"] if review else None})
            wait = c.execute("SELECT predicate_json FROM continuations WHERE task_id=? AND preferred_worker=? "
                             "AND state='waiting' ORDER BY created_at DESC LIMIT 1", (task_id, worker_id)).fetchone()
            member = c.execute("SELECT checkpoint_json FROM task_members WHERE task_id=? AND worker_id=?",
                               (task_id, worker_id)).fetchone()
            checkpoint = json.loads(member["checkpoint_json"]) if member and member["checkpoint_json"] else None
            messages = {
                "human": c.execute("SELECT count(*) FROM human_messages m WHERE worker_id=? AND direction='to_worker' "
                                   "AND acknowledged_at IS NULL AND " + NOT_WITHDRAWN, (worker_id,)).fetchone()[0],
                "peer": c.execute("SELECT count(*) FROM peer_messages WHERE recipient_id=? AND acknowledged_at IS NULL",
                                  (worker_id,)).fetchone()[0]}
            return {"task": {"id": task["id"], "title": task["contract"]["title"], "revision": task["revision"],
                             "state": task["state"], "contract_file": (task.get("files") or {}).get("path")},
                    "members": members, "candidates": candidates, "messages": messages,
                    "wait": json.loads(wait["predicate_json"]) if wait else None,
                    "checkpoint": checkpoint if isinstance(checkpoint, dict) else None}
        return await self.db.read(read)

    async def release_conversation(self, worker_id: str, reason: str) -> None:
        """The worker's next activation opens a new conversation."""
        await self.db.call(lambda c: self._release_conversation(c, worker_id, reason), write=True)

    # -- reviewer reports -------------------------------------------------------------

    async def review_record(self, worker_id: str) -> dict | None:
        """The reviewer's recorded verdict: an acceptance's report, or a rejection's receipt."""
        def read(c):
            accepted = c.execute("SELECT review_task_id,candidate_id,report_json FROM review_acceptances "
                                 "WHERE reviewer_id=? ORDER BY created_at DESC LIMIT 1", (worker_id,)).fetchone()
            if accepted is not None:
                report = json.loads(accepted["report_json"])
                return {"review_task_id": accepted["review_task_id"], "candidate_id": accepted["candidate_id"],
                        "verdict": report.get("verdict", "approve"), "fields": report}
            rejected = c.execute("SELECT review_task_id,candidate_id,verdict,rationale,evidence_json,qualifications_json "
                                 "FROM peer_review_receipts WHERE reviewer_id=? ORDER BY created_at DESC LIMIT 1",
                                 (worker_id,)).fetchone()
            if rejected is None:
                return None
            return {"review_task_id": rejected["review_task_id"], "candidate_id": rejected["candidate_id"],
                    "verdict": rejected["verdict"],
                    "fields": {"rationale": rejected["rationale"], "evidence": json.loads(rejected["evidence_json"]),
                               "qualifications": json.loads(rejected["qualifications_json"])}}
        return await self.db.read(read)

    async def record_review_report(self, worker_id: str, review_task_id: str, path: str, sha256: str) -> None:
        def record(c):
            c.execute("INSERT INTO review_reports VALUES(?,?,?,?,?) ON CONFLICT(review_task_id) DO UPDATE SET "
                      "path=excluded.path,sha256=excluded.sha256,created_at=excluded.created_at",
                      (review_task_id, worker_id, path, sha256, self.clock()))
            self._event(c, "review.report_recorded", review_task_id, {"path": path, "sha256": sha256},
                        review_task_id, worker_id)
        await self.db.call(record, write=True)

    async def review_report(self, worker_id: str) -> dict | None:
        """The reviewer's recorded report file, if one was written."""
        def read(c):
            row = c.execute("SELECT * FROM review_reports WHERE reviewer_id=? ORDER BY created_at DESC LIMIT 1",
                            (worker_id,)).fetchone()
            return dict(row) if row else None
        return await self.db.read(read)

    def _record_turn_usage(self, c, worker_id: str, sample: Any, previous: Any) -> None:
        """Charge a usage sample to the worker's live turn. The turn's first request shows what
        resuming the conversation cost uncached, and its largest the context. The turn's own
        tokens, which price it, are what the conversation's cumulative counts grew since its
        previous sample (`previous`, None before the first)."""
        live = c.execute("SELECT id FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound') "
                         "ORDER BY created_at DESC LIMIT 1", (worker_id,)).fetchone()
        if live is None or not isinstance(sample, dict):
            return
        last = sample.get("lastTokenUsage")
        if isinstance(last, dict) and type(last.get("inputTokens")) is int:
            cached = last.get("cachedInputTokens") if type(last.get("cachedInputTokens")) is int else 0
            c.execute("UPDATE turn_usage_totals SET first_input_tokens=COALESCE(first_input_tokens,?),"
                      "first_cached_tokens=COALESCE(first_cached_tokens,?),"
                      "max_input_tokens=MAX(COALESCE(max_input_tokens,0),?) WHERE intent_id=?",
                      (last["inputTokens"], cached, last["inputTokens"], live["id"]))
        counts, before = token_counts(sample), token_counts(previous)
        if counts is not None and before is not None:
            grown: list[int] | None = [now - then for now, then in zip(counts, before)]
        elif previous is None:
            grown = counts  # The conversation's first sample: all of it.
        else:
            # A reconciled total without counts: only the request just made is known to be
            # this turn's.
            grown = token_counts(last)
        if grown is None or min(grown) < 0 or not any(grown):
            return  # A repeated sample adds nothing.
        c.execute("UPDATE turn_usage_totals SET input_tokens=COALESCE(input_tokens,0)+?,"
                  "cached_tokens=COALESCE(cached_tokens,0)+?,output_tokens=COALESCE(output_tokens,0)+?,"
                  "requests=COALESCE(requests,0)+1 WHERE intent_id=?", (*grown, live["id"]))

    async def rotation_memory_report(self) -> dict:
        """Whether each rotated conversation kept the old one's working memory, from the
        transcripts, the Lean service's checks and the review receipts (`rotation_review`)."""
        def read(c):
            rotations = [dict(row) for row in c.execute(
                "SELECT r.*,w.kind AS worker_kind FROM rotations r JOIN workers w ON w.id=r.worker_id "
                "WHERE r.state='completed' AND r.new_thread_id IS NOT NULL ORDER BY r.completed_at,r.id")]
            reviews = [dict(row) for row in c.execute(
                "SELECT a.producer,a.created_at AS submitted_at,p.verdict FROM peer_review_receipts p "
                "JOIN artifacts a ON a.id=p.candidate_id")]
            return rotations, reviews
        rotations, reviews = await self.db.read(read)
        store = self.state_dir / "lean-service" / "service.sqlite3"  # The Lean service's jobs.
        # Transcript and job-store reads stay off the coordination thread.
        return await asyncio.to_thread(memory_report, rotations, reviews, TranscriptStore(self.root).events_path,
                                       store, now=self.clock() / 1000)

    async def rotation_summary(self) -> dict:
        """Rotations by reason, mode and handoff outcome, and per backend and mode the uncached
        tokens of each turn's and each conversation's first request, the turns' largest and
        average request context, and the credit units (`CREDIT_WEIGHTS`) of workers' turns and of
        finished reviews: the data for comparing the modes and backends over time."""
        def read(c):
            rows = c.execute("SELECT r.reason,r.mode,r.state,r.automatic,w.backend,COUNT(*) AS count "
                             "FROM rotations r JOIN workers w ON w.id=r.worker_id "
                             "GROUP BY r.reason,r.mode,r.state,r.automatic,w.backend").fetchall()
            overrides = c.execute("SELECT COUNT(*) FROM workers WHERE rotation_mode IS NOT NULL").fetchone()[0]
            turns = c.execute(
                "SELECT w.backend,t.rotation_mode AS mode,COUNT(*) AS turns,"
                "AVG(u.first_input_tokens-u.first_cached_tokens) AS first_request_uncached,"
                "AVG(u.max_input_tokens) AS largest_context FROM turn_intents t "
                "JOIN turn_usage_totals u ON u.intent_id=t.id JOIN workers w ON w.id=t.worker_id "
                "WHERE u.first_input_tokens IS NOT NULL GROUP BY w.backend,t.rotation_mode").fetchall()
            starts = c.execute(
                "SELECT w.backend,t.rotation_mode AS mode,COUNT(*) AS conversations,"
                "AVG(u.first_input_tokens-u.first_cached_tokens) AS uncached_at_start FROM turn_intents t "
                "JOIN turn_usage_totals u ON u.intent_id=t.id JOIN workers w ON w.id=t.worker_id "
                "WHERE u.first_input_tokens IS NOT NULL "
                "AND t.thread_id IS NOT NULL AND t.created_at=(SELECT MIN(o.created_at) FROM turn_intents o "
                "WHERE o.worker_id=t.worker_id AND o.thread_id=t.thread_id) "
                "GROUP BY w.backend,t.rotation_mode").fetchall()
            priced = c.execute(
                "SELECT w.backend,t.rotation_mode AS mode,COUNT(*) AS turns,"
                "SUM(u.input_tokens)*1.0/SUM(u.requests) AS context,"
                "AVG(u.input_tokens-u.cached_tokens) AS uncached,AVG(u.cached_tokens) AS cached,"
                "AVG(u.output_tokens) AS output FROM turn_intents t JOIN turn_usage_totals u ON u.intent_id=t.id "
                "JOIN workers w ON w.id=t.worker_id WHERE u.requests>0 AND w.kind='ordinary' "
                "GROUP BY w.backend,t.rotation_mode").fetchall()
            # A review is the reviewer's every turn; it has finished once its verdict is recorded.
            reviews = c.execute(
                "SELECT w.backend,CASE WHEN COUNT(DISTINCT COALESCE(t.rotation_mode,''))=1 "
                "THEN MAX(t.rotation_mode) ELSE 'mixed' END AS mode,SUM(u.input_tokens-u.cached_tokens) AS uncached,"
                "SUM(u.cached_tokens) AS cached,SUM(u.output_tokens) AS output FROM workers w "
                "JOIN turn_intents t ON t.worker_id=w.id JOIN turn_usage_totals u ON u.intent_id=t.id "
                "WHERE w.kind='review' AND u.requests>0 "
                "AND (w.id IN (SELECT reviewer_id FROM review_acceptances) "
                "OR w.id IN (SELECT reviewer_id FROM peer_review_receipts)) GROUP BY w.id").fetchall()
            return {**self._rotation_settings(c), "worker_overrides": overrides,
                    "rotations": [dict(row) for row in rows], "turns": [dict(row) for row in turns],
                    "conversation_starts": [dict(row) for row in starts],
                    "credit_weights": CREDIT_WEIGHTS, "credits": [_priced_turns(row) for row in priced],
                    "reviews": _priced_reviews(reviews)}
        return await self.db.read(read)


def _priced_turns(row) -> dict:
    """A backend's turns in one mode: their average request's context, and their tokens and price
    per turn."""
    return {"backend": row["backend"], "mode": row["mode"], "turns": row["turns"],
            "request_context": round(row["context"]),
            "tokens_per_turn": {key: round(row[key]) for key in ("uncached", "cached", "output")},
            "credits_per_turn": credit_units(row["uncached"], row["cached"], row["output"], row["backend"])}


def _priced_reviews(rows) -> list[dict]:
    """Each backend's and mode's finished reviews and the median price of one."""
    groups: dict[tuple[str, str | None], list[dict[str, int]]] = {}
    for row in rows:
        groups.setdefault((row["backend"], row["mode"]), []).append(
            credit_units(row["uncached"], row["cached"], row["output"], row["backend"]))
    return [{"backend": backend, "mode": mode, "reviews": len(prices),
             "median_credits": {name: statistics.median(price[name] for price in prices)
                                for name in CREDIT_WEIGHTS[backend]}}
            for (backend, mode), prices in sorted(groups.items(), key=lambda item: (item[0][0], str(item[0][1])))]
