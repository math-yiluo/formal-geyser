"""Durable native-call identity around mutable gateway request preparation."""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from .errors import HubError
from .models import Actor, Command, Role
from .storage import canonical, digest

GATEWAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS native_call_receipts(
 worker_id TEXT NOT NULL REFERENCES workers(id), thread_id TEXT NOT NULL,
 call_id TEXT NOT NULL, attempt_id TEXT NOT NULL REFERENCES attempts(id),
 fencing_token INTEGER NOT NULL, request_hash TEXT NOT NULL,
 command_json TEXT, core_response_json TEXT, response_json TEXT,
 validation_artifact_id TEXT, validation_state TEXT,
 validation_result_json TEXT, validation_report_json TEXT, validation_reason TEXT,
 validation_generation INTEGER NOT NULL DEFAULT 0,
 validation_after_sequence INTEGER NOT NULL DEFAULT 0, validation_retry_at INTEGER,
 preserve_reply INTEGER NOT NULL DEFAULT 0 CHECK(preserve_reply IN (0,1)),
 created_at INTEGER NOT NULL, PRIMARY KEY(worker_id,thread_id,call_id)
);
CREATE INDEX IF NOT EXISTS native_call_validation_idx ON native_call_receipts(validation_artifact_id);
CREATE INDEX IF NOT EXISTS native_validation_retry ON native_call_receipts(validation_retry_at)
 WHERE validation_state='completed' AND validation_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS native_validation_open ON native_call_receipts(validation_state)
 WHERE validation_state IN ('pending','queued');
CREATE INDEX IF NOT EXISTS native_validation_running ON native_call_receipts(validation_state)
 WHERE validation_state='running';
"""

RETRY_BASE_MS = 5 * 60 * 1000
RETRY_MAX_MS = 3 * 60 * 60 * 1000


def _reported_processes_quiet(value):
    if isinstance(value, dict):
        if value.get("tree_quiet") is False or value.get("process_quiet") is False:
            return False
        return all(_reported_processes_quiet(item) for item in value.values())
    if isinstance(value, list):
        return all(_reported_processes_quiet(item) for item in value)
    return True


class GatewayReceipts:
    if TYPE_CHECKING:
        _event: Any
        db: Any
        clock: Any
        _role: Any
        _active: Any
        _get: Any
        _run_controls: Any
        _admission_scope: Any
        _scope_admits: Any
        _candidate_binding: Any
        _review_basis: Any
        _basis_valid: Any
        _same_substance: Any
        _task: Any
        _wake: Any
        reports: Any
        config: Any

    def _native_call_actor(self, c, actor, thread_id):
        self._role(actor, Role.WORKER)
        attempt = self._get(c, "attempts", actor.attempt_id)
        if attempt["worker_id"] != actor.worker_id or attempt["fencing_token"] != actor.fencing_token:
            raise HubError("STALE_BINDING", "Native call does not belong to this fenced worker", 403)
        worker = self._get(c, "workers", actor.worker_id)
        if worker["thread_id"] is not None and worker["thread_id"] != thread_id:
            raise HubError("NATIVE_CALL_IDENTITY", "Native call belongs to another bound conversation", 403)

    def _native_call_row(self, c, call, actor):
        self._role(actor, Role.WORKER)
        row = c.execute("SELECT * FROM native_call_receipts WHERE worker_id=? AND thread_id=? AND call_id=?",
                        (actor.worker_id, call["thread_id"], call["call_id"])).fetchone()
        if row is None or row["request_hash"] != call["request_hash"]:
            raise HubError("IDEMPOTENCY_CONFLICT", "Native call identity was reused with different data")
        if row["attempt_id"] != actor.attempt_id or row["fencing_token"] != actor.fencing_token:
            raise HubError("IDEMPOTENCY_CONFLICT", "Native call has a different execution binding")
        self._native_call_actor(c, actor, call["thread_id"])
        result = dict(row)
        if result["core_response_json"]:
            saved = json.loads(result["core_response_json"])
            if "core_receipt" in saved:
                reference = saved["core_receipt"]
                receipt = c.execute("SELECT request_hash,response_json FROM receipts WHERE principal=? AND command=? AND key=?",
                                    (reference["principal"], reference["command"], reference["key"])).fetchone()
                if receipt is None or receipt["request_hash"] != reference["request_hash"]:
                    raise HubError("RECEIPT_MISSING", "The exact committed command receipt is unavailable")
                result["core_response_json"] = receipt["response_json"]
        return result

    async def begin_native_call(self, actor: Actor, thread_id: str, call_id: str, request: dict,
                                *, create: bool = True) -> dict | None:
        """Only the runner supplies native identities; model arguments cannot select them.

        Committed receipt reads survive the operation's own drain or an operator
        pause. Starting or resuming uncommitted work still needs current authority.
        """
        if not all(isinstance(value, str) and value for value in (thread_id, call_id)):
            raise HubError("NATIVE_CALL_IDENTITY", "Native thread and call identities are required", 422)
        request_hash = digest({"request": request, "attempt_id": actor.attempt_id,
                               "fencing_token": actor.fencing_token})
        call = {"thread_id": thread_id, "call_id": call_id, "request_hash": request_hash}

        def prepare(c):
            prior = c.execute("SELECT * FROM native_call_receipts WHERE worker_id=? AND thread_id=? AND call_id=?",
                              (actor.worker_id, thread_id, call_id)).fetchone()
            if prior is not None:
                row = self._native_call_row(c, call, actor)
                if row["core_response_json"] is None and row["response_json"] is None:
                    self._active(c, actor)
                return row
            if not create:
                return None
            self._native_call_actor(c, actor, thread_id)
            self._active(c, actor)
            c.execute("INSERT INTO native_call_receipts(worker_id,thread_id,call_id,attempt_id,fencing_token,request_hash,created_at) "
                      "VALUES(?,?,?,?,?,?,?)", (actor.worker_id, thread_id, call_id, actor.attempt_id,
                                                actor.fencing_token, request_hash, self.clock()))
            return self._native_call_row(c, call, actor)
        return await self.db.call(prepare, write=True)

    async def prepare_native_command(self, call: dict, actor: Actor, command: Command) -> Command:
        def prepare(c):
            row = self._native_call_row(c, call, actor)
            if row["command_json"] is not None:
                return Command.model_validate_json(row["command_json"])
            self._active(c, actor)
            c.execute("UPDATE native_call_receipts SET command_json=? WHERE worker_id=? AND thread_id=? AND call_id=?",
                      (canonical(command.model_dump(mode="json")), actor.worker_id, call["thread_id"], call["call_id"]))
            return command
        return await self.db.call(prepare, write=True)

    def _record_native_core(self, c, call: dict, actor: Actor, command: Command, response: Any) -> None:
        """Called inside the core command/receipt transaction, never afterward."""
        row = self._native_call_row(c, call, actor)
        if row["command_json"] != canonical(command.model_dump(mode="json")):
            raise HubError("IDEMPOTENCY_CONFLICT", "Native call changed its retained normalized command")
        body = canonical(response)
        if row["core_response_json"] is not None and row["core_response_json"] != body:
            raise HubError("IDEMPOTENCY_CONFLICT", "Native call committed receipt cannot change")
        principal = f"{actor.role}:{actor.worker_id or 'operator'}"
        receipt = c.execute("SELECT request_hash FROM receipts WHERE principal=? AND command=? AND key=?",
                            (principal, command.name, command.key)).fetchone()
        if receipt is None:
            raise HubError("RECEIPT_MISSING", "A native mutation must retain its core command receipt")
        body = canonical({"core_receipt": {"principal": principal, "command": command.name,
                                           "key": command.key, "request_hash": receipt["request_hash"]}})
        c.execute("UPDATE native_call_receipts SET core_response_json=?,preserve_reply=? "
                  "WHERE worker_id=? AND thread_id=? AND call_id=?",
                  (body, int(response.get("runtime_directive") == "drain_for_submission"),
                   actor.worker_id, call["thread_id"], call["call_id"]))
        if command.name == "artifact.stage" and response.get("artifact_id"):
            c.execute("UPDATE native_call_receipts SET validation_artifact_id=?,"
                      "validation_state=COALESCE(validation_state,'pending') "
                      "WHERE worker_id=? AND thread_id=? AND call_id=?",
                      (response["artifact_id"], actor.worker_id, call["thread_id"], call["call_id"]))

    def _record_contribution_validation(self, c, attempt_id, artifact_id, *, native_call=None):
        """Bind the post-drain candidate to its already committed native call."""
        rows = c.execute("SELECT * FROM native_call_receipts WHERE attempt_id=? "
                         "AND json_extract(command_json,'$.name')='artifact.submit' "
                         "AND core_response_json IS NOT NULL", (attempt_id,)).fetchall()
        if not rows:
            return  # Direct non-native callers retain their explicit validation path.
        if native_call is not None:
            rows = [row for row in rows if all(row[key] == native_call[key]
                    for key in ("worker_id", "thread_id", "call_id"))]
            if len(rows) != 1:
                raise HubError("NATIVE_CALL_IDENTITY", "The selected call is not this exact quiet contribution")
        if len(rows) != 1 or rows[0]["validation_artifact_id"] not in {None, artifact_id}:
            reason = "A draining contribution has ambiguous or conflicting native-call identity; validation is held"
            attempt = self._get(c, "attempts", attempt_id)
            self._event(c, "validation.identity_held", artifact_id,
                        {"reason": reason, "attempt_id": attempt_id},
                        worker_id=attempt["worker_id"], task_id=attempt["task_id"])
            return reason
        row = rows[0]
        c.execute("UPDATE native_call_receipts SET validation_artifact_id=?,"
                  "validation_state=COALESCE(validation_state,'pending') "
                  "WHERE worker_id=? AND thread_id=? AND call_id=?",
                  (artifact_id, row["worker_id"], row["thread_id"], row["call_id"]))

    def _recover_missing_contribution_validation(self, c, artifact_id, native_call=None):
        """An explicit operator recovery may reconstruct a missing phase from exact receipts."""
        artifact = self._get(c, "artifacts", artifact_id)
        quiet = c.execute("SELECT response_json FROM receipts WHERE command='runtime.quiescent' "
                          "AND json_extract(response_json,'$.directive') IN ('verify','validation_held') "
                          "AND json_extract(response_json,'$.artifact_id')=? "
                          "AND json_extract(response_json,'$.snapshot')=?",
                          (artifact_id, artifact["snapshot"])).fetchone()
        if quiet is None:
            raise HubError("VALIDATION_NOT_HELD", "No exact quiet contribution receipt exists for this candidate")
        quiet_attempt = json.loads(quiet[0]).get("attempt_id")
        if quiet_attempt is None:
            attempts = c.execute("SELECT id FROM attempts WHERE worker_id=? AND task_id=? AND task_revision=? "
                                 "AND snapshot=? AND state='submitted' AND ended_at IS NOT NULL",
                                 (artifact["producer"], artifact["task_id"], artifact["task_revision"], artifact["snapshot"])).fetchall()
            if len(attempts) != 1:
                raise HubError("VALIDATION_NOT_HELD", "The old quiet receipt does not identify one exact contribution attempt")
            quiet_attempt = attempts[0][0]
        # Only a stopped-service repair of pre-fix receipts uses this query.
        # Match durable execution identity, not a re-derived/normalized content body.
        matches = c.execute("SELECT n.* FROM native_call_receipts n JOIN attempts a ON a.id=n.attempt_id "
                            "WHERE n.worker_id=? AND a.task_id=? AND a.task_revision=? AND a.snapshot=? AND a.id=? "
                            "AND a.state='submitted' AND a.ended_at IS NOT NULL "
                            "AND n.validation_artifact_id IS NULL AND n.core_response_json IS NOT NULL "
                            "AND json_extract(n.command_json,'$.name')='artifact.submit'",
                            (artifact["producer"], artifact["task_id"], artifact["task_revision"],
                             artifact["snapshot"], quiet_attempt)).fetchall()
        if native_call is not None:
            if (not isinstance(native_call, dict) or set(native_call) != {"worker_id", "thread_id", "call_id"}
                    or not all(isinstance(value, str) and value for value in native_call.values())):
                raise HubError("NATIVE_CALL_IDENTITY", "Supply exact worker_id, thread_id and call_id", 422)
            matches = [row for row in matches if all(row[key] == value for key, value in native_call.items())]
        if len(matches) != 1:
            raise HubError("VALIDATION_NOT_HELD", "The missing validation must match one exact committed contribution")
        reason = self._record_contribution_validation(c, matches[0]["attempt_id"], artifact_id, native_call=native_call)
        if reason:
            raise HubError("VALIDATION_NOT_HELD", reason)
        c.execute("UPDATE native_call_receipts SET validation_state='uncertain' WHERE validation_artifact_id=?",
                  (artifact_id,))
        if native_call:
            self._event(c, "validation.identity_recovered", artifact_id, native_call)

    def _record_native_validation(self, c, command, actor, response, *, preserve_reply=False):
        """Store the actual result in the same transaction as artifact.accept."""
        self._role(actor, Role.OPERATOR, Role.VALIDATOR)
        for row in c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=? "
                             "AND validation_result_json IS NULL", (command.payload["artifact_id"],)).fetchall():
            binding = self._candidate_binding(c, row["validation_artifact_id"])
            if command.payload.get("policy") == binding["policy"]:
                uncertain = command.payload.get("verdict") == "unavailable" and not _reported_processes_quiet(command.payload)
                retry_at = (self.clock() + min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** min(row["validation_generation"], 6))
                            if command.payload.get("verdict") == "unavailable" and not uncertain else None)
                c.execute("UPDATE native_call_receipts SET validation_state=?,validation_result_json=?,"
                          "validation_reason=?,validation_retry_at=?,response_json=CASE WHEN ? OR preserve_reply=1 "
                          "OR validation_generation>0 "
                          "THEN response_json ELSE NULL END "
                          "WHERE validation_artifact_id=? AND validation_result_json IS NULL",
                          ("uncertain" if uncertain else "completed", canonical(response),
                           "Checker process cleanup is unconfirmed; confirm quiescence before recovery" if uncertain else None,
                           retry_at, int(preserve_reply), row["validation_artifact_id"]))

    def _retry_unavailable_validation(self, c, artifact_id, *, confirmed_stopped=False):
        row = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
        allowed = {"completed", "uncertain", "blocked"} if confirmed_stopped else {"completed"}
        if row is None or row["validation_state"] not in allowed:
            raise HubError("VALIDATION_NOT_RETRYABLE", "Only a completed unavailable local check can be retried")
        if (not confirmed_stopped and row["validation_report_json"] is not None
                and not _reported_processes_quiet(json.loads(row["validation_report_json"]))):
            c.execute("UPDATE native_call_receipts SET validation_state='uncertain',validation_retry_at=NULL,"
                      "validation_reason='Checker process cleanup is unconfirmed; confirm quiescence before recovery' "
                      "WHERE validation_artifact_id=?", (artifact_id,))
            return {"artifact_id": artifact_id, "validation_state": "uncertain"}
        binding = self._candidate_binding(c, artifact_id)
        evidence = c.execute("SELECT sequence,verdict FROM evidence WHERE artifact_id=? AND policy=? "
                             "ORDER BY sequence DESC LIMIT 1", (artifact_id, binding["policy"])).fetchone()
        if not evidence or evidence["verdict"] != "unavailable":
            c.execute("UPDATE native_call_receipts SET validation_retry_at=NULL WHERE validation_artifact_id=?", (artifact_id,))
            raise HubError("VALIDATION_NOT_RETRYABLE", "The latest exact policy result is not unavailable")
        if self._obsolete_native_validation(c, row, binding):
            return {"artifact_id": artifact_id, "validation_state": "obsolete"}
        if not self._basis_valid(c, binding):
            c.execute("UPDATE native_call_receipts SET validation_state='blocked',validation_retry_at=NULL,"
                      "validation_reason='Accepted input basis changed; explicit recovery is required' WHERE validation_artifact_id=?", (artifact_id,))
            return {"artifact_id": artifact_id, "validation_state": "blocked"}
        # Previous reports remain immutable in evidence and command receipts.
        # A new generation gets its own idempotency key; no model call is replayed.
        c.execute("UPDATE native_call_receipts SET validation_state='pending',validation_generation=validation_generation+1,"
                  "validation_after_sequence=?,validation_report_json=NULL,validation_result_json=NULL,"
                  "validation_reason=NULL,validation_retry_at=NULL WHERE validation_artifact_id=?",
                  (evidence["sequence"], artifact_id))
        self._event(c, "validation.retry_queued", artifact_id, {"previous_evidence_sequence": evidence["sequence"],
                    "generation": row["validation_generation"] + 1}, worker_id=row["worker_id"])
        return {"artifact_id": artifact_id, "validation_state": "pending"}

    def _queue_due_validations(self, c, run_id, scope):
        for row in c.execute("SELECT n.validation_artifact_id,n.worker_id FROM native_call_receipts n "
                             "JOIN workers w ON w.id=n.worker_id WHERE w.run_id=? AND n.validation_state='completed' "
                             "AND n.validation_retry_at<=?", (run_id, self.clock())).fetchall():
            if self._scope_admits(c, run_id, scope, row["worker_id"]):
                try:
                    self._retry_unavailable_validation(c, row["validation_artifact_id"])
                except (HubError, ValueError, OSError) as error:
                    if isinstance(error, HubError) and error.code == "VALIDATION_NOT_RETRYABLE":
                        continue
                    # Malformed one-candidate metadata must not stop other checks.
                    c.execute("UPDATE native_call_receipts SET validation_retry_at=? WHERE validation_artifact_id=?",
                              (self.clock() + RETRY_BASE_MS, row["validation_artifact_id"]))
                    self._event(c, "validation.retry_deferred", row["validation_artifact_id"], {"reason": str(error)})

    def _validation_queue_status(self, c, artifact_id):
        phase = c.execute("SELECT validation_state,validation_reason,validation_retry_at FROM native_call_receipts "
                          "WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
        evidence = c.execute("SELECT e.verdict,e.qualifications_json FROM evidence e JOIN candidate_bindings b ON b.artifact_id=e.artifact_id "
                             "WHERE e.artifact_id=? AND e.policy=b.policy ORDER BY e.sequence DESC LIMIT 1",
                             (artifact_id,)).fetchone()
        if evidence and evidence["verdict"] == "accepted" and not json.loads(evidence["qualifications_json"]):
            return {"validation_ready": True, "validation_reason": None, "validation_retry_at": None}
        identity_held = not phase and c.execute("SELECT 1 FROM events WHERE object_id=? AND kind='validation.identity_held' LIMIT 1",
                                              (artifact_id,)).fetchone()
        reason = ("Original validation identity needs operator reconciliation" if identity_held else
                  (phase["validation_reason"] if phase and phase["validation_reason"] else None) or
                  ("Original validation: " + evidence["verdict"] if evidence else
                   "Original validation: " + (phase["validation_state"] if phase else "not recorded")))
        return {"validation_ready": False, "validation_reason": reason,
                "validation_retry_at": phase["validation_retry_at"] if phase else None}

    def _validations_survive(self) -> bool:
        """Whether the domain plugin's validations run outside TaskHub and outlive it."""
        from .plugins import load_plugin

        return bool(getattr(load_plugin(self.config.domain), "validations_survive_interruption", False))

    def _recover_native_validations(self, c):
        c.execute("UPDATE native_call_receipts SET validation_state='pending' WHERE validation_state='running' "
                  "AND validation_report_json IS NOT NULL")
        if self._validations_survive():
            # Their service kept them: asking again attaches to the same validation.
            c.execute("UPDATE native_call_receipts SET validation_state='queued',"
                      "validation_reason='TaskHub restarted during this validation; it continues outside TaskHub' "
                      "WHERE validation_state='running'")
            # So did a wait that timed out against the busy service before such a timeout was
            # known to leave the check there (the Linux pilot's ten stuck candidates).
            c.execute("UPDATE native_call_receipts SET validation_state='queued',"
                      "validation_reason='The service did not answer in time; asking again attaches to the check' "
                      "WHERE validation_state='uncertain' AND validation_result_json IS NULL AND validation_reason IN "
                      "('ReadTimeout','WriteTimeout','PoolTimeout','TimeoutException') AND validation_artifact_id IN "
                      "(SELECT id FROM artifacts WHERE status='staged')")
            return
        for row in c.execute("SELECT DISTINCT worker_id FROM native_call_receipts WHERE validation_state='running'").fetchall():
            self._hold_native_validation(c, row["worker_id"], "Coordinator stopped during original validation")

    def _hold_native_validation(self, c, worker_id, reason):
        c.execute("UPDATE native_call_receipts SET validation_state='uncertain',validation_reason=? "
                  "WHERE worker_id=? AND validation_state='running'", (reason, worker_id))
        self._wake(c)  # A wait for its result ends: it needs recovery first.
        # An uncertain checker quarantines its candidate, not the author's
        # conversation. The worker may continue unrelated exploration.

    def _recover_candidate_validation(self, c, p, actor):
        self._role(actor, Role.OPERATOR)
        if p.get("process_confirmed_stopped") is not True or not isinstance(p.get("reason"), str) or not p["reason"].strip():
            raise HubError("QUIESCENCE_REQUIRED", "Confirm this checker's process stopped and record the evidence")
        row = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?",
                        (p["artifact_id"],)).fetchone()
        if row is not None and p.get("native_call") is not None:
            expected = {key: row[key] for key in ("worker_id", "thread_id", "call_id")}
            if p["native_call"] != expected:
                raise HubError("NATIVE_CALL_IDENTITY", "The selected call differs from the retained validation identity")
        if row is None:
            self._recover_missing_contribution_validation(c, p["artifact_id"], p.get("native_call"))
            row = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?",
                            (p["artifact_id"],)).fetchone()
        if row["validation_state"] != "uncertain":
            raise HubError("VALIDATION_NOT_HELD", "Only an uncertain original candidate check needs this recovery")
        if row["validation_result_json"] is not None:
            return self._retry_unavailable_validation(c, p["artifact_id"], confirmed_stopped=True)
        binding = self._candidate_binding(c, row["validation_artifact_id"])
        if not self._obsolete_native_validation(c, row, binding):
            c.execute("UPDATE native_call_receipts SET validation_state='pending',validation_reason=NULL "
                      "WHERE validation_artifact_id=?", (row["validation_artifact_id"],))
        self._event(c, "validation.recovered", row["validation_artifact_id"],
                    {"reason": p["reason"], "process_confirmed_stopped": True}, worker_id=row["worker_id"])
        return {"artifact_id": row["validation_artifact_id"], "validation_state": c.execute(
            "SELECT validation_state FROM native_call_receipts WHERE validation_artifact_id=?",
            (row["validation_artifact_id"],)).fetchone()[0]}

    def _reconcile_native_validations(self, c, worker_id):
        # Only the existing operator recovery path calls this, after confirming
        # all owned processes stopped. It grants no run-resumption authority.
        for row in c.execute("SELECT * FROM native_call_receipts WHERE worker_id=? "
                             "AND validation_state IN ('uncertain','blocked')", (worker_id,)).fetchall():
            binding = self._candidate_binding(c, row["validation_artifact_id"])
            if self._obsolete_native_validation(c, row, binding):
                continue
            if row["validation_state"] == "blocked" and not self._basis_valid(c, binding):
                continue  # An unrelated worker recovery must not restore this old basis.
            if row["validation_result_json"] is not None:
                try:
                    self._retry_unavailable_validation(c, row["validation_artifact_id"], confirmed_stopped=True)
                except HubError as error:
                    if error.code != "VALIDATION_NOT_RETRYABLE":
                        raise
                continue
            c.execute("UPDATE native_call_receipts SET validation_state='pending',validation_reason=NULL "
                      "WHERE validation_artifact_id=?", (row["validation_artifact_id"],))

    def _end_held_validation(self, c, artifact_id, reason):
        """A validation that could not finish (uncertain, or blocked on its basis) is obsolete
        once its candidate can no longer be accepted. In the pilot a withdrawn candidate's
        uncertain validation otherwise waited for an operator."""
        c.execute("UPDATE native_call_receipts SET validation_state='obsolete',validation_reason=? "
                  "WHERE validation_artifact_id=? AND validation_state IN ('uncertain','blocked')",
                  ("The candidate can no longer be accepted: " + reason, artifact_id))

    def _native_validation_held(self, c, worker_id):
        return c.execute("SELECT 1 FROM native_call_receipts WHERE worker_id=? AND validation_state IN ('uncertain','blocked')",
                         (worker_id,)).fetchone() is not None

    async def native_validation_queued(self, artifact_id, reason, details=None):
        """The plugin's service continues this validation outside TaskHub; the driver attaches to
        it again whatever its author is doing."""
        def queue(c):
            row = c.execute("SELECT worker_id FROM native_call_receipts WHERE validation_artifact_id=? "
                            "AND validation_state='running' AND validation_result_json IS NULL",
                            (artifact_id,)).fetchone()
            if row is None:
                return
            c.execute("UPDATE native_call_receipts SET validation_state='queued',validation_reason=? "
                      "WHERE validation_artifact_id=?", (reason, artifact_id))
            self._event(c, "validation.queued", artifact_id,
                        {key: value for key, value in (details or {}).items() if value is not None},
                        worker_id=row["worker_id"])
        await self.db.call(queue, write=True)

    async def native_validation_uncertain(self, artifact_id, reason):
        def hold(c):
            row = c.execute("SELECT worker_id FROM native_call_receipts WHERE validation_artifact_id=? "
                            "AND validation_state='running'", (artifact_id,)).fetchone()
            if row is not None:
                self._hold_native_validation(c, row["worker_id"], reason)
        await self.db.call(hold, write=True)

    def _obsolete_native_validation(self, c, row, binding):
        task, artifact = self._task(c, binding["task_id"]), self._get(c, "artifacts", binding["artifact_id"])
        # As for the candidate's review basis, a revision of only the task's title, context or
        # priority ends nothing.
        obsolete = (not self._same_substance(c, task, binding["task_revision"], binding["contract_hash"])
                    or task["state"] == "retired"
                    or artifact["status"] in {"withdrawn", "superseded"}
                    or artifact["body_sha"] != binding["body_sha"] or artifact["snapshot"] != binding["snapshot"]
                    or json.loads(artifact["input_refs_json"]) != binding["input_refs"])
        if obsolete:
            c.execute("UPDATE native_call_receipts SET validation_state='obsolete',validation_reason=? "
                      "WHERE validation_artifact_id=?", ("The staged candidate's exact source or task basis has ended",
                                                         row["validation_artifact_id"]))
        return obsolete

    async def retain_native_validation_report(self, artifact_id, report):
        from .storage import collected_thread_call
        report = await collected_thread_call(self.reports.report, report)
        def retain(c):
            row = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
            body = canonical(report)
            if row["validation_report_json"] is not None and row["validation_report_json"] != body:
                raise HubError("IDEMPOTENCY_CONFLICT", "The completed original validation report cannot change")
            c.execute("UPDATE native_call_receipts SET validation_report_json=?,validation_state='pending' "
                      "WHERE validation_artifact_id=? AND validation_result_json IS NULL", (body, artifact_id))
        await self.db.call(retain, write=True)

    async def block_native_validation(self, artifact_id, reason):
        def block(c):
            row = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
            if row["validation_result_json"] is None and not self._obsolete_native_validation(
                    c, row, self._candidate_binding(c, artifact_id)):
                c.execute("UPDATE native_call_receipts SET validation_state='blocked',validation_reason=? "
                          "WHERE validation_artifact_id=?", (reason, artifact_id))
                self._wake(c)
        await self.db.call(block, write=True)

    async def native_validation(self, artifact_id, *, start=False):
        """Runner-only access to the existing receipt's original-validation phase."""
        def read(c):
            found = c.execute("SELECT * FROM native_call_receipts WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
            if found is None:
                return None
            row = dict(found)
            if row["validation_result_json"] is not None:
                return row
            binding = self._candidate_binding(c, artifact_id)
            # Reconcile the same generation without rerunning it. Only an explicit
            # unavailable retry starts a new generation beyond this evidence floor.
            evidence = c.execute("SELECT * FROM evidence WHERE artifact_id=? AND policy=? ORDER BY sequence DESC LIMIT 1",
                                 (artifact_id, binding["policy"])).fetchone()
            if (evidence and evidence["sequence"] > row["validation_after_sequence"]
                    and json.loads(evidence["details_json"]).get("candidate_binding") == self._review_basis(binding)):
                saved = c.execute("SELECT response_json FROM receipts WHERE command='artifact.accept' "
                                  "AND json_extract(response_json,'$.id')=? ORDER BY rowid DESC LIMIT 1", (artifact_id,)).fetchone()
                if saved:
                    result = json.loads(saved[0])
                    if any(item["id"] == evidence["id"] for item in result.get("evidence", [])):
                        self._record_native_validation(c, Command(name="artifact.accept", key="retained",
                            payload={"artifact_id": artifact_id, "policy": binding["policy"], "verdict": evidence["verdict"]}),
                            Actor(role=Role.VALIDATOR), result, preserve_reply=True)
                        return {**row, "validation_state": "completed", "validation_result_json": saved[0]}
            if row["validation_state"] in {"pending", "queued", "blocked"} and self._obsolete_native_validation(c, row, binding):
                row = {**row, "validation_state": "obsolete", "validation_reason": "The exact candidate basis has ended"}
            if not start:
                return row
            if row["validation_state"] in {"obsolete", "blocked"}:
                return row
            if row["validation_state"] not in {"pending", "queued"}:
                raise HubError("VALIDATION_RECONCILIATION_REQUIRED", "Original validation is running or needs confirmed process recovery")
            attempt = self._get(c, "attempts", row["attempt_id"])
            worker = self._get(c, "workers", row["worker_id"])
            run_id = worker["run_id"] or attempt["run_id"]
            self._run_controls(c, run_id, row["worker_id"])
            if worker["state"] == "recovery_required":
                raise HubError("VALIDATION_RECONCILIATION_REQUIRED", "Recover the original execution before starting missing validation")
            if attempt["ended_at"] is None:
                self._active(c, Actor(role=Role.WORKER, worker_id=row["worker_id"],
                                      attempt_id=row["attempt_id"], fencing_token=row["fencing_token"]))
            if not self._basis_valid(c, binding):
                reason = "Original validation requires explicit recovery after its accepted input basis changes"
                c.execute("UPDATE native_call_receipts SET validation_state='blocked',validation_reason=? "
                          "WHERE validation_artifact_id=?", (reason, artifact_id))
                return {**row, "validation_state": "blocked", "validation_reason": reason}
            state = "pending" if row["validation_report_json"] is not None else "running"
            c.execute("UPDATE native_call_receipts SET validation_state=?,validation_reason=NULL "
                      "WHERE validation_artifact_id=?", (state, artifact_id))
            return {**row, "validation_state": state, "validation_run_id": run_id}
        return await self.db.call(read, write=True)

    @contextmanager
    def attached_validation(self, artifact_id: str) -> Iterator[None]:
        """While this TaskHub waits for a validation's result (runner._validate), whatever runner
        or worker turn waits: only such a wait leaves its validation 'running'."""
        attached = self.__dict__.setdefault("_attached_validations", Counter())
        attached[artifact_id] += 1
        try:
            yield
        finally:
            attached[artifact_id] -= 1
            if attached[artifact_id] <= 0:
                del attached[artifact_id]

    def _requeue_stranded_validations(self, c, run_id, attached) -> None:
        """A 'running' validation that no wait in this TaskHub holds any more goes back as a restart
        would put it (_recover_native_validations). Its wait ended before it could record why: in
        the Linux pilot, on a full disk, the report of a finished check could not be kept, nor the
        state changed, and two validations stayed 'running' with their results in the Lean service."""
        survive = None
        for row in c.execute("SELECT n.validation_artifact_id,n.worker_id,n.validation_report_json "
                             "FROM native_call_receipts n JOIN workers w ON w.id=n.worker_id "
                             "WHERE n.validation_state='running' AND w.run_id=?", (run_id,)).fetchall():
            if row["validation_artifact_id"] in attached:
                continue
            if row["validation_report_json"] is not None:
                state, reason = "pending", None
            else:
                if survive is None:
                    survive = self._validations_survive()
                state, reason = (("queued", "Its wait ended without a result; asking again attaches to the check")
                                 if survive else
                                 ("uncertain", "Its wait ended without a result; confirm its checker stopped"))
            c.execute("UPDATE native_call_receipts SET validation_state=?,validation_reason=? "
                      "WHERE validation_artifact_id=? AND validation_state='running'",
                      (state, reason, row["validation_artifact_id"]))
            self._event(c, "validation.stranded", row["validation_artifact_id"], {"state": state},
                        worker_id=row["worker_id"])
            if state == "uncertain":
                self._wake(c)

    async def pending_native_validations(self, run_id, *, worker_id=None):
        # Taken with the call, before any validation that starts later can reach the database.
        attached = frozenset(self.__dict__.get("_attached_validations", ()))

        def read(c):
            scope = self._admission_scope(c, run_id)
            self._requeue_stranded_validations(c, run_id, attached)
            self._queue_due_validations(c, run_id, scope)
            # A queued validation is attached to again whatever its author does, paused or
            # quarantined included; the author's own activation only finishes interrupted ones first.
            # The open states select through their partial index (native_validation_open): the
            # driver asks every couple of seconds, and scanning every receipt (19,229 in the Linux
            # pilot) took the coordination thread 0.16 s a call when it was otherwise idle.
            return [dict(row) for row in c.execute(
                "SELECT n.validation_artifact_id,n.validation_generation,n.validation_state,a.task_id,a.task_revision,"
                "a.id AS attempt_id,w.id AS worker_id FROM native_call_receipts n "
                "JOIN attempts a ON a.id=n.attempt_id JOIN workers w ON w.id=n.worker_id "
                "WHERE n.validation_state IN ('pending','queued') AND w.run_id=? AND ("
                "n.validation_state='pending' AND w.state<>'recovery_required' AND a.ended_at IS NOT NULL "
                "AND (? IS NULL AND (w.active_attempt IS NULL OR n.validation_generation>0) OR w.id=?) "
                "OR n.validation_state='queued' AND ? IS NULL)",
                (run_id, worker_id, worker_id, worker_id))
                if row["validation_state"] == "queued" or self._scope_admits(c, run_id, scope, row["worker_id"])]
        return await self.db.call(read, write=True)

    async def finish_native_call(self, call: dict, actor: Actor, response: dict) -> dict:
        def finish(c):
            row = self._native_call_row(c, call, actor)
            if row["validation_artifact_id"] and row["validation_state"] != "completed":
                return response  # A pending phase must not become an immutable partial reply.
            if row["response_json"] is not None:
                return json.loads(row["response_json"])
            # Core commits have their recovery response in the same transaction.
            # Rejections before a commit may be retained only by a current actor.
            if row["core_response_json"] is None:
                self._active(c, actor)
            c.execute("UPDATE native_call_receipts SET response_json=? WHERE worker_id=? AND thread_id=? AND call_id=?",
                      (canonical(response), actor.worker_id, call["thread_id"], call["call_id"]))
            return response
        return await self.db.call(finish, write=True)
