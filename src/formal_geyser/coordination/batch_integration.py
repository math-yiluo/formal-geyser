"""Persistent batch integration; independent acceptance remains per contribution.

The runtime calls reserve/prepare/freeze outside the database thread for Git work.
Publication is journaled before draining and reconciled against an exact Git head.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .errors import HubError
from .models import Actor, Role, TaskContract, ValidationReport
from .storage import canonical, collected_thread_call

logger = logging.getLogger(__name__)

# When a new batch starts, unless the operator chose otherwise (runtime.set_integrator_batch):
# once this many contributions wait, or once this long has passed since the previous batch of
# mathematics ended (before the first, since the oldest has waited).
BATCH_DELAY_MS = 13 * 60 * 1000
BATCH_THRESHOLD = 5
BATCH_DELAY_MAX_MINUTES = 7 * 24 * 60
BATCH_THRESHOLD_MAX = 1000
BATCH_STATES = ("collecting", "preparing", "prepared", "frozen", "validated", "validation_failed", "publishing", "published", "empty")
ROUND_STATES = ("frozen", "validated", "validation_failed", "published", "invalidated")
ITEM_STATES = ("included", "deferred", "published")
BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS integrator_control(
 id INTEGER PRIMARY KEY CHECK(id=1),worker_id TEXT REFERENCES workers(id),
 task_id TEXT REFERENCES tasks(id),last_finished_at INTEGER,enabled_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS integrator_batch_preferences(
 id INTEGER PRIMARY KEY CHECK(id=1),
 delay_minutes INTEGER CHECK(delay_minutes IS NULL OR delay_minutes BETWEEN 0 AND 10080),
 threshold INTEGER CHECK(threshold IS NULL OR threshold BETWEEN 1 AND 1000),updated_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_integrator ON workers(kind) WHERE kind='integrator';
CREATE TABLE IF NOT EXISTS integration_batches(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL REFERENCES workers(id),task_id TEXT NOT NULL REFERENCES tasks(id),
 state TEXT NOT NULL,created_at INTEGER NOT NULL,ended_at INTEGER,workspace_id TEXT,
 target_head TEXT,prepared_json TEXT,round_id TEXT,error TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_integration_batch ON integration_batches((1))
 WHERE ended_at IS NULL;
CREATE TABLE IF NOT EXISTS integration_batch_items(
 batch_id TEXT NOT NULL REFERENCES integration_batches(id),candidate_id TEXT NOT NULL REFERENCES artifacts(id),
 review_task_id TEXT NOT NULL REFERENCES tasks(id),reviewer_id TEXT NOT NULL REFERENCES workers(id),
 binding_json TEXT NOT NULL,accepted_at INTEGER NOT NULL,state TEXT NOT NULL,reason TEXT,
 PRIMARY KEY(batch_id,candidate_id));
CREATE TABLE IF NOT EXISTS integration_batch_rounds(
 id TEXT PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES integration_batches(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id),workspace_id TEXT NOT NULL,
 target_head TEXT NOT NULL,commit_sha TEXT NOT NULL,snapshot_sha TEXT NOT NULL,
 candidates_json TEXT NOT NULL,validation_json TEXT,state TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS integration_batch_projections(
 round_id TEXT NOT NULL REFERENCES integration_batch_rounds(id),
 candidate_id TEXT NOT NULL REFERENCES artifacts(id),integration_id TEXT NOT NULL REFERENCES review_integrations(id),
 PRIMARY KEY(round_id,candidate_id));
CREATE TABLE IF NOT EXISTS integration_deferred(
 candidate_id TEXT PRIMARY KEY REFERENCES artifacts(id),reason TEXT NOT NULL,created_at INTEGER NOT NULL);
"""


for _table, _states in (("integration_batches", BATCH_STATES),
                        ("integration_batch_rounds", ROUND_STATES),
                        ("integration_batch_items", ITEM_STATES)):
    _allowed_states = ",".join("'" + state + "'" for state in _states)
    for _operation in ("INSERT", "UPDATE"):
        BATCH_SCHEMA += f"""
CREATE TRIGGER IF NOT EXISTS {_table}_state_{_operation.lower()}
 BEFORE {_operation} ON {_table} WHEN NEW.state NOT IN ({_allowed_states})
 BEGIN SELECT RAISE(ABORT, 'Invalid {_table} state'); END;
"""




class BatchIntegration:
    _batch_admission_hint: tuple | None = None
    if TYPE_CHECKING:
        _workspace_snapshot: Any
        _wake_after_batch: Any
        _release_conversation: Any
        db: Any
        clock: Any
        config: Any
        blobs: Any
        workspace: Any
        _get: Any
        _active: Any
        _role: Any
        _event: Any
        _task: Any
        _artifact: Any
        _record_body: Any
        _candidate_binding: Any
        _review_basis: Any
        _basis_valid: Any
        _require_original_validation: Any
        _review_acceptance: Any
        _candidate_repository: Any
        _internal_task: Any
        _new_worker: Any
        _lifecycle: Any
        _member: Any
        _claim: Any
        _assignment_metadata: Any
        _scope_admits: Any
        _repository: Any
        _require_recorded_repository_head: Any
        _integration: Any
        _archive_publication: Any
        _promote_candidate: Any
        _set_member_work: Any
        _recompute_task: Any
        _notify: Any
        _wake: Any
        _refresh_waits: Any
        _can_review: Any
        _admission_fault: Any
        _dispatch: Any
        _conditions: Any
        content: Any
        _archive_ledger: Any
        _checked_record_body: Any

    def _batch_enabled(self, c):
        return c.execute("SELECT 1 FROM integrator_control WHERE id=1").fetchone() is not None

    def _enable_integrator(self, c):
        if not self.config.repository.enabled:
            return False
        if self._batch_enabled(c):
            return True
        c.execute(
            "INSERT OR IGNORE INTO integrator_control(id,enabled_at) VALUES(1,?)", (self.clock(),)
        )
        self._event(c, "integration.enabled", "singleton", {})
        return True

    def _batch_waiting(self, c):
        """Cheap scheduling hint; exact eligibility is checked only at admission."""
        return c.execute(
            "SELECT count(*) AS count,min(a.created_at) AS oldest,"
            "COALESCE(sum(CASE WHEN f.kind='note.wiki' THEN 1 ELSE 0 END),0) AS documents "
            "FROM review_acceptances a JOIN artifacts f ON f.id=a.candidate_id "
            "JOIN review_requests r ON r.task_id=a.review_task_id WHERE r.state='integrating' "
            "AND NOT EXISTS(SELECT 1 FROM integration_deferred d WHERE d.candidate_id=a.candidate_id) "
            "AND NOT EXISTS(SELECT 1 FROM integration_batch_items i JOIN integration_batches b ON b.id=i.batch_id "
            "WHERE i.candidate_id=a.candidate_id AND b.ended_at IS NULL AND i.state='included')"
        ).fetchone()

    def _batch_rule(self, c) -> dict:
        """When a new batch starts: once `threshold` contributions wait, or once `delay_minutes`
        have passed since the previous batch of mathematics ended (a batch of public notes does
        not count; before the first, since the oldest contribution has waited). The operator's
        choice applies from the next admission pass; a field left unset is the default."""
        row = c.execute("SELECT delay_minutes,threshold FROM integrator_batch_preferences WHERE id=1").fetchone()
        delay = row["delay_minutes"] if row is not None else None
        threshold = row["threshold"] if row is not None else None
        return {"delay_minutes": BATCH_DELAY_MS // 60_000 if delay is None else delay,
                "threshold": BATCH_THRESHOLD if threshold is None else threshold,
                "source": {"delay_minutes": "config" if delay is None else "operator",
                           "threshold": "config" if threshold is None else "operator"}}

    def _batch_rule_command(self, c, name, p, actor):
        """runtime.set_integrator_batch: `delay_minutes` (0 to a week) and `threshold` (1 to
        1000); null returns a field to its default, and a field left out keeps its value."""
        self._role(actor, Role.OPERATOR)
        row = c.execute("SELECT delay_minutes,threshold FROM integrator_batch_preferences WHERE id=1").fetchone()
        values = {"delay_minutes": row["delay_minutes"] if row else None, "threshold": row["threshold"] if row else None}
        for key, low, high, unit in (("delay_minutes", 0, BATCH_DELAY_MAX_MINUTES, "minutes"),
                                     ("threshold", 1, BATCH_THRESHOLD_MAX, "contributions")):
            if key not in p:
                continue
            value = p[key]
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or not low <= value <= high):
                raise HubError("INVALID_INPUT", f"Expected {key} from {low} to {high} {unit}, or null for the default", 422)
            values[key] = value
        c.execute("INSERT INTO integrator_batch_preferences(id,delay_minutes,threshold,updated_at) VALUES(1,?,?,?) "
                  "ON CONFLICT(id) DO UPDATE SET delay_minutes=excluded.delay_minutes,threshold=excluded.threshold,"
                  "updated_at=excluded.updated_at", (values["delay_minutes"], values["threshold"], self.clock()))
        self._event(c, "runtime.integrator_batch_selected", "runtime", values)
        return self._batch_rule(c)

    def _batch_admission_key(self, c, waiting):
        """A negative scheduling hint, never cached publication authority."""
        evidence = c.execute("SELECT COALESCE(max(sequence),0) FROM evidence").fetchone()[0]
        event = c.execute("SELECT COALESCE(max(sequence),0) FROM events WHERE "
                          "kind GLOB 'artifact.*' OR kind GLOB 'review.*' OR kind GLOB 'integration.*' "
                          "OR kind GLOB 'repository.*' OR kind GLOB 'validation.*' "
                          "OR kind IN ('task.revised','task.retired','task.completed')").fetchone()[0]
        return waiting["count"], waiting["oldest"], evidence, event

    def _batch_eligible(self, c, *, report_failures=False):
        result = []
        for row in c.execute(
            "SELECT a.* FROM review_acceptances a JOIN review_requests r ON r.task_id=a.review_task_id "
            "WHERE r.state='integrating' AND NOT EXISTS(SELECT 1 FROM integration_deferred d "
            "WHERE d.candidate_id=a.candidate_id) AND NOT EXISTS(SELECT 1 FROM integration_batch_items i "
            "JOIN integration_batches b ON b.id=i.batch_id WHERE i.candidate_id=a.candidate_id "
            "AND b.ended_at IS NULL AND i.state='included') ORDER BY a.created_at,a.review_task_id"
        ).fetchall():
            try:
                binding = self._candidate_binding(c, row["candidate_id"])
                if (
                    self._get(c, "artifacts", row["candidate_id"])["status"] != "staged"
                    or not self._basis_valid(c, binding)
                    or not self._can_review(c, row["review_task_id"], row["reviewer_id"])
                ):
                    continue
                self._require_original_validation(c, binding)
                result.append({**dict(row), "binding": binding,
                               "documentation": self._get(c, "artifacts", row["candidate_id"])["kind"] == "note.wiki"})
            except (HubError, ValueError, OSError) as error:
                if report_failures:
                    self._admission_fault(c, row["candidate_id"], row["review_task_id"], error)
                continue  # A concrete candidate blocker never halts other work.
        return result

    def _batch_assignment(self, c, run_id, scope):
        if not self._batch_enabled(c):
            if scope is not None or not self._enable_integrator(c):
                return None
        control = c.execute("SELECT * FROM integrator_control WHERE id=1").fetchone()
        batch = c.execute("SELECT * FROM integration_batches WHERE ended_at IS NULL").fetchone()
        created = batch is None
        if batch is None:
            if scope is not None and control["worker_id"] not in scope:
                # An individual resume creates no new work, except that a resumed Integrator
                # receives the next batch under the usual rules.
                return None
            waiting = self._batch_waiting(c)
            if not waiting["count"]:
                return None
            rule = self._batch_rule(c)
            delay, threshold = rule["delay_minutes"] * 60_000, rule["threshold"]
            origin = control["last_finished_at"] if control["last_finished_at"] is not None else waiting["oldest"]
            if not waiting["documents"] and waiting["count"] < threshold and self.clock() < origin + delay:
                return None
            # A changed rule is a new decision, whatever was decided under the old one.
            hint_key = (*self._batch_admission_key(c, waiting), delay, threshold)
            hint = self._batch_admission_hint
            if hint is not None and hint[0] == hint_key and self.clock() < hint[1]:
                return None
            eligible = self._batch_eligible(c, report_failures=True)
            if not eligible:
                self._batch_admission_hint = (hint_key, self.clock() + 30_000)
                return None
            # Public notes form a cheap standalone batch; never mix them with mathematics.
            documents = [item for item in eligible if item["documentation"]]
            eligible = documents or eligible
            # The cheap count is an upper bound. Preserve the exact threshold for other work.
            origin = control["last_finished_at"] if control["last_finished_at"] is not None else eligible[0]["created_at"]
            if not documents and len(eligible) < threshold and self.clock() < origin + delay:
                self._batch_admission_hint = (hint_key, min(self.clock() + 30_000, origin + delay))
                return None
            self._batch_admission_hint = None
            worker_id = control["worker_id"]
            if worker_id is None:
                worker_id = self._new_worker(c, kind="integrator")
                c.execute("UPDATE integrator_control SET worker_id=? WHERE id=1", (worker_id,))
            worker = self._get(c, "workers", worker_id)
            if (
                worker["active_attempt"]
                or worker["state"] == "recovery_required"
                or not self._scope_admits(c, run_id, scope, worker_id)
            ):
                return None
            task_id = control["task_id"]
            if task_id is None:
                root_id = c.execute(
                    "SELECT root_task_id FROM project_lifecycle WHERE id=1"
                ).fetchone()[0]
                root = self._task(c, root_id)
                payload = root["contract"].get("payload", {})
                environment = payload.get("environment_artifact")
                contract = TaskContract(
                    title="Integrate accepted contributions",
                    domain=self.config.domain,
                    kind="integration",
                    objective="Integrate the assigned accepted batch, preserve mathematical meaning, validate its complete final tree, and publish exact per-contribution evidence.",
                    acceptance_policy="lean.peer@1"
                    if self.config.domain == "lean"
                    else "general.peer@1",
                    inputs=[environment] if environment else [],
                    payload={"environment_artifact": environment} if environment else {},
                )
                task_id = self._internal_task(c, contract.model_dump(mode="json"), None, worker_id)
                self._lifecycle(c, task_id)
                c.execute(
                    "UPDATE task_lifecycle SET recruitment_decision='no' WHERE task_id=?",
                    (task_id,),
                )
                self._member(c, task_id, worker_id, executor=True, runnable=False)
                c.execute("UPDATE integrator_control SET task_id=? WHERE id=1", (task_id,))
            batch_id = uuid4().hex
            c.execute(
                "INSERT INTO integration_batches(id,worker_id,task_id,state,created_at) VALUES(?,?,?,'collecting',?)",
                (batch_id, worker_id, task_id, self.clock()),
            )
            for item in eligible:
                c.execute(
                    "INSERT INTO integration_batch_items VALUES(?,?,?,?,?,?,'included',NULL)",
                    (
                        batch_id,
                        item["candidate_id"],
                        item["review_task_id"],
                        item["reviewer_id"],
                        canonical(self._review_basis(item["binding"])),
                        item["created_at"],
                    ),
                )
            self._event(
                c,
                "integration.batch_created",
                batch_id,
                {"candidates": [x["candidate_id"] for x in eligible]},
                task_id,
                worker_id,
            )
            batch = c.execute(
                "SELECT * FROM integration_batches WHERE id=?", (batch_id,)
            ).fetchone()
        worker = self._get(c, "workers", batch["worker_id"])
        if (
            worker["active_attempt"]
            or worker["state"] == "recovery_required"
            or not self._scope_admits(c, run_id, scope, worker["id"])
        ):
            return None
        if worker["work_intention"] == "waiting" and not created:
            return None
        if c.execute(
            "SELECT 1 FROM interruption_checkpoints WHERE worker_id=? AND state='parked'",
            (worker["id"],),
        ).fetchone():
            return None
        if created:
            # Batches are independent: each starts in a new conversation.
            self._release_conversation(c, worker["id"], "new_batch")
        c.execute(
            "UPDATE workers SET state='idle',work_intention='working' WHERE id=?", (worker["id"],)
        )
        c.execute("UPDATE tasks SET state='ready' WHERE id=?", (batch["task_id"],))
        self._set_member_work(c, batch["task_id"], worker["id"], True, "integration_batch")
        return self._assignment_metadata(
            c, self._claim(c, batch["task_id"], 1, worker["id"], run_id)
        )

    def _batch(self, c, batch_id=None):
        row = c.execute(
            "SELECT * FROM integration_batches WHERE id=?"
            if batch_id
            else "SELECT * FROM integration_batches WHERE ended_at IS NULL",
            (batch_id,) if batch_id else (),
        ).fetchone()
        if row is None:
            raise HubError("NO_INTEGRATION_BATCH", "There is no selected integration batch", 404)
        result = dict(row)
        last_error = result.pop("error")
        if last_error:
            result["last_error"] = last_error
        result["items"] = [
            dict(x)
            for x in c.execute(
                "SELECT * FROM integration_batch_items WHERE batch_id=? ORDER BY accepted_at,candidate_id",
                (row["id"],),
            )
        ]
        selected = [item for item in result["items"] if item["state"] == "included"]
        result["content_kind"] = "public-notes" if selected and all(
            self._get(c, "artifacts", item["candidate_id"])["kind"] == "note.wiki"
            for item in selected) else "library"
        if row["workspace_id"]:
            result["workspace"] = str(
                self.workspace(row["worker_id"]) / ".taskhub-integration" / row["workspace_id"]
            )
        if row["round_id"]:
            result["round"] = dict(
                c.execute(
                    "SELECT * FROM integration_batch_rounds WHERE id=?", (row["round_id"],)
                ).fetchone()
            )
        return result

    def _active_batch(self, c, actor, batch_id=None):
        attempt, _ = self._active(c, actor)
        batch = self._batch(c, batch_id)
        if (
            batch["worker_id"] != attempt["worker_id"]
            or batch["task_id"] != attempt["task_id"]
            or self._get(c, "workers", attempt["worker_id"])["kind"] != "integrator"
            or attempt["execution_mode"] != "work"
            or batch["ended_at"] is not None
        ):
            raise HubError("INTEGRATOR_REQUIRED", "Only the active integrator owns this batch", 403)
        return attempt, batch

    def _batch_basis(self, c, batch):
        reconciling = bool(
            batch.get("round")
            and batch["state"] == "publishing"
            and self._repository().head() == batch["round"]["commit_sha"]
        )
        selected = [item for item in batch["items"] if item["state"] == "included"]
        if not selected:
            raise HubError("EMPTY_BATCH", "Defer the empty batch instead of publishing it")
        for item in selected:
            binding = self._candidate_binding(c, item["candidate_id"])
            acceptance = self._review_acceptance(c, item["review_task_id"])
            request = c.execute(
                "SELECT state FROM review_requests WHERE task_id=?", (item["review_task_id"],)
            ).fetchone()
            if (
                request["state"] != "integrating"
                or acceptance is None
                or acceptance["reviewer_id"] != item["reviewer_id"]
                or not self._can_review(c, item["review_task_id"], item["reviewer_id"])
                or not self._basis_valid(c, binding)
                or json.loads(item["binding_json"]) != self._review_basis(binding)
            ):
                raise HubError(
                    "BATCH_INPUT_CHANGED",
                    "An accepted candidate changed or was withdrawn; exclude it and prepare again",
                )
            self._record_body(
                c, self._get(c, "artifacts", item["candidate_id"]), allow_relocation=reconciling
            )
            self._require_original_validation(c, binding)
        return selected

    def _integrator_command(self, c, name, p, actor):
        operation = INTEGRATOR_OPERATIONS.get(name)
        if operation is None:
            raise HubError("UNKNOWN_COMMAND", "Unknown integrator operation", 422)
        handler, required = operation
        for field, kind in required.items():
            if not isinstance(p.get(field), kind) or (kind is str and not p[field].strip()):
                raise HubError("INVALID_INTEGRATOR_INPUT", f"{name} requires {field} ({kind.__name__})", 422)
        if "batch_id" in p and (not isinstance(p["batch_id"], str) or not p["batch_id"]):
            raise HubError("INVALID_INTEGRATOR_INPUT", "batch_id must be a nonempty string", 422)
        for field, required_keys in (("prepared", ("target_head", "candidate_commit")),
                                     ("frozen", ("target_head", "commit", "snapshot"))):
            if field in required and any(not isinstance(p[field].get(key), str) or not p[field][key]
                                         for key in required_keys):
                raise HubError("INVALID_INTEGRATOR_INPUT", f"{field} lacks its exact Git/snapshot identity", 422)
        attempt, batch = (None, None) if name in {"integrator.enable", "integrator.retry"} else self._active_batch(c, actor, p.get("batch_id"))
        return handler(self, c, p, actor, attempt, batch)

    def _integrator_enable(self, c, p, actor, attempt, batch):
        self._role(actor, Role.OPERATOR, Role.RUNNER)
        return {"enabled": self._enable_integrator(c)}

    def _integrator_retry(self, c, p, actor, attempt, batch):
        if actor.role != Role.OPERATOR:
            self._active(c, actor)
        if actor.role != Role.OPERATOR and self._get(c, "workers", actor.worker_id)["kind"] != "integrator":
            raise HubError(
                "INTEGRATOR_REQUIRED",
                "Only the integrator or operator may requeue a deferred contribution",
                403,
            )
        request = c.execute(
            "SELECT state FROM review_requests WHERE candidate_id=?", (p["candidate_id"],)
        ).fetchone()
        if not request or request["state"] != "integrating":
            raise HubError(
                "REVIEW_ACCEPTANCE_REQUIRED",
                "Only an accepted unpublished contribution may be retried",
            )
        c.execute("DELETE FROM integration_deferred WHERE candidate_id=?", (p["candidate_id"],))
        self._event(c, "integration.retry_requested", p["candidate_id"], {"reason": p["reason"]})
        return {"candidate_id": p["candidate_id"], "queued_for_next_batch": True}

    @staticmethod
    def _deferred_integrations(c):
        return [dict(row) for row in c.execute(
            "SELECT d.* FROM integration_deferred d JOIN review_requests r ON r.candidate_id=d.candidate_id "
            "WHERE r.state='integrating' ORDER BY d.created_at,d.candidate_id")]

    def _integrator_status(self, c, p, actor, attempt, batch):
        return {**batch, "deferred": self._deferred_integrations(c)}

    def _integrator_prepare_reserve(self, c, p, actor, attempt, batch):
        self._require_recorded_repository_head(c, self._repository())
        selected = self._batch_basis(c, batch)
        if batch["state"] == "publishing":
            raise HubError("PUBLICATION_PENDING", "Finish the submitted publication")
        if not batch["workspace_id"]:
            workspace_id = uuid4().hex
            c.execute(
                "UPDATE integration_batches SET workspace_id=?,state='preparing' WHERE id=?",
                (workspace_id, batch["id"]),
            )
        else:
            workspace_id = batch["workspace_id"]
        return {
            **self._batch(c, batch["id"]),
            "sources": [
                {
                    "candidate": {key: self._get(c, "artifacts", x["candidate_id"])[key]
                                  for key in ("id", "snapshot", "body_sha")},
                    "repository": self._candidate_repository(c, x["candidate_id"]),
                }
                for x in selected
            ],
        }

    def _integrator_prepare_store(self, c, p, actor, attempt, batch):
        self._role(actor, Role.RUNNER)
        self._batch_basis(c, batch)
        if p["workspace_id"] != batch["workspace_id"]:
            raise HubError(
                "BATCH_CHANGED", "The preparation belongs to an earlier batch workspace"
            )
        c.execute(
            "UPDATE integration_batches SET state='prepared',target_head=?,prepared_json=? WHERE id=?",
            (p["prepared"]["target_head"], canonical(p["prepared"]), batch["id"]),
        )
        return self._batch(c, batch["id"])

    def _integrator_freeze_store(self, c, p, actor, attempt, batch):
        self._role(actor, Role.RUNNER)
        selected = self._batch_basis(c, batch)
        frozen = p["frozen"]
        if (
            p["workspace_id"] != batch["workspace_id"]
            or frozen["target_head"] != batch["target_head"]
        ):
            raise HubError("BATCH_CHANGED", "The frozen round no longer belongs to this batch")
        round_id = uuid4().hex
        candidates = [x["candidate_id"] for x in selected]
        c.execute(
            "INSERT INTO integration_batch_rounds VALUES(?,?,?,?,?,?,?,?,NULL,'frozen',?)",
            (
                round_id,
                batch["id"],
                attempt["id"],
                batch["workspace_id"],
                batch["target_head"],
                frozen["commit"],
                frozen["snapshot"],
                canonical(candidates),
                self.clock(),
            ),
        )
        # Each artifact retains the established immutable integration/archive shape.
        prepared = json.loads(batch["prepared_json"])
        for item in selected:
            iid = uuid4().hex
            c.execute(
                "INSERT INTO review_integrations(id,workspace_id,review_task_id,candidate_id,reviewer_id,prepare_attempt,"
                "workspace_relative_path,base_commit,target_head,candidate_commit,commit_sha,snapshot_sha,state,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'frozen',?)",
                (
                    iid,
                    batch["workspace_id"],
                    item["review_task_id"],
                    item["candidate_id"],
                    item["reviewer_id"],
                    attempt["id"],
                    ".taskhub-integration/" + batch["workspace_id"],
                    batch["target_head"],
                    batch["target_head"],
                    prepared["candidate_commit"],
                    frozen["commit"],
                    frozen["snapshot"],
                    self.clock(),
                ),
            )
            c.execute(
                "INSERT INTO integration_batch_projections VALUES(?,?,?)",
                (round_id, item["candidate_id"], iid),
            )
        c.execute(
            "UPDATE integration_batches SET state='frozen',round_id=? WHERE id=?",
            (round_id, batch["id"]),
        )
        self._event(
            c,
            "integration.batch_frozen",
            batch["id"],
            {"round_id": round_id, "snapshot": frozen["snapshot"]},
        )
        return self._batch(c, batch["id"])

    def _integrator_validation(self, c, p, actor, attempt, batch):
        self._role(actor, Role.RUNNER)
        self._batch_basis(c, batch)
        if batch["round_id"] != p["round_id"] or batch["state"] != "frozen":
            raise HubError(
                "BATCH_CHANGED", "Validation must name the current frozen batch round"
            )
        reports = p["reports"]
        expected = json.loads(batch["round"]["candidates_json"])
        if set(reports) != set(expected):
            raise HubError(
                "VALIDATION_COVERAGE", "Every selected candidate requires its own final report"
            )
        ok = True
        for candidate_id in expected:
            binding = self._candidate_binding(c, candidate_id)
            report = ValidationReport.model_validate(reports[candidate_id])
            if (
                report.policy != binding["policy"]
                or report.details.get("integration_snapshot") != batch["round"]["snapshot_sha"]
            ):
                raise HubError(
                    "INTEGRATION_BINDING_MISMATCH",
                    "A report does not bind the final snapshot and candidate policy",
                )
            ok &= report.verdict == "accepted" and not report.qualifications
            projection = c.execute(
                "SELECT integration_id FROM integration_batch_projections WHERE round_id=? AND candidate_id=?",
                (p["round_id"], candidate_id),
            ).fetchone()[0]
            c.execute(
                "UPDATE review_integrations SET validation_json=?,state=? WHERE id=?",
                (
                    canonical(report.model_dump(mode="json")),
                    "validated"
                    if report.verdict == "accepted" and not report.qualifications
                    else "validation_failed",
                    projection,
                ),
            )
        phase = "validated" if ok else "validation_failed"
        c.execute(
            "UPDATE integration_batch_rounds SET state=?,validation_json=? WHERE id=?",
            (phase, canonical(reports), p["round_id"]),
        )
        c.execute("UPDATE integration_batches SET state=? WHERE id=?", (phase, batch["id"]))
        return self._batch(c, batch["id"])

    def _integrator_defer(self, c, p, actor, attempt, batch):
        if batch["state"] == "publishing":
            raise HubError(
                "PUBLICATION_PENDING", "Reconcile publication before changing its batch"
            )
        reason = p["reason"]  # _integrator_command requires it, nonblank.
        item = next(
            (
                x
                for x in batch["items"]
                if x["candidate_id"] == p["candidate_id"] and x["state"] == "included"
            ),
            None,
        )
        if item is None:
            raise HubError("NOT_IN_BATCH", "Select an included candidate", 404)
        c.execute(
            "UPDATE integration_batch_items SET state='deferred',reason=? WHERE batch_id=? AND candidate_id=?",
            (reason, batch["id"], p["candidate_id"]),
        )
        c.execute(
            "INSERT OR REPLACE INTO integration_deferred VALUES(?,?,?)",
            (p["candidate_id"], reason, self.clock()),
        )
        c.execute(
            "UPDATE integration_batches SET state='collecting',workspace_id=NULL,target_head=NULL,prepared_json=NULL,round_id=NULL WHERE id=?",
            (batch["id"],),
        )
        self._notify(
            c,
            self._candidate_binding(c, p["candidate_id"])["task_id"],
            p["candidate_id"],
            reason,
        )
        self._event(
            c,
            "integration.deferred",
            p["candidate_id"],
            {"batch_id": batch["id"], "reason": reason},
        )
        return self._batch(c, batch["id"])

    def _integrator_publish(self, c, p, actor, attempt, batch):
        if batch["state"] != "validated" or p["round_id"] != batch["round_id"]:
            raise HubError(
                "MERGE_VALIDATION_REQUIRED",
                "Publish only the exact successfully validated batch round",
            )
        # Each original review is trusted for its fixed Library version; publication
        # needs the exact validated final tree, not a second semantic review.
        self._batch_basis(c, batch)
        # Durable intent precedes Git publication. Only normal process quiescence commits it.
        c.execute("UPDATE integration_batches SET state='publishing' WHERE id=?", (batch["id"],))
        c.execute(
            "UPDATE attempts SET state='draining',submission_json=? WHERE id=?",
            (canonical({"integration_batch": batch["id"]}), attempt["id"]),
        )
        c.execute("UPDATE workers SET state='draining' WHERE id=?", (attempt["worker_id"],))
        return {
            "runtime_directive": "drain_for_review",
            "batch_id": batch["id"],
            "round_id": batch["round_id"],
        }

    def _integrator_finish_empty(self, c, p, actor, attempt, batch):
        if any(x["state"] == "included" for x in batch["items"]):
            raise HubError("BATCH_NOT_EMPTY", "Publish or defer remaining contributions first")
        c.execute(
            "UPDATE attempts SET state='draining',submission_json=? WHERE id=?",
            (canonical({"integration_batch": batch["id"]}), attempt["id"]),
        )
        c.execute("UPDATE integration_batches SET state='empty' WHERE id=?", (batch["id"],))
        c.execute("UPDATE workers SET state='draining' WHERE id=?", (attempt["worker_id"],))
        return {"runtime_directive": "drain_for_review", "batch_id": batch["id"]}

    def _batch_source_tree(self, plan):
        repository = self._repository()
        target = repository.head()
        original = {
            x["path"]: x["sha256"]
            for x in self.blobs.get_json(repository.snapshot(target, self.blobs))["files"]
        }
        files = dict(original)
        conflicts = []
        for source in plan["sources"]:
            artifact, binding = source["candidate"], source["repository"]
            baseline = {
                x["path"]: x["sha256"]
                for x in self.blobs.get_json(
                    repository.snapshot(binding["base_commit"], self.blobs)
                )["files"]
            }
            incoming = {
                x["path"]: x["sha256"] for x in self.blobs.get_json(artifact["snapshot"])["files"]
            }
            for path in binding["paths"]:
                before, current, new = baseline.get(path), files.get(path), incoming.get(path)
                if current == new or new == before:
                    continue
                if current == before:
                    if new is None:
                        files.pop(path, None)
                    else:
                        files[path] = new
                else:
                    ours = self.blobs.get(current).decode("utf-8") if current else ""
                    theirs = self.blobs.get(new).decode("utf-8") if new else ""
                    body = f"<<<<<<< Batch before {artifact['id']}\n{ours}\n=======\n{theirs}\n>>>>>>> Accepted {artifact['id']}\n"
                    files[path] = self.blobs.put(body.encode())
                    conflicts.append({"path": path, "candidate_id": artifact["id"]})
        snapshot = self.blobs.put_json(
            {"files": [{"path": p, "sha256": s} for p, s in sorted(files.items())]}
        )
        changed = sorted(p for p in set(original) | set(files) if original.get(p) != files.get(p))
        return snapshot, target, changed, conflicts

    async def resolve_integration_batch(self, actor, paths):
        _, batch = await self.db.call(lambda c: self._active_batch(c, actor))
        if not batch.get("workspace"):
            raise HubError("BATCH_NOT_PREPARED", "Prepare the batch first", 422)
        return await asyncio.to_thread(self._repository().resolve, Path(batch["workspace"]), paths)

    async def freeze_integration_batch(self, actor):
        _, batch = await self.db.call(lambda c: self._active_batch(c, actor))
        if not batch.get("workspace"):
            raise HubError("BATCH_NOT_PREPARED", "Prepare the batch before validating it", 422)
        return await asyncio.to_thread(self._repository().finalize, Path(batch["workspace"]), self.blobs)

    async def batch_items_for_validation(self, actor, round_id):
        def read(c):
            _, batch = self._active_batch(c, actor)
            if batch["round_id"] != round_id or batch["state"] != "frozen":
                raise HubError("BATCH_CHANGED", "Validation belongs to a different round")
            items = []
            for item in batch["items"]:
                if item["state"] != "included":
                    continue
                artifact = self._artifact(c, item["candidate_id"])
                task = self._task(c, artifact["task_id"], artifact["task_revision"])
                task = {**task, "contract": {**task["contract"], "acceptance_policy": artifact["publication_policy"]}}
                items.append({"task": task, "artifact": {**artifact, "integration_base_commit": batch["target_head"]},
                              "selected_paths": self._candidate_repository(c, artifact["id"])["paths"]})
            return items
        return await self.db.call(read)

    async def batch_contributions(self, actor):
        await self._check_batch_files(actor.worker_id)

        def read(c):
            _, batch = self._active_batch(c, actor)
            result = []
            for item in batch["items"]:
                artifact = self._artifact(c, item["candidate_id"])
                host = self._task(c, artifact["task_id"], artifact["task_revision"])
                result.append({"item": item, "artifact": artifact, "host": host,
                               "review": self._task(c, item["review_task_id"]),
                               "conditions": self._conditions(c, host),
                               "acceptance": self._review_acceptance(c, item["review_task_id"])})
            return result
        return await self.db.call(read)

    async def _check_batch_files(self, worker_id):
        """Check the task and archive files of an open batch before the coordination call that
        builds its view. That call repeats every check and reuses one only while its files are
        unchanged, so a first read after a restart no longer holds the coordination thread."""
        def plan(c):
            batch = c.execute("SELECT id,task_id FROM integration_batches WHERE worker_id=? AND ended_at IS NULL",
                              (worker_id,)).fetchone()
            if batch is None:
                return [], []
            records = []
            tasks: set[tuple[str, int | None]] = {(batch["task_id"], None)}  # None: the current revision.
            for item in c.execute("SELECT candidate_id,review_task_id FROM integration_batch_items WHERE batch_id=?",
                                  (batch["id"],)).fetchall():
                artifact = c.execute("SELECT id,kind,body_sha,task_id,task_revision FROM artifacts WHERE id=?",
                                     (item["candidate_id"],)).fetchone()
                if artifact is None:
                    continue
                native = c.execute("SELECT * FROM native_record_files WHERE record_id=?",
                                   (artifact["id"],)).fetchone()
                if native is not None:
                    records.append((dict(artifact), dict(native), *self._archive_ledger(c, artifact)))
                tasks |= {(artifact["task_id"], artifact["task_revision"]), (artifact["task_id"], 1),
                          (item["review_task_id"], None)}
            files = [tuple(row) for task_id, revision in tasks for row in c.execute(
                "SELECT n.task_id,n.revision,n.contract_sha256 FROM native_task_files n JOIN tasks t ON t.id=n.task_id "
                "WHERE n.task_id=? AND n.revision=COALESCE(?,t.current_revision)", (task_id, revision))]
            return records, files

        def check(records, files):
            for task_id, revision, contract_sha256 in files:
                self.content.read_task(task_id, revision, contract_sha256)
            for record, native, pinned, relocating in records:
                if pinned is not None:  # A check of an unpinned archive is never reused.
                    self._checked_record_body(record, native, pinned, relocating, None)
        try:
            records, files = await self.db.read(plan)
            await asyncio.to_thread(check, records, files)
        except (HubError, OSError, ValueError, KeyError, TypeError) as error:
            # The coordination call repeats every check and reports any real failure.
            logger.warning("Batch file checks before the coordination call did not complete: %s", error)

    async def prune_integration_workspaces(self, actor):
        def plan(c):
            self._active_batch(c, actor)
            latest = c.execute("SELECT id FROM integration_batches WHERE ended_at IS NOT NULL "
                               "ORDER BY ended_at DESC,created_at DESC,id DESC LIMIT 1").fetchone()
            if latest is None:
                return []
            return [dict(row) for row in c.execute(
                "SELECT DISTINCT r.workspace_id,b.worker_id,b.id AS batch_id FROM integration_batch_rounds r "
                "JOIN integration_batches b ON b.id=r.batch_id WHERE b.ended_at IS NOT NULL AND b.id<>?",
                (latest[0],))]
        candidates = await self.db.call(plan)
        results = []
        for item in candidates:
            workspace = self.workspace(item["worker_id"]) / ".taskhub-integration" / item["workspace_id"]
            try:
                result = await asyncio.to_thread(self._repository().prune_integration_workspace, workspace, self.blobs)
            except (OSError, ValueError) as error:
                result = {"workspace": str(workspace), "state": "retained_error", "reason": str(error), "bytes": 0}
            if result["state"] != "already_absent":
                results.append({**result, "batch_id": item["batch_id"]})
        report = {"checked": len(candidates), "reclaimed_bytes": sum(r["bytes"] for r in results), "workspaces": results}
        if results:
            await self.db.call(lambda c: self._event(c, "integration.workspace_retention", actor.worker_id, report), write=True)
        return report

    async def prepare_batch_publication(self, command):
        """Check the Git publication that a quiet Integrator will make, before the transaction
        that records it; that transaction then only moves the branch (see repository.publish)."""
        if command.name != "runtime.quiescent" or not self.config.repository.enabled:
            return
        worker_id = command.payload.get("worker_id")

        def target(c):
            row = c.execute("SELECT a.submission_json FROM workers w JOIN attempts a ON a.id=w.active_attempt "
                            "WHERE w.id=?", (worker_id,)).fetchone()
            submission = json.loads(row[0]) if row and row[0] else {}
            batch_id = submission.get("integration_batch") if isinstance(submission, dict) else None
            if not batch_id:
                return None
            batch = c.execute("SELECT b.state,r.state,r.commit_sha,r.target_head FROM integration_batches b "
                              "JOIN integration_batch_rounds r ON r.id=b.round_id WHERE b.id=?",
                              (batch_id,)).fetchone()
            if batch is None or tuple(batch[:2]) != ("publishing", "validated"):
                return None
            return batch[2], batch[3]
        try:
            publication = await self.db.read(target)
            if publication is not None:
                await collected_thread_call(self._repository().verify_publication, *publication)
        except (HubError, OSError, ValueError, KeyError, TypeError) as error:
            # The transaction repeats every check and reports any real failure.
            logger.warning("Publication checks before quiescence did not complete: %s", error)

    async def prepare_integration_batch(self, actor, command):
        plan = await command("integrator.prepare.reserve", {}, actor=actor, suffix=":reserve")
        retention = await self.prune_integration_workspaces(actor)
        snapshot, target, paths, conflicts = await asyncio.to_thread(self._batch_source_tree, plan)
        prepared = await asyncio.to_thread(
            self._repository().prepare, self.blobs, snapshot, target, Path(plan["workspace"]), paths
        )
        prepared["candidate_conflicts"] = conflicts
        prepared["workspace_retention"] = retention
        bound = Actor(
            role=Role.RUNNER,
            worker_id=actor.worker_id,
            attempt_id=actor.attempt_id,
            fencing_token=actor.fencing_token,
        )
        return await command(
            "integrator.prepare.store",
            {"batch_id": plan["id"], "workspace_id": plan["workspace_id"], "prepared": prepared},
            actor=bound,
            suffix=":store",
        )

    def _recover_integration_batch(self, c, worker, attempt):
        submission = json.loads(attempt["submission_json"] or "{}")
        batch_id = submission.get("integration_batch")
        if batch_id is None:
            return None
        batch = self._batch(c, batch_id)
        snapshot = self._workspace_snapshot(worker["id"])
        c.execute("SAVEPOINT batch_recovery")
        try:
            c.execute("UPDATE attempts SET state='draining' WHERE id=?", (attempt["id"],))
            c.execute("UPDATE workers SET state='draining' WHERE id=?", (worker["id"],))
            result = self._dispatch(
                c,
                "runtime.quiescent",
                {"worker_id": worker["id"], "snapshot": snapshot},
                Actor(role=Role.RUNNER),
            )
        except HubError as error:
            c.execute("ROLLBACK TO batch_recovery")
            c.execute("RELEASE batch_recovery")
            if batch.get("round") and self._repository().head() == batch["round"]["commit_sha"]:
                # Git advanced: retain the exact intent for safe reconciliation.
                return None
            c.execute(
                "UPDATE integration_batches SET state='collecting',workspace_id=NULL,"
                "target_head=NULL,prepared_json=NULL,round_id=NULL,error=? WHERE id=?",
                (error.code + ": " + error.message, batch_id),
            )
            if batch["round_id"]:
                c.execute(
                    "UPDATE integration_batch_rounds SET state='invalidated' WHERE id=?",
                    (batch["round_id"],),
                )
            c.execute("UPDATE attempts SET submission_json=NULL WHERE id=?", (attempt["id"],))
            self._event(
                c,
                "integration.batch_reprepare_required",
                batch_id,
                {"reason": error.code},
                batch["task_id"],
                worker["id"],
            )
            return None
        else:
            c.execute("RELEASE batch_recovery")
            self._event(
                c,
                "integration.batch_reconciled",
                batch_id,
                {"attempt_id": attempt["id"]},
                batch["task_id"],
                worker["id"],
            )
            return result

    def _finish_integration_batch(self, c, attempt, batch_id):
        """Called only after the runtime has recorded process quiescence."""
        batch = self._batch(c, batch_id)
        if batch["worker_id"] != attempt["worker_id"]:
            raise HubError("INTEGRATOR_REQUIRED", "The draining attempt does not own this batch")
        if batch["state"] == "publishing":
            selected = self._batch_basis(c, batch)
            round_record = batch["round"]
            if round_record["state"] != "validated":
                raise HubError(
                    "MERGE_VALIDATION_REQUIRED", "The batch no longer has a validated final round"
                )
            repository = self._repository()
            current = repository.head()
            if current not in {round_record["target_head"], round_record["commit_sha"]}:
                raise HubError(
                    "REPOSITORY_HEAD_CHANGED",
                    "The Library moved; preserve this batch and prepare a new final round",
                )
            for item in selected:
                projection = c.execute(
                    "SELECT integration_id FROM integration_batch_projections WHERE round_id=? AND candidate_id=?",
                    (batch["round_id"], item["candidate_id"]),
                ).fetchone()[0]
                if current == round_record["commit_sha"]:
                    c.execute(
                        "UPDATE review_integrations SET state='reconciling' WHERE id=? AND state='validated'",
                        (projection,),
                    )
                record = self._integration(c, projection)
                if (
                    record["state"] not in {"validated", "reconciling"}
                    or record["validation"]["verdict"] != "accepted"
                    or record["validation"]["qualifications"]
                ):
                    raise HubError(
                        "MERGE_VALIDATION_REQUIRED",
                        "Every batch member needs successful exact final validation",
                    )
            # This one expected-head update publishes the entire fixed batch.
            # Its durable publishing intent permits exact crash reconciliation.
            repository.publish(round_record["commit_sha"], round_record["target_head"])
            for item in selected:
                candidate_id, review_id = item["candidate_id"], item["review_task_id"]
                projection = c.execute(
                    "SELECT integration_id FROM integration_batch_projections WHERE round_id=? AND candidate_id=?",
                    (batch["round_id"], candidate_id),
                ).fetchone()[0]
                accepted = self._review_acceptance(c, review_id)
                report = {
                    **accepted["report"],
                    "integration_id": projection,
                    "batch_id": batch["id"],
                    "integrator_id": attempt["worker_id"],
                    "verdict": "approve",
                }
                c.execute(
                    "INSERT OR IGNORE INTO repository_publications VALUES(?,?,?,?,?,?,?,?)",
                    (
                        projection,
                        candidate_id,
                        item["reviewer_id"],
                        round_record["commit_sha"],
                        round_record["snapshot_sha"],
                        round_record["target_head"],
                        canonical(report),
                        self.clock(),
                    ),
                )
                c.execute(
                    "UPDATE review_integrations SET state='published',publication_attempt=?,published_at=? WHERE id=?",
                    (attempt["id"], self.clock(), projection),
                )
                publication = c.execute(
                    "SELECT * FROM repository_publications WHERE integration_id=?", (projection,)
                ).fetchone()
                self._archive_publication(c, candidate_id, publication)
                binding = self._candidate_binding(c, candidate_id)
                receipt_id = uuid4().hex
                c.execute(
                    "INSERT OR IGNORE INTO peer_review_receipts VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        receipt_id,
                        review_id,
                        candidate_id,
                        item["reviewer_id"],
                        item["binding_json"],
                        "approve",
                        accepted["report"]["rationale"],
                        canonical(accepted["report"]["evidence"]),
                        "[]",
                        self.clock(),
                    ),
                )
                c.execute(
                    "UPDATE review_requests SET state='resolved' WHERE task_id=?", (review_id,)
                )
                c.execute(
                    "UPDATE tasks SET state='done',active_attempt=NULL WHERE id=?", (review_id,)
                )
                c.execute(
                    "UPDATE task_members SET runnable=0,work_reason='batch_published' WHERE task_id=?",
                    (review_id,),
                )
                # An in-flight answer may finish; do not kill it to retire a reviewer.
                c.execute(
                    "UPDATE workers SET state='retired',work_intention='retired' WHERE id=? AND active_attempt IS NULL",
                    (item["reviewer_id"],),
                )
                self._event(
                    c,
                    "review.submitted",
                    receipt_id,
                    {
                        "candidate_id": candidate_id,
                        "verdict": "approve",
                        "basis_valid": True,
                        "report_material_sha256": self.blobs.put_json(report),
                    },
                    binding["task_id"],
                    item["reviewer_id"],
                )
                self._event(
                    c,
                    "repository.published",
                    projection,
                    {
                        "candidate_id": candidate_id,
                        "commit": round_record["commit_sha"],
                        "snapshot": round_record["snapshot_sha"],
                        "batch_id": batch["id"],
                        "integrator_id": attempt["worker_id"],
                    },
                    binding["task_id"],
                )
                self._promote_candidate(c, candidate_id)
                self._set_member_work(
                    c, binding["task_id"], binding["author_id"], True, "peer_review_received"
                )
                c.execute(
                    "UPDATE continuations SET state='reconsider',wake_reason='peer_review_received' WHERE task_id=? "
                    "AND preferred_worker=? AND state='waiting'",
                    (binding["task_id"], binding["author_id"]),
                )
                self._notify(
                    c,
                    binding["task_id"],
                    candidate_id,
                    "Accepted contribution was published in an integrated batch.",
                )
                self._recompute_task(c, binding["task_id"])
                c.execute(
                    "UPDATE integration_batch_items SET state='published' WHERE batch_id=? AND candidate_id=?",
                    (batch["id"], candidate_id),
                )
            c.execute(
                "UPDATE integration_batch_rounds SET state='published' WHERE id=?",
                (batch["round_id"],),
            )
            state = "published"
        elif batch["state"] == "empty":
            state = "empty"
        else:
            raise HubError(
                "BATCH_NOT_SUBMITTED",
                "The batch has no quiescent publication or empty-batch intent",
            )
        c.execute(
            "UPDATE integration_batches SET state=?,ended_at=? WHERE id=?",
            (state, self.clock(), batch["id"]),
        )
        kinds = {row[0] for row in c.execute("SELECT a.kind FROM integration_batch_items i JOIN artifacts a "
                                             "ON a.id=i.candidate_id WHERE i.batch_id=?", (batch["id"],))}
        if kinds != {"note.wiki"}:
            # The delay counts from the last batch of mathematics: public notes, which go at
            # once, would otherwise hold mathematics back each time (Linux pilot, 2026-10-22).
            c.execute("UPDATE integrator_control SET last_finished_at=? WHERE id=1", (self.clock(),))
        if state == "published":
            # The waits subscribed to the tasks it published Lean from are judged now.
            self._wake_after_batch(c, batch["id"], attempt["run_id"])
        c.execute("UPDATE attempts SET state='integrated' WHERE id=?", (attempt["id"],))
        c.execute(
            "UPDATE workers SET state='suspended',work_intention='waiting',active_attempt=NULL WHERE id=?",
            (attempt["worker_id"],),
        )
        self._set_member_work(
            c, batch["task_id"], attempt["worker_id"], False, "between_integration_batches"
        )
        c.execute(
            "UPDATE tasks SET state='waiting',active_attempt=NULL WHERE id=?", (batch["task_id"],)
        )
        self._event(
            c,
            "integration.batch_finished",
            batch["id"],
            {"state": state},
            batch["task_id"],
            attempt["worker_id"],
        )
        self._wake(c)
        self._refresh_waits(c)
        return {"directive": "batch_finished", "batch_id": batch["id"], "state": state}


# The integrator's operations: each one's handler and the fields it requires.
INTEGRATOR_OPERATIONS: dict[str, tuple[Callable[..., Any], dict[str, type]]] = {
    "integrator.enable": (BatchIntegration._integrator_enable, {}),
    "integrator.retry": (BatchIntegration._integrator_retry, {"candidate_id": str, "reason": str}),
    "integrator.status": (BatchIntegration._integrator_status, {}),
    "integrator.prepare.reserve": (BatchIntegration._integrator_prepare_reserve, {}),
    "integrator.prepare.store": (BatchIntegration._integrator_prepare_store, {"workspace_id": str, "prepared": dict}),
    "integrator.freeze.store": (BatchIntegration._integrator_freeze_store, {"workspace_id": str, "frozen": dict}),
    "integrator.validation": (BatchIntegration._integrator_validation, {"round_id": str, "reports": dict}),
    "integrator.defer": (BatchIntegration._integrator_defer, {"candidate_id": str, "reason": str}),
    "integrator.publish": (BatchIntegration._integrator_publish, {"round_id": str}),
    "integrator.finish_empty": (BatchIntegration._integrator_finish_empty, {}),
}
