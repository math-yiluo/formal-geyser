"""Task admission and durable worker intentions, independent of native execution.

Only this layer reserves opportunities. Helpers choose; they never become
workers, acquire task authority, or lend their conversations to execution.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any

from .collaboration import Collaboration, contract_substance, new_id
from .commands import CommandTable
from .errors import HubError
from .models import BACKENDS, Actor, Checkpoint, Role, TaskContract, file_name_fields
from .session_settings import SESSION_BACKEND
from .stall_watchdog import watched
from .storage import canonical, digest
from .text_bounds import shorten
from .worker_names import kept_worker_number, next_worker_number, unkept_number, worker_identity
from .workspace_preparation import SnapshotRequired

logger = logging.getLogger(__name__)

# Admission errors that stop every item, rather than one task or worker.
ADMISSION_GLOBAL_ERRORS = frozenset({"RUN_PAUSED", "PROJECT_STOPPED", "LEAN_INITIALIZATION_REQUIRED"})
# Failures caused by one item's files or records (ContentError is a ValueError).
ITEM_FAULTS = (HubError, OSError, ValueError, LookupError, TypeError, AttributeError)

DECISION_TITLES = 10  # Previous task titles shown for each idle worker.
IMPORTED_TITLE_CHARACTERS = 300
DECISION_ATTEMPTS = 2  # Failed decisions for a task before a new worker takes it.
CONTRIBUTION_PLAN_CHARACTERS = 5000
TERMINAL = {"done", "retired"}
REVIEW_KINDS = {"review"}
STALE_ADMISSION = {"ALREADY_CLAIMED", "RECRUITMENT_CLOSED", "STALE_REVISION",
                   "REVIEWER_INELIGIBLE", "INPUTS_CHANGED", "PREREQUISITES_MISSING",
                   "NOT_FOUND", "MEMBERSHIP_ENDED", "LEAN_ENVIRONMENT_MISMATCH"}


# The lifecycle's commands, and its part of some core ones; it goes before the core table.
LIFECYCLE = CommandTable()


class Lifecycle(Collaboration):
    if TYPE_CHECKING:
        _ledger_task: Any
        _workspace_snapshot: Any
        prepare_workspace_snapshots: Any
        with_workspace_snapshots: Any
        _with_deferred_files: Any
        finish_deferred_files: Any
        _enable_integrator: Any
        _batch_assignment: Any
        _close_failed_native_turns: Any
        _close_stopped_history: Any
        _hold_worker_usage: Any
        _hold_helper_usage: Any
        _refresh_usage_uncertainty: Any
        _dispatch_core: Any
        _dispatch_holds: Any
        _cancel_rotations: Any
        _complete_rotation: Any
        _creation_backend: Any
        _worker_plan: Any
        _adopt_requested_backend: Any
        _admission_backend: Any
        _prepare_backend_switch: Any
        _session_limit: Any
        _saturated: set[str]
        _ramp_started: int | None
        _charge_backend: Any
        prepare_assignment_workspace: Any
        discard_workspace: Any
        _withdraw: Any
        _end_closed_subscriptions: Any
        _wake_again: Any
        _normalize_worker_fields: Any
        _reconcile_native_validations: Any
        _native_validation_held: Any
        _admission_scope: Any
        _assignment_scope: Any
        _paused_backends: Any
        _resumed_individually: Any
        _scope_admits: Any
        _worker_control: Any
        _controlled_workers: Any
        _effective_run_state: Any
        config: Any
        _claim: Any
        _requirement: Any
        _versions_named_by: Any
        _run_controls: Any
        _recompute_task: Any

    def _lifecycle(self, c, task_id, *, persist=True):
        row = c.execute("SELECT * FROM task_lifecycle WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            task = self._task(c, task_id)
            review = task["contract"]["kind"] in REVIEW_KINDS
            if not persist:
                # A newly created protocol task can be inspected before its
                # first claim. Opportunity discovery must not initialize execution
                # state as a side effect of a read-only dashboard or tool read.
                return {"task_id": task_id, "initial_worker_id": None,
                        "recruitment_decision": "not_applicable" if review else "pending",
                        "opportunity_state": "closed", "invitation": "",
                        "opportunity_opened_at": None, "first_turn_id": None,
                        "decision_helper_id": None, "suspended": 0, "closed_reason": None}
            c.execute("INSERT INTO task_lifecycle(task_id,recruitment_decision) VALUES(?,?)",
                      (task_id, "not_applicable" if review else "pending"))
            row = c.execute("SELECT * FROM task_lifecycle WHERE task_id=?", (task_id,)).fetchone()
        return dict(row)

    def _require_current_mutation(self, c, name, payload, actor):
        if (not name.startswith(("task.", "artifact.", "review.", "continuation.", "interruption."))
                or name in {"task.publish", "task.complete.inspect"}):
            return
        # Preserve operation-specific role rejection before looking up a target.
        # Participant/binding checks remain in each handler; quiet operator
        # maintenance must not acquire an artificial active-execution requirement.
        roles = {
            "artifact.accept": (Role.OPERATOR, Role.VALIDATOR),
            "artifact.record_replacement": (Role.OPERATOR,),
            "artifact.register": (Role.OPERATOR, Role.WORKER),
            "artifact.withdraw": (Role.OPERATOR, Role.VALIDATOR, Role.WORKER),
            "task.retire": (Role.OPERATOR,),
            "task.set_priority": (Role.OPERATOR,),
            "task.set_pinned": (Role.OPERATOR,),
            "task.claim": (Role.OPERATOR, Role.RUNNER),
            "task.join": (Role.OPERATOR,),
            "task.invite_back": (Role.OPERATOR, Role.WORKER),
            "task.subscribe": (Role.OPERATOR, Role.WORKER),
            "task.complete.feedback_unavailable": (Role.OPERATOR, Role.RUNNER),
            "task.complete.cancel": (Role.OPERATOR,),
            "review.reassign": (Role.OPERATOR, Role.RUNNER),
            "continuation.resume": (Role.RUNNER,),
            "interruption.resume": (Role.RUNNER,),
            "interruption.import": (Role.OPERATOR,),
        }.get(name)
        if roles is not None:
            self._role(actor, *roles)
        if name == "artifact.withdraw" and actor.role in {Role.OPERATOR, Role.VALIDATOR}:
            # Retraction changes current availability, never historical receipts
            # or the retired task's execution/completion protocol.
            return
        task_id = payload.get("task_id")
        if task_id is None and payload.get("artifact_id"):
            task_id = self._get(c, "artifacts", payload["artifact_id"])["task_id"]
        if task_id is None and payload.get("declaration_id"):
            task_id = self._completion_record(c, payload["declaration_id"])["task_id"]
        if task_id is None and actor.attempt_id:
            task_id = self._get(c, "attempts", actor.attempt_id)["task_id"]
        if task_id is None:
            return
        self._task(c, task_id)  # The target must exist and its native files must verify.

    def _completion_invalidations(self, c, task_id, revision):
        results = []
        for row in c.execute("SELECT * FROM completion_evidence_invalidations WHERE task_id=? AND task_revision=? "
                             "ORDER BY created_at,id", (task_id, revision)):
            result = dict(row)
            result["artifact_ids"] = json.loads(result.pop("artifact_ids_json"))
            result["observations"] = json.loads(result.pop("observations_json"))
            results.append(result)
        return results

    def _repair_conditions(self, c, task_id):
        row = c.execute("SELECT d.acceptance_json FROM completion_evidence_invalidations i "
                        "JOIN task_completion_declarations d ON d.id=i.declaration_id WHERE i.repair_task_id=?",
                        (task_id,)).fetchone()
        return json.loads(row["acceptance_json"]) if row else {}

    def _invalidate_completed_evidence(self, c, artifact_ids, reason, source_ref):
        """Keep fulfilled history and create one runnable repair obligation per closure."""
        lost = set(artifact_ids)
        if not lost:
            return
        producer_versions = {artifact_id: (row["task_id"], row["task_revision"])
                             for artifact_id in lost for row in c.execute(
                                 "SELECT task_id,task_revision FROM artifacts WHERE id=? AND task_id IS NOT NULL", (artifact_id,))}
        # A requirement naming an earlier version that a lost one corrects relied on the lost one.
        lost_versions = {named: artifact_id for artifact_id in lost for named in self._versions_named_by(c, artifact_id)}
        closures: dict[str, tuple[Any, dict[str, Any]]] = {}
        affected_closures: dict[str, set[str]] = {}
        dependents: dict[tuple[str, int], set[str]] = {}
        for declaration in c.execute("SELECT d.* FROM task_completion_declarations d JOIN tasks t ON t.id=d.task_id "
                                     "JOIN task_collaboration k ON k.task_id=d.task_id "
                                     "WHERE d.state='closed' AND t.state='done' AND k.mode='collaborative'").fetchall():
            # Every closure is read, for the dependencies among them: from the ledger alone.
            task = self._ledger_task(c, declaration["task_id"], declaration["task_revision"])
            evidence = json.loads(declaration["evidence_json"])
            references = {result["artifact_id"] for item in evidence.values() for result in item.get("results", [])}
            references.update(task["contract"]["inputs"])
            references.update(self._required_contributions(c, task["id"]).values())
            affected = lost.intersection(references)
            original = self._ledger_task(c, task["id"], self._named_revision(c, task["id"], 1))["contract"]
            for requirement in original["requirements"] + task["contract"]["requirements"]:
                requirement = self._named_requirement(c, requirement)
                if requirement.get("artifact_id") in lost_versions:
                    affected.add(lost_versions[requirement["artifact_id"]])
                required_version = (requirement.get("task_id"), requirement.get("task_revision"))
                if requirement.get("task_id") is not None:
                    dependents.setdefault((requirement["task_id"], requirement["task_revision"]), set()).add(declaration["id"])
                affected.update(artifact_id for artifact_id, version in producer_versions.items() if version == required_version)
            closures[declaration["id"]] = (declaration, task)
            if affected:
                affected_closures[declaration["id"]] = affected
        pending = list(affected_closures)
        while pending:
            declaration_id = pending.pop()
            declaration, task = closures[declaration_id]
            for dependent_id in dependents.get((task["id"], task["revision"]), set()):
                previous = affected_closures.setdefault(dependent_id, set())
                additions = affected_closures[declaration_id] - previous
                if additions:
                    previous.update(additions)
                    pending.append(dependent_id)
        for declaration, task in closures.values():
            affected = affected_closures.get(declaration["id"], set())
            if not affected:
                continue
            observation = {"artifact_ids": sorted(affected), "reason": reason, "source_ref": source_ref}
            records = c.execute("SELECT i.*,t.state AS repair_state FROM completion_evidence_invalidations i "
                                "JOIN tasks t ON t.id=i.repair_task_id WHERE i.declaration_id=? ORDER BY i.created_at,i.rowid",
                                (declaration["id"],)).fetchall()
            if any(observation in json.loads(record["observations_json"]) for record in records):
                continue
            prior = next((record for record in reversed(records) if record["repair_state"] not in TERMINAL), None)
            if prior:
                observations = json.loads(prior["observations_json"])
                observations.append(observation)
                c.execute("UPDATE completion_evidence_invalidations SET artifact_ids_json=?,observations_json=? WHERE id=?",
                          (canonical(sorted(set(json.loads(prior["artifact_ids_json"])) | affected)), canonical(observations), prior["id"]))
                repair_id, invalidation_id = prior["repair_task_id"], prior["id"]
            else:
                invalidation_id = new_id()
                history = {"task_id": task["id"], "task_revision": task["revision"],
                           "contract_hash": task["contract_hash"], "completion_declaration_id": declaration["id"],
                           "invalidation_id": invalidation_id}
                # Historical targets belong in the repair brief, not unavailable
                # admission inputs. Exact new proof targets can be bound by revision.
                repair = self._dispatch(c, "task.publish", {"deduplicate": False, "contract": {
                    "title": shorten("Restore current evidence: " + task["contract"]["title"], 300),
                    "objective": "Establish independently accepted and published replacement evidence for the original completed task's acceptance conditions. Identify exact replacement references and their correspondence; a repair plan alone does not restore invalid evidence.",
                    "domain": task["contract"]["domain"],
                    "acceptance_policy": task["contract"]["domain"] + ".peer@1",
                    "context": "System-generated repair obligation. Inspect the original immutable contract, acceptance conditions and exact completion evidence at these references. Completion must map every repair-original condition separately; these are derived from the closed declaration. The original task's current_evidence metadata lists every invalidated artifact and reason. Historical targets are repair work, not admission prerequisites:\n" + canonical(history),
                    "inputs": [], "requirements": [], "priority": task["contract"]["priority"],
                }}, Actor(role=Role.OPERATOR))
                repair_id = repair["id"]
                c.execute("INSERT INTO completion_evidence_invalidations VALUES(?,?,?,?,?,?,?,?)",
                          (invalidation_id, task["id"], task["revision"], declaration["id"], canonical(sorted(affected)),
                           canonical([observation]), repair_id, self.clock()))
            self._event(c, "task.completed_evidence_invalidated", invalidation_id,
                        {**observation, "repair_task_id": repair_id, "task_revision": task["revision"],
                         "declaration_id": declaration["id"], "repair_origin": "system_generated"}, task["id"])
            self._notify(c, repair_id, invalidation_id, "completed_task_evidence_requires_repair")

    def _collaboration(self, c, task_id):
        result = super()._collaboration(c, task_id)
        row = c.execute("SELECT * FROM task_lifecycle WHERE task_id=?", (task_id,)).fetchone()
        if row:
            result["recruitment"] = dict(row)
        if result["mode"] == "collaborative":
            result["members"] = [worker_identity(dict(item)) for item in c.execute(
                "SELECT m.worker_id,m.executor,m.runnable,m.work_reason,m.membership_state,m.contribution_plan,"
                "w.worker_number,w.reviewer_number,w.kind "
                "FROM task_members m JOIN workers w ON w.id=m.worker_id "
                "WHERE m.task_id=? ORDER BY m.joined_at,w.worker_number,w.reviewer_number", (task_id,))]
        return result

    def _worker_task(self, c, worker_id):
        """The worker's current task."""
        return self._get(c, "workers", worker_id)["current_task_id"]

    def _require_one_task(self, c, worker_id, task_id):
        worker = self._get(c, "workers", worker_id)
        if worker["active_attempt"]:
            attempt = self._get(c, "attempts", worker["active_attempt"])
            if attempt["task_id"] != task_id:
                raise HubError("ALREADY_CLAIMED", "A worker executes at most one task at a time")
        return task_id

    def _member(self, c, task_id, worker_id, *, executor=False, runnable=False, reason=None):
        super()._member(c, task_id, worker_id, executor=executor, runnable=runnable, reason=reason)

    def _independent(self, c, binding, worker_id):
        authors = {binding["author_id"], *binding["coauthors"]}
        row = c.execute("SELECT worker_ids_json FROM candidate_provenance WHERE artifact_id=?",
                        (binding["artifact_id"],)).fetchone()
        if row:
            authors.update(json.loads(row["worker_ids_json"]))
        return worker_id not in authors

    def _can_review(self, c, review_id, worker_id):
        request = c.execute("SELECT * FROM review_requests WHERE task_id=?", (review_id,)).fetchone()
        if not request:
            return False
        binding = self._candidate_binding(c, request["candidate_id"])
        acceptance = self._review_acceptance(c, review_id)
        return (request["state"] in {"pending", "integrating"} and self._basis_valid(c, binding)
                and self._independent(c, binding, worker_id)
                and (request["state"] != "integrating" or acceptance is not None)
                and (acceptance is None or acceptance["reviewer_id"] == worker_id))

    def _review_validated(self, c, review_id):
        """Machine validation accepted the exact candidate this review is bound to."""
        request = c.execute("SELECT candidate_id FROM review_requests WHERE task_id=?", (review_id,)).fetchone()
        return (request is not None
                and self._accepted_validation(c, self._candidate_binding(c, request["candidate_id"])) is not None)

    def _admit_member(self, c, task, worker, *, plan="", resume=False):
        state = self._lifecycle(c, task["id"])
        row = c.execute("SELECT * FROM task_members WHERE task_id=? AND worker_id=?",
                        (task["id"], worker["id"])).fetchone()
        if row and row["executor"]:
            if row["membership_state"] != "member":
                raise HubError("MEMBERSHIP_ENDED", "Ended executor membership cannot be readmitted")
            return
        if task["contract"]["kind"] in REVIEW_KINDS:
            if worker["kind"] != "review":
                raise HubError("REVIEWER_REQUIRED", "Reviews execute only in a separate temporary reviewer conversation")
            if not self._can_review(c, task["id"], worker["id"]):
                raise HubError("REVIEWER_INELIGIBLE", "Review requires independent provenance")
            if not self._review_validated(c, task["id"]):
                raise HubError("VALIDATION_PENDING", "A review starts only after machine validation accepts its exact candidate")
            if c.execute("SELECT 1 FROM task_members WHERE task_id=? AND executor=1 AND membership_state='member'",
                         (task["id"],)).fetchone():
                raise HubError("ALREADY_CLAIMED", "The review already has a recoverable executor")
        elif state["initial_worker_id"] is None:
            c.execute("UPDATE task_lifecycle SET initial_worker_id=? WHERE task_id=?", (worker["id"], task["id"]))
        else:
            if state["opportunity_state"] != "open" or state["suspended"]:
                raise HubError("RECRUITMENT_CLOSED", "The task has no authorized collaborator opportunity")
            if not isinstance(plan, str) or not plan.strip() or len(plan) > CONTRIBUTION_PLAN_CHARACTERS:
                raise HubError("CONTRIBUTION_PLAN_REQUIRED", "A collaborator must state the part it will undertake", 422)
            count = c.execute("SELECT count(*) FROM task_members WHERE task_id=? AND executor=1",
                              (task["id"],)).fetchone()[0]
            if count >= 2:
                raise HubError("TASK_EXECUTOR_LIMIT", "Ordinary tasks admit at most two executors over their lifetime")
            c.execute("UPDATE task_lifecycle SET opportunity_state='consumed' WHERE task_id=? AND opportunity_state='open'",
                      (task["id"],))
        self._member(c, task["id"], worker["id"], executor=True)
        c.execute("UPDATE task_members SET contribution_plan=? WHERE task_id=? AND worker_id=?",
                  (plan or None, task["id"], worker["id"]))

    def _cancel_recruitment(self, c, task_id, reason):
        lifecycle = self._lifecycle(c, task_id)
        if lifecycle["recruitment_decision"] == "not_applicable":
            return
        c.execute("UPDATE task_lifecycle SET recruitment_decision=CASE WHEN recruitment_decision='pending' "
                  "THEN 'cancelled' ELSE recruitment_decision END,opportunity_state=CASE WHEN opportunity_state='consumed' "
                  "THEN opportunity_state ELSE 'cancelled' END,closed_reason=? WHERE task_id=?", (reason, task_id))
        # An already running/ambiguous helper remains live until its process is
        # confirmed quiet; cancellation never permits a replacement model call.
        c.execute("UPDATE helper_intents SET state='cancelled',ended_at=?,reason=? "
                  "WHERE source_task_id=? AND kind='recruitment' AND state='prepared'",
                  (self.clock(), reason, task_id))

    def _reconsider_dependents(self, c, task_id, reason):
        for row in c.execute("SELECT * FROM continuations WHERE state IN ('waiting','eligible')").fetchall():
            requirements = json.loads(row["predicate_json"]).get("requirements", [])
            if any(item.get("task_id") == task_id for item in requirements):
                c.execute("UPDATE continuations SET state='reconsider',wake_reason=? WHERE id=?", (reason, row["id"]))
                self._notify(c, row["task_id"], task_id, reason)

    def _end_member_resumptions(self, c, task_id, worker_id, state):
        """Keep departed members' history without retaining executable tickets."""
        c.execute("UPDATE continuations SET state=?,wake_reason=? WHERE task_id=? AND preferred_worker=? "
                  "AND state IN ('waiting','eligible','reconsider')", (state, state, task_id, worker_id))
        c.execute("UPDATE interruption_checkpoints SET state=? WHERE task_id=? AND worker_id=? AND state='parked'",
                  (state, task_id, worker_id))

    def _release_ended_member(self, c, task_id, worker_id):
        """Apply the same permanent departure at active and already-quiet boundaries."""
        member = c.execute("SELECT membership_state FROM task_members WHERE task_id=? AND worker_id=?",
                           (task_id, worker_id)).fetchone()
        worker = self._get(c, "workers", worker_id)
        if (not member or member["membership_state"] == "member" or worker["active_attempt"]
                or worker["state"] == "recovery_required"
                or c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN "
                             "('prepared','sending','bound','uncertain','reconciled')", (worker_id,)).fetchone()):
            return
        outcome = "satisfied_externally" if self._get(c, "tasks", task_id)["state"] == "done" else "membership_ended"
        self._end_member_resumptions(c, task_id, worker_id, outcome)
        self._feedback_boundary(c, worker_id, task_id)
        if worker["kind"] == "ordinary" and worker["current_task_id"] == task_id:
            c.execute("UPDATE workers SET current_task_id=NULL,work_intention='idle',state='idle' WHERE id=?",
                      (worker_id,))

    def _queue_dependency_reconsideration(self, c, task_id, artifact_id, *, reason="artifact_withdrawn"):
        task = self._task(c, task_id)
        if (task["collaboration"]["mode"] != "collaborative" or task["state"] in TERMINAL
                or task["contract"]["kind"] in REVIEW_KINDS):
            return
        for member in c.execute("SELECT m.*,w.active_attempt,w.state AS worker_state FROM task_members m "
                                "JOIN workers w ON w.id=m.worker_id WHERE m.task_id=? AND m.executor=1 "
                                "AND m.membership_state='member' AND w.current_task_id=m.task_id AND w.kind='ordinary'",
                                (task_id,)).fetchall():
            worker_id = member["worker_id"]
            if member["worker_state"] == "retired":
                continue
            waiting = c.execute("SELECT id FROM continuations WHERE task_id=? AND preferred_worker=? "
                                "AND state IN ('waiting','eligible','reconsider')", (task_id, worker_id)).fetchall()
            if waiting:
                for ticket in waiting:
                    c.execute("UPDATE continuations SET state='reconsider',wake_reason=? WHERE id=?", (reason, ticket["id"]))
                continue
            if c.execute("SELECT 1 FROM interruption_checkpoints WHERE task_id=? AND worker_id=? AND state='parked'",
                         (task_id, worker_id)).fetchone():
                continue
            origin = c.execute("SELECT * FROM attempts WHERE task_id=? AND worker_id=? AND execution_mode<>'message_only' "
                               "ORDER BY created_at DESC,rowid DESC LIMIT 1", (task_id, worker_id)).fetchone()
            if origin is None or origin["state"] == "preflight_held":
                continue
            checkpoint = origin["checkpoint_json"] or member["checkpoint_json"] or canonical({
                "summary": "Dependencies changed while this original execution was retained.",
                "next_action": "Reconsider the saved task against current inputs; the previous inputs are not established as satisfied.",
                "input_refs": json.loads(origin["input_refs_json"])})
            ticket_id = new_id()
            c.execute("INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,"
                      "snapshot,state,wake_reason,origin_attempt,created_at) VALUES(?,?,?,?,?,?,?,'reconsider',?,?,?)",
                      (ticket_id, task_id, task["revision"], worker_id,
                       canonical({"mode": "all", "requirements": task["contract"]["requirements"]}), checkpoint,
                       origin["snapshot"] if origin["ended_at"] is not None else None,
                        reason, origin["id"], self.clock()))
            self._event(c, "continuation.reconsider", ticket_id,
                        {"reason": reason, "artifact_id": artifact_id,
                         "quiet_boundary_pending": origin["ended_at"] is None}, task_id, worker_id)

    def _seal_dependency_reconsideration(self, c, attempt, snapshot):
        pending = c.execute("SELECT id,wake_reason FROM continuations WHERE origin_attempt=? "
                            "AND state='reconsider' AND wake_reason='artifact_withdrawn'",
                            (attempt["id"],)).fetchall()
        tickets = [row["id"] for row in pending]
        if not tickets:
            return
        # A worker may have saved a newer voluntary wait before this boundary.
        # Reconsider that exact plan once instead of leaving two return tickets.
        authored = (c.execute("SELECT * FROM continuations WHERE id=? AND origin_attempt=? "
                              "AND state IN ('waiting','eligible')", (attempt["continuation_id"], attempt["id"])).fetchone()
                    if attempt["continuation_id"] else None)
        if authored:
            c.execute("UPDATE continuations SET state='superseded' WHERE origin_attempt=? "
                      "AND state='reconsider' AND wake_reason='artifact_withdrawn'", (attempt["id"],))
            c.execute("UPDATE continuations SET state='reconsider',wake_reason=?,snapshot=? WHERE id=?",
                      (pending[0]["wake_reason"], snapshot, authored["id"]))
            self._event(c, "continuation.reconsider", authored["id"],
                        {"reason": pending[0]["wake_reason"], "superseded_continuation_ids": tickets},
                        attempt["task_id"], attempt["worker_id"])
        else:
            c.execute("UPDATE continuations SET snapshot=?,checkpoint_json=COALESCE(?,checkpoint_json) "
                      "WHERE origin_attempt=? AND state='reconsider' AND wake_reason='artifact_withdrawn'",
                      (snapshot, attempt["checkpoint_json"], attempt["id"]))

    def _reconsider_task_revision(self, c, task_id, revision):
        """A changed specification wakes saved work without proving its predicate."""
        tickets = [dict(row) for row in c.execute(
            "SELECT * FROM continuations WHERE task_id=? AND state IN ('waiting','eligible','reconsider')", (task_id,))]
        for ticket in tickets:
            c.execute("UPDATE continuations SET state='superseded' WHERE id=?", (ticket["id"],))
            if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? "
                             "AND executor=1 AND membership_state='member'", (task_id, ticket["preferred_worker"])).fetchone():
                continue
            replacement_id = new_id()
            c.execute("INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,"
                      "snapshot,state,wake_reason,origin_attempt,created_at) VALUES(?,?,?,?,?,?,?,'reconsider',?,?,?)",
                      (replacement_id, task_id, revision, ticket["preferred_worker"], ticket["predicate_json"],
                       ticket["checkpoint_json"], ticket["snapshot"], "task_revision_changed", ticket["origin_attempt"], self.clock()))
            self._event(c, "continuation.reconsider", replacement_id,
                        {"reason": "task_revision_changed", "superseded_continuation_id": ticket["id"],
                         "previous_revision": ticket["task_revision"], "revision": revision}, task_id, ticket["preferred_worker"])
        for ticket in c.execute("SELECT * FROM interruption_checkpoints WHERE task_id=? AND state='parked'", (task_id,)).fetchall():
            if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? "
                             "AND executor=1 AND membership_state='member'", (task_id, ticket["worker_id"])).fetchone():
                continue
            replacement_id = new_id()
            c.execute("INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,"
                      "snapshot,state,wake_reason,origin_attempt,created_at) VALUES(?,?,?,?,?,?,?,'reconsider',?,?,?)",
                      (replacement_id, task_id, revision, ticket["worker_id"], canonical({"mode": "all", "requirements": []}),
                       ticket["checkpoint_json"], ticket["snapshot"], "task_revision_changed", ticket["origin_attempt"], self.clock()))
            self._event(c, "continuation.reconsider", replacement_id,
                        {"reason": "task_revision_changed", "superseded_interruption_id": ticket["id"],
                         "previous_revision": ticket["task_revision"], "revision": revision}, task_id, ticket["worker_id"])
        self._invalidate_interruptions(c, task_id, "superseded")

    def _helper(self, c, intent_id):
        row = c.execute("SELECT * FROM helper_intents WHERE id=?", (intent_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Helper intent does not exist", 404)
        result = dict(row)
        # An assignment decision's offer; recruitment decisions have no candidates.
        result["candidates"] = json.loads(result.pop("candidates_json"))
        result["result"] = json.loads(result["result_json"]) if result["result_json"] else None
        return result

    def _helper_reply(self, c, intent_id):
        """A command reply that counts the offered workers instead of listing them."""
        result = self._helper(c, intent_id)
        candidates = result.pop("candidates") or {}
        result["candidate_count"] = len(candidates.get("workers", [])) if isinstance(candidates, dict) else 0
        return result

    @staticmethod
    def _check_saved_helper_decision(helper, decision):
        if helper["raw_result_json"]:
            raw = json.loads(helper["raw_result_json"])
            saved = raw.get("decision") if isinstance(raw.get("decision"), dict) else raw
            # Every field the model answered must be committed unchanged; the runtime may
            # add derived ones, such as a recruitment's standard invitation.
            if not isinstance(saved, dict) or any(decision.get(key) != value for key, value in saved.items()):
                raise HubError("HELPER_RESULT_CONFLICT", "Commit the exact retained native helper decision")

    @staticmethod
    def _turn_dispatched(c, worker):
        """Whether any turn may have reached the worker's current native thread."""
        return c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND thread_id=? AND state<>'not_sent' LIMIT 1",
                         (worker["id"], worker["thread_id"])).fetchone() is not None

    @staticmethod
    def _latest_turn(c, worker):
        """The latest turn of the worker's current thread with a known native outcome."""
        row = c.execute("SELECT remote_turn_id FROM turn_intents WHERE worker_id=? AND thread_id=? "
                        "AND state IN ('completed','interrupted','reconciled') AND remote_turn_id IS NOT NULL "
                        "ORDER BY created_at DESC,rowid DESC LIMIT 1", (worker["id"], worker["thread_id"])).fetchone()
        return row["remote_turn_id"] if row else None

    async def release_empty_thread(self, worker_id):
        """Forget a bound thread that never received a turn; the next activation starts a new one."""
        def release(c):
            worker = self._get(c, "workers", worker_id)
            if not worker["thread_id"] or self._turn_dispatched(c, worker):
                return False
            # Codex saves a thread only after its first turn; nothing is resumed from it.
            c.execute("UPDATE workers SET thread_id=NULL WHERE id=?", (worker_id,))
            self._event(c, "worker.empty_thread_released", worker_id, {"thread_id": worker["thread_id"]},
                        worker_id=worker_id)
            self._adopt_requested_backend(c, worker_id)
            return True
        return await self.db.call(release, write=True)

    def _record_first_turn(self, c, worker_id, task_id, turn_id=None):
        task, worker = self._task(c, task_id), self._get(c, "workers", worker_id)
        lifecycle = self._lifecycle(c, task_id)
        if (task["collaboration"]["mode"] != "collaborative" or task["contract"]["kind"] in REVIEW_KINDS or lifecycle["initial_worker_id"] != worker_id
                or lifecycle["recruitment_decision"] != "pending" or lifecycle["decision_helper_id"]):
            return None
        latest = self._latest_turn(c, worker)
        first_turn_id = lifecycle["first_turn_id"] or turn_id or latest or new_id()
        c.execute("UPDATE task_lifecycle SET first_turn_id=? WHERE task_id=?", (first_turn_id, task_id))
        if task["state"] in TERMINAL or lifecycle["suspended"] or worker["work_intention"] != "working":
            self._cancel_recruitment(c, task_id, "inactive_at_first_turn")
            return None
        if (worker["state"] == "recovery_required" or not worker["run_id"]
                or self._effective_run_state(c, self._get(c, "runs", worker["run_id"]), worker["id"]) != "running"
                or c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running"):
            return None
        helper_id = new_id()
        # The cheap decision model decides in a new, small conversation of its own.
        c.execute("INSERT INTO helper_intents(id,kind,source_worker_id,source_task_id,run_id,"
                  "source_turn_id,state,created_at) VALUES(?,'recruitment',?,?,?,?,'prepared',?)",
                  (helper_id, worker_id, task_id, worker["run_id"], turn_id or latest, self.clock()))
        c.execute("UPDATE task_lifecycle SET decision_helper_id=? WHERE task_id=?", (helper_id, task_id))
        failed = c.execute("SELECT count(*) FROM helper_intents WHERE kind='recruitment' AND source_task_id=? "
                           "AND state='cancelled' AND reason='automatic_retry_after_error'", (task_id,)).fetchone()[0]
        if failed >= DECISION_ATTEMPTS:
            # The decision model failed twice: as with an invalid answer, no collaborator.
            self._decide_recruitment(c, helper_id, False)
            self._event(c, "recruitment.defaulted", task_id, {"reason": "decision_failed"}, task_id, worker_id)
        return self._helper(c, helper_id)

    async def record_first_turn(self, worker_id, task_id, turn_id=None):
        return await self.db.call(lambda c: self._record_first_turn(c, worker_id, task_id, turn_id), write=True)

    async def due_recruitment_intents(self, run_id):
        def read(c):
            try:
                # Deciding on a first turn's recruitment is part of that worker's own work.
                scope = self._admission_scope(c, run_id)
            except HubError:
                return []
            if "codex" in set(self._dispatch_holds(c)):
                return []  # Decisions use the Codex account, whatever the worker's backend.
            for row in c.execute("SELECT l.task_id,l.initial_worker_id FROM task_lifecycle l JOIN workers w ON w.id=l.initial_worker_id "
                                 "WHERE l.recruitment_decision='pending' AND l.first_turn_id IS NOT NULL AND l.decision_helper_id IS NULL "
                                 "AND w.active_attempt IS NULL AND w.state='idle' AND w.work_intention='working' AND w.run_id=?", (run_id,)).fetchall():
                # One unreadable task must not withhold recruitment helpers from the others.
                self._admit_item(c, "recruitment_item", row["task_id"], row["task_id"], (),
                                 lambda: self._record_first_turn(c, row["initial_worker_id"], row["task_id"]))
            return [self._helper(c, row["id"]) for row in c.execute(
                "SELECT h.id,h.source_worker_id FROM helper_intents h JOIN workers w ON w.id=h.source_worker_id "
                "WHERE h.kind='recruitment' AND (h.state='prepared' OR (h.state='running' AND h.raw_result_json IS NOT NULL)) AND h.run_id=? "
                "AND w.active_attempt IS NULL AND w.state='idle' AND w.work_intention='working' ORDER BY h.created_at,h.id", (run_id,))
                if self._scope_admits(c, run_id, scope, row["source_worker_id"])]
        return await self.db.call(read, write=True)

    def _helper_controls(self, c, helper):
        """A recruitment decision is part of its worker's own task; choosing new
        work (assignment) needs the whole run, or a group of workers resumed while it is
        paused. Neither runs for a paused worker."""
        source = helper["source_worker_id"]
        if helper["kind"] == "recruitment":
            return self._run_controls(c, helper["run_id"], source)
        result = self._assignment_scope(c, helper["run_id"])
        if self._worker_control(c, helper["run_id"], source) == "paused":
            raise HubError("WORKER_PAUSED", "The operator paused this worker")
        return result

    def _helper_start(self, c, intent_id, thread_id):
        helper = self._helper(c, intent_id)
        self._helper_controls(c, helper)
        if helper["state"] != "prepared":
            raise HubError("HELPER_NOT_PREPARED", "Do not replay a running or uncertain helper")
        c.execute("UPDATE helper_intents SET state='running',thread_id=? WHERE id=?", (thread_id, intent_id))
        c.execute("UPDATE runs SET turns_used=turns_used+1 WHERE id=?", (helper["run_id"],))
        return self._helper(c, intent_id)

    async def helper_uncertain(self, intent_id, reason, *, inference_uncertain=True):
        def update(c):
            helper = self._helper(c, intent_id)
            if helper["state"] in {"prepared", "running", "uncertain"}:
                c.execute("UPDATE helper_intents SET state='uncertain',reason=?,inference_uncertain=MAX(inference_uncertain,?) WHERE id=?",
                          (reason, int(inference_uncertain), intent_id))
                if inference_uncertain:
                    self._hold_helper_usage(c, helper, reason)
            return self._helper(c, intent_id)
        return await self.db.call(update, write=True)

    def _decide_recruitment(self, c, intent_id, decision, invitation=""):
        helper = self._helper(c, intent_id)
        if helper["kind"] != "recruitment" or type(decision) is not bool:
            raise HubError("INVALID_INPUT", "A recruitment helper returns one Boolean decision", 422)
        result = {"decision": decision, "invitation": invitation}
        self._check_saved_helper_decision(helper, result)
        if helper["state"] == "completed":
            if helper["result"] != result:
                raise HubError("HELPER_RESULT_CONFLICT", "A helper decision is immutable")
            return self._lifecycle(c, helper["source_task_id"])
        if helper["state"] == "cancelled" and c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
            return self._lifecycle(c, helper["source_task_id"])
        if helper["state"] not in {"prepared", "running"}:
            raise HubError("HELPER_RECONCILIATION_REQUIRED", "Reconcile the helper before committing its decision")
        self._helper_controls(c, helper)
        task = self._task(c, helper["source_task_id"])
        state = self._lifecycle(c, task["id"])
        worker = self._get(c, "workers", helper["source_worker_id"])
        valid = (state["recruitment_decision"] == "pending" and state["opportunity_state"] == "closed"
                 and not state["suspended"] and task["state"] not in TERMINAL
                 and worker["work_intention"] == "working")
        if valid:
            if decision and (not isinstance(invitation, str) or not invitation.strip() or len(invitation) > 5000):
                raise HubError("INVITATION_REQUIRED", "An open opportunity needs a brief bounded invitation", 422)
            c.execute("UPDATE task_lifecycle SET recruitment_decision=?,opportunity_state=?,invitation=?,"
                      "opportunity_opened_at=? WHERE task_id=?",
                      ("yes" if decision else "no", "open" if decision else "closed", invitation,
                       self.clock() if decision else None, task["id"]))
        c.execute("UPDATE helper_intents SET state='completed',result_json=?,ended_at=? WHERE id=?",
                  (canonical(result), self.clock(), intent_id))
        self._event(c, "recruitment.decided", task["id"], {**result, "applied": valid,
                    **self._default_answer(helper)}, task["id"], worker["id"])
        return self._lifecycle(c, task["id"])

    async def decide_recruitment(self, intent_id, decision, invitation=""):
        return await self.db.call(lambda c: self._decide_recruitment(c, intent_id, decision, invitation), write=True)

    _OPPORTUNITY_ROWS = ("SELECT t.id,l.suspended,l.initial_worker_id,l.opportunity_state,r.task_id AS review_id,"
                  "r.candidate_id,r.state AS review_state,p.priority AS operator_priority,p.pinned_at FROM tasks t "
                  "JOIN task_collaboration s ON s.task_id=t.id AND s.mode='collaborative' "
                  "LEFT JOIN task_lifecycle l ON l.task_id=t.id "
                  "LEFT JOIN review_requests r ON r.task_id=t.id "
                  "LEFT JOIN task_admission p ON p.task_id=t.id "
                  "WHERE t.state NOT IN ('done','retired','draining','recovery_required')")

    def _open_candidate(self, c, task_id):
        """This one task's open opportunity, under the same rules as every other."""
        row = c.execute(self._OPPORTUNITY_ROWS + " AND t.id=?", (task_id,)).fetchone()
        return self._opportunity(c, row) if row else None

    def _opportunities(self, c):
        candidates = []
        for row in c.execute(self._OPPORTUNITY_ROWS + " ORDER BY t.created_at,t.id").fetchall():
            try:
                candidate = self._opportunity(c, row)
            except ITEM_FAULTS as error:
                # One unreadable task must not hide every other opportunity.
                self._admission_fault(c, row["id"], row["id"], error)
                continue
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _opportunity(self, c, row):
        # These durable exclusions cannot grant admission. Apply them before
        # opening task files or rechecking every pinned Lean dependency.
        if row["suspended"] or c.execute("SELECT 1 FROM integrator_control WHERE task_id=?", (row["id"],)).fetchone():
            return None
        if row["review_id"]:
            if (row["review_state"] not in {"pending", "integrating"}
                    or c.execute("SELECT 1 FROM attempts WHERE task_id=? AND ended_at IS NULL AND execution_mode<>'message_only'",
                                 (row["id"],)).fetchone()
                    or c.execute("SELECT 1 FROM task_members WHERE task_id=? AND executor=1 "
                                 "AND membership_state='member'", (row["id"],)).fetchone()):
                return None
        elif row["initial_worker_id"] is not None and row["opportunity_state"] != "open":
            return None
        task = self._task(c, row["id"])
        lifecycle = self._lifecycle(c, task["id"], persist=False)
        if lifecycle["suspended"]:
            return None
        try:
            from ..bootstrap_runtime import require_contract_environment

            require_contract_environment(self, c, task["contract"])
            self._valid_inputs(c, task["contract"])
        except HubError:
            return None
        review = task["contract"]["kind"] in REVIEW_KINDS
        if review:
            request = c.execute("SELECT candidate_id,state FROM review_requests WHERE task_id=?", (task["id"],)).fetchone()
            binding = self._candidate_binding(c, request["candidate_id"]) if request else None
            if (not request or request["state"] not in {"pending", "integrating"} or not self._basis_valid(c, binding)
                    or self._accepted_validation(c, binding) is None):
                return None  # Not open, or machine validation has not accepted the exact candidate.
            if self._active_attempts(c, task["id"]) or c.execute(
                    "SELECT 1 FROM task_members WHERE task_id=? AND executor=1 AND membership_state='member'",
                    (task["id"],)).fetchone():
                return None
            opportunity, opened_at = "review", task["created_at"]
        elif lifecycle["initial_worker_id"] is None:
            opportunity, opened_at = "initial", task["created_at"]
        elif lifecycle["opportunity_state"] == "open":
            opportunity, opened_at = "collaborator", lifecycle["opportunity_opened_at"]
        else:
            return None
        # The operator's priority, if any, replaces the contract's (task.set_priority).
        priority = task["contract"]["priority"] if row["operator_priority"] is None else row["operator_priority"]
        return {"task_id": task["id"], "revision": task["revision"],
                           "contract_hash": task["contract_hash"], "title": task["contract"]["title"],
                           "objective": task["contract"]["objective"], "kind": task["contract"]["kind"],
                           "priority": priority, "pinned_at": row["pinned_at"], "opportunity": opportunity,
                           "published_at": task["created_at"], "opened_at": opened_at, "invitation": lifecycle["invitation"],
                           "executors": [item["worker_id"] for item in task["collaboration"]["members"]
                                         if item["executor"] and item.get("membership_state", "member") == "member"],
                           "files": task["files"]}

    def _dispatcher(self, c):
        """The system identity that owns assignment decisions; it never executes a task."""
        row = c.execute("SELECT id FROM workers WHERE kind='dispatcher'").fetchone()
        if row is not None:
            return row["id"]
        worker_id = self._new_worker(c, kind="dispatcher", backend="codex")
        c.execute("UPDATE workers SET work_intention='retired' WHERE id=?", (worker_id,))
        return worker_id

    def _idle_workers(self, c, run_id, candidate=None, scope=None):
        """Workers that finished their previous task and may take an opportunity now; given a
        candidate, only those that never executed its task (_new_executors).

        A worker that paused mid-task is waiting, not idle; so is one with saved work or a
        message to handle, one the operator paused (on its own or with its group, or outside
        the `scope` of a paused run), and one whose account is held or has no free session. For a
        worker that asked to move to the other backend, that is the account it moves to: a new
        task starts a new conversation, where it moves (in the Linux pilot, nine Codex workers
        waiting to move to a full Claude were given new tasks and ran on Codex).
        """
        held = set(self._dispatch_holds(c))
        unavailable = sorted(held | {backend for backend in BACKENDS if self._backend_full(c, backend)})
        paused = self._controlled_workers(c, run_id, "paused")
        idle = [dict(worker) for worker in c.execute(
            "SELECT * FROM workers w WHERE kind='ordinary' AND work_intention='idle' AND state='idle' "
            "AND active_attempt IS NULL AND pending_wake=0 "
            "AND COALESCE((SELECT q.backend FROM worker_backend_requests q WHERE q.worker_id=w.id),backend) "
            "NOT IN (" + ",".join("?" * len(unavailable)) + ") "
            "AND NOT EXISTS(SELECT 1 FROM continuations WHERE preferred_worker=w.id "
            "AND state IN ('eligible','reconsider','waiting')) "
            # A message whose delivery is unresolved belongs to the current conversation.
            "AND NOT EXISTS(SELECT 1 FROM message_delivery_holds WHERE worker_id=w.id "
            "AND state IN ('sending','uncertain')) "
            "AND NOT EXISTS(SELECT 1 FROM interruption_checkpoints WHERE worker_id=w.id AND state='parked') "
            "ORDER BY created_at,id", unavailable)
            if worker["id"] not in paused and (scope is None or worker["id"] in scope)]
        return idle if candidate is None else self._new_executors(c, candidate, idle)

    @staticmethod
    def _new_executors(c, candidate, idle):
        """The idle workers that never executed the candidate's task."""
        executed = {row[0] for row in c.execute("SELECT worker_id FROM task_members WHERE task_id=? AND executor=1",
                                                (candidate["task_id"],))}
        return [worker for worker in idle if worker["id"] not in executed]

    @staticmethod
    def _previous_titles(c, worker_id):
        """The titles of the tasks a worker executed, most recent first, then those it executed
        in a project it was carried over from (worker.import_history)."""
        titles = []
        for row in c.execute(
                "SELECT r.contract_json,MAX(a.created_at) AS last FROM attempts a "
                "JOIN tasks t ON t.id=a.task_id JOIN task_revisions r ON r.task_id=t.id AND r.revision=t.current_revision "
                "WHERE a.worker_id=? AND a.execution_mode<>'message_only' GROUP BY t.id ORDER BY last DESC,t.id LIMIT ?",
                (worker_id, DECISION_TITLES)):
            try:
                title = json.loads(row["contract_json"]).get("title")
            except (TypeError, ValueError, AttributeError):
                continue  # An unreadable record names no previous task.
            if isinstance(title, str):
                titles.append(title)
        for row in c.execute("SELECT title FROM worker_imported_tasks WHERE worker_id=? ORDER BY position",
                             (worker_id,)):
            if len(titles) >= DECISION_TITLES:
                break
            if row["title"] not in titles:
                titles.append(row["title"])
        return titles

    def _admission(self, c, task):
        """A task's place in admission: the priority it is admitted by (the operator's, else its
        contract's), both of those, and since when the operator pinned it (task_admission)."""
        row = c.execute("SELECT priority,pinned_at FROM task_admission WHERE task_id=?", (task["id"],)).fetchone()
        operator = row["priority"] if row else None
        return {"task_id": task["id"], "priority": task["contract"]["priority"] if operator is None else operator,
                "contract_priority": task["contract"]["priority"], "operator_priority": operator,
                "pinned_at": row["pinned_at"] if row else None}

    def _admission_target(self, c, p, actor):
        """The task an operator places in admission: an unfinished one, neither a review (it goes
        with the task it reviews) nor the Integrator's."""
        self._role(actor, Role.OPERATOR)
        task = self._task(c, p.get("task_id"))
        if task["state"] in TERMINAL:
            raise HubError("TASK_FINISHED", "A done or retired task is not admitted again", 409)
        if (task["contract"]["kind"] in REVIEW_KINDS
                or c.execute("SELECT 1 FROM integrator_control WHERE task_id=?", (task["id"],)).fetchone()):
            raise HubError("INVALID_INPUT", "A review goes with the task it reviews, and the Integrator's task "
                           "with its batches", 422)
        return task

    @LIFECYCLE.handles("task.set_priority")
    def _on_task_set_priority(self, c, name, p, actor):
        """The operator's priority for a task, 0 (first) to 3 (last), in place of its contract's;
        null returns to the contract's. It orders admission only: the task is not revised."""
        task = self._admission_target(c, p, actor)
        priority = p.get("priority", "missing")
        if priority is not None and (type(priority) is not int or not 0 <= priority <= 3):
            raise HubError("INVALID_INPUT", "Give a priority from 0 (first) to 3 (last), or null for the contract's", 422)
        c.execute("INSERT INTO task_admission(task_id,priority,updated_at) VALUES(?,?,?) ON CONFLICT(task_id) "
                  "DO UPDATE SET priority=excluded.priority,updated_at=excluded.updated_at",
                  (task["id"], priority, self.clock()))
        self._event(c, "task.priority_set", task["id"], {"priority": priority}, task["id"])
        return self._admission(c, task)

    @LIFECYCLE.handles("task.set_pinned")
    def _on_task_set_pinned(self, c, name, p, actor):
        """Pin a task first in admission, or unpin it. Its saved work and its opportunities go
        before every unpinned task's, and its reviews before other reviews; pinned tasks go in
        the order they were pinned. A pin stays until it is removed; the task is not revised."""
        task = self._admission_target(c, p, actor)
        if type(p.get("pinned")) is not bool:
            raise HubError("INVALID_INPUT", "Say whether the task is pinned: true or false", 422)
        now = self.clock()
        c.execute("INSERT INTO task_admission(task_id,pinned_at,updated_at) VALUES(?,?,?) ON CONFLICT(task_id) "
                  "DO UPDATE SET pinned_at=CASE WHEN ? THEN COALESCE(pinned_at,excluded.pinned_at) END,"
                  "updated_at=excluded.updated_at", (task["id"], now if p["pinned"] else None, now, int(p["pinned"])))
        self._event(c, "task.pinned" if p["pinned"] else "task.unpinned", task["id"], task_id=task["id"])
        return self._admission(c, task)

    @LIFECYCLE.handles("worker.import_history")
    def _on_worker_import_history(self, c, name, p, actor):
        """Record the tasks a worker carried over from another project executed there, most
        recent first: assignment decisions show them as its previous tasks."""
        self._role(actor, Role.OPERATOR)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if worker["kind"] != "ordinary":
            raise HubError("INVALID_INPUT", "Only ordinary workers take tasks by decision", 422)
        titles = p.get("tasks")
        if (not isinstance(titles, list) or len(titles) > DECISION_TITLES
                or not all(isinstance(title, str) and title.strip() and len(title) <= IMPORTED_TITLE_CHARACTERS
                           for title in titles)):
            raise HubError("INVALID_INPUT", f"Give at most {DECISION_TITLES} task titles of at most "
                           f"{IMPORTED_TITLE_CHARACTERS} characters", 422)
        titles = [title.strip() for title in titles]
        c.execute("DELETE FROM worker_imported_tasks WHERE worker_id=?", (worker["id"],))
        c.executemany("INSERT INTO worker_imported_tasks VALUES(?,?,?)",
                      [(worker["id"], position, title) for position, title in enumerate(titles)])
        self._event(c, "worker.history_imported", worker["id"], {"tasks": len(titles)}, worker_id=worker["id"])
        return {"worker_id": worker["id"], "tasks": titles}

    def _open_opportunities(self, c, worth=None, pinned=None):
        """Open ordinary opportunities, a task's first executor or its collaborator, in admission
        order (_admission_key). `pinned` keeps only the pinned tasks' (True) or only the others'
        (False); `worth`, given a task ID, says whether the task is worth reading in full."""
        result = []
        pin = "" if pinned is None else " AND p.pinned_at IS NOT NULL" if pinned else " AND p.pinned_at IS NULL"
        # Reviews get their own new reviewers (_select_task); a suspended task, or one whose first
        # executor came and whose opportunity is closed, has none (_opportunity). Left out here,
        # they are not fetched or judged worth reading: most of the pilot's 323 open tasks were.
        for row in c.execute(self._OPPORTUNITY_ROWS + " AND r.task_id IS NULL AND NOT COALESCE(l.suspended,0) "
                             "AND (l.initial_worker_id IS NULL OR l.opportunity_state='open')" + pin
                             + " ORDER BY t.created_at,t.id").fetchall():
            if worth is not None and not worth(row["id"]):
                continue
            try:
                candidate = self._opportunity(c, row)
            except ITEM_FAULTS as error:
                # One unreadable task must not hide every other opportunity.
                self._admission_fault(c, row["id"], row["id"], error)
                continue
            if candidate is not None and candidate["opportunity"] in {"initial", "collaborator"}:
                result.append(candidate)
        return sorted(result, key=self._admission_key)

    @staticmethod
    def _admission_key(candidate):
        """Admission order: tasks the operator pinned first, as they were pinned; then priority, 0
        first (the operator's, else the contract's); then the earliest published. Opening time
        came first before, so priority only decided between tasks published together."""
        return (candidate["pinned_at"] is None, candidate["pinned_at"] or 0, candidate["priority"],
                candidate["published_at"], candidate["task_id"])

    # An assignment decision that ended in an error, or whose claim failed.
    _FAILED_DECISION = ("kind='assignment' AND ((state='cancelled' AND COALESCE(reason,'') NOT IN "
                        "('project_shutdown','run_control')) OR (state='completed' "
                        "AND json_valid(result_json) AND json_extract(result_json,'$.outcome')='claim_failed'))")

    def _failed_decisions(self, c, task_id):
        """Decisions for this task that ended in an error, or whose claim failed."""
        return c.execute("SELECT count(*) FROM helper_intents WHERE source_task_id=? AND " + self._FAILED_DECISION,
                         (task_id,)).fetchone()[0]

    def _exhausted_decisions(self, c):
        """The tasks whose decisions failed DECISION_ATTEMPTS times: a new worker takes them."""
        return {row[0] for row in c.execute(
            "SELECT source_task_id FROM helper_intents WHERE " + self._FAILED_DECISION
            + " GROUP BY source_task_id HAVING count(*)>=?", (DECISION_ATTEMPTS,))}

    @staticmethod
    def _executors_among(c, worker_ids):
        """For each unfinished task, which of these workers executed it."""
        executed: dict[str, set[str]] = {}
        if worker_ids:
            for row in c.execute("SELECT m.task_id,m.worker_id FROM task_members m JOIN tasks t ON t.id=m.task_id "
                                 "WHERE m.executor=1 AND t.state NOT IN ('done','retired') AND m.worker_id IN ("
                                 + ",".join("?" * len(worker_ids)) + ")", worker_ids):
                executed.setdefault(row[0], set()).add(row[1])
        return executed

    def _assign_opportunities(self, c, run_id, scope=None, pinned=None):
        """Give every open ordinary opportunity (those `pinned` selects, as _open_opportunities)
        a worker without waiting, in admission order.

        With no idle worker for it, a new worker takes it at once. Otherwise one decision
        of the cheap decision model chooses an idle worker or none, and none means a new
        worker. One decision runs at a time, so each sees the workers still idle; the
        opportunities that need a decision wait for theirs. Decisions that keep failing,
        or cannot run while the Codex account they use is held, give way to a new worker.
        Workers paused on their own or with their group, or outside the `scope` of a paused
        run, take none, and no new worker starts on an account whose workers are paused.
        """
        held = set(self._dispatch_holds(c))
        closed = held | self._paused_backends(c, "ordinary", run_id)
        live = c.execute("SELECT source_task_id,state FROM helper_intents WHERE kind='assignment' "
                         "AND state IN ('prepared','running','uncertain')").fetchall()
        # A decision whose outcome is unknown holds only its own task until it is reconciled.
        deciding = any(row["state"] != "uncertain" for row in live)
        waiting = {row["source_task_id"] for row in live}
        # Who can take an opportunity is the same for every one of them until one is claimed.
        # Without a session for a new worker, and with no idle worker or a decision already
        # running, none can be given a worker now: finding that out by reading every open task
        # in full took about 800 statements per admission in the pilot, a few seconds each.
        idle, fresh = self._idle_workers(c, run_id, scope=scope), self._new_worker_free(c, closed)
        if not fresh and (not idle or deciding):
            return None
        # What the decisions and executors of every open task say, read at once: per task it
        # was three statements, about 1,000 a pass that admitted nothing in the Linux pilot, each
        # slow while the coordination thread shares the interpreter. Choosing does not change it
        # for the other tasks: a claim ends the pass, and a decision begun only adds its own.
        exhausted, offers = self._exhausted_decisions(c), self._new_worker_offers(c)
        executed = self._executors_among(c, [worker["id"] for worker in idle])
        # A task that keeps its number has a new worker as its first executor, never an idle one.
        keeping = {row[0] for row in c.execute("SELECT task_id FROM kept_worker_numbers WHERE worker_id IS NULL")}

        def choices(task_id):
            """The idle workers a decision would choose among for this task; none when a new
            worker takes it without one (the decisions' account is held, or they kept failing,
            or the task keeps its number for its first executor)."""
            available = [worker for worker in idle if worker["id"] not in executed.get(task_id, ())]
            if available and "codex" not in held and task_id not in exhausted and task_id not in keeping:
                return available
            return []

        def worth(task_id):
            # While a decision runs, an opportunity that would wait for another is not read in
            # full: with 26 open and 43 idle workers, that was about 800 statements a pass.
            return task_id not in waiting and (not deciding or task_id in offers or not choices(task_id))

        for candidate in self._open_opportunities(c, worth, pinned):
            if candidate["task_id"] in waiting:
                continue
            c.execute("SAVEPOINT opportunity")
            try:
                assignment = None
                if self._new_worker_decided(offers.get(candidate["task_id"]), candidate):
                    # A decision chose a new worker while that worker's account was held or full.
                    if fresh:
                        assignment = self._new_worker_claim(c, candidate, run_id)
                elif available := choices(candidate["task_id"]):
                    if not deciding:
                        self._begin_assignment(c, candidate, available, run_id)
                        deciding = True
                elif fresh:
                    assignment = self._new_worker_claim(c, candidate, run_id)
            except ITEM_FAULTS as error:
                c.execute("ROLLBACK TO opportunity")
                c.execute("RELEASE opportunity")
                if isinstance(error, HubError) and error.code in ADMISSION_GLOBAL_ERRORS:
                    raise
                # One task that cannot be assigned must not hold up the others.
                self._admission_fault(c, candidate["task_id"], candidate["task_id"], error)
                continue
            c.execute("RELEASE opportunity")
            if assignment is not None:
                return assignment
        return None

    @staticmethod
    def _new_worker_offers(c):
        """By task, the offer of its latest decision where that decision chose a new worker whose
        account was held or full."""
        return {row[0]: json.loads(row[1]) for row in c.execute(
            "SELECT h.source_task_id,h.candidates_json FROM helper_intents h WHERE h.kind='assignment' "
            "AND h.state='completed' AND json_valid(h.result_json) "
            "AND json_extract(h.result_json,'$.outcome')='new_worker_held' "
            "AND NOT EXISTS(SELECT 1 FROM helper_intents l WHERE l.kind='assignment' "
            "AND l.source_task_id=h.source_task_id AND l.state='completed' "
            "AND (l.created_at>h.created_at OR (l.created_at=h.created_at AND l.rowid>h.rowid)))")}

    @staticmethod
    def _new_worker_decided(offer, candidate):
        """The latest decision for this opportunity (`offer`, from _new_worker_offers) chose a new
        worker whose account was held or full."""
        return (offer is not None and offer["opportunity"] == candidate["opportunity"]
                and offer["task_revision"] == candidate["revision"])

    async def assignment_offer_open(self, intent):
        """Whether an assignment decision's offer still describes its task's opportunity."""
        def read(c):
            offer, candidate = intent["candidates"], self._open_candidate(c, intent["source_task_id"])
            return (candidate is not None and candidate["opportunity"] == offer["opportunity"]
                    and candidate["revision"] == offer["task_revision"])
        return await self.db.call(read)

    @staticmethod
    def _default_answer(helper):
        """Whether the kind's default decided because the decision model answered outside its
        schema, or because its turn reached the time limit."""
        raw = json.loads(helper["raw_result_json"] or "{}")
        return {"invalid": bool(raw.get("invalid_output")), "timed_out": bool(raw.get("timed_out"))}

    def _begin_assignment(self, c, candidate, idle, run_id):
        offer = {"task_revision": candidate["revision"], "opportunity": candidate["opportunity"],
                 "invitation": candidate["invitation"] if candidate["opportunity"] == "collaborator" else "",
                 "workers": [{"worker_id": worker["id"], "name": worker_identity(worker)["display_name"],
                              "previous_tasks": self._previous_titles(c, worker["id"])} for worker in idle]}
        helper_id = new_id()
        c.execute("INSERT INTO helper_intents(id,kind,source_worker_id,source_task_id,run_id,candidates_json,"
                  "state,created_at) VALUES(?,'assignment',?,?,?,?,'prepared',?)",
                  (helper_id, self._dispatcher(c), candidate["task_id"], run_id, canonical(offer), self.clock()))
        self._event(c, "assignment.requested", helper_id,
                    {"task_id": candidate["task_id"], "opportunity": candidate["opportunity"],
                     "idle_workers": [worker["id"] for worker in idle]}, candidate["task_id"])

    def _claim_opportunity(self, c, worker_id, candidate, run_id):
        plan = ""
        if candidate["opportunity"] == "collaborator":
            # The joining plan quotes the invitation within the plan's length limit.
            prefix = "Undertake the advertised contribution: "
            invitation, room = candidate["invitation"], CONTRIBUTION_PLAN_CHARACTERS - len(prefix)
            plan = prefix + (invitation if len(invitation) <= room else invitation[:room - 1] + "…")
        return self._assignment_metadata(c, self._claim(c, candidate["task_id"], candidate["revision"], worker_id,
                                                        run_id, contribution_plan=plan))

    def _new_worker_claim(self, c, candidate, run_id):
        """A new worker takes the opportunity, in a new conversation. A failed claim leaves no worker."""
        c.execute("SAVEPOINT fresh_claim")
        worker_id = None
        try:
            kept = kept_worker_number(c, candidate["task_id"]) if candidate["opportunity"] == "initial" else None
            if kept is None:
                self._skip_full_numbers(c)
                worker_id = self._new_worker(c, kind="ordinary")
            else:
                worker_id = self._new_worker(c, kind="ordinary", backend=self._kept_number_backend(c, kept),
                                             number=kept)
            result = self._claim_opportunity(c, worker_id, candidate, run_id)
        except HubError as error:
            c.execute("ROLLBACK TO fresh_claim")
            c.execute("RELEASE fresh_claim")
            if worker_id is not None:
                self.discard_workspace(worker_id)
            if error.code == "WORKER_WORKSPACE_UNAVAILABLE":
                self._quarantine_workspace_failure(c, error)
            elif error.code not in STALE_ADMISSION:
                raise
            return None
        c.execute("RELEASE fresh_claim")
        self._event(c, "worker.fresh_created", worker_id,
                    {"task_id": candidate["task_id"], "opportunity": candidate["opportunity"], "kind": "ordinary"},
                    candidate["task_id"], worker_id)
        return result

    def _release_conversation(self, c, worker_id, reason, *, keep_rotation=None):
        """The worker's next activation starts a new conversation; its identity and workspace stay."""
        self._cancel_rotations(c, worker_id, reason, keep=keep_rotation)
        worker = self._get(c, "workers", worker_id)
        if worker["thread_id"] is None:
            return
        c.execute("UPDATE workers SET thread_id=NULL WHERE id=?", (worker_id,))
        self._event(c, "worker.conversation_released", worker_id,
                    {"thread_id": worker["thread_id"], "reason": reason}, worker_id=worker_id)
        self._adopt_requested_backend(c, worker_id)

    def _complete_assignment(self, c, intent_id, worker=None):
        helper = self._helper(c, intent_id)
        if helper["kind"] != "assignment":
            raise HubError("INVALID_INPUT", "Expected an assignment decision", 422)
        if worker is not None and not isinstance(worker, str):
            raise HubError("INVALID_INPUT", "An assignment decision names one worker or none", 422)
        chosen = {"worker": worker}
        self._check_saved_helper_decision(helper, chosen)
        if helper["state"] == "completed":
            if helper["result"].get("choice") != chosen:
                raise HubError("HELPER_RESULT_CONFLICT", "An assignment decision is immutable")
            return helper["result"]
        if (helper["state"] == "cancelled"
                and c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running"):
            return {"choice": chosen, "assignment": None, "outcome": "cancelled"}
        if helper["state"] not in {"prepared", "running"}:
            raise HubError("HELPER_RECONCILIATION_REQUIRED", "Reconcile the decision before committing it")
        scope = self._assignment_scope(c, helper["run_id"])
        offer = helper["candidates"]
        result: dict[str, Any] = {"choice": chosen, "assignment": None}
        candidate = self._open_candidate(c, helper["source_task_id"])
        if (candidate is None or candidate["opportunity"] != offer["opportunity"]
                or candidate["revision"] != offer["task_revision"]):
            result["outcome"] = "opportunity_changed"  # The next admission looks at it again.
        else:
            offered = {item["worker_id"]: item for item in offer["workers"]}
            named = None if worker is None else next(
                (worker_id for worker_id, item in offered.items()
                 if worker.strip().casefold() in {worker_id.casefold(), item["name"].casefold()}), None)
            idle = {item["id"] for item in self._idle_workers(c, helper["run_id"], candidate, scope)}
            if named is not None and named in idle:
                c.execute("SAVEPOINT chosen_claim")
                try:
                    self._release_conversation(c, named, "new_task")
                    self._adopt_requested_backend(c, named)  # Also for a worker that had no conversation.
                    if c.execute("SELECT 1 FROM worker_backend_requests WHERE worker_id=?", (named,)).fetchone():
                        # Its new account could not start it after all: it takes no session on the old one.
                        raise HubError("SWITCH_WAITING", "This worker waits to move to the backend it asked for", 409)
                    result["assignment"] = self._claim_opportunity(c, named, candidate, helper["run_id"])
                    c.execute("RELEASE chosen_claim")
                    result.update(outcome="idle_worker", worker_id=named)
                except HubError as error:
                    c.execute("ROLLBACK TO chosen_claim")
                    c.execute("RELEASE chosen_claim")
                    if error.code == "WORKER_WORKSPACE_UNAVAILABLE":
                        self._quarantine_workspace_failure(c, error)
                    elif error.code in ADMISSION_GLOBAL_ERRORS:
                        raise
                    # The decision still ends; the next admission looks at its task again.
                    result.update(outcome="claim_failed", reason=error.code)
            else:
                # None suitable, an unknown name, or a worker no longer idle: a new worker.
                result["reason"] = ("none_suitable" if worker is None else "not_offered" if named is None
                                    else "no_longer_idle")
                if not self._new_worker_free(c, set(self._dispatch_holds(c))
                                             | self._paused_backends(c, "ordinary", helper["run_id"])):
                    # The answer stands; the new worker starts once its account is free again.
                    result["outcome"] = "new_worker_held"
                else:
                    try:
                        result["assignment"] = self._new_worker_claim(c, candidate, helper["run_id"])
                    except HubError as error:
                        if error.code in ADMISSION_GLOBAL_ERRORS:
                            raise
                        self._admission_fault(c, candidate["task_id"], candidate["task_id"], error)
                        result["reason"] = error.code
                    result.update(outcome="new_worker" if result["assignment"] else "claim_failed",
                                  worker_id=(result["assignment"] or {}).get("worker_id"))
        c.execute("UPDATE helper_intents SET state='completed',result_json=?,ended_at=? WHERE id=?",
                  (canonical(result), self.clock(), intent_id))
        self._event(c, "assignment.decided", intent_id,
                    {"task_id": helper["source_task_id"], "worker": worker, "outcome": result["outcome"],
                     "reason": result.get("reason"), "worker_id": result.get("worker_id"),
                     **self._default_answer(helper)},
                    helper["source_task_id"], result.get("worker_id"))
        return result

    async def complete_assignment(self, intent_id, worker=None):
        result = await self.db.call(lambda c: self._complete_assignment(c, intent_id, worker), write=True)
        prepared = await self.prepare_assignment_workspace(result, tolerate_failure=True)
        if prepared != result:
            await self.db.write(lambda c: c.execute(
                "UPDATE helper_intents SET result_json=? WHERE id=? AND result_json=?",
                (canonical(prepared), intent_id, canonical(result))))
        return prepared

    async def due_assignment_intents(self, run_id):
        """Decisions ready to run, or whose retained answer still needs its commit."""
        def read(c):
            try:
                self._assignment_scope(c, run_id)
            except HubError:
                return []
            if "codex" in set(self._dispatch_holds(c)):
                return []  # Decisions use the Codex account.
            return [self._helper(c, row["id"]) for row in c.execute(
                "SELECT id FROM helper_intents WHERE kind='assignment' AND run_id=? "
                "AND (state='prepared' OR (state='running' AND raw_result_json IS NOT NULL)) "
                "ORDER BY created_at,id", (run_id,))]
        return await self.db.call(read)

    def _assignment_metadata(self, c, assignment, execution_mode=None):
        worker = self._get(c, "workers", assignment["worker_id"])
        return {**assignment, "execution_mode": execution_mode or worker["execution_mode"], "worker_kind": worker["kind"]}

    def _quarantine_workspace_failure(self, c, error):
        """Hold only an identified local workspace after rolling back admission."""
        worker_id, task_id = error.worker_id, error.task_id
        worker = c.execute("SELECT * FROM workers WHERE id=?", (worker_id,)).fetchone()
        if worker is not None:
            if worker["active_attempt"]:
                raise error
            c.execute("UPDATE workers SET state='recovery_required' WHERE id=?", (worker_id,))
            c.execute("UPDATE task_members SET runnable=0,work_reason='workspace_recovery_required' "
                      "WHERE worker_id=? AND task_id=? AND membership_state='member'", (worker_id, task_id))
            self._recompute_task(c, task_id)
        self._event(c, "worker.admission_failed", worker_id,
                    {"code": error.code, "reason": error.message, "worker_retained": worker is not None,
                     "attempt_admitted": False}, task_id, worker_id if worker else None)
        self._notify(c, task_id, worker_id, "workspace_recovery_required")
        return worker is not None

    def _wake_for_message(self, c, worker_id, message_id, *, notice=False):
        """A message turns the idle or waiting `worker_id` to it, except a peer's notice to an
        ordinary worker that waits (paused): that one reads it when its wait ends. The Integrator
        and reviewers are woken by every message."""
        worker = self._get(c, "workers", worker_id)
        if worker["work_intention"] == "retired" or notice and worker["kind"] == "ordinary" and worker["work_intention"] == "waiting":
            return
        c.execute("INSERT OR IGNORE INTO worker_wake_messages VALUES(?,?,?)", (worker_id, message_id, self.clock()))
        c.execute("UPDATE workers SET pending_wake=1 WHERE id=?", (worker_id,))

    def _refresh_message_wake(self, c, worker_id):
        pending = c.execute("SELECT 1 FROM worker_wake_messages w WHERE w.worker_id=? "
                            "AND NOT EXISTS(SELECT 1 FROM peer_delivery_items i JOIN message_delivery_holds h "
                            "ON h.kind='peer' AND h.delivery_id=i.delivery_id WHERE i.message_id=w.message_id AND h.state IN ('sending','uncertain')) "
                            "AND NOT EXISTS(SELECT 1 FROM human_delivery_items i JOIN message_delivery_holds h "
                            "ON h.kind='human' AND h.delivery_id=i.delivery_id WHERE i.message_id=w.message_id AND h.state IN ('sending','uncertain')) LIMIT 1",
                            (worker_id,)).fetchone() is not None
        c.execute("UPDATE workers SET pending_wake=? WHERE id=?", (int(pending), worker_id))
        return pending

    def _consume_message_wake(self, c, worker_id, message_id):
        c.execute("DELETE FROM worker_wake_messages WHERE worker_id=? AND message_id=?", (worker_id, message_id))
        self._refresh_message_wake(c, worker_id)

    def _ordinary_turn_completed(self, c, worker_id, attempt_id):
        worker, attempt = self._get(c, "workers", worker_id), self._get(c, "attempts", attempt_id)
        if attempt["worker_id"] != worker_id or worker["active_attempt"] not in {None, attempt_id}:
            raise HubError("STALE_BINDING", "The turn does not belong to this worker's current attempt")
        if attempt["ended_at"] is not None:
            return {"directive": "continued" if worker["work_intention"] == "working" else "waiting", "worker_id": worker_id}
        if attempt["state"] != "running":
            raise HubError("NOT_RUNNING", "A draining turn must complete its coordinated transition")
        if c.execute("SELECT 1 FROM turn_intents WHERE attempt_id=? AND state IN ('prepared','sending','bound','uncertain')",
                     (attempt_id,)).fetchone():
            raise HubError("QUIESCENCE_REQUIRED", "A turn must have a confirmed terminal outcome before continuation")
        if attempt["execution_mode"] == "message_only":
            snapshot = self._workspace_snapshot(worker_id)
            c.execute("UPDATE attempts SET state='message_handled',snapshot=?,ended_at=? WHERE id=?", (snapshot, self.clock(), attempt_id))
            c.execute("UPDATE workers SET active_attempt=NULL,state='idle',execution_mode='work',work_intention='idle' WHERE id=?", (worker_id,))
            c.execute("DELETE FROM worker_wake_messages WHERE worker_id=? AND message_id LIKE 'activation:%'", (worker_id,))
            if worker["kind"] in {"review", "integrator"}:
                pending = worker["kind"] == "integrator" or c.execute(
                    "SELECT 1 FROM review_acceptances a JOIN review_requests r ON r.task_id=a.review_task_id "
                    "WHERE a.reviewer_id=? AND r.state='integrating'", (worker_id,)).fetchone()
                c.execute("UPDATE workers SET state=?,work_intention=? WHERE id=?",
                          ("suspended" if pending else "retired", "waiting" if pending else "retired", worker_id))
            self._refresh_message_wake(c, worker_id)
            self._finish_shutdown_if_quiet(c)
            return {"directive": "message_handled", "worker_id": worker_id, "snapshot": snapshot}
        if worker["kind"] == "ordinary":
            self._record_first_turn(c, worker_id, attempt["task_id"])
        if worker["kind"] == "review" and self._review_acceptance(c, attempt["task_id"]):
            c.execute("UPDATE workers SET work_intention='waiting' WHERE id=?", (worker_id,))
            worker = self._get(c, "workers", worker_id)
        snapshot = self._workspace_snapshot(worker_id)
        waiting = worker["work_intention"] == "waiting"
        c.execute("UPDATE attempts SET state='continued',snapshot=?,ended_at=? WHERE id=?", (snapshot, self.clock(), attempt_id))
        c.execute("UPDATE workers SET active_attempt=NULL,state=?,execution_mode='work' WHERE id=?",
                  ("suspended" if waiting else "idle", worker_id))
        self._seal_dependency_reconsideration(c, attempt, snapshot)
        self._refresh_message_wake(c, worker_id)
        self._preserve_member(c, attempt, snapshot, runnable=not waiting,
                              reason="message_handled" if waiting else "automatic_continuation")
        self._feedback_boundary(c, worker_id, attempt["task_id"])
        self._recompute_task(c, attempt["task_id"])
        return {"directive": "waiting" if waiting else "continued", "worker_id": worker_id, "snapshot": snapshot}

    async def ordinary_turn_completed(self, worker_id, attempt_id):
        ended = await self.db.read(lambda c: self._get(c, "attempts", attempt_id)["ended_at"] is not None)
        prepared = {} if ended else await self.prepare_workspace_snapshots([worker_id])
        return await self.db.call(lambda c: self.with_workspace_snapshots(
            c, prepared, lambda connection: self._ordinary_turn_completed(connection, worker_id, attempt_id)), write=True)

    def _admission_candidates(self, c, *, reviews, unstartable=(), fresh_reviews=True, pinned=None, moves=()):
        """In one statement, the reviews (or other tasks) _saved_work_possible may accept, in
        admission order (reviews first; then pinned tasks, or reviews of pinned tasks, as they
        were pinned; then priority and age): a superset it then checks exactly. `pinned` keeps
        only the pinned other tasks (True) or only the rest (False). Saved work of a worker
        whose account cannot start a session now
        (`unstartable`) is left out, as _select_task would skip it, unless the worker, on an
        account of `moves[1]`, asked to move to one of `moves[0]` (_admission_backend); a first
        turn's pending recruitment decision is not, nor is a review that would get a new reviewer
        unless `fresh_reviews`. Checking and reading every open task in full took about 1,100
        statements per admission; on the pilot's busy coordination thread each cost
        milliseconds, two to nine seconds in all."""
        blocked = sorted(unstartable)
        params = list(blocked)
        movable = ""
        if moves:
            targets, sources = sorted(moves[0]), sorted(moves[1])
            movable = (" OR w.backend IN (" + ",".join("?" * len(sources)) + ") AND EXISTS(SELECT 1 FROM "
                       "worker_backend_requests q WHERE q.worker_id=w.id AND q.backend IN ("
                       + ",".join("?" * len(targets)) + "))")
            params += sources + targets
        idle_member = (
            "SELECT 1 FROM task_members m JOIN workers w ON w.id=m.worker_id WHERE m.task_id=t.id "
            "AND m.executor=1 AND m.membership_state='member' AND m.runnable AND w.active_attempt IS NULL "
            "AND w.state='idle' AND (w.backend NOT IN (" + ",".join("?" * len(blocked)) + ")" + movable + ")")
        # A review goes with the pin of the task it reviews; its own priority is its contract's.
        order = ("ORDER BY COALESCE(json_extract(v.contract_json,'$.kind'),'')<>'review',p.pinned_at IS NULL,p.pinned_at,"
                 + ("COALESCE(json_extract(v.contract_json,'$.priority'),1)" if reviews
                    else "COALESCE(p.priority,json_extract(v.contract_json,'$.priority'),1)") + ",t.created_at,t.id")
        current = "JOIN task_revisions v ON v.task_id=t.id AND v.revision=t.current_revision "
        if reviews:
            query = (
                "SELECT t.id FROM tasks t JOIN review_requests r ON r.task_id=t.id " + current +
                "LEFT JOIN task_admission p ON p.task_id=r.host_task_id " +
                "WHERE t.state NOT IN ('done','retired','draining','recovery_required') AND r.state<>'integrating' "
                "AND NOT EXISTS(SELECT 1 FROM integrator_control i WHERE i.task_id=t.id) "
                "AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.ended_at IS NULL "
                "AND a.execution_mode<>'message_only') "
                f"AND (EXISTS({idle_member})" + (" OR NOT EXISTS(SELECT 1 FROM task_members m WHERE m.task_id=t.id "
                "AND m.executor=1 AND m.membership_state='member') AND EXISTS(SELECT 1 FROM evidence e "
                "WHERE e.artifact_id=r.candidate_id AND e.verdict='accepted')" if fresh_reviews else "") + ") " + order)
        else:
            query = (
                "SELECT t.id FROM tasks t LEFT JOIN task_lifecycle l ON l.task_id=t.id " + current +
                "LEFT JOIN task_admission p ON p.task_id=t.id "
                "WHERE t.state NOT IN ('done','retired','draining','recovery_required') "
                + ("" if pinned is None else "AND p.pinned_at IS NOT NULL " if pinned else "AND p.pinned_at IS NULL ") +
                "AND NOT EXISTS(SELECT 1 FROM review_requests r WHERE r.task_id=t.id) "
                "AND NOT EXISTS(SELECT 1 FROM integrator_control i WHERE i.task_id=t.id) "
                "AND (l.task_id IS NULL OR l.suspended=0 AND ("
                "l.first_turn_id IS NOT NULL AND l.recruitment_decision='pending' AND l.decision_helper_id IS NULL "
                "AND EXISTS(SELECT 1 FROM task_members m WHERE m.task_id=t.id AND m.executor=1 "
                "AND m.membership_state='member' AND m.runnable) "
                f"OR EXISTS({idle_member} AND w.work_intention='working' AND w.current_task_id=t.id))) " + order)
        return [row[0] for row in c.execute(query, params)]

    def _saved_work_possible(self, c, task_id):
        """Exclude busy/held saved executions without materializing task content."""
        lifecycle = c.execute("SELECT * FROM task_lifecycle WHERE task_id=?", (task_id,)).fetchone()
        review = c.execute("SELECT state,candidate_id FROM review_requests WHERE task_id=?", (task_id,)).fetchone()
        if review and review["state"] == "integrating":
            return False  # Accepted reviewers answer questions; only the integrator merges.
        if c.execute("SELECT 1 FROM integrator_control WHERE task_id=?", (task_id,)).fetchone():
            return False  # Batch admission owns the dedicated system task.
        if review and c.execute("SELECT 1 FROM attempts WHERE task_id=? AND ended_at IS NULL AND execution_mode<>'message_only'",
                                (task_id,)).fetchone():
            return False
        members = c.execute(
            "SELECT m.runnable,w.* FROM task_members m JOIN workers w ON w.id=m.worker_id "
            "WHERE m.task_id=? AND m.executor=1 AND m.membership_state='member' "
            "ORDER BY m.joined_at,w.worker_number", (task_id,)).fetchall()
        if review:
            # An unowned review gets a new reviewer once machine validation accepted its
            # exact candidate. Keep the full provenance and basis checks in _select_task/_claim.
            if not members:
                return self._accepted_validation(c, self._candidate_binding(c, review["candidate_id"])) is not None
            return bool(members[0]["runnable"] and members[0]["active_attempt"] is None
                        and members[0]["state"] == "idle")
        if lifecycle is None:
            return True  # Protocol tasks initialize lifecycle metadata at their first claim.
        if lifecycle["suspended"]:
            return False
        for member in members:
            if not member["runnable"]:
                continue
            if (lifecycle["first_turn_id"] and lifecycle["recruitment_decision"] == "pending"
                    and not lifecycle["decision_helper_id"]):
                return True  # _select_task also retains the first-turn recruitment decision.
            if (member["active_attempt"] is None and member["state"] == "idle"
                    and member["work_intention"] == "working" and member["current_task_id"] == task_id
                    and not c.execute("SELECT 1 FROM helper_intents WHERE source_worker_id=? AND kind='recruitment' "
                                      "AND state IN ('prepared','running','uncertain')", (member["id"],)).fetchone()):
                return True
        return False


    def _select_task(self, c, task, run_id, scope=None, startable=None, fresh_reviews=True):
        """Dispatch only saved work or an independently authorized opportunity.

        A scope (see _admission_scope) limits dispatch to those workers' own work. `startable`,
        when given, says whether a worker's account can start a session now; a worker whose
        account cannot is not claimed. `fresh_reviews`: whether an open review may get its own
        new reviewer now, not while reviewers on its account are paused (by the run or their group).
        """
        if task["state"] in TERMINAL | {"draining", "recovery_required"}:
            return None
        lifecycle = self._lifecycle(c, task["id"])
        if task["contract"]["kind"] in REVIEW_KINDS:
            if self._active_attempts(c, task["id"]):
                return None
            # The same temporary reviewer owns interrupted/integrating work.
            for member in task["collaboration"]["members"]:
                if not member["executor"] or member.get("membership_state", "member") != "member":
                    continue
                worker = self._get(c, "workers", member["worker_id"])
                if (member["runnable"] and worker["active_attempt"] is None and worker["state"] == "idle"
                        and (startable is None or startable(worker))
                        and self._scope_admits(c, run_id, scope, worker["id"])):
                    return self._assignment_metadata(c, self._claim(c, task["id"], task["revision"], worker["id"], run_id))
                return None
            if fresh_reviews:
                # Once machine validation accepts its exact candidate, an open review gets
                # its own new reviewer at once, in a new conversation: no worker selects it
                # and nobody's history is copied.
                candidate = self._open_candidate(c, task["id"])
                if candidate is not None and candidate["opportunity"] == "review":
                    return self._fresh_review(c, candidate, run_id)
            return None
        if lifecycle["suspended"]:
            return None
        for member in task["collaboration"]["members"]:
            if not member["executor"] or not member["runnable"] or member.get("membership_state", "member") != "member":
                continue
            worker = self._get(c, "workers", member["worker_id"])
            if lifecycle["first_turn_id"] and lifecycle["recruitment_decision"] == "pending" and not lifecycle["decision_helper_id"]:
                self._record_first_turn(c, worker["id"], task["id"])
            if (worker["active_attempt"] is None and worker["state"] == "idle"
                    and worker["work_intention"] == "working" and worker["current_task_id"] == task["id"]
                    and (startable is None or startable(worker))
                    and self._scope_admits(c, run_id, scope, worker["id"])
                    and not c.execute("SELECT 1 FROM helper_intents WHERE source_worker_id=? AND kind='recruitment' "
                                      "AND state IN ('prepared','running','uncertain')", (worker["id"],)).fetchone()):
                return self._assignment_metadata(c, self._claim(c, task["id"], task["revision"], worker["id"], run_id))
        return None

    def _fresh_review(self, c, candidate, run_id):
        """Give one open review its own new reviewer; a failed claim leaves no reviewer behind."""
        c.execute("SAVEPOINT fresh_review")
        reviewer_id = None
        try:
            reviewer_id = self._new_worker(c, kind="review")
            result = self._assignment_metadata(c, self._claim(
                c, candidate["task_id"], candidate["revision"], reviewer_id, run_id))
        except BaseException:
            c.execute("ROLLBACK TO fresh_review")
            c.execute("RELEASE fresh_review")
            if reviewer_id is not None:
                self.discard_workspace(reviewer_id)
            raise
        c.execute("RELEASE fresh_review")
        self._event(c, "worker.fresh_created", reviewer_id,
                    {"task_id": candidate["task_id"], "opportunity": "review", "kind": "review"},
                    candidate["task_id"], reviewer_id)
        return result

    def _message_assignment(self, c, worker, run_id):
        if not worker["pending_wake"] or worker["active_attempt"] or worker["work_intention"] not in {"waiting", "idle"}:
            return None
        if not self._refresh_message_wake(c, worker["id"]):
            return None
        task_id = worker["current_task_id"]
        if worker["state"] == "recovery_required" or c.execute(
                "SELECT 1 FROM interruption_checkpoints WHERE worker_id=? AND state='parked'", (worker["id"],)).fetchone():
            return None
        answers_only = (worker["kind"] == "integrator"
                        or (worker["kind"] == "review" and self._review_acceptance(c, task_id))
                        or not task_id or self._task(c, task_id)["state"] in TERMINAL)
        if answers_only:
            return self._message_only_assignment(c, worker, run_id)
        continuation = c.execute("SELECT * FROM continuations WHERE preferred_worker=? AND task_id=? AND state='waiting' "
                                 "ORDER BY created_at DESC LIMIT 1", (worker["id"], task_id)).fetchone()
        if continuation and (continuation["snapshot"] is None or self._get(c, "attempts", continuation["origin_attempt"])["ended_at"] is None):
            return None
        result = self._claim(c, task_id, self._task(c, task_id)["revision"], worker["id"], run_id,
                             continuation["id"] if continuation else None, message_wake=True)
        c.execute("UPDATE workers SET work_intention='waiting',execution_mode='message',pending_wake=0 WHERE id=?", (worker["id"],))
        return self._assignment_metadata(c, result, "message")

    def _message_only_assignment(self, c, worker, run_id):
        previous = c.execute("SELECT task_id FROM attempts WHERE worker_id=? AND execution_mode<>'message_only' "
                             "ORDER BY created_at DESC,rowid DESC LIMIT 1", (worker["id"],)).fetchone()
        if previous is None:
            root = c.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()["root_task_id"]
            if root is None:
                return None
            anchor = root
        else:
            anchor = previous["task_id"]
        task = self._task(c, anchor)
        self._run_controls(c, run_id, worker["id"])
        attempt_id, fence = new_id(), task["fencing_counter"] + 1
        c.execute("INSERT INTO attempts(id,task_id,task_revision,worker_id,run_id,fencing_token,state,created_at,execution_mode) "
                  "VALUES(?,?,?,?,?,?,'running',?,'message_only')", (attempt_id, task["id"], task["revision"], worker["id"], run_id, fence,
                  self.clock()))
        c.execute("UPDATE workers SET active_attempt=?,run_id=?,state='running',execution_mode='message_only',pending_wake=0 WHERE id=?",
                  (attempt_id, run_id, worker["id"]))
        c.execute("INSERT INTO worker_wake_messages VALUES(?,?,?)", (worker["id"], "activation:" + attempt_id, self.clock()))
        result = self._get(c, "attempts", attempt_id)
        result["task"] = task
        self._event(c, "worker.message_activation", attempt_id, {"audit_anchor": task["id"], "task_authority": False}, worker_id=worker["id"])
        return self._assignment_metadata(c, result, "message_only")

    def _admission_fault(self, c, subject, task_id, error):
        """Skip one item admission cannot evaluate; report each distinct fault once."""
        detail = [getattr(error, "code", None) or type(error).__name__,
                  str(getattr(error, "message", error))[:1000]]
        reported = self.__dict__.setdefault("_reported_admission_faults", {})
        if reported.get(subject) == detail:
            return
        reported[subject] = detail
        logger.warning("Admission skipped %s: %s: %s", subject, *detail,
                       exc_info=None if isinstance(error, HubError) else error)
        self._event(c, "admission.item_skipped", subject,
                    {"error_code": detail[0], "error": detail[1]}, task_id)

    def _backend_full(self, c, backend, *, admitted=False):
        """Whether an ordinary worker must wait for one of this backend's concurrent sessions
        (`concurrency`; a limit of 0 starts none, -1 sets no limit). Reviewers and the
        Integrator, which keep accepted work moving, are admitted anyway but count.
        `admitted`: the session being admitted is already counted."""
        limit = self._session_limit(c, backend)
        if limit < 0:
            return False
        live = c.execute("SELECT COUNT(*) FROM attempts a JOIN workers w ON w.id=a.worker_id "
                         "WHERE a.ended_at IS NULL AND " + SESSION_BACKEND + "=?", (backend,)).fetchone()[0]
        return live > limit if admitted else live >= limit

    def _ramping(self):
        """Whether dispatch is still ramping up: launch spacing applies for launch_ramp_minutes
        after the service starts or the run starts or resumes (0: until each account fills)."""
        if self._ramp_started is None:
            self._ramp_started = self.clock()  # The service's first admission.
        minutes = self.config.concurrency.launch_ramp_minutes
        return not minutes or self.clock() - self._ramp_started < minutes * 60_000

    def _restart_ramp(self):
        """The run starts or resumes: worker sessions ramp up again on every account."""
        self._ramp_started = self.clock()
        self._saturated.clear()

    def _spacing_blocked(self, c):
        """The accounts whose next ordinary worker session must wait for launch spacing now (see
        _admit_worker_session): while dispatch ramps up, those not yet filled whose own last
        worker session started recently."""
        seconds = self.config.concurrency.launch_seconds
        if not seconds or not self._ramping():
            return set()
        recent = self.clock() - seconds * 1000
        return {backend for backend, last in c.execute(
            "SELECT w.backend,MAX(a.created_at) FROM attempts a JOIN workers w ON w.id=a.worker_id "
            "WHERE w.kind='ordinary' GROUP BY w.backend") if backend not in self._saturated and last > recent}

    def _admit_worker_session(self, c, backend, attempt_id):
        """Refuse an ordinary worker's admitted session when its backend has no free session,
        or, while dispatch ramps up and until the backend first fills, when the backend's
        previous worker session started less than launch_seconds ago."""
        if self._backend_full(c, backend, admitted=True):
            raise HubError("BACKEND_BUSY", "This backend's concurrent sessions are all in use", 409)
        seconds = self.config.concurrency.launch_seconds
        if seconds and backend not in self._saturated and self._ramping():
            last = c.execute("SELECT MAX(a.created_at) FROM attempts a JOIN workers w ON w.id=a.worker_id "
                             "WHERE w.kind='ordinary' AND " + SESSION_BACKEND + "=? AND a.id<>?",
                             (backend, attempt_id)).fetchone()[0]
            if last is not None and self.clock() - last < seconds * 1000:
                raise HubError("LAUNCH_SPACING", "Worker sessions are starting one at a time", 409)
        if self._backend_full(c, backend):
            self._saturated.add(backend)  # Filled once: freed sessions are taken at once from now on.

    def _new_worker_free(self, c, held):
        """Whether a new worker can take a session now: on any account of the assignment plan,
        since a new worker skips the numbers of full accounts (_free_number), or else on the
        operator's choice."""
        plan = self._worker_plan(c)
        return any(backend not in held and not self._backend_full(c, backend)
                   for backend in (dict.fromkeys(plan) if plan else [self._creation_backend(c)]))

    def _free_number(self, c):
        """The number the next new worker gets. The assignment plan fixes a worker's account by
        its number: when the next number's account is held, full or has its workers paused while
        another account of the plan is free, it is the first later number on a free account.
        Each account keeps its numbers, and the skipped ones stay unused."""
        number, plan = next_worker_number(c), self._worker_plan(c)
        free = self._free_plan_backends(c, plan)
        while free and self.config.assignment.backend("ordinary", number) not in free:
            number = unkept_number(c, number + 1)  # A number a task keeps is never skipped to.
        return number

    def _free_plan_backends(self, c, plan):
        held = set(self._dispatch_holds(c)) | self._paused_backends(c, "ordinary")
        return {backend for backend in plan if backend not in held and not self._backend_full(c, backend)}

    def _skip_full_numbers(self, c):
        c.execute("UPDATE worker_name_sequence SET next_number=max(next_number,?) WHERE id=1", (self._free_number(c),))

    def _kept_number_backend(self, c, number):
        """The account of a new worker that takes the number its task keeps: the plan's for that
        number, or, when that account is held, full or paused, another free account of the plan
        (the number cannot move instead). Without a plan, the operator's or the project's choice."""
        plan = self._worker_plan(c)
        if not plan:
            return None
        planned, free = self.config.assignment.backend("ordinary", number), self._free_plan_backends(c, plan)
        return planned if planned in free or not free else next(b for b in dict.fromkeys(plan) if b in free)

    def _admit_item(self, c, savepoint, subject, task_id, skippable, operation):
        """Run one admission candidate; isolate its failure from every other item."""
        c.execute("SAVEPOINT " + savepoint)
        try:
            result = operation()
        except BaseException as error:
            c.execute("ROLLBACK TO " + savepoint)
            c.execute("RELEASE " + savepoint)
            if not isinstance(error, ITEM_FAULTS) or isinstance(error, sqlite3.Error):
                raise
            if isinstance(error, HubError):
                if error.code == "WORKER_WORKSPACE_UNAVAILABLE":
                    return ("quarantined", self._quarantine_workspace_failure(c, error))
                if error.code in ADMISSION_GLOBAL_ERRORS:
                    raise
                if error.code in skippable:
                    return ("skipped", None)
            self._admission_fault(c, subject, task_id, error)
            return ("skipped", None)
        c.execute("RELEASE " + savepoint)
        return ("done", result)

    async def unlaunched_claim(self, attempt_id):
        """Whether a claim still waits for its activation: its worker's running attempt, with no
        turn reserved yet. The run driver launches an operator's claim (task.join) only then."""
        return await self.db.read(lambda c: c.execute(
            "SELECT 1 FROM attempts a JOIN workers w ON w.active_attempt=a.id WHERE a.id=? "
            "AND a.state='running' AND a.ended_at IS NULL "
            "AND NOT EXISTS(SELECT 1 FROM turn_intents t WHERE t.attempt_id=a.id)", (attempt_id,)).fetchone() is not None)

    async def next_assignment(self, run_id, *, prepare=True):
        """The next admissible work, claimed. `prepare=False` returns the claim before its Library
        checkout: the run driver prepares that in the worker's own activation, and no model turn
        starts before it (turn.reserve). Prepared here, each admission waited about 20 s for it,
        and admission takes one item at a time."""
        def select(c):
            try:
                # While the run is paused, individually resumed workers continue only their own
                # current work: no new task or new worker. A resumed group runs in full.
                scope = self._admission_scope(c, run_id)
            except HubError as error:
                if error.code in {"RUN_PAUSED", "PROJECT_STOPPED", "USAGE_UNCERTAIN"}:
                    return None
                raise
            held = set(self._dispatch_holds(c))
            if held >= set(BACKENDS):
                return None  # Recovery bookkeeping continues; no new model input is admitted.
            # Any worker of a held account, and an ordinary worker of an account whose sessions
            # are all in use or whose next session waits for launch spacing, cannot start now: it
            # is skipped before its claim, which unheld would only roll back. Such claims were
            # most of each pass, with every session in use and later during the spacing.
            closed = {backend for backend in BACKENDS if self._backend_full(c, backend)} | self._spacing_blocked(c)
            # Likewise the accounts whose workers or reviewers are paused, by the run or by their
            # group, except for people resumed on their own: with Codex's workers paused, their
            # saved work would otherwise be read in full on every pass only to be skipped.
            stopped = {kind: self._paused_backends(c, kind, run_id) for kind in ("ordinary", "review")}
            exempt = self._resumed_individually(c, run_id)
            paused = {kind: backends - exempt for kind, backends in stopped.items()}
            fresh_reviews = self._creation_backend(c, "review") not in stopped["review"]

            def startable(worker, worker_id=None):
                # A worker that asked to move needs a session on the account it moves to (None: it waits).
                backend = self._admission_backend(c, worker_id or worker["id"], worker["kind"], worker["backend"])
                return (backend is not None and backend not in held
                        and (worker["kind"] != "ordinary" or backend not in closed))

            def unheld(assignment):
                # The savepoint undoes an admission (and any fresh worker) for a held account,
                # or for one whose concurrent turns are all in use.
                worker_id = (assignment or {}).get("worker_id")
                if not worker_id:
                    return assignment
                worker = self._get(c, "workers", worker_id)
                backend = self._admission_backend(c, worker_id, worker["kind"], worker["backend"])
                if backend is None:
                    raise HubError("SWITCH_WAITING", "This worker waits to move to the backend it asked for", 409)
                if backend in held:
                    raise HubError("DISPATCH_HELD", "This worker's backend account is held", 409)
                if worker["kind"] == "ordinary":
                    if backend != worker["backend"]:
                        # It moves: this session runs, and counts, on the account it asked for.
                        self._prepare_backend_switch(c, worker_id, assignment["id"])
                    self._admit_worker_session(c, backend, assignment["id"])
                return assignment
            runner = Actor(role=Role.RUNNER)
            if scope is None and self.config.repository.enabled:
                self._enable_integrator(c)  # A pending handoff does not block unrelated work.
            skippable = {"INTERRUPTION_NOT_READY", "CONTINUATION_NOT_READY", "ALREADY_CLAIMED",
                         "STALE_REVISION", "PREREQUISITES_MISSING", "INPUTS_CHANGED", "NOT_FOUND",
                         "INTERRUPTION_RESERVED", "CONTINUATION_RESERVED", "REVIEWER_INELIGIBLE",
                         "RECOVERY_REQUIRED", "WORKER_RETIRED", "MEMBERSHIP_ENDED", "LEAN_ENVIRONMENT_MISMATCH",
                         "INTEGRATION_QUEUED", "WORKER_PAUSED", "DISPATCH_HELD", "BACKEND_BUSY",
                         "LAUNCH_SPACING", "SWITCH_WAITING"}
            def assign_tasks(reviews, pinned=None):
                # Reviewers are not ordinary workers: only a held account, or their pause, stops them.
                unstartable = held | paused["review"] if reviews else held | closed | paused["ordinary"]
                # A worker on an unpaused account that asked to move counts with the account it moves to.
                moves = () if reviews else (set(BACKENDS) - unstartable, set(BACKENDS) - paused["ordinary"])
                # In admission order already; a task is read in full only when its turn comes.
                for task_id in self._admission_candidates(c, reviews=reviews, unstartable=unstartable,
                                                          fresh_reviews=fresh_reviews, pinned=pinned, moves=moves):
                    while True:
                        outcome, result = self._admit_item(
                            c, "lifecycle_assignment", task_id, task_id, skippable,
                            lambda: unheld(self._select_task(c, self._task(c, task_id), run_id, scope, startable,
                                                             fresh_reviews))
                            if self._saved_work_possible(c, task_id) else None)
                        if outcome == "done" and result:
                            return result
                        if outcome == "quarantined" and result:
                            continue  # Another current member may still be runnable.
                        break
                return None

            # A due batch goes first, then reviews, then resumptions, messages and other tasks.
            # Each call admits one item in this order. Behind resumptions and messages a due
            # batch waited 20 minutes on a busy run with fifteen contributions ready, and no new
            # reviewer started for over half an hour with 33 reviews pending (Linux pilot). When
            # no batch is due its check costs a count, or a row while a batch is open.
            outcome, batch_work = self._admit_item(c, "batch_admission", "integrator", None, skippable,
                lambda: unheld(self._batch_assignment(c, run_id, scope)))
            if outcome == "done" and batch_work:
                return batch_work
            review = assign_tasks(reviews=True)
            if review is not None:
                return review
            # A ticket whose worker is busy (`free` fails) would only end in ALREADY_CLAIMED, after
            # reading its task in full: five of them did on every admission in the pilot.
            for table, command, key, owner, predicate, free in (
                ("interruption_checkpoints", "interruption.resume", "interruption_id", "worker_id", "state='parked'",
                 "w.active_attempt IS NULL AND w.state='suspended'"),
                ("continuations", "continuation.resume", "continuation_id", "preferred_worker",
                 "state IN ('eligible','reconsider')",
                 "w.active_attempt IS NULL AND w.state NOT IN ('preparing','running','draining')"),
            ):
                for ticket in c.execute(
                        f"SELECT x.id,x.task_id,x.{owner} AS owner,w.kind,w.backend FROM {table} x "
                        f"LEFT JOIN workers w ON w.id=x.{owner} WHERE x.{predicate} AND (w.id IS NULL OR {free}) "
                        "ORDER BY x.created_at,x.id").fetchall():
                    if ticket["kind"] is not None and not startable(ticket, ticket["owner"]):
                        continue  # Both resume on their original worker.
                    if ticket["owner"] in failed_snapshots or not self._scope_admits(c, run_id, scope, ticket["owner"]):
                        continue
                    outcome, result = self._admit_item(
                        c, "lifecycle_resume", ticket["id"], ticket["task_id"], skippable,
                        lambda: unheld(self._dispatch(c, command, {key: ticket["id"], "run_id": run_id}, runner)))
                    if outcome == "done":
                        return self._assignment_metadata(c, result)
            # A worker with work in hand reads its messages in that work's turn: _message_assignment
            # declines it at once, so it is not even tried (about thirty per admission in the pilot).
            for row in c.execute("SELECT * FROM workers WHERE pending_wake=1 AND active_attempt IS NULL "
                                 "AND work_intention IN ('waiting','idle') ORDER BY created_at,id").fetchall():
                if not startable(row) or not self._scope_admits(c, run_id, scope, row["id"]):
                    continue
                outcome, result = self._admit_item(
                    c, "lifecycle_message", row["id"], row["current_task_id"], skippable,
                    lambda: unheld(self._message_assignment(c, dict(row), run_id)))
                if outcome == "done" and result:
                    return result

            def assign_opportunities(pinned):
                if scope is not None and not any(kind == "ordinary" for _, kind in scope.groups):
                    return None  # While the run is paused, new work goes only to a resumed group of workers.
                if paused["ordinary"] >= set(BACKENDS):
                    return None  # Every account's workers are paused.
                outcome, result = self._admit_item(c, "lifecycle_opportunities", "opportunities", None, skippable,
                                                   lambda: unheld(self._assign_opportunities(c, run_id, scope, pinned)))
                return result if outcome == "done" else None
            # Tasks the operator pinned go before other saved work and new tasks: their saved work,
            # then their opportunities (task.set_pinned). Without a pin this costs one row.
            if c.execute("SELECT 1 FROM task_admission WHERE pinned_at IS NOT NULL LIMIT 1").fetchone():
                pinned = assign_tasks(reviews=False, pinned=True) or assign_opportunities(pinned=True)
                if pinned is not None:
                    return pinned
            other = assign_tasks(reviews=False, pinned=False)
            if other is not None:
                return other
            return assign_opportunities(pinned=False)
        prepared: dict[str, dict] = {}
        failed_snapshots: set[str] = set()
        with watched("admission") as watch:
            while True:
                try:
                    watch.detail = "selecting"
                    result = await self.db.call(lambda c: self._with_deferred_files(
                        c, lambda c: self.with_workspace_snapshots(c, prepared, select)), write=True)
                    await self.finish_deferred_files("next_assignment", archive_moves=False)
                    if not prepare:
                        return result
                    if isinstance(result, dict) and result.get("worker_id"):
                        watch.detail = f"preparing the workspace of worker {result['worker_id']}"
                    ready = await self.prepare_assignment_workspace(result, tolerate_failure=True)
                    if result is not None and ready is None:
                        prepared.clear()
                        continue  # A failed local checkout must not defer unrelated work.
                    return ready
                except SnapshotRequired as needed:
                    worker_id = needed.worker_id
                    watch.detail = f"capturing the workspace of worker {worker_id}"
                    try:
                        prepared.update(await self.prepare_workspace_snapshots([worker_id]))
                    except ITEM_FAULTS as error:
                        if isinstance(error, sqlite3.Error):
                            raise
                        # A corrupt waiting workspace must not monopolize admission.
                        # Keep its ticket intact and report it once; consider others.
                        await self.db.write(lambda c, failure=error: self._admission_fault(c, worker_id, None, failure))
                        failed_snapshots.add(worker_id)

    def _authorize_participant(self, c, task, actor):
        if actor.role == Role.OPERATOR:
            return
        self._role(actor, Role.WORKER)
        self._active(c, actor)
        if not c.execute("SELECT 1 FROM task_publishers WHERE task_id=? AND worker_id=? UNION "
                         "SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? AND executor=1 "
                         "AND membership_state='member'", (task["id"], actor.worker_id, task["id"], actor.worker_id)).fetchone():
            raise HubError("PARTICIPANT_REQUIRED", "Original publishers and current executors maintain this task", 403)

    def _conditions(self, c, task):
        original = self._original_task(c, task["id"])["contract"]
        conditions = {"objective": original["objective"]}
        conditions.update({f"constraint:{index}": value for index, value in enumerate(original["constraints"])})
        conditions.update({f"requirement:{item['key']}": canonical(item) for item in original["requirements"]})
        # Later additions strengthen the obligation. Earlier requirements remain
        # historical obligations even when execution instructions are narrowed.
        conditions.update({f"current-requirement:{item['key']}": canonical(item)
                           for item in task["contract"]["requirements"] if item not in original["requirements"]})
        conditions.update({f"current-constraint:{index}": value for index, value in enumerate(task["contract"]["constraints"])
                           if value not in original["constraints"]})
        if task["contract"]["objective"] != original["objective"]:
            conditions["current-objective"] = task["contract"]["objective"]
        original_acceptance = original.get("payload", {}).get("acceptance_conditions", [])
        for index, value in enumerate(original_acceptance):
            conditions[f"acceptance:{index}"] = str(value)
        for index, value in enumerate(task["contract"].get("payload", {}).get("acceptance_conditions", [])):
            if value not in original_acceptance:
                conditions[f"current-acceptance:{index}"] = str(value)
        conditions.update({"repair-original:" + key: value for key, value in self._repair_conditions(c, task["id"]).items()})
        return conditions

    def _accepted_reference(self, c, artifact_id):
        from .native import MATHEMATICAL_KINDS
        artifact = self._artifact(c, artifact_id)
        if artifact["status"] != "accepted":
            raise HubError("ACCEPTANCE_EVIDENCE_REQUIRED", "Completion evidence must reference currently accepted results")
        if not self._available_artifact(c, artifact_id):
            raise HubError("ACCEPTANCE_EVIDENCE_REQUIRED", "Completion evidence depends on a withdrawn input")
        if artifact["kind"] in MATHEMATICAL_KINDS and not self._merge_published(c, artifact_id):
            raise HubError("ARTIFACT_GATE", "Mathematical acceptance needs an exact successful Library publication receipt")
        binding = artifact.get("candidate_binding")
        receipt = None
        if binding:
            receipt = c.execute("SELECT id FROM peer_review_receipts WHERE candidate_id=? AND verdict='approve' "
                                "AND qualifications_json='[]'", (artifact_id,)).fetchone()
            if receipt is None or not self._merge_published(c, artifact_id):
                raise HubError("ARTIFACT_GATE", "Required results need independent acceptance and successful publication")
        publication = c.execute("SELECT integration_id,commit_sha,snapshot_sha FROM repository_publications "
                                "WHERE candidate_id=? ORDER BY created_at DESC LIMIT 1", (artifact_id,)).fetchone()
        return {"artifact_id": artifact_id, "body_sha": artifact["body_sha"],
                "review_receipt_id": receipt["id"] if receipt else None,
                "publication": dict(publication) if publication else None,
                "evidence_ids": [item["id"] for item in artifact["evidence"]
                                 if item["verdict"] == "accepted" and not item["qualifications"]]}

    def _completion_evidence(self, c, task, evidence):
        conditions = self._conditions(c, task)
        if not isinstance(evidence, dict) or set(evidence) != set(conditions):
            raise HubError("ACCEPTANCE_MAP_REQUIRED", "Map every original and current acceptance condition to exact accepted results", 422)
        normalized = {}
        for key, mapping in evidence.items():
            if (not isinstance(mapping, dict) or not isinstance(mapping.get("explanation"), str)
                    or not mapping["explanation"].strip() or len(mapping["explanation"]) > 10000):
                raise HubError("ACCEPTANCE_MAP_REQUIRED", "Each acceptance condition needs a bounded explanation", 422)
            ids = mapping.get("artifact_ids", [item["artifact_id"] for item in mapping.get("results", [])])
            if not isinstance(ids, list) or not ids or len(ids) > 100 or not all(isinstance(item, str) for item in ids):
                raise HubError("ACCEPTANCE_EVIDENCE_REQUIRED", "Each condition needs accepted result references", 422)
            normalized[key] = {"explanation": mapping["explanation"],
                               "results": [self._accepted_reference(c, artifact_id) for artifact_id in ids]}
            if key in {"objective", "current-objective"} and not any(
                    result["review_receipt_id"] for result in normalized[key]["results"]):
                raise HubError("INDEPENDENT_EVIDENCE_REQUIRED", "Objective fulfillment needs independently accepted results")
        original = self._original_task(c, task["id"])["contract"]
        if any(not self._requirement(c, requirement) for requirement in original["requirements"] + task["contract"]["requirements"]):
            raise HubError("PREREQUISITES_MISSING", "Original and current acceptance dependencies must remain satisfied")
        return conditions, normalized

    def _required_contributions(self, c, task_id):
        obligations = [dict(row) for row in c.execute("SELECT * FROM artifact_obligations WHERE task_id=?", (task_id,))]
        children = {item["predecessor_id"]: item["artifact_id"] for item in obligations if item["predecessor_id"]}
        required = {}
        for obligation in obligations:
            if not obligation["required"] or obligation["withdrawn_reason"]:
                continue
            artifact_id, visited = obligation["artifact_id"], set()
            while artifact_id in children and artifact_id not in visited:
                visited.add(artifact_id)
                artifact_id = children[artifact_id]
            required[obligation["artifact_id"]] = artifact_id
        return required

    def _artifact_gate(self, c, task_id):
        for attempt in self._active_attempts(c, task_id):
            submission = json.loads(attempt["submission_json"] or "{}")
            if submission.get("kind") and not submission.get("release") and "review" not in submission:
                raise HubError("ARTIFACT_GATE", "A durable contribution is still draining into its frozen review candidate; complete that submission before closing the task")
        for original, artifact_id in self._required_contributions(c, task_id).items():
            try:
                self._accepted_reference(c, artifact_id)
            except HubError as error:
                raise HubError("ARTIFACT_GATE", "Required contribution " + original
                               + " still needs independent acceptance and publication (latest version " + artifact_id + ")") from error

    def _completion_record(self, c, declaration_id):
        row = c.execute("SELECT * FROM task_completion_declarations WHERE id=?", (declaration_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Completion declaration does not exist", 404)
        result = dict(row)
        for name in ("acceptance", "evidence", "feedback", "resolution"):
            value = result.pop(name + "_json")
            result[name] = json.loads(value) if value else None
        invalidations = [item for item in self._completion_invalidations(c, result["task_id"], result["task_revision"])
                         if item["declaration_id"] == declaration_id]
        if invalidations:
            result["current_evidence"] = {"state": "invalid", "invalidations": invalidations}
        return result

    def _ongoing_executor(self, c, task_id, worker_id):
        return bool(c.execute("SELECT 1 FROM task_members m JOIN workers w ON w.id=m.worker_id "
                              "WHERE m.task_id=? AND m.worker_id=? AND m.executor=1 AND m.membership_state='member' "
                              "AND w.kind='ordinary' AND w.current_task_id=m.task_id AND w.work_intention='working'",
                              (task_id, worker_id)).fetchone())

    def _declare_completion(self, c, task, p, actor):
        self._authorize_participant(c, task, actor)
        self._expected_revision(task, p)
        if task["contract"]["kind"] in REVIEW_KINDS or task["state"] in TERMINAL:
            raise HubError("COMPLETION_PROTOCOL", "Only unfinished ordinary tasks use a completion declaration")
        self._artifact_gate(c, task["id"])
        conditions, evidence = self._completion_evidence(c, task, p.get("evidence"))
        if c.execute("SELECT 1 FROM task_completion_declarations WHERE task_id=? AND state='pending'", (task["id"],)).fetchone():
            raise HubError("COMPLETION_PENDING", "Resolve the existing completion declaration first")
        peer = next((row["worker_id"] for row in c.execute(
            "SELECT worker_id FROM task_members WHERE task_id=? ORDER BY joined_at,worker_id", (task["id"],))
            if row["worker_id"] != actor.worker_id and self._ongoing_executor(c, task["id"], row["worker_id"])), None)
        declaration_id = new_id()
        c.execute("INSERT INTO task_completion_declarations(id,task_id,task_revision,original_contract_hash,declarer_id,"
                  "acceptance_json,evidence_json,feedback_worker_id,feedback_state,state,created_at) "
                  "VALUES(?,?,?,?,?,?,?,?,?,'pending',?)",
                  (declaration_id, task["id"], task["revision"], self._original_task(c, task["id"])["contract_hash"],
                   actor.worker_id or "operator", canonical(conditions), canonical(evidence), peer,
                   "pending" if peer else "not_required", self.clock()))
        self._event(c, "task.completion_declared", declaration_id,
                    {"declaration_id": declaration_id, "feedback_worker_id": peer,
                     "acceptance_conditions": conditions, "evidence": evidence}, task["id"], actor.worker_id)
        if peer:
            c.execute("INSERT INTO notifications(id,worker_id,task_id,object_id,reason,created_at) VALUES(?,?,?,?,?,?)",
                      (new_id(), peer, task["id"], declaration_id, "completion_feedback_opportunity", self.clock()))
        return self._completion_record(c, declaration_id)

    def _cancel_completion(self, c, p, actor):
        """Recover a lost declarer's reservation without inheriting its decision."""
        self._role(actor, Role.OPERATOR)
        declaration = self._completion_record(c, p["declaration_id"])
        if declaration["state"] != "pending":
            raise HubError("STALE_COMPLETION", "Only a pending declaration can be cancelled for recovery")
        reason = p.get("reason")
        if (p.get("recovery_impossible") is not True or p.get("process_confirmed_stopped") is not True
                or not isinstance(reason, str) or not reason.strip() or len(reason) > 20000):
            raise HubError("QUIESCENCE_REQUIRED", "Record why the declarer cannot recover and confirm its processes stopped")
        if declaration["declarer_id"] == "operator":
            raise HubError("DECLARER_REQUIRED", "The operator can resolve its own declaration with a continue decision")
        worker = self._get(c, "workers", declaration["declarer_id"])
        if worker["active_attempt"]:
            raise HubError("QUIESCENCE_REQUIRED", "Recover or drain the declarer's active attempt before cancelling its declaration")
        if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? "
                     "AND state IN ('prepared','sending','bound','uncertain','reconciled')", (worker["id"],)).fetchone():
            raise HubError("TURN_RECONCILIATION_REQUIRED", "Reconcile the declarer's native turn and usage first")
        recovery = {"declarer_id": worker["id"], "recovery_impossible": True, "process_confirmed_stopped": True,
                    "snapshot": self._workspace_snapshot(worker["id"]), "reason": reason}
        c.execute("UPDATE task_completion_declarations SET state='cancelled',decision_reason=?,resolution_json=?,decided_at=? WHERE id=?",
                  (reason, canonical({"recovery": recovery}), self.clock(), declaration["id"]))
        for ticket in c.execute("SELECT id,predicate_json FROM continuations WHERE state IN ('waiting','eligible')").fetchall():
            if json.loads(ticket["predicate_json"]).get("completion_declaration_id") == declaration["id"]:
                c.execute("UPDATE continuations SET state='reconsider',wake_reason='completion_declaration_cancelled' WHERE id=?", (ticket["id"],))
        self._event(c, "task.completion_cancelled", declaration["id"], recovery, declaration["task_id"])
        self._notify(c, declaration["task_id"], declaration["id"], "completion_declaration_cancelled")
        return self._completion_record(c, declaration["id"])

    def _feedback_boundary(self, c, worker_id, task_id):
        for declaration in c.execute("SELECT * FROM task_completion_declarations WHERE task_id=? "
                                     "AND feedback_worker_id=? AND state='pending' AND feedback_state='pending'",
                                     (task_id, worker_id)).fetchall():
            event = c.execute("SELECT sequence FROM events WHERE kind='task.completion_declared' AND object_id=?",
                              (declaration["id"],)).fetchone()
            delivered = c.execute("SELECT MAX(sequence) FROM deliveries WHERE worker_id=? AND state='delivered'",
                                  (worker_id,)).fetchone()[0]
            if event and delivered is not None and delivered >= event["sequence"]:
                c.execute("UPDATE task_completion_declarations SET feedback_state='opportunity_passed',feedback_json=? WHERE id=?",
                          (canonical({"conditions": [], "reason": "A delivered feedback opportunity reached a safe turn boundary"}),
                           declaration["id"]))
                self._wake(c)
            else:
                worker = self._get(c, "workers", worker_id)
                member = c.execute("SELECT membership_state FROM task_members WHERE task_id=? AND worker_id=?",
                                   (task_id, worker_id)).fetchone()
                departed = member is not None and member["membership_state"] != "member"
                if (worker["active_attempt"] is None and worker["state"] != "recovery_required"
                        and not self._ongoing_executor(c, task_id, worker_id)
                        and not c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN "
                                          "('prepared','sending','bound','uncertain','reconciled')", (worker_id,)).fetchone()):
                    reason = ("Collaborator permanently left this task and its saved execution is quiet; no feedback was submitted."
                              if departed else "Collaborator saved its work and voluntarily slept before the notice was delivered; no opinion-only wake was requested.")
                    c.execute("UPDATE task_completion_declarations SET feedback_state='unavailable',feedback_json=? WHERE id=?",
                              (canonical({"conditions": [], "reason": reason}), declaration["id"]))
                    self._wake(c)

    def _capture_completion_drain(self, c, task, declaration_id):
        root = c.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()["root_task_id"] == task["id"]
        workers = []
        for row in c.execute("SELECT id,current_task_id,active_attempt,state,work_intention FROM workers "
                             "WHERE current_task_id=? OR (? AND active_attempt IS NOT NULL)", (task["id"], root)).fetchall():
            worker = dict(row)
            member = c.execute("SELECT runnable,work_reason FROM task_members WHERE task_id=? AND worker_id=?",
                               (worker["current_task_id"], worker["id"])).fetchone()
            worker["member"] = dict(member) if member else None
            workers.append(worker)
        snapshot = {"task_id": task["id"], "task_revision": task["revision"], "root": root,
                    "event_sequence": c.execute("SELECT COALESCE(MAX(sequence),0) FROM events").fetchone()[0],
                    "workers": workers, "lifecycle": self._lifecycle(c, task["id"]),
                    "run_ids": [row["id"] for row in c.execute("SELECT id FROM runs WHERE state='running'")] if root else [],
                    "helpers": [dict(row) for row in c.execute(
                        "SELECT id,state FROM helper_intents WHERE state IN ('prepared','running','uncertain') AND (? OR source_task_id=?)", (root, task["id"]))]}
        c.execute("INSERT INTO completion_drains(declaration_id,snapshot_json) VALUES(?,?)", (declaration_id, canonical(snapshot)))

    def _rollback_completion_drain(self, c, declaration_id):
        row = c.execute("SELECT * FROM completion_drains WHERE declaration_id=? AND state='draining'", (declaration_id,)).fetchone()
        if row is None:
            return
        saved = json.loads(row["snapshot_json"])
        if any(self._get(c, "workers", worker["id"])["active_attempt"] for worker in saved["workers"]):
            return
        touched = set()
        for previous in saved["workers"]:
            task_id = previous["current_task_id"]
            if not task_id or self._get(c, "tasks", task_id)["state"] in TERMINAL:
                continue
            worker = self._get(c, "workers", previous["id"])
            member = c.execute("SELECT membership_state FROM task_members WHERE task_id=? AND worker_id=?",
                               (task_id, worker["id"])).fetchone()
            if (not member or member["membership_state"] != "member" or worker["current_task_id"] != task_id
                    or worker["state"] in {"recovery_required", "retired"}
                    or c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN "
                                 "('prepared','sending','bound','uncertain','reconciled')", (worker["id"],)).fetchone()):
                continue
            if previous["active_attempt"]:
                attempt = self._get(c, "attempts", previous["active_attempt"])
                if attempt["state"] in {"preflight_held", "catalog_held"}:
                    continue
                stop = c.execute("SELECT reason FROM attempt_stop_requests WHERE attempt_id=?", (attempt["id"],)).fetchone()
                if not stop or stop["reason"] not in {"task_completion", "project_shutdown"}:
                    continue  # Preserve a committed submission, wait, departure or revision.
                if attempt["continuation_id"]:
                    c.execute("UPDATE continuations SET snapshot=? WHERE id=? AND origin_attempt=? "
                              "AND state IN ('waiting','eligible','reconsider')", (attempt["snapshot"], attempt["continuation_id"], attempt["id"]))
            intention = previous["work_intention"]
            runnable = int(intention == "working") if previous["active_attempt"] else (previous["member"] or {}).get("runnable", 0)
            c.execute("UPDATE task_members SET runnable=?,work_reason='completion_requires_reconsideration' "
                      "WHERE task_id=? AND worker_id=?", (runnable, task_id, worker["id"]))
            c.execute("UPDATE workers SET state=?,work_intention=? WHERE id=?",
                      ("suspended" if intention == "waiting" else "idle", intention, worker["id"]))
            touched.add(task_id)
        lifecycle = saved["lifecycle"]
        if self._get(c, "tasks", saved["task_id"])["state"] not in TERMINAL:
            c.execute("UPDATE task_lifecycle SET recruitment_decision=?,opportunity_state=?,closed_reason=? "
                      "WHERE task_id=? AND closed_reason='task_completion'",
                      (lifecycle["recruitment_decision"], lifecycle["opportunity_state"], lifecycle["closed_reason"], saved["task_id"]))
        for previous in saved["helpers"]:
            helper_id = previous["id"]
            helper = self._helper(c, helper_id)
            if helper["source_task_id"] and self._get(c, "tasks", helper["source_task_id"])["state"] in TERMINAL:
                continue
            if previous["state"] != "prepared" and not helper["raw_result_json"]:
                continue
            restored = "prepared" if previous["state"] == "prepared" else "running"
            c.execute("UPDATE helper_intents SET state=?,reason=NULL,ended_at=NULL WHERE id=? AND state='cancelled' "
                      "AND reason IN ('task_completion','project_shutdown')", (restored, helper_id))
        if saved["root"]:
            c.execute("UPDATE project_lifecycle SET state='running',shutdown_reason=NULL WHERE id=1 AND state='shutting_down'")
            for run_id in saved["run_ids"]:
                # An operator pause/stop after provisional drain owns the control.
                controlled = c.execute("SELECT 1 FROM events WHERE object_id=? AND sequence>? "
                                       "AND kind IN ('run.pause','run.pause_after_turn','run.stop')",
                                       (run_id, saved["event_sequence"])).fetchone()
                if not controlled:
                    c.execute("UPDATE runs SET state='running' WHERE id=? AND state='paused'", (run_id,))
        c.execute("UPDATE completion_drains SET state='rolled_back' WHERE declaration_id=?", (declaration_id,))
        for task_id in touched:
            self._recompute_task(c, task_id)
        self._event(c, "task.completion_drain_rolled_back", declaration_id,
                    {"root_shutdown_cancelled": saved["root"]}, saved["task_id"])

    def _reconcile_completion_drains(self, c):
        for row in c.execute("SELECT d.declaration_id,d.snapshot_json FROM completion_drains d "
                             "JOIN task_completion_declarations t ON t.id=d.declaration_id "
                             "WHERE d.state='draining' AND t.state NOT IN ('finalizing','closed')").fetchall():
            if not json.loads(row["snapshot_json"])["root"]:
                self._rollback_completion_drain(c, row["declaration_id"])

    def _finalize_completion(self, c, p, actor):
        declaration = self._completion_record(c, p["declaration_id"])
        task = self._task(c, declaration["task_id"])
        self._authorize_participant(c, task, actor)
        if declaration["declarer_id"] != (actor.worker_id or "operator"):
            raise HubError("DECLARER_REQUIRED", "The initiating maintainer resolves feedback and decides this declaration", 403)
        if declaration["state"] != "pending" or task["revision"] != declaration["task_revision"]:
            raise HubError("STALE_COMPLETION", "Reevaluate completion against the current task version")
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 20000:
            raise HubError("REASON_REQUIRED", "Record an evidence-based completion decision", 422)
        if declaration["feedback_state"] == "pending":
            raise HubError("FEEDBACK_PENDING", "The executing collaborator has not reached its feedback opportunity")
        decision = p.get("decision", "close")
        if decision not in {"close", "continue"}:
            raise HubError("INVALID_INPUT", "Choose close or continue", 422)
        resolution = p.get("resolution", {})
        objections = (declaration["feedback"] or {}).get("conditions", [])
        if decision == "close":
            self._artifact_gate(c, task["id"])
            conditions, evidence = self._completion_evidence(c, task, p.get("evidence", declaration["evidence"]))
            if conditions != declaration["acceptance"]:
                raise HubError("STALE_COMPLETION", "Acceptance requirements changed after declaration")
            if objections:
                if not isinstance(resolution, dict) or set(resolution) != set(objections):
                    raise HubError("FEEDBACK_UNRESOLVED", "Answer every concrete unmet acceptance condition with evidence")
                for key in objections:
                    item = resolution[key]
                    if (not isinstance(item, dict) or item.get("outcome") not in {"satisfied", "optional_extension"}
                            or not isinstance(item.get("explanation"), str) or not item["explanation"].strip()
                            or not item.get("artifact_ids")):
                        raise HubError("FEEDBACK_UNRESOLVED", "A bare assertion cannot resolve a concrete objection")
                    for artifact_id in item["artifact_ids"]:
                        self._accepted_reference(c, artifact_id)
            self._capture_completion_drain(c, task, declaration["id"])
            c.execute("UPDATE task_completion_declarations SET state='finalizing',decision_reason=?,resolution_json=?,"
                      "evidence_json=?,decided_at=? WHERE id=?",
                      (reason, canonical(resolution), canonical(evidence), self.clock(), declaration["id"]))
            self._cancel_recruitment(c, task["id"], "task_completion")
            self._drain_task(c, task["id"], "task_completion")
            c.execute("UPDATE task_members SET runnable=0 WHERE task_id=?", (task["id"],))
            project = c.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()
            if project["root_task_id"] == task["id"]:
                c.execute("UPDATE project_lifecycle SET state='shutting_down',shutdown_reason='task_0_complete' WHERE id=1")
                c.execute("UPDATE runs SET state='paused' WHERE state='running'")
                c.execute("DELETE FROM worker_controls")
                c.execute("DELETE FROM group_controls")
                for active in c.execute("SELECT DISTINCT task_id FROM attempts WHERE ended_at IS NULL").fetchall():
                    self._drain_task(c, active["task_id"], "project_shutdown")
                c.execute("UPDATE helper_intents SET state='cancelled',reason='project_shutdown',ended_at=? WHERE state='prepared'", (self.clock(),))
            self._recompute_collaborative(c, task["id"])
        else:
            c.execute("UPDATE task_completion_declarations SET state='continued',decision_reason=?,resolution_json=?,decided_at=? WHERE id=?",
                      (reason, canonical(resolution), self.clock(), declaration["id"]))
            self._wake(c)  # Others waiting on this declaration wait for its decision.
        self._event(c, "task.completion_decided", declaration["id"], {"decision": decision, "reason": reason}, task["id"], actor.worker_id)
        return self._completion_record(c, declaration["id"])

    def _finish_task(self, c, task_id):
        c.execute("UPDATE tasks SET state='done',active_attempt=NULL,state_version=state_version+1 WHERE id=?", (task_id,))
        c.execute("UPDATE task_completion_declarations SET state='closed' WHERE task_id=? AND state='finalizing'", (task_id,))
        c.execute("UPDATE task_members SET runnable=0,membership_state='ended',work_reason='task_complete' WHERE task_id=?", (task_id,))
        for member in c.execute("SELECT worker_id FROM task_members WHERE task_id=?", (task_id,)).fetchall():
            self._release_ended_member(c, task_id, member["worker_id"])
        c.execute("UPDATE continuations SET state='satisfied_externally' WHERE task_id=? AND state IN ('waiting','eligible','reconsider')", (task_id,))
        self._invalidate_interruptions(c, task_id, "satisfied_externally")
        self._end_closed_subscriptions(c, task_id, "subscribed_task_done")
        self._wake(c)
        self._event(c, "task.completed", task_id, task_id=task_id)
        for drain in c.execute("SELECT d.declaration_id,d.snapshot_json FROM completion_drains d "
                               "JOIN task_completion_declarations t ON t.id=d.declaration_id "
                               "WHERE t.task_id=? AND d.state='draining'", (task_id,)).fetchall():
            if not json.loads(drain["snapshot_json"])["root"]:
                c.execute("UPDATE completion_drains SET state='committed' WHERE declaration_id=?", (drain["declaration_id"],))
        self._finish_shutdown_if_quiet(c)

    @staticmethod
    def _work_in_flight(c) -> bool:
        """An attempt that has not ended, or a decision that is running or uncertain."""
        return bool(c.execute("SELECT 1 FROM attempts WHERE ended_at IS NULL").fetchone()
                    or c.execute("SELECT 1 FROM helper_intents WHERE state IN ('running','uncertain')").fetchone())

    def _finish_shutdown_if_quiet(self, c):
        self._reconcile_completion_drains(c)
        project = c.execute("SELECT * FROM project_lifecycle WHERE id=1").fetchone()
        if project["state"] != "shutting_down" or self._work_in_flight(c):
            return
        drain = c.execute("SELECT d.declaration_id FROM completion_drains d JOIN task_completion_declarations t "
                          "ON t.id=d.declaration_id WHERE t.task_id=? AND d.state='draining' ORDER BY t.created_at DESC LIMIT 1",
                          (project["root_task_id"],)).fetchone()
        prior = (None if drain else c.execute("SELECT id FROM task_completion_declarations WHERE task_id=? "
                                             "ORDER BY created_at DESC,rowid DESC LIMIT 1", (project["root_task_id"],)).fetchone())
        declaration_id = drain["declaration_id"] if drain else prior["id"] if prior else None
        declaration = self._completion_record(c, declaration_id) if declaration_id else None
        valid = declaration is not None and declaration["state"] == "closed"
        if valid:
            assert declaration is not None
            try:
                root = self._task(c, project["root_task_id"])
                self._artifact_gate(c, root["id"])
                self._completion_evidence(c, root, declaration["evidence"])
                valid = root["state"] == "done" and not root.get("current_evidence")
            except HubError:
                valid = False
        if not valid:
            if drain:
                self._rollback_completion_drain(c, drain["declaration_id"])
            return
        if drain:
            c.execute("UPDATE completion_drains SET state='committed' WHERE declaration_id=?", (drain["declaration_id"],))
        c.execute("UPDATE project_lifecycle SET state='stopped' WHERE id=1")
        c.execute("UPDATE runs SET state='stopped' WHERE state='paused'")

    def _recompute_collaborative(self, c, task_id):
        task = self._task(c, task_id)
        if task["contract"]["kind"] in REVIEW_KINDS:
            super()._recompute_collaborative(c, task_id)
            if self._get(c, "tasks", task_id)["state"] in TERMINAL:
                c.execute("UPDATE workers SET work_intention='retired',state=CASE WHEN active_attempt IS NULL THEN 'retired' ELSE state END "
                          "WHERE kind='review' AND current_task_id=?", (task_id,))
                c.execute("UPDATE task_members SET membership_state='ended',runnable=0 WHERE task_id=?", (task_id,))
            return
        if task["state"] in TERMINAL:
            return
        attempts = self._refresh_aggregate(c, task_id)
        finalizing = c.execute("SELECT * FROM task_completion_declarations WHERE task_id=? AND state='finalizing'", (task_id,)).fetchone()
        if finalizing and not attempts:
            try:
                self._artifact_gate(c, task_id)
                self._completion_evidence(c, task, json.loads(finalizing["evidence_json"]))
                if finalizing["task_revision"] != task["revision"]:
                    raise HubError("STALE_COMPLETION", "Task version changed during completion")
            except HubError as error:
                c.execute("UPDATE task_completion_declarations SET state='invalidated',decision_reason=? WHERE id=?", (error.message, finalizing["id"]))
                self._notify(c, task_id, finalizing["id"], "completion_requires_reconsideration")
                self._reconcile_completion_drains(c)
            else:
                self._finish_task(c, task_id)
                return
        if attempts:
            state = ("recovery_required" if any(item["state"] == "recovery_required" for item in attempts)
                     else "draining" if finalizing or any(item["task_revision"] != task["revision"] for item in attempts) else "claimed")
        elif any(not self._available_artifact(c, ref) for ref in task["contract"]["inputs"]):
            state = "needs_attention"
        elif c.execute("SELECT 1 FROM task_members WHERE task_id=? AND runnable=1 AND membership_state='member'", (task_id,)).fetchone():
            state = "ready"
        elif c.execute("SELECT 1 FROM review_requests WHERE host_task_id=? AND state IN ('pending','integrating')", (task_id,)).fetchone():
            state = "verifying"
        elif self._lifecycle(c, task_id)["initial_worker_id"]:
            state = "waiting"
        else:
            state = "ready"
        c.execute("UPDATE tasks SET state=?,state_version=state_version+1 WHERE id=?", (state, task_id))

    def _stage_candidate(self, c, attempt, task, p, snapshot):
        inferred = None if p.get("replaces") else self._corrected_candidate(c, attempt, task, p)
        predecessor = p.get("replaces") or inferred
        inherited_authors = set()
        if predecessor:
            prior = c.execute("SELECT a.status,o.task_id FROM artifacts a JOIN artifact_obligations o ON o.artifact_id=a.id WHERE a.id=?", (predecessor,)).fetchone()
            replaceable = prior is not None and (prior["status"] in {"rejected", "withdrawn", "superseded"}
                or (prior["status"] == "accepted" and not self._available_artifact(c, predecessor)))
            if not replaceable or prior["task_id"] != task["id"]:
                raise HubError("INVALID_REPLACEMENT", "Corrected contributions must identify a rejected, withdrawn, superseded, or no-longer-available accepted version of this task")
            if c.execute("SELECT 1 FROM artifact_obligations WHERE predecessor_id=?", (predecessor,)).fetchone():
                raise HubError("REPLACEMENT_EXISTS", "This version already has a correction; replace its latest version")
            inherited_authors.update(self._candidate_authors(c, predecessor))
            p = {**p, "coauthors": sorted((inherited_authors | set(p.get("coauthors", []))) - {attempt["worker_id"]})}
        result = super()._stage_candidate(c, attempt, task, p, snapshot)
        artifact_id = result["artifact_id"]
        c.execute("INSERT INTO artifact_obligations(artifact_id,task_id,predecessor_id) VALUES(?,?,?)", (artifact_id, task["id"], predecessor))
        authors = {attempt["worker_id"], *p.get("coauthors", []), *inherited_authors}
        c.execute("INSERT INTO candidate_provenance VALUES(?,?)", (artifact_id, canonical(sorted(authors))))
        if inferred:
            self._event(c, "artifact.replacement_inferred", artifact_id, {"replaces": inferred}, task["id"], attempt["worker_id"])
            result = {**result, "replaces": inferred, "replaces_note": (
                "No replaces was given: this candidate is recorded as the correction of " + inferred
                + ", the only uncorrected rejected or withdrawn candidate of this task of the same kind"
                " with the same files that you contributed to.")}
        return result

    def _candidate_authors(self, c, artifact_id):
        """Everyone a candidate credits: its author, its coauthors and its recorded provenance."""
        binding = self._candidate_binding(c, artifact_id)
        authors = {binding["author_id"], *binding["coauthors"]}
        provenance = c.execute("SELECT worker_ids_json FROM candidate_provenance WHERE artifact_id=?", (artifact_id,)).fetchone()
        if provenance:
            authors.update(json.loads(provenance["worker_ids_json"]))
        return authors

    def _corrected_candidate(self, c, attempt, task, p):
        """The candidate a submission corrects when it names none in `replaces`: the only one
        of the task that was rejected or withdrawn and not corrected since, of the same kind and
        with the same source files, that its author contributed to. In the Linux pilot 5
        corrections like that named none, and a requirement naming the one they corrected could
        never be met; of the 192 that named one, 176 had such a candidate, always the one named."""
        from .plugins import load_plugin
        try:
            paths = self._publication_paths(task, p, load_plugin(task["contract"]["domain"]))
        except ITEM_FAULTS:
            return None  # Staging itself reports what is wrong with the submission.
        found = [row["artifact_id"] for row in c.execute(
            "SELECT o.artifact_id FROM artifact_obligations o JOIN artifacts a ON a.id=o.artifact_id "
            "JOIN candidate_repositories r ON r.artifact_id=a.id WHERE o.task_id=? AND a.kind=? "
            "AND a.status IN ('rejected','withdrawn') AND r.paths_json=? "
            "AND NOT EXISTS(SELECT 1 FROM artifact_obligations later WHERE later.predecessor_id=a.id) "
            "AND NOT EXISTS(SELECT 1 FROM artifact_replacements x WHERE x.original_id=a.id)",
            (task["id"], p.get("kind"), canonical(paths))).fetchall()
            if attempt["worker_id"] in self._candidate_authors(c, row["artifact_id"])]
        return found[0] if len(found) == 1 else None

    def _commit_review(self, c, attempt, review, report):
        if report["verdict"] == "uncertain" or (report["verdict"] == "approve" and report["qualifications"]):
            raise HubError("REVIEW_PROTOCOL", "Unresolved verification requires a checkpointed pause before any terminal review")
        result = super()._commit_review(c, attempt, review, report)
        if result.get("directive") == "reviewed":
            c.execute("UPDATE workers SET work_intention='retired',state='retired' WHERE id=?", (attempt["worker_id"],))
            c.execute("UPDATE task_members SET membership_state='ended',runnable=0 WHERE task_id=?", (review["id"],))
            if report["verdict"] == "request_changes":
                c.execute("UPDATE artifacts SET status='rejected' WHERE id=? AND status NOT IN ('withdrawn','accepted')", (report["candidate_id"],))
                self._reconsider_artifact_waits(c, report["candidate_id"], "candidate_rejected")
                archive = getattr(self, "_archive_submission", None)
                if callable(archive):
                    archive(c, report["candidate_id"], "rejected", report["rationale"])
        return result

    def _review_reassign(self, c, p, actor):
        self._role(actor, Role.OPERATOR, Role.RUNNER)
        review = self._task(c, p["task_id"])
        if review["contract"]["kind"] != "review" or review["state"] in TERMINAL:
            raise HubError("REVIEW_NOT_PENDING", "Only an unfinished candidate review may be reassigned")
        if p.get("recovery_impossible") is not True or p.get("process_confirmed_stopped") is not True or not p.get("reason"):
            raise HubError("QUIESCENCE_REQUIRED", "Confirm unrecoverable original execution and stopped processes with a reason")
        worker_id = p["worker_id"]
        if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? AND executor=1 AND membership_state='member'",
                         (review["id"], worker_id)).fetchone():
            raise HubError("REVIEWER_INELIGIBLE", "The named worker does not own this review")
        request = c.execute("SELECT * FROM review_requests WHERE task_id=?", (review["id"],)).fetchone()
        if self.config.repository.enabled:
            require_head = getattr(self, "_require_recorded_repository_head", None)
            repository = getattr(self, "_repository", None)
            if callable(require_head) and callable(repository):
                require_head(c, repository())
        if c.execute("SELECT 1 FROM repository_publications WHERE candidate_id=?", (request["candidate_id"],)).fetchone():
            raise HubError("PUBLICATION_RECONCILIATION_REQUIRED", "A publication receipt exists; reconcile completion before replacement")
        if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound','uncertain')",
                     (worker_id,)).fetchone():
            raise HubError("TURN_RECONCILIATION_REQUIRED", "Resolve the old native turn and usage before replacement")
        worker = self._get(c, "workers", worker_id)
        snapshot = self._workspace_snapshot(worker_id)
        if worker["active_attempt"]:
            c.execute("UPDATE attempts SET state='review_reassigned',ended_at=?,snapshot=? WHERE id=?",
                      (self.clock(), snapshot, worker["active_attempt"]))
        acceptance = self._review_acceptance(c, review["id"])
        c.execute("INSERT INTO review_recovery_history VALUES(?,?,?,?,?,?,?)",
                  (new_id(), review["id"], worker_id, p["reason"], snapshot,
                   canonical(acceptance) if acceptance else None, self.clock()))
        # A replacement performs its own independent acceptance. The immutable
        # earlier acceptance remains in recovery history and its original event.
        c.execute("DELETE FROM review_acceptances WHERE review_task_id=?", (review["id"],))
        c.execute("UPDATE review_requests SET state='pending' WHERE task_id=?", (review["id"],))
        c.execute("UPDATE tasks SET state='ready',active_attempt=NULL WHERE id=?", (review["id"],))
        c.execute("UPDATE workers SET state='retired',work_intention='retired',active_attempt=NULL WHERE id=?", (worker_id,))
        c.execute("UPDATE task_members SET membership_state='reassigned',runnable=0 WHERE task_id=? AND worker_id=?", (review["id"], worker_id))
        c.execute("UPDATE continuations SET state='review_reassigned' WHERE preferred_worker=? AND state IN ('waiting','eligible','reconsider')", (worker_id,))
        self._invalidate_interruptions(c, review["id"], "review_reassigned")
        self._event(c, "review.reassigned", review["id"], {"prior_worker_id": worker_id, "reason": p["reason"], "snapshot": snapshot}, review["id"])
        return self._task(c, review["id"])

    def _lifecycle_command(self, c, name, p, actor):
        """A lifecycle command's result, or None when the core handler runs next: some
        `_before_` handlers check or prepare a core command and then return None."""
        handler = LIFECYCLE.get(name)
        return None if handler is None else handler(self, c, name, p, actor)

    @LIFECYCLE.handles("worker.recover")
    def _before_worker_recover(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        self._close_stopped_history(c, worker, p)
        member_hold = c.execute(
            "SELECT 1 FROM task_members m JOIN attempts a ON a.worker_id=m.worker_id "
            "AND a.task_id=m.task_id WHERE m.worker_id=? AND m.task_id=? "
            "AND m.membership_state='member' AND m.runnable=0 "
            "AND m.work_reason='execution_recovery_required' "
            "AND a.state='abandoned' AND a.ended_at IS NOT NULL "
            "AND a.id=(SELECT id FROM attempts WHERE worker_id=m.worker_id "
            "ORDER BY created_at DESC,rowid DESC LIMIT 1)",
            (worker["id"], worker["current_task_id"]),
        ).fetchone()
        recovery_held = (worker["state"] == "recovery_required" or (worker["state"] == "idle" and worker["current_task_id"]
                         and (member_hold or self._get(c, "tasks", worker["current_task_id"])["state"] == "recovery_required")))
        if (not recovery_held and worker["active_attempt"] is None and worker["state"] in {"idle", "suspended"}
                and self._native_validation_held(c, worker["id"])):
            if p.get("process_confirmed_stopped") is not True or not p.get("reason"):
                raise HubError("QUIESCENCE_REQUIRED", "Confirm the uncertain local validation process stopped and retain the recovery reason")
            if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN "
                         "('prepared','sending','bound','uncertain','reconciled')", (worker["id"],)).fetchone():
                raise HubError("TURN_RECONCILIATION_REQUIRED", "Reconcile the saved native execution before recovering local validation")
            self._reconcile_native_validations(c, worker["id"])
            self._event(c, "worker.recovered", worker["id"], {"reason": p["reason"], "validation_only": True},
                        worker["current_task_id"], worker["id"])
            return {"directive": "recovered", "worker_id": worker["id"], "task_id": worker["current_task_id"], "validation_only": True}
        if worker["state"] == "recovery_required" and worker["active_attempt"] is None and worker["current_task_id"] is None:
            if p.get("process_confirmed_stopped") is not True or not p.get("reason"):
                raise HubError("QUIESCENCE_REQUIRED", "Confirm the held workspace is recoverable and preserve stopped-process evidence")
            if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound','uncertain','reconciled')",
                          (worker["id"],)).fetchone():
                raise HubError("TURN_RECONCILIATION_REQUIRED", "Reconcile the saved native execution before releasing its local hold")
            self._reconcile_native_validations(c, worker["id"])
            retired = p.get("recovery_impossible") is True
            c.execute("UPDATE workers SET state=?,work_intention=? WHERE id=?",
                      ("retired" if retired else "idle", "retired" if retired else "idle", worker["id"]))
            self._event(c, "worker.recovered", worker["id"], {"reason": p["reason"], "retired": retired}, worker_id=worker["id"])
            return {"directive": "worker_retired" if retired else "recovered", "worker_id": worker["id"], "task_id": None}
        if recovery_held and worker["active_attempt"] is None and worker["current_task_id"]:
            self._role(actor, Role.OPERATOR)
            if p.get("process_confirmed_stopped") is not True or not p.get("reason"):
                raise HubError("QUIESCENCE_REQUIRED", "Confirm failed execution stopped and record recovery evidence")
            if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound','uncertain','reconciled')", (worker["id"],)).fetchone():
                raise HubError("TURN_RECONCILIATION_REQUIRED", "Reconcile the failed native turn before recovering its continuation")
            self._reconcile_native_validations(c, worker["id"])
            if p.get("recovery_impossible") is True:
                task = self._task(c, worker["current_task_id"])
                if task["contract"]["kind"] in REVIEW_KINDS:
                    return self._review_reassign(c, {**p, "task_id": task["id"]}, actor)
                c.execute("UPDATE task_members SET membership_state='unrecoverable',runnable=0 WHERE task_id=? AND worker_id=?", (task["id"], worker["id"]))
                self._end_member_resumptions(c, task["id"], worker["id"], "worker_unrecoverable")
                self._cancel_recruitment(c, task["id"], "saved_execution_unrecoverable")
                self._notify(c, task["id"], task["id"], p["reason"])
                c.execute("UPDATE workers SET current_task_id=NULL,state='retired',work_intention='retired' WHERE id=?", (worker["id"],))
                c.execute("UPDATE task_completion_declarations SET feedback_state='unavailable',feedback_json=? WHERE feedback_worker_id=? AND state='pending' AND feedback_state='pending'",
                          (canonical({"conditions": [], "reason": p["reason"], "recovery_impossible": True}), worker["id"]))
                survivors = c.execute("SELECT 1 FROM task_members WHERE task_id=? AND executor=1 AND membership_state='member'", (task["id"],)).fetchone()
                if not survivors:
                    c.execute("UPDATE tasks SET state='retired' WHERE id=?", (task["id"],))
                    self._invalidate_reviews(c, task["id"], "host_execution_unrecoverable")
                    self._reconsider_dependents(c, task["id"], "dependency_retired")
                    self._end_closed_subscriptions(c, task["id"], "subscribed_task_retired")
                else:
                    self._recompute_task(c, task["id"])
                self._wake(c)
                return {"directive": "worker_retired" if survivors else "task_retired", "task_id": task["id"], "reason": p["reason"]}
            origin = c.execute("SELECT * FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                               (worker["id"],)).fetchone()
            if origin and origin["ended_at"] is not None and origin["snapshot"]:
                self._seal_dependency_reconsideration(c, origin, origin["snapshot"])
            # An accepted reviewer waits for the Integrator's questions, as at the end of its turn.
            accepted_review = worker["kind"] == "review" and self._review_acceptance(c, worker["current_task_id"])
            c.execute("UPDATE workers SET state=?,work_intention=? WHERE id=?",
                      ("suspended", "waiting", worker["id"]) if accepted_review else ("idle", "working", worker["id"]))
            self._release_ended_member(c, worker["current_task_id"], worker["id"])
            recovered = self._get(c, "workers", worker["id"])
            if recovered["current_task_id"] and not accepted_review:
                self._set_member_work(c, recovered["current_task_id"], worker["id"], True, "execution_recovered")
            self._recompute_task(c, worker["current_task_id"])
            self._event(c, "worker.recovered", worker["id"], {"reason": p["reason"]}, worker["current_task_id"], worker["id"])
            return {"directive": "recovered", "worker_id": worker["id"], "task_id": recovered["current_task_id"]}
        return None  # The core handler of the command runs next.

    @LIFECYCLE.handles("runtime.abandon", "runtime.park_interrupted")
    def _before_runtime_stop(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if worker["execution_mode"] == "message_only" and worker["active_attempt"]:
            self._role(actor, Role.RUNNER)
            quiet = p.get("quiescent") is True if name == "runtime.abandon" else p.get("process_quiet") is True and p.get("terminal_observed") is True
            if not quiet:
                raise HubError("QUIESCENCE_REQUIRED", "Preserve the message conversation until its owned processes stop")
            if name == "runtime.abandon":
                self._close_failed_native_turns(c, worker["id"], worker["active_attempt"], str(p.get("reason", "Message execution stopped")))
            snapshot = self._workspace_snapshot(worker["id"])
            attempt_id = worker["active_attempt"]
            c.execute("UPDATE attempts SET state='message_interrupted',snapshot=?,ended_at=? WHERE id=?", (snapshot, self.clock(), attempt_id))
            c.execute("UPDATE workers SET active_attempt=NULL,state=?,execution_mode='work' WHERE id=?",
                      ("recovery_required" if name == "runtime.abandon" else "idle", worker["id"]))
            self._refresh_message_wake(c, worker["id"])
            self._event(c, "worker.message_interrupted", attempt_id, {"snapshot": snapshot, "process_quiet": True,
                        "reason": p.get("reason", p.get("stop_reason"))}, worker_id=worker["id"])
            return {"directive": "message_interrupted", "worker_id": worker["id"], "snapshot": snapshot}
        return None  # The core handler of the command runs next.

    @LIFECYCLE.handles("worker.recover_message")
    def _on_worker_recover_message(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if worker["active_attempt"] or worker["state"] != "recovery_required" or not p.get("reason") or p.get("process_confirmed_stopped") is not True:
            raise HubError("QUIESCENCE_REQUIRED", "Recover a failed message turn only after proving its process stopped")
        self._close_stopped_history(c, worker, p)
        if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound','uncertain')", (worker["id"],)).fetchone():
            raise HubError("TURN_RECONCILIATION_REQUIRED", "Reconcile the failed message turn before another activation")
        c.execute("UPDATE workers SET state='idle' WHERE id=?", (worker["id"],))
        self._refresh_message_wake(c, worker["id"])
        return self._get(c, "workers", worker["id"])

    @LIFECYCLE.handles("usage.record")
    def _before_usage_record(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        epoch = p.get("epoch", worker["usage_epoch"])
        if epoch != worker["usage_epoch"]:
            raise HubError("STALE_USAGE", "Usage belongs to another native binding")
        raw = p["sample"]
        c.execute("INSERT INTO native_usage_samples VALUES(?,?,?,?,?) ON CONFLICT(worker_id,epoch) DO UPDATE SET "
                  "sha256=excluded.sha256,sample_json=excluded.sample_json,created_at=excluded.created_at "
                  "WHERE native_usage_samples.sha256<>excluded.sha256",
                  (worker["id"], epoch, digest(raw), canonical(raw), self.clock()))
        return None

    @LIFECYCLE.handles("helper.reconcile")
    def _on_helper_reconcile(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        helper = self._helper(c, p["intent_id"])
        if helper["state"] != "uncertain" or p.get("process_confirmed_stopped") is not True or not p.get("reason"):
            raise HubError("QUIESCENCE_REQUIRED", "Reconcile an uncertain helper only after its process definitively stopped")
        if p.get("retry_preflight") is True:
            if (helper["inference_uncertain"] or helper["thread_id"] or helper["remote_turn_id"]
                    or helper["raw_result_json"] or helper["result_json"] or helper["usage_json"]
                    or "result" in p):
                raise HubError("PREFLIGHT_NOT_SAFE", "Retry only a helper with no bound thread, dispatched inference, result or usage")
            if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
                raise HubError("PROJECT_STOPPED", "A completed project cannot retry helpers")
            c.execute("UPDATE helper_intents SET state='prepared',reason=?,ended_at=NULL WHERE id=?",
                      (p["reason"], helper["id"]))
            self._event(c, "helper.preflight_recovered", helper["id"], {"reason": p["reason"]},
                        helper["source_task_id"], helper["source_worker_id"])
            return self._helper(c, helper["id"])
        c.execute("UPDATE helper_intents SET inference_uncertain=0 WHERE id=?", (helper["id"],))
        if "result" in p:
            raw = canonical(p["result"])
            if helper["raw_result_json"] and helper["raw_result_json"] != raw:
                raise HubError("HELPER_RESULT_CONFLICT", "Retained helper output cannot change during recovery")
            shutting_down = c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running"
            c.execute("UPDATE helper_intents SET state=?,raw_result_json=?,reason=?,ended_at=? WHERE id=?",
                      ("cancelled" if shutting_down else "running", raw,
                       "project_shutdown" if shutting_down else p["reason"], self.clock() if shutting_down else None, helper["id"]))
        else:
            c.execute("UPDATE helper_intents SET state='cancelled',ended_at=?,reason=? WHERE id=?", (self.clock(), p["reason"], helper["id"]))
            if helper["kind"] == "recruitment":
                self._cancel_recruitment(c, helper["source_task_id"], "decision_unrecoverable")
            # A cancelled assignment decision is decided again at the next admission.
        self._refresh_usage_uncertainty(c, helper["run_id"])
        self._event(c, "helper.reconciled", helper["id"], {"reason": p["reason"], "result_recovered": "result" in p}, helper["source_task_id"], helper["source_worker_id"])
        self._finish_shutdown_if_quiet(c)
        return self._helper(c, helper["id"])

    @LIFECYCLE.handles("runtime.catalog_hold", "runtime.preflight_hold")
    def _on_preflight_hold(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker, attempt = self._get(c, "workers", p["worker_id"]), self._get(c, "attempts", p["attempt_id"])
        if worker["active_attempt"] != attempt["id"] or attempt["worker_id"] != worker["id"]:
            raise HubError("STALE_BINDING", "Preflight hold requires the current execution binding")
        if not p.get("reason") or c.execute("SELECT 1 FROM turn_intents WHERE attempt_id=?", (attempt["id"],)).fetchone():
            raise HubError("PREFLIGHT_NOT_SAFE", "Hold is only available before inference admission, with a reason")
        snapshot = self._workspace_snapshot(worker["id"])
        catalog = name == "runtime.catalog_hold"
        state = "catalog_held" if catalog else "preflight_held"
        c.execute("UPDATE attempts SET state=?,snapshot=?,ended_at=? WHERE id=?", (state, snapshot, self.clock(), attempt["id"]))
        paused = not catalog and self._held_by_control(c, worker, attempt)
        if paused:
            # Nothing ran and nothing failed: the run, this person or the account stopped
            # dispatching before the session started. The claim waits as it was before it was
            # made, with no recovery or backoff: admission starts it again once dispatch resumes,
            # however that happens. A worker that was waiting keeps waiting, with its message.
            c.execute("UPDATE task_members SET checkpoint_json=COALESCE(?,checkpoint_json),snapshot=? "
                      "WHERE task_id=? AND worker_id=?",
                      (attempt["checkpoint_json"], snapshot, attempt["task_id"], worker["id"]))
            if worker["work_intention"] == "working" and attempt["execution_mode"] == "work":
                c.execute("UPDATE workers SET active_attempt=NULL,state='idle' WHERE id=?", (worker["id"],))
                self._set_member_work(c, attempt["task_id"], worker["id"], True, "held_before_start")
                self._release_ended_member(c, attempt["task_id"], worker["id"])
            else:
                c.execute("UPDATE workers SET active_attempt=NULL,state='suspended',"
                          "pending_wake=CASE WHEN ? THEN 1 ELSE pending_wake END WHERE id=?",
                          (attempt["execution_mode"] != "work", worker["id"]))
        else:
            c.execute("UPDATE workers SET active_attempt=NULL,state=? WHERE id=?",
                      ("recovery_required", worker["id"]))
            self._preserve_member(c, attempt, snapshot, runnable=False, reason=p["reason"])
        self._seal_dependency_reconsideration(c, attempt, snapshot)
        self._recompute_task(c, attempt["task_id"])
        self._event(c, "runtime." + state, attempt["id"], {"reason": p["reason"], "snapshot": snapshot,
                    "process_quiet": True, "inference_dispatched": False, "paused": paused},
                    attempt["task_id"], worker["id"])
        self._finish_shutdown_if_quiet(c)
        return {"directive": state, "worker_id": worker["id"], "snapshot": snapshot, "reason": p["reason"]}

    def _held_by_control(self, c, worker, attempt):
        """Whether a claim was held only because dispatch stopped for its run, its person or its
        account, and is a plain one that Resume can admit again. A resumed continuation or
        interruption goes through recovery, which reconciles what it was resuming."""
        if attempt["continuation_id"] or c.execute(
                "SELECT 1 FROM interruption_checkpoints WHERE resume_attempt=?", (attempt["id"],)).fetchone():
            return False
        run = self._get(c, "runs", attempt["run_id"])
        return (self._effective_run_state(c, run, worker["id"]) != "running"
                or worker["backend"] in set(self._dispatch_holds(c)))

    @LIFECYCLE.handles("helper.")
    def _on_helper(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        helper = self._helper(c, p["intent_id"])
        if name == "helper.cancelled":
            stopped = c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running"
            cancelled = (helper["kind"] == "recruitment" and helper["source_task_id"] is not None and
                         self._lifecycle(c, helper["source_task_id"])["opportunity_state"] == "cancelled")
            # A pause stopped it before it answered: asked again later, and no failure.
            paused = p.get("reason") == "run_control" and self._effective_run_state(
                c, self._get(c, "runs", helper["run_id"]), helper["source_worker_id"]) != "running"
            known_terminal = p.get("inference_dispatched") is True and p.get("terminal_observed") is True
            not_dispatched = (p.get("inference_dispatched") is False and p.get("terminal_observed") is False
                              and helper["remote_turn_id"] is None)
            if ((helper["kind"] != "recruitment" and not paused) or helper["state"] != "running"
                    or helper["inference_uncertain"] or not (stopped or cancelled or paused)
                    or p.get("process_quiet") is not True or not (known_terminal or not_dispatched)
                    or not isinstance(p.get("reason"), str) or not p["reason"].strip()):
                raise HubError("QUIESCENCE_REQUIRED", "Cancel an obsolete recruitment helper only after a quiet process and a known terminal or explicit pre-dispatch outcome")
            c.execute("UPDATE helper_intents SET state='cancelled',ended_at=?,reason=? WHERE id=?",
                      (self.clock(), p["reason"], helper["id"]))
            if paused and helper["kind"] == "recruitment":
                c.execute("UPDATE task_lifecycle SET decision_helper_id=NULL WHERE decision_helper_id=? "
                          "AND recruitment_decision='pending'", (helper["id"],))
            if paused and helper["kind"] == "wake":
                self._wake_again(c, helper)  # Asked again once the run goes on.
            self._event(c, "helper.cancelled", helper["id"], {
                "reason": p["reason"], "inference_dispatched": p["inference_dispatched"],
                "terminal_observed": p["terminal_observed"], "process_quiet": True},
                helper["source_task_id"], helper["source_worker_id"])
            self._finish_shutdown_if_quiet(c)
            return self._helper_reply(c, helper["id"])
        if name == "helper.started":
            self._helper_start(c, helper["id"], p["thread_id"])
            return self._helper_reply(c, helper["id"])
        if name == "helper.turn.bind":
            if helper["state"] != "running" or (helper["remote_turn_id"] and helper["remote_turn_id"] != p["remote_turn_id"]):
                raise HubError("HELPER_RECONCILIATION_REQUIRED", "A helper binds one native turn exactly once")
            c.execute("UPDATE helper_intents SET remote_turn_id=? WHERE id=?", (p["remote_turn_id"], helper["id"]))
        elif name == "helper.usage":
            delta = p.get("delta_tokens")
            if type(delta) is not int or delta < 0 or not isinstance(p.get("sample_id"), str) or not p["sample_id"]:
                raise HubError("INVALID_USAGE", "Helper usage needs its own nonnegative delta and sample identity", 422)
            sample_json = canonical(p.get("sample", {}))
            prior = c.execute("SELECT * FROM helper_usage_samples WHERE helper_id=? AND sample_id=?", (helper["id"], p["sample_id"])).fetchone()
            if prior and (prior["delta_tokens"] != delta or prior["sample_json"] != sample_json):
                raise HubError("USAGE_CONFLICT", "A helper usage sample cannot change")
            if not prior:
                c.execute("INSERT INTO helper_usage_samples VALUES(?,?,?,?,?)", (helper["id"], p["sample_id"], delta, sample_json, self.clock()))
                self._charge_backend(c, helper["run_id"], "codex", delta)  # Decisions run on Codex.
        elif name == "helper.result":
            if p.get("process_quiet") is not True or helper["state"] not in {"running", "completed"}:
                raise HubError("QUIESCENCE_REQUIRED", "A helper result needs a confirmed quiet native execution")
            raw = canonical(p["result"])
            if helper["raw_result_json"] and helper["raw_result_json"] != raw:
                raise HubError("HELPER_RESULT_CONFLICT", "The visible helper result is immutable")
            c.execute("UPDATE helper_intents SET raw_result_json=? WHERE id=?", (raw, helper["id"]))
            if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
                c.execute("UPDATE helper_intents SET state='cancelled',reason='project_shutdown',ended_at=? WHERE id=?", (self.clock(), helper["id"]))
                self._finish_shutdown_if_quiet(c)
        else:
            raise HubError("UNKNOWN_COMMAND", "Unknown helper operation", 422)
        return self._helper_reply(c, helper["id"])

    @LIFECYCLE.handles("recruitment.decide")
    def _on_recruitment_decide(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        return self._decide_recruitment(c, p["intent_id"], p["decision"], p.get("invitation", ""))

    @LIFECYCLE.handles("task.invitation.update", "task.invitation.withdraw")
    def _on_invitation(self, c, name, p, actor):
        task = self._task(c, p["task_id"])
        if task["contract"]["kind"] in REVIEW_KINDS:
            raise HubError("REVIEW_PROTOCOL", "A review has exactly one executor and never recruits")
        self._authorize_participant(c, task, actor)
        self._expected_revision(task, p)
        lifecycle = self._lifecycle(c, task["id"])
        if lifecycle["opportunity_state"] != "open" or task["state"] in TERMINAL:
            raise HubError("RECRUITMENT_CLOSED", "Recruitment is decided only once after the first executor's first turn")
        if name == "task.invitation.withdraw":
            if not p.get("reason"):
                raise HubError("REASON_REQUIRED", "Invitation withdrawal needs a reason", 422)
            self._cancel_recruitment(c, task["id"], p["reason"])
        else:
            invitation = p.get("invitation", p.get("reason"))
            if not isinstance(invitation, str) or not invitation.strip() or len(invitation) > 5000:
                raise HubError("INVITATION_REQUIRED", "A bounded contribution invitation is required", 422)
            c.execute("UPDATE task_lifecycle SET invitation=? WHERE task_id=?", (invitation, task["id"]))
        return self._lifecycle(c, task["id"])

    @LIFECYCLE.handles("task.join")
    def _on_task_join(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        task = self._task(c, p["task_id"])
        self._expected_revision(task, p)
        worker_id = p["worker_id"]
        worker = self._get(c, "workers", worker_id)
        if worker["active_attempt"]:
            raise HubError("ALREADY_CLAIMED", "A worker with active work joins no other task")
        run_id = p.get("run_id", worker["run_id"])
        if run_id is None:
            row = c.execute("SELECT id FROM runs WHERE state='running'").fetchone()
            run_id = row["id"] if row else None
        if run_id is None:
            raise HubError("RUN_PAUSED", "Task admission requires an active project run")
        self._run_controls(c, run_id, worker_id)
        result = self._claim(c, task["id"], task["revision"], worker_id, run_id, contribution_plan=p.get("plan", ""))
        return self._assignment_metadata(c, result)

    @staticmethod
    def _require_contract_text(contract):
        """Refuse a published or revised contract whose objective or context is a file name
        instead of its text: the task would name no target."""
        named = file_name_fields(contract)
        if named:
            raise HubError("CONTRACT_TEXT_REQUIRED", f"The contract's {' and '.join(named)} is only a file name "
                           "(such as objective.md). Write the Markdown text itself in the contract, or, in a "
                           "task file, the path of an adjacent Markdown file that holds it", 422)

    @LIFECYCLE.handles("task.revise")
    def _on_task_revise(self, c, name, p, actor):
        task = self._task(c, p["task_id"])
        if task["contract"]["kind"] in REVIEW_KINDS:
            raise HubError("REVIEW_PROTOCOL", "An immutable candidate review cannot become an ordinary maintenance task")
        self._authorize_participant(c, task, actor)
        self._expected_revision(task, p)
        if task["state"] in TERMINAL:
            raise HubError("TASK_TERMINAL", "A completed task retains its fulfilled specification")
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 20000:
            raise HubError("REASON_REQUIRED", "A versioned maintenance change requires a bounded reason", 422)
        from ..bootstrap_runtime import bind_contract

        contract = TaskContract.model_validate(bind_contract(self, c, p["contract"]))
        self._require_contract_text(contract)
        if contract.domain != self.config.domain:
            raise HubError("DOMAIN_MISMATCH", f"Maintenance keeps the task in the project domain: set the contract's "
                           f"\"domain\" to {self.config.domain!r}, not {contract.domain[:80]!r}", 422)
        if contract.kind in REVIEW_KINDS:
            raise HubError("DOMAIN_MISMATCH", "Maintenance keeps the ordinary task in its domain", 422)
        from .plugins import load_plugin
        load_plugin(contract.domain).validate_contract(contract)
        result = self._apply_revision(c, task, contract.model_dump())
        before, after = contract_substance(task["contract"]), contract_substance(result["contract"])
        material = any(before.get(key) != after.get(key) for key in ("objective", "constraints", "requirements", "acceptance_policy"))
        c.execute("INSERT INTO task_revision_history VALUES(?,?,?,?,?,?,?)", (task["id"], result["revision"], task["revision"],
                  actor.worker_id or "operator", reason, int(material), self.clock()))
        c.execute("UPDATE task_completion_declarations SET state='invalidated',decision_reason='task_revision_changed' "
                  "WHERE task_id=? AND state IN ('pending','finalizing')", (task["id"],))
        self._reconsider_dependents(c, task["id"], "dependency_revision_changed")
        if material:
            # Only open tasks whose contract names this task are read (`_withdraw` says why).
            for row in c.execute("SELECT t.id,r.contract_json FROM tasks t JOIN task_revisions r "
                                 "ON r.task_id=t.id AND r.revision=t.current_revision "
                                 "WHERE t.state NOT IN ('done','retired') AND instr(r.contract_json,?)>0",
                                 (task["id"],)).fetchall():
                if any(item.get("task_id") == task["id"] for item in json.loads(row["contract_json"])["requirements"]):
                    self._notify(c, row["id"], task["id"], "dependency_requirement_changed")
        return result

    @LIFECYCLE.handles("task.pause", "task.wait")
    def _before_task_wait(self, c, name, p, actor):
        attempt, task = self._active(c, actor)
        self._cancel_recruitment(c, task["id"], "explicit_task_pause")
        c.execute("UPDATE workers SET work_intention='waiting' WHERE id=?", (attempt["worker_id"],))
        # Notices it has not read yet no longer turn it to them (_wake_for_message).
        if self._get(c, "workers", attempt["worker_id"])["kind"] == "ordinary" and c.execute("DELETE FROM worker_wake_messages WHERE worker_id=? AND message_id IN (SELECT m.id FROM "
                     "peer_messages m WHERE m.recipient_id=? AND NOT EXISTS(SELECT 1 FROM peer_message_kinds k "
                     "WHERE k.message_id=m.id))", (attempt["worker_id"], attempt["worker_id"])).rowcount:
            self._refresh_message_wake(c, attempt["worker_id"])
        if name == "task.pause":
            checkpoint = p.get("checkpoint", {"summary": p.get("reason", "Paused"), "next_action": "Reconsider saved work when actionable"})
            # No predicate is no condition: task.wait refuses it (WAIT_CONDITION_REQUIRED).
            return self._dispatch_core(c, "task.wait", {"checkpoint": checkpoint, "predicate": p.get("predicate") or {}}, actor)
        return None

    @LIFECYCLE.handles("task.resume_work", "worker.resume_work")
    def _on_resume_work(self, c, name, p, actor):
        attempt, task = self._active(c, actor)
        if attempt["execution_mode"] == "message_only":
            c.execute("UPDATE workers SET work_intention='idle' WHERE id=?", (attempt["worker_id"],))
            return {"worker_id": attempt["worker_id"], "work_intention": "idle"}
        c.execute("UPDATE workers SET work_intention='working' WHERE id=?", (attempt["worker_id"],))
        c.execute("UPDATE continuations SET state='superseded_by_worker' WHERE preferred_worker=? "
                  "AND state IN ('waiting','eligible','reconsider')", (attempt["worker_id"],))
        c.execute("UPDATE task_lifecycle SET suspended=0 WHERE task_id=?", (task["id"],))
        return {"worker_id": attempt["worker_id"], "work_intention": "working"}

    @LIFECYCLE.handles("runtime.ordinary_completed")
    def _on_runtime_ordinary_completed(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        return self._ordinary_turn_completed(c, p["worker_id"], p["attempt_id"])

    @LIFECYCLE.handles("task.complete.declare")
    def _on_task_complete_declare(self, c, name, p, actor):
        return self._declare_completion(c, self._task(c, p["task_id"]), p, actor)

    @LIFECYCLE.handles("task.complete.inspect")
    def _on_task_complete_inspect(self, c, name, p, actor):
        task = self._task(c, p["task_id"])
        self._authorize_participant(c, task, actor)
        return {"task_id": task["id"], "revision": task["revision"], "acceptance_conditions": self._conditions(c, task),
                "declarations": [self._completion_record(c, row["id"]) for row in c.execute(
                    "SELECT id FROM task_completion_declarations WHERE task_id=? ORDER BY created_at,id", (task["id"],))]}

    @LIFECYCLE.handles("task.complete.feedback")
    def _on_task_complete_feedback(self, c, name, p, actor):
        declaration = self._completion_record(c, p["declaration_id"])
        self._active(c, actor)
        if declaration["state"] != "pending" or declaration["feedback_worker_id"] != actor.worker_id:
            raise HubError("FEEDBACK_UNAUTHORIZED", "Only the executing collaborator has this feedback opportunity", 403)
        conditions = p.get("conditions", [])
        if not isinstance(conditions, list) or not all(item in declaration["acceptance"] for item in conditions):
            raise HubError("INVALID_FEEDBACK", "Identify specific declared acceptance conditions", 422)
        if conditions and (not isinstance(p.get("reason"), str) or not p["reason"].strip()):
            raise HubError("REASON_REQUIRED", "Explain the specific unmet acceptance condition", 422)
        checkpoint = Checkpoint.model_validate(p["checkpoint"])
        c.execute("UPDATE attempts SET checkpoint_json=? WHERE id=?", (canonical(checkpoint.model_dump()), actor.attempt_id))
        c.execute("UPDATE task_completion_declarations SET feedback_state='responded',feedback_json=? WHERE id=?",
                  (canonical({"conditions": conditions, "reason": p.get("reason", "No unmet condition identified")}), declaration["id"]))
        self._wake(c)
        return self._completion_record(c, declaration["id"])

    @LIFECYCLE.handles("task.complete.feedback_unavailable")
    def _on_task_complete_feedback_unavailable(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.RUNNER)
        declaration = self._completion_record(c, p["declaration_id"])
        worker = self._get(c, "workers", declaration["feedback_worker_id"])
        if worker["active_attempt"] or p.get("recovery_impossible") is not True or p.get("process_confirmed_stopped") is not True or not p.get("reason"):
            raise HubError("QUIESCENCE_REQUIRED", "Establish the failed collaborator's state and preserve its work")
        c.execute("UPDATE task_completion_declarations SET feedback_state='unavailable',feedback_json=? WHERE id=?",
                  (canonical({"conditions": [], "reason": p["reason"]}), declaration["id"]))
        self._wake(c)
        return self._completion_record(c, declaration["id"])

    @LIFECYCLE.handles("task.complete.finalize")
    def _on_task_complete_finalize(self, c, name, p, actor):
        return self._finalize_completion(c, p, actor)

    @LIFECYCLE.handles("task.complete.cancel")
    def _on_task_complete_cancel(self, c, name, p, actor):
        return self._cancel_completion(c, p, actor)

    @LIFECYCLE.handles("artifact.withdraw")
    def _on_artifact_withdraw(self, c, name, p, actor):
        artifact = self._artifact(c, p["artifact_id"])
        if actor.role not in {Role.OPERATOR, Role.VALIDATOR}:
            if artifact["status"] == "accepted":
                raise HubError("PUBLISHED_WITHDRAWAL_REQUIRES_OPERATOR", "Removing already published Library content is a separate operation", 403)
            self._authorize_participant(c, self._task(c, artifact["task_id"]), actor)
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise HubError("REASON_REQUIRED", "Explicit withdrawal requires a retained reason", 422)
        withdrawn = self._withdraw(c, artifact["id"], reason)
        for withdrawn_id in withdrawn:
            request = c.execute("SELECT task_id FROM review_requests WHERE candidate_id=? AND state IN ('pending','integrating')", (withdrawn_id,)).fetchone()
            if request:
                c.execute("UPDATE review_requests SET state='withdrawn' WHERE task_id=?", (request["task_id"],))
                self._drain_task(c, request["task_id"], "candidate_withdrawn")
                self._recompute_collaborative(c, request["task_id"])
        if artifact["status"] != "accepted":
            c.execute("UPDATE artifact_obligations SET withdrawn_reason=?,withdrawn_at=? WHERE artifact_id=?", (reason, self.clock(), artifact["id"]))
        archive = getattr(self, "_archive_submission", None)
        if callable(archive):
            for withdrawn_id in withdrawn:
                archive(c, withdrawn_id, "withdrawn", reason)
        if artifact["task_id"]:
            c.execute("UPDATE task_completion_declarations SET state='invalidated',decision_reason='artifact_withdrawn' "
                      "WHERE task_id=? AND state IN ('pending','finalizing')", (artifact["task_id"],))
            self._notify(c, artifact["task_id"], artifact["id"], "withdrawal_requires_acceptance_recheck")
            self._reconsider_dependents(c, artifact["task_id"], "dependency_artifact_withdrawn")
        self._refresh_waits(c)
        return {"withdrawn": withdrawn}

    @LIFECYCLE.handles("review.reassign")
    def _on_review_reassign(self, c, name, p, actor):
        return self._review_reassign(c, p, actor)

    @LIFECYCLE.handles("task.leave")
    def _on_task_leave(self, c, name, p, actor):
        task = self._task(c, p["task_id"])
        self._authorize_participant(c, task, actor)
        if task["contract"]["kind"] in REVIEW_KINDS:
            raise HubError("REVIEW_RECOVERY_REQUIRED", "An unfinished review follows recovery and reassignment")
        worker_id = actor.worker_id if actor.role == Role.WORKER else p["worker_id"]
        if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? AND executor=1 AND membership_state='member'",
                         (task["id"], worker_id)).fetchone():
            raise HubError("EXECUTOR_REQUIRED", "Only a current executor may permanently leave its membership", 403)
        if not p.get("reason") or p.get("permanent") is not True:
            raise HubError("REASON_REQUIRED", "Leaving is permanent and requires a retained reason", 422)
        self._cancel_recruitment(c, task["id"], "executor_left")
        c.execute("UPDATE task_members SET membership_state='left',runnable=0,work_reason=? WHERE task_id=? AND worker_id=?",
                  (p["reason"], task["id"], worker_id))
        self._end_member_resumptions(c, task["id"], worker_id, "membership_ended")
        worker = self._get(c, "workers", worker_id)
        if worker["active_attempt"]:
            attempt = self._get(c, "attempts", worker["active_attempt"])
            if attempt["task_id"] == task["id"]:
                c.execute("UPDATE attempts SET state='draining' WHERE id=?", (attempt["id"],))
                c.execute("UPDATE workers SET state='draining' WHERE id=?", (worker_id,))
                c.execute("INSERT OR REPLACE INTO attempt_stop_requests VALUES(?,?,?)", (attempt["id"], "executor_left", self.clock()))
        self._release_ended_member(c, task["id"], worker_id)
        if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND executor=1 AND membership_state='member'", (task["id"],)).fetchone():
            c.execute("UPDATE tasks SET state='retired' WHERE id=?", (task["id"],))
            self._drain_task(c, task["id"], "all_executors_left")
            self._invalidate_reviews(c, task["id"], "host_executors_left")
            self._notify(c, task["id"], task["id"], p["reason"])
            self._reconsider_dependents(c, task["id"], "dependency_retired")
            self._end_closed_subscriptions(c, task["id"], "subscribed_task_retired")
            self._refresh_waits(c)
        return self._task(c, task["id"])

    @LIFECYCLE.handles("task.invite_back")
    def _on_task_invite_back(self, c, name, p, actor):
        """A current executor, or the operator, invites a worker that left the task back to it. It
        becomes a member again, like a paused member whose wait has just ended: its next turn
        resumes the task from its saved checkpoint and files, and reads who invited it and why.
        Only a former executor returns, so the task's two executors over its lifetime stay two."""
        task = self._task(c, p["task_id"])
        if task["contract"]["kind"] in REVIEW_KINDS:
            raise HubError("REVIEW_PROTOCOL", "A review has exactly one executor and never invites one back")
        if task["state"] in TERMINAL:
            raise HubError("TASK_TERMINAL", "A done or retired task takes no executor back", 409)
        if actor.role == Role.WORKER:
            self._active(c, actor)
            if not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND worker_id=? AND executor=1 "
                             "AND membership_state='member'", (task["id"], actor.worker_id)).fetchone():
                raise HubError("EXECUTOR_REQUIRED", "Only a current executor of this task may invite a worker back", 403)
        else:
            self._role(actor, Role.OPERATOR)
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 4000:
            raise HubError("REASON_REQUIRED", "Say why the worker is invited back, in at most 4000 characters", 422)
        self._normalize_worker_fields(c, p, "worker_id")
        invitee = self._get(c, "workers", p["worker_id"])
        label = worker_identity(dict(invitee)).get("display_name", invitee["id"])
        member = c.execute("SELECT * FROM task_members WHERE task_id=? AND worker_id=? AND executor=1",
                           (task["id"], invitee["id"])).fetchone()
        if member is None or member["membership_state"] == "member":
            raise HubError("FORMER_EXECUTOR_REQUIRED", f"{label} is " + ("still an executor of this task" if member else
                           "not a former executor of this task") + ": only a worker that left it can be invited back", 409)
        if member["membership_state"] != "left":
            raise HubError("MEMBERSHIP_ENDED", f"{label}'s membership ended with the task's completion and cannot return", 409)
        busy = self._invitation_blocker(c, invitee, task["id"])
        if busy:
            raise HubError("WORKER_BUSY", f"{label} {busy}, so it cannot come back now. Message it with talk_send, "
                           "or invite it again later.", 409)
        attempt = c.execute("SELECT * FROM attempts WHERE task_id=? AND worker_id=? AND execution_mode<>'message_only' "
                            "ORDER BY created_at DESC,rowid DESC LIMIT 1", (task["id"], invitee["id"])).fetchone()
        snapshot = (attempt["snapshot"] if attempt else None) or member["snapshot"]
        if attempt is None or attempt["ended_at"] is None or not snapshot:
            raise HubError("WORKER_BUSY", f"{label}'s last turn on this task has not finished saving its files; "
                           "invite it again later", 409)
        checkpoint = (attempt["checkpoint_json"] or member["checkpoint_json"]
                      or canonical({"summary": "You left this task: " + (member["work_reason"] or "no reason recorded"),
                                    "next_action": "Read why you were invited back, then continue the task."}))
        cid = new_id()
        c.execute("INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,"
                  "snapshot,state,wake_reason,origin_attempt,created_at) VALUES(?,?,?,?,?,?,?,'reconsider','invited_back',?,?)",
                  (cid, task["id"], task["revision"], invitee["id"], canonical({"mode": "all"}), checkpoint, snapshot,
                   attempt["id"], self.clock()))
        c.execute("UPDATE task_members SET membership_state='member',runnable=0,work_reason='invited_back' "
                  "WHERE task_id=? AND worker_id=?", (task["id"], invitee["id"]))
        c.execute("UPDATE workers SET current_task_id=?,work_intention='waiting' WHERE id=?", (task["id"], invitee["id"]))
        inviter = "the operator" if actor.role != Role.WORKER else worker_identity(
            dict(self._get(c, "workers", actor.worker_id))).get("display_name", actor.worker_id)
        self._event(c, "task.invited_back", task["id"], {"invited_by": inviter, "reason": reason, "continuation_id": cid},
                    task["id"], invitee["id"])
        return {"task_id": task["id"], "worker_id": invitee["id"], "continuation_id": cid, "invited_by": inviter}

    def _invitation_blocker(self, c, worker, task_id):
        """Why `worker` cannot return to `task_id` now (a few words), or None when it can."""
        if worker["kind"] != "ordinary":
            return "is not an ordinary worker"
        if worker["work_intention"] == "retired" or worker["state"] == "retired":
            return "is retired"
        if worker["state"] == "recovery_required":
            return "is recovering an interrupted execution"
        if worker["active_attempt"] or worker["state"] in {"preparing", "running", "draining"}:
            return "is in a turn"
        if c.execute("SELECT 1 FROM interruption_checkpoints WHERE worker_id=? AND state='parked'", (worker["id"],)).fetchone():
            return "holds an interrupted turn to resume"
        if worker["current_task_id"] and worker["current_task_id"] != task_id and \
                self._get(c, "tasks", worker["current_task_id"])["state"] not in TERMINAL:
            return "is working on another unfinished task"
        return None

    @LIFECYCLE.handles("run.resume")
    def _before_run_resume(self, c, name, p, actor):
        if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
            self._role(actor, Role.OPERATOR)
            if self._work_in_flight(c):
                raise HubError("QUIESCENCE_REQUIRED", "Finish orderly shutdown before explicitly resuming retained work")
            c.execute("UPDATE project_lifecycle SET state='running',shutdown_reason=NULL WHERE id=1")
        # A parked ticket owns the saved session. Earlier preflight errors
        # must not change its state and make interruption.resume skip it.
        # Old idle tickets are repaired once at startup, not on every Resume.
        c.execute("UPDATE workers SET state='idle' WHERE state='suspended' AND work_intention='working' AND active_attempt IS NULL "
                  "AND id IN (SELECT a.worker_id FROM attempts a WHERE a.state='preflight_held' "
                  "AND a.id=(SELECT latest.id FROM attempts latest WHERE latest.worker_id=a.worker_id "
                  "ORDER BY latest.created_at DESC,latest.rowid DESC LIMIT 1)) "
                  "AND NOT EXISTS(SELECT 1 FROM interruption_checkpoints i WHERE i.worker_id=workers.id AND i.state='parked')")
        return None  # The core handler of the command runs next.

    @LIFECYCLE.handles("run.start")
    def _before_run_start(self, c, name, p, actor):
        if (c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running"
                and self._work_in_flight(c)):
            raise HubError("QUIESCENCE_REQUIRED", "Complete and reconcile orderly shutdown before starting another run")
        return None  # The core handler of the command runs next.

    def _lifecycle_after(self, c, name, p, actor, result):
        if name == "run.start":
            c.execute("UPDATE project_lifecycle SET state='running',shutdown_reason=NULL WHERE id=1")
            c.execute("UPDATE workers SET state='idle',work_intention='working' WHERE active_attempt IS NULL AND id IN "
                      "(SELECT worker_id FROM task_members WHERE work_reason='project_shutdown' AND membership_state='member')")
            c.execute("UPDATE task_members SET runnable=1 WHERE work_reason='project_shutdown' AND membership_state='member'")
            c.execute("UPDATE workers SET run_id=? WHERE active_attempt IS NULL AND work_intention IN ('working','waiting','idle') "
                      "AND state NOT IN ('retired','recovery_required')", (result["id"],))
            # Assignment decisions belong to the project, not to its Dispatcher's run: unfinished
            # ones continue in the new run (a retained answer is committed, an unknown outcome
            # reconciled), so none is left behind holding all later assignment.
            c.execute("UPDATE helper_intents SET run_id=? WHERE (kind='assignment' AND state IN "
                      "('prepared','running','uncertain')) OR (state='prepared' AND source_worker_id IN "
                      "(SELECT id FROM workers WHERE active_attempt IS NULL AND run_id=?))", (result["id"], result["id"]))
        elif name == "task.publish":
            self._lifecycle(c, result["id"])
            c.execute("UPDATE project_lifecycle SET root_task_id=? WHERE id=1 AND root_task_id IS NULL", (result["id"],))
        elif name == "worker.bind_session":
            self._complete_rotation(c, p["worker_id"], p["thread_id"])
        elif name == "turn.finish" and p["status"] == "completed":
            intent = self._get(c, "turn_intents", p["intent_id"])
            attempt = self._get(c, "attempts", intent["attempt_id"])
            if self._get(c, "workers", intent["worker_id"])["kind"] == "ordinary":
                self._record_first_turn(c, intent["worker_id"], attempt["task_id"], intent["remote_turn_id"])
        elif name in {"continuation.resume", "interruption.resume"}:
            c.execute("UPDATE workers SET work_intention='working',pending_wake=0 WHERE id=?", (result["worker_id"],))
            c.execute("UPDATE task_lifecycle SET suspended=0 WHERE task_id=?", (result["task_id"],))
        elif name == "runtime.quiescent":
            worker = self._get(c, "workers", p["worker_id"])
            attempt = c.execute("SELECT * FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (worker["id"],)).fetchone()
            self._feedback_boundary(c, worker["id"], attempt["task_id"])
            self._seal_dependency_reconsideration(c, attempt, result["snapshot"])
            if result.get("reason") == "task_revision_changed":
                c.execute("UPDATE continuations SET snapshot=? WHERE task_id=? AND preferred_worker=? "
                          "AND state='reconsider' AND wake_reason='task_revision_changed'",
                          (result["snapshot"], attempt["task_id"], worker["id"]))
            if result.get("directive") == "verify" and worker["kind"] == "ordinary":
                c.execute("UPDATE workers SET work_intention='working' WHERE id=?", (worker["id"],))
                self._set_member_work(c, attempt["task_id"], worker["id"], True, "submitted_continue_work")
                self._recompute_task(c, attempt["task_id"])
                self._record_first_turn(c, worker["id"], attempt["task_id"])
            elif result.get("directive") in {"released", "waiting"}:
                c.execute("UPDATE workers SET work_intention='waiting',state='suspended' WHERE id=?", (worker["id"],))
                self._set_member_work(c, attempt["task_id"], worker["id"], False, "voluntary_wait")
            elif result.get("reason") in {"executor_left", "all_executors_left", "task.retire"} and worker["kind"] == "ordinary":
                self._release_ended_member(c, attempt["task_id"], worker["id"])
            elif result.get("reason") == "project_shutdown" and self._get(c, "tasks", attempt["task_id"])["state"] not in TERMINAL:
                checkpoint = attempt["checkpoint_json"] or canonical({
                    "summary": "Project shutdown preserved this unfinished execution and its exact native conversation.",
                    "next_action": "Resume the saved task from this snapshot after explicit operator resumption.", "input_refs": []})
                c.execute("UPDATE task_members SET checkpoint_json=?,runnable=1,work_reason='project_shutdown' "
                          "WHERE task_id=? AND worker_id=? AND membership_state='member'", (checkpoint, attempt["task_id"], worker["id"]))
                c.execute("UPDATE workers SET state='suspended',work_intention='working' WHERE id=?", (worker["id"],))
            self._finish_shutdown_if_quiet(c)
            self._release_ended_member(c, attempt["task_id"], worker["id"])
        elif name == "runtime.abandon" and result.get("directive") == "abandoned":
            worker = self._get(c, "workers", p["worker_id"])
            if worker["current_task_id"]:
                snapshot = self._workspace_snapshot(worker["id"])
                attempt = c.execute("SELECT * FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (worker["id"],)).fetchone()
                c.execute("UPDATE attempts SET snapshot=? WHERE id=?", (snapshot, attempt["id"]))
                self._seal_dependency_reconsideration(c, attempt, snapshot)
                self._preserve_member(c, dict(attempt), snapshot, reason="execution_recovery_required")
                c.execute("UPDATE workers SET state='recovery_required',work_intention='working' WHERE id=?", (worker["id"],))
                c.execute("UPDATE tasks SET state='recovery_required' WHERE id=? AND state NOT IN ('done','retired')", (worker["current_task_id"],))
                result = {**result, "directive": "recovery_required", "snapshot": snapshot}
            else:
                c.execute("UPDATE workers SET work_intention='idle' WHERE id=?", (worker["id"],))
            # Failure is not a voluntary wait and never cancels recruitment.
        elif name == "worker.recover" and result.get("directive") == "recovery_required" and result.get("reason") != "repository_reconciliation_required":
            saved = c.execute("SELECT checkpoint_json FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                              (p["worker_id"],)).fetchone()
            result = {**result, "checkpoint": json.loads(saved["checkpoint_json"]) if saved and saved["checkpoint_json"] else None}
            unresolved = c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? AND state IN ('prepared','sending','bound','uncertain','reconciled')", (p["worker_id"],)).fetchone()
            if unresolved:
                attempt = c.execute("SELECT checkpoint_json FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (p["worker_id"],)).fetchone()
                result = {**result, "checkpoint": json.loads(attempt["checkpoint_json"]) if attempt and attempt["checkpoint_json"] else None,
                          "terminal_reconciliation_required": True}
            else:
                recovered = self._lifecycle_command(c, "worker.recover", p, actor)
                if recovered is not None:
                    result = {**result, **recovered}
        elif name == "task.retire":
            self._cancel_recruitment(c, p["task_id"], "task_retired")
            c.execute("UPDATE task_members SET membership_state='ended',runnable=0 WHERE task_id=?", (p["task_id"],))
            for member in c.execute("SELECT worker_id FROM task_members WHERE task_id=?", (p["task_id"],)).fetchall():
                self._release_ended_member(c, p["task_id"], member["worker_id"])
            self._reconsider_dependents(c, p["task_id"], "dependency_retired")
            self._end_closed_subscriptions(c, p["task_id"], "subscribed_task_retired")
        if name == "worker.recover" and result.get("directive") == "recovered":
            c.execute("UPDATE workers SET state='suspended' WHERE id=? AND active_attempt IS NULL AND state='idle' "
                      "AND EXISTS(SELECT 1 FROM interruption_checkpoints i WHERE i.worker_id=workers.id AND i.state='parked')",
                      (p["worker_id"],))
        return result
