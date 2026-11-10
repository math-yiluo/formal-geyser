"""Durable task membership, immutable review candidates, and shared review transitions.

Historical contracts and receipts remain readable; active admission and ordinary
completion are owned by the temporary-membership lifecycle.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .errors import HubError
from .integration_notes import checked_integration_notes
from .models import Actor, Role, TaskContract, file_name_fields
from .storage import canonical, digest, snapshot_entries
from .text_bounds import shorten


def new_id():
    return str(uuid4())


# A revision that changes only these keeps the task's candidates and their reviews.
CONTEXT_FIELDS = frozenset({"title", "context", "priority"})
# A requirement's substance in SQL: a stored one may carry the former `policy`, which asks nothing.
_REQUIREMENT_SQL = "json_remove({v},'$.policy')"
# The same comparison in SQL, between two task_revisions rows {a} and {b}.
SAME_SUBSTANCE_SQL = (
    "json_remove({a}.contract_json,'$.title','$.context','$.priority','$.requirements')="
    "json_remove({b}.contract_json,'$.title','$.context','$.priority','$.requirements') "
    "AND (SELECT json_group_array(json(" + _REQUIREMENT_SQL.format(v="value") + ")) "
    "FROM json_each({a}.contract_json,'$.requirements'))="
    "(SELECT json_group_array(json(" + _REQUIREMENT_SQL.format(v="value") + ")) "
    "FROM json_each({b}.contract_json,'$.requirements'))")
# Whether revision {a} asks for at least what {b} asks: the same apart from those keys, with every
# requirement of {b} and perhaps more. Its accepted evidence then meets a requirement naming {b}. In
# the Linux pilot a revision only added a requirement, and a task whose original revision required
# the earlier one could never be completed with the later one's proof.
AT_LEAST_SUBSTANCE_SQL = (
    "json_remove({a}.contract_json,'$.title','$.context','$.priority','$.requirements')="
    "json_remove({b}.contract_json,'$.title','$.context','$.priority','$.requirements') "
    "AND NOT EXISTS(SELECT 1 FROM json_each({b}.contract_json,'$.requirements') asked "
    "WHERE " + _REQUIREMENT_SQL.format(v="asked.value") + " NOT IN (SELECT "
    + _REQUIREMENT_SQL.format(v="value") + " FROM json_each({a}.contract_json,'$.requirements')))")


def requirement_substance(requirement: Any) -> Any:
    """What a requirement asks for: a stored one's former `policy` asks nothing."""
    if isinstance(requirement, dict) and "policy" in requirement:
        return {key: value for key, value in requirement.items() if key != "policy"}
    return requirement


def contract_substance(contract: dict) -> dict:
    """What a task contract asks for: everything but its title, context and priority."""
    substance = {key: value for key, value in contract.items() if key not in CONTEXT_FIELDS}
    if isinstance(substance.get("requirements"), list):
        substance["requirements"] = [requirement_substance(item) for item in substance["requirements"]]
    return substance


class Collaboration:
    if TYPE_CHECKING:
        _prepared_candidate: Any
        _restore_library_copy: Any
        db: Any
        blobs: Any
        clock: Any
        workspace: Any
        _get: Any
        _task: Any
        _artifact: Any
        _reconsider_artifact_waits: Any
        _role: Any
        _wake_publication_waits: Any
        _active: Any
        _event: Any
        _notify: Any
        _dispatch: Any
        _insert_artifact: Any
        _valid_inputs: Any
        _wake: Any
        _refresh_waits: Any
        _invalidate_interruptions: Any
        _publication_paths: Any
        _require_source_files: Any
        _bind_publication_repository: Any
        _candidate_repository: Any
        _reconsider_task_revision: Any
        _independent: Any
        _can_review: Any
        _require_one_task: Any
        _merge_published: Any
        _persist_task_content: Any
        _record_body: Any
        _available_artifact: Any
        _normalize_worker_fields: Any

    def _collaboration(self, c, task_id):
        row = c.execute("SELECT * FROM task_collaboration WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Task collaboration does not exist", 404)
        result = dict(row)
        result["members"] = [dict(item) for item in c.execute(
            "SELECT worker_id,executor,runnable,work_reason FROM task_members WHERE task_id=? "
            "ORDER BY joined_at,worker_id", (task_id,))]
        result["publishers"] = [item["worker_id"] for item in c.execute(
            "SELECT worker_id FROM task_publishers WHERE task_id=? ORDER BY worker_id", (task_id,))]
        result["review_feedback"] = [self._review_feedback(c, item["task_id"]) for item in c.execute(
            "SELECT r.task_id FROM review_requests r JOIN peer_review_receipts p ON p.review_task_id=r.task_id "
            "WHERE r.host_task_id=? ORDER BY p.created_at DESC,p.rowid DESC LIMIT 8", (task_id,))]
        request = c.execute("SELECT candidate_id,state FROM review_requests WHERE task_id=?", (task_id,)).fetchone()
        if request:
            binding = self._candidate_binding(c, request["candidate_id"])
            result["review"] = {**self._review_basis(binding), "state": request["state"],
                                "author_id": binding["author_id"], "coauthors": binding["coauthors"],
                                "completion": bool(binding["completion"]), "snapshot": binding["snapshot"],
                                "contract_hash": binding["contract_hash"],
                                "host_task_id": binding["task_id"]}
            acceptance = self._review_acceptance(c, task_id)
            if acceptance is not None:
                result["review"]["acceptance"] = acceptance
            feedback = self._review_feedback(c, task_id)
            if feedback is not None:
                result["review"]["feedback"] = feedback
            repository = self._candidate_repository(c, request["candidate_id"])
            if repository:
                result["review"]["repository"] = repository
        return result

    def _task_family(self, c, task_id):
        """Linked protocol work shares its host's conversation delivery cursor."""
        row = c.execute("SELECT host_task_id FROM task_collaboration WHERE task_id=?", (task_id,)).fetchone()
        return row["host_task_id"] if row and row["host_task_id"] else task_id

    def _member(self, c, task_id, worker_id, *, executor=False, runnable=False, reason=None):
        self._get(c, "workers", worker_id)
        if executor:
            self._require_one_task(c, worker_id, task_id)
        c.execute("INSERT INTO task_members(task_id,worker_id,executor,runnable,work_reason,joined_at) "
                  "VALUES(?,?,?,?,?,?) ON CONFLICT(task_id,worker_id) DO UPDATE SET "
                  "executor=MAX(executor,excluded.executor),runnable=MAX(runnable,excluded.runnable),"
                  "work_reason=CASE WHEN excluded.runnable=1 THEN excluded.work_reason ELSE work_reason END",
                  (task_id, worker_id, int(executor), int(runnable), reason, self.clock()))
        c.execute("INSERT OR IGNORE INTO task_subscriptions VALUES(?,?,NULL,'membership',?)",
                  (task_id, worker_id, self.clock()))

    def _init_collaboration(self, c, task_id, actor, *, host_task_id=None, internal=False):
        governance = "operator" if actor.role == Role.OPERATOR else "publishers"
        c.execute("INSERT INTO task_collaboration(task_id,mode,host_task_id,governance,created_by,created_at) "
                  "VALUES(?,'collaborative',?,?,?,?)",
                  (task_id, host_task_id, governance, actor.worker_id or "operator", self.clock()))
        publishers = set()
        if actor.role == Role.WORKER:
            attempt, host = self._active(c, actor)
            publishers.add(attempt["worker_id"])
            # Capture current independent executors, including their durable paused membership.
            if attempt["execution_mode"] != "message_only":
                publishers.update(row["worker_id"] for row in c.execute(
                    "SELECT worker_id FROM task_members WHERE task_id=? AND executor=1 AND membership_state='member'", (host["id"],)))
                publishers.update(row["worker_id"] for row in c.execute(
                    "SELECT worker_id FROM attempts WHERE task_id=? AND ended_at IS NULL AND execution_mode<>'message_only'", (host["id"],)))
        for worker_id in sorted(publishers):
            self._publisher(c, task_id, worker_id)

    def _publisher(self, c, task_id, worker_id):
        self._get(c, "workers", worker_id)
        c.execute("INSERT OR IGNORE INTO task_publishers VALUES(?,?,?)", (task_id, worker_id, self.clock()))
        c.execute("INSERT OR IGNORE INTO task_subscriptions VALUES(?,?,NULL,'publisher',?)",
                  (task_id, worker_id, self.clock()))

    def _active_attempts(self, c, task_id):
        return [dict(row) for row in c.execute(
            "SELECT * FROM attempts WHERE task_id=? AND ended_at IS NULL AND execution_mode<>'message_only' ORDER BY created_at,id", (task_id,))]

    def _refresh_aggregate(self, c, task_id):
        """The task's first active attempt, for display and quick checks; never execution authority."""
        attempts = self._active_attempts(c, task_id)
        c.execute("UPDATE tasks SET active_attempt=?,state_version=state_version+1 WHERE id=?",
                  (attempts[0]["id"] if attempts else None, task_id))
        return attempts

    def _set_member_work(self, c, task_id, worker_id, runnable, reason):
        c.execute("UPDATE task_members SET runnable=?,work_reason=? WHERE task_id=? AND worker_id=?",
                  (int(runnable), reason, task_id, worker_id))

    def _preserve_member(self, c, attempt, snapshot, *, runnable=False, reason=None):
        # A member that left keeps the reason it gave (shown to whoever may invite it back),
        # not the stop reason of the turn that ended after it left.
        c.execute("UPDATE task_members SET checkpoint_json=COALESCE(?,checkpoint_json),snapshot=?,"
                  "runnable=CASE WHEN membership_state='left' THEN 0 ELSE ? END,"
                  "work_reason=CASE WHEN membership_state='left' THEN work_reason ELSE ? END WHERE task_id=? AND worker_id=?",
                  (attempt["checkpoint_json"], snapshot, int(runnable), reason,
                   attempt["task_id"], attempt["worker_id"]))

    def _candidate_binding(self, c, artifact_id):
        row = c.execute("SELECT * FROM candidate_bindings WHERE artifact_id=?", (artifact_id,)).fetchone()
        if row is None:
            raise HubError("NOT_REVIEW_CANDIDATE", "The artifact has no peer-review binding")
        result = dict(row)
        result["input_refs"] = json.loads(result.pop("input_refs_json"))
        result["coauthors"] = json.loads(result.pop("coauthors_json"))
        return result

    @staticmethod
    def _review_basis(binding):
        return {"candidate_id": binding["artifact_id"], "candidate_hash": binding["body_sha"],
                "task_revision": binding["task_revision"], "input_refs": binding["input_refs"],
                "policy": binding["policy"]}

    def _same_substance(self, c, task, revision, contract_hash):
        """The task's current revision asks for what its revision `revision` (with this contract
        hash) asked for: they differ at most in title, context and priority."""
        if revision == task["revision"]:
            return contract_hash == task["contract_hash"]
        if revision > task["revision"]:
            return False
        earlier = self._task(c, task["id"], revision)
        return (earlier["contract_hash"] == contract_hash
                and contract_substance(earlier["contract"]) == contract_substance(task["contract"]))

    @staticmethod
    def _named_revision(c, task_id, revision):
        """The revision that a reference to `revision` of a task means: that revision, unless its
        objective is only a file name such as objective.md, so that it named no target (contracts
        were stored so until 2026-10-22); then the first later revision that names one, once
        there is one."""
        for row in c.execute("SELECT revision,json_extract(contract_json,'$.objective') AS objective "
                             "FROM task_revisions WHERE task_id=? AND revision>=? ORDER BY revision",
                             (task_id, revision)):
            if not file_name_fields({"objective": row["objective"]}):
                return row["revision"]
        return revision

    def _original_task(self, c, task_id):
        """The task as its publisher first asked for it: its first revision that names a target."""
        return self._task(c, task_id, self._named_revision(c, task_id, 1))

    def _named_requirement(self, c, requirement):
        """A requirement on a task's revision, pinned to the revision `_named_revision` means."""
        if requirement.get("artifact_id") or not requirement.get("task_id"):
            return requirement
        revision = self._named_revision(c, requirement["task_id"], requirement["task_revision"])
        return requirement if revision == requirement["task_revision"] else {**requirement, "task_revision": revision}

    def _basis_valid(self, c, binding):
        task = self._task(c, binding["task_id"])
        artifact = self._get(c, "artifacts", binding["artifact_id"])
        return (self._same_substance(c, task, binding["task_revision"], binding["contract_hash"])
                and task["state"] != "retired" and artifact["status"] not in {"withdrawn", "superseded"}
                and artifact["body_sha"] == binding["body_sha"]
                and artifact["snapshot"] == binding["snapshot"]
                and json.loads(artifact["input_refs_json"]) == binding["input_refs"]
                and all(self._available_artifact(c, ref) for ref in binding["input_refs"]))

    def _internal_task(self, c, contract, host_task_id, created_by):
        task_id = new_id()
        c.execute("INSERT INTO tasks(id,current_revision,state,created_at) VALUES(?,1,'ready',?)",
                  (task_id, self.clock()))
        c.execute("INSERT INTO task_revisions VALUES(?,1,?,?)",
                  (task_id, canonical(contract), digest(contract)))
        self._persist_task_content(c, task_id, 1, contract)
        c.execute("INSERT INTO search_index VALUES(?,?,?,?)",
                  (task_id, "task", contract["title"], contract["objective"]))
        c.execute("INSERT INTO task_collaboration(task_id,mode,host_task_id,governance,created_by,created_at) "
                  "VALUES(?,'collaborative',?,'protocol',?,?)", (task_id, host_task_id, created_by, self.clock()))
        return task_id

    def _candidate_snapshot(self, worker_id):
        prepared = self._prepared_candidate(worker_id)
        if prepared is not None:
            return prepared
        self._restore_library_copy(worker_id)
        workspace = self.workspace(worker_id)
        io_workspace = workspace.resolve()
        # Measure what the snapshot will hold before it stores anything.
        count, size = 0, 0
        for _, entry in snapshot_entries(io_workspace):
            if entry.is_symlink():  # No linked directory is entered, so only a linked file escapes.
                raise HubError("INVALID_SNAPSHOT", "Candidate snapshots cannot contain linked files", 422)
            count += 1
            size += entry.stat(follow_symlinks=False).st_size
            if count > self.blobs.max_files or size > self.blobs.max_bytes:
                raise HubError("SNAPSHOT_TOO_LARGE", "Candidate snapshot exceeds the configured capture limits", 413)
        # Content evidence: both passes read every file.
        snapshot = self.blobs.snapshot(workspace, exact=True)
        if self.blobs.snapshot(workspace, exact=True) != snapshot:
            raise HubError("SNAPSHOT_CHANGED", "The workspace changed while the immutable candidate was captured")
        return snapshot

    def _stage_candidate(self, c, attempt, task, p, snapshot):
        """Stage the active worker's immutable candidate for independent review."""
        author_id = attempt["worker_id"]
        if task["contract"]["kind"] in {"review"}:
            raise HubError("REVIEW_PROTOCOL", "Review tasks submit receipts and cannot create nested artifact reviews")
        self._valid_inputs(c, task["contract"])
        if len(canonical(p["data"]).encode("utf-8")) > 1048576:
            raise HubError("CANDIDATE_TOO_LARGE", "Candidate objects are limited to one MiB; use snapshot references for files", 413)
        explicit_refs = p.get("input_refs", [])
        coauthors = p.get("coauthors", [])
        if (not isinstance(explicit_refs, list) or not all(isinstance(ref, str) for ref in explicit_refs)
                or len(explicit_refs) > 200 or not isinstance(coauthors, list)
                or not all(isinstance(worker, str) for worker in coauthors) or len(coauthors) > 32):
            raise HubError("INVALID_INPUT", "Candidate references and coauthors must be bounded ID lists", 422)
        refs = sorted(set(json.loads(attempt["input_refs_json"])) | set(explicit_refs))
        if any(not self._available_artifact(c, ref) for ref in refs):
            raise HubError("INPUTS_CHANGED", "Only public accepted artifacts can be candidate inputs")
        # Authorship follows the contributed work, not the task's execution team.
        # Identity, provenance, and reviewer exclusion still use durable workers.
        for coauthor in coauthors:
            if not c.execute("SELECT 1 FROM workers WHERE id=?", (coauthor,)).fetchone():
                raise HubError("INVALID_COAUTHOR", "Coauthors must identify existing workers")
        if "completion" in p:
            raise HubError("INVALID_INPUT", "Task completion uses a separate evidence-backed declaration", 422)
        policy = p.get("policy", task["contract"]["acceptance_policy"])
        from .plugins import load_plugin
        plugin = load_plugin(task["contract"]["domain"])
        hook = getattr(plugin, "validate_publication", None)
        if callable(hook):
            hook(TaskContract.model_validate(task["contract"]), p["kind"], p["data"], policy)
        if policy != task["contract"]["acceptance_policy"]:
            if not callable(hook):
                raise HubError("POLICY_MISMATCH", "The domain must explicitly authorize an alternate publication policy")
        manifest = self.blobs.get_json(snapshot)
        if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
            raise HubError("INVALID_SNAPSHOT", "An immutable worker workspace snapshot is required", 422)
        snapshot_hook = getattr(plugin, "validate_candidate_snapshot", None)
        if callable(snapshot_hook):
            snapshot_hook(self, task, p, snapshot)
        source_row = c.execute("SELECT base_commit FROM repository_bases WHERE worker_id=?", (author_id,)).fetchone()
        source_base = source_row["base_commit"] if source_row else None
        publication_paths = self._publication_paths(task, p, plugin)
        self._require_source_files(snapshot, publication_paths)
        path_hook = getattr(plugin, "validate_publication_paths", None)
        if callable(path_hook):
            path_hook(self, task, p, snapshot, source_base, publication_paths)
        introduction = p.get("introduction")
        introduction_source = "author_introduction"
        if introduction is None:
            data = p["data"] if isinstance(p["data"], dict) else {}
            introduction = next((data[key] for key in ("summary", "name", "description")
                                 if isinstance(data.get(key), str) and data[key].strip()), None)
            introduction_source = "authored_record_text" if introduction is not None else "generated_label"
            if introduction is None:
                introduction = p["kind"] + ": " + task["contract"]["title"]
            elif len(introduction) > 4000:
                introduction = shorten(introduction, 4000)
                introduction_source = "authored_record_excerpt"
        if not isinstance(introduction, str) or not introduction.strip() or len(introduction) > 4000:
            raise HubError("INTRODUCTION_REQUIRED", "Supply a brief artifact introduction of at most 4000 characters", 422)
        artifact = self._insert_artifact(c, p["kind"], p["data"], snapshot=snapshot, task=task,
                                         producer=author_id, input_refs=refs, source_paths=publication_paths)
        self._bind_publication_repository(c, attempt, artifact, publication_paths)
        review_contract = TaskContract(
            title=shorten("Review: " + task["contract"]["title"], 300),
            objective="Review the exact immutable candidate, its evidence, and the host task contract. "
                      "Reject demonstrated defects with concrete suggestions, or independently accept the candidate. "
                      "After acceptance, end this turn; the dedicated Integrator performs final integration, "
                      "validation, and publication, and may ask you for clarification. "
                      "Acceptance alone does not make the candidate available in the Library. "
                      "After publication or rejection this temporary reviewer is retired.",
            domain=task["contract"]["domain"], kind="review", acceptance_policy="core.peer-review@1",
            context=introduction, priority=0,
            payload={"candidate_id": artifact["id"], "host_task_id": task["id"],
                     "introduction": introduction, "introduction_source": introduction_source},
        ).model_dump()
        review_id = self._internal_task(c, review_contract, task["id"], author_id)
        c.execute("INSERT INTO candidate_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (artifact["id"], task["id"], task["revision"], task["contract_hash"], artifact["body_sha"],
                   snapshot, canonical(refs), policy, author_id, canonical(sorted(set(coauthors))),
                   0, review_id, self.clock()))
        c.execute("INSERT INTO review_requests VALUES(?,?,?,'pending',?)",
                  (review_id, artifact["id"], task["id"], self.clock()))
        c.execute("UPDATE artifacts SET status='staged' WHERE id=?", (artifact["id"],))
        c.execute("DELETE FROM search_index WHERE object_id=?", (artifact["id"],))
        self._event(c, "artifact.staged", artifact["id"],
                    {"candidate_hash": artifact["body_sha"], "review_task_id": review_id,
                     "policy": policy, "author_id": author_id},
                    task["id"], author_id)
        self._event(c, "review.requested", review_id, {"candidate_id": artifact["id"]}, task["id"])
        result = self._artifact(c, artifact["id"])
        return {**result, "artifact_id": artifact["id"], "review_task_id": review_id, "artifact": result}

    def _review_acceptance(self, c, review_id):
        row = c.execute("SELECT * FROM review_acceptances WHERE review_task_id=?", (review_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["report"] = json.loads(result.pop("report_json"))
        return result

    def _review_feedback(self, c, review_id):
        receipt = c.execute("SELECT * FROM peer_review_receipts WHERE review_task_id=?", (review_id,)).fetchone()
        if receipt is None:
            return None
        result = {key: receipt[key] for key in (
            "id", "review_task_id", "candidate_id", "reviewer_id", "verdict", "rationale", "created_at")}
        result["evidence"] = json.loads(receipt["evidence_json"])
        result["qualifications"] = json.loads(receipt["qualifications_json"])
        event = c.execute("SELECT payload_json FROM events WHERE kind='review.submitted' AND object_id=? "
                          "ORDER BY sequence DESC LIMIT 1", (receipt["id"],)).fetchone()
        reference = json.loads(event["payload_json"]).get("report_material_sha256") if event else None
        report = self.blobs.get_json(reference) if reference else {}
        result["suggestions"] = report.get("suggestions", [])
        if reference:
            result["report_material_sha256"] = reference
            # Native readers receive the report with the authorized feedback.
            # Large surrounding replies become exact local files at delivery.
            result["report"] = report
        return result

    def _validate_review(self, c, actor, p, *, phase):
        attempt, review = self._active(c, actor)
        request = c.execute("SELECT * FROM review_requests WHERE task_id=?", (review["id"],)).fetchone()
        if not request or request["state"] not in {"pending", "integrating"}:
            raise HubError("REVIEW_NOT_PENDING", "The active task is not an open candidate review")
        binding = self._candidate_binding(c, request["candidate_id"])
        expected = self._review_basis(binding)
        if any(p.get(key) != value for key, value in expected.items()):
            raise HubError("REVIEW_BINDING_MISMATCH", "Review must bind the exact candidate hash, revision, inputs, and policy")
        if not self._can_review(c, review["id"], attempt["worker_id"]):
            raise HubError("REVIEWER_INELIGIBLE", "The exact candidate requires its independent accepting reviewer")
        p = {**p, "verdict": "approve" if phase == "accept" else "request_changes"}
        rationale, evidence, qualifications = p.get("rationale"), p.get("evidence"), p.get("qualifications", [])
        if (not isinstance(rationale, str) or not rationale.strip() or len(rationale) > 20000
                or not isinstance(evidence, list) or len(evidence) > 100
                or not all(isinstance(item, str) and 0 < len(item) <= 8192 for item in evidence)
                or not isinstance(qualifications, list) or len(qualifications) > 100
                or not all(isinstance(item, str) and len(item) <= 2000 for item in qualifications)):
            raise HubError("INVALID_INPUT", "A bounded rationale, evidence list, and qualifications are required", 422)
        if p["verdict"] == "approve" and not evidence:
            raise HubError("EVIDENCE_REQUIRED", "Approval requires a concrete evidence reference or observation", 422)
        if phase == "accept" and qualifications:
            raise HubError("REVIEW_QUALIFIED", "Resolve qualifications or reject with concrete suggestions before accepting")
        report = {**expected, "verdict": p["verdict"], "rationale": rationale,
                  "evidence": evidence, "qualifications": qualifications}
        if "semantic_checks" in p:
            report["semantic_checks"] = p["semantic_checks"]
        if "integration_notes" in p:
            # What the duplication and placement checks found, for the Integrator only.
            if phase != "accept":
                raise HubError("INVALID_INPUT", "Integration notes accompany an acceptance; give a rejected "
                                                "author suggestions instead", 422)
            report["integration_notes"] = checked_integration_notes(p["integration_notes"])
        from .plugins import load_plugin
        review_hook = getattr(load_plugin(review["contract"]["domain"]), "validate_review_report", None)
        if callable(review_hook):
            review_hook(self._artifact(c, binding["artifact_id"]), report)
        if phase == "reject":
            suggestions = p.get("suggestions")
            if (not isinstance(suggestions, list) or not 1 <= len(suggestions) <= 100
                    or not all(isinstance(item, str) and item.strip() and len(item) <= 2000 for item in suggestions)):
                raise HubError("SUGGESTIONS_REQUIRED", "Rejection requires concrete suggestions for the original author", 422)
            report["suggestions"] = suggestions
        report["review_phase"] = phase
        return attempt, review, report

    def _require_original_validation(self, c, binding):
        if self._accepted_validation(c, binding) is None:
            raise HubError("VALIDATION_REQUIRED", "The original candidate needs successful exact domain validation")

    def _accept_review(self, c, attempt, review, report):
        acceptance = self._review_acceptance(c, review["id"])
        if acceptance is None:
            c.execute("INSERT INTO review_acceptances VALUES(?,?,?,?,?,?)",
                      (review["id"], report["candidate_id"], attempt["worker_id"], attempt["id"],
                       canonical(report), self.clock()))
            c.execute("UPDATE review_requests SET state='integrating' WHERE task_id=?", (review["id"],))
            self._event(c, "review.accepted", review["id"],
                        {"candidate_id": report["candidate_id"], "reviewer_id": attempt["worker_id"],
                         "report_material_sha256": self.blobs.put_json(report)},
                        review["collaboration"]["host_task_id"], attempt["worker_id"])
        elif acceptance["report"] != report:
            raise HubError("REVIEW_ALREADY_ACCEPTED", "The recorded candidate acceptance is immutable")
        return {"state": "integrating", "review_task_id": review["id"], "candidate_id": report["candidate_id"],
                "next_action": ("Acceptance is recorded. End this review turn. The Integrator publishes the result; "
                                "a question it has about your review starts a new conversation from your report.")}

    def _commit_review(self, c, attempt, review, report):
        binding = self._candidate_binding(c, report["candidate_id"])
        request = c.execute("SELECT state FROM review_requests WHERE task_id=?", (review["id"],)).fetchone()
        # A report remains immutable evidence even when its basis changed during process drain.
        valid = (request["state"] in {"pending", "integrating"}
                 and self._can_review(c, review["id"], attempt["worker_id"])
                 and all(report.get(key) == value for key, value in self._review_basis(binding).items()))
        receipt_id = new_id()
        c.execute("INSERT INTO peer_review_receipts VALUES(?,?,?,?,?,?,?,?,?,?)",
                  (receipt_id, review["id"], binding["artifact_id"], attempt["worker_id"],
                   canonical(self._review_basis(binding)), report["verdict"], report["rationale"],
                   canonical(report["evidence"]), canonical(report["qualifications"]), self.clock()))
        c.execute("UPDATE review_requests SET state=? WHERE task_id=?",
                  ("resolved" if valid else "invalidated", review["id"]))
        c.execute("UPDATE tasks SET state='done',active_attempt=NULL WHERE id=?", (review["id"],))
        c.execute("UPDATE attempts SET state='reviewed' WHERE id=?", (attempt["id"],))
        self._set_member_work(c, review["id"], attempt["worker_id"], False, "review_submitted")
        self._event(c, "review.submitted", receipt_id,
                    {"candidate_id": binding["artifact_id"], "verdict": report["verdict"], "basis_valid": valid,
                     "report_material_sha256": self.blobs.put_json({"receipt_id": receipt_id, **report})},
                    binding["task_id"], attempt["worker_id"])
        self._set_member_work(c, binding["task_id"], binding["author_id"], True, "peer_review_received")
        # A wait on this candidate's outcome ends with that outcome (_wake_publication_waits), which
        # an approval that still awaits validation or integration is not.
        c.execute("UPDATE continuations SET state='reconsider',wake_reason='peer_review_received' "
                  "WHERE task_id=? AND preferred_worker=? AND state='waiting' AND NOT EXISTS("
                  "SELECT 1 FROM json_each(predicate_json,'$.publications') WHERE value=?)",
                  (binding["task_id"], binding["author_id"], binding["artifact_id"]))
        if valid:
            self._promote_candidate(c, binding["artifact_id"])
        self._wake_publication_waits(c, binding["artifact_id"])
        self._recompute_collaborative(c, binding["task_id"])
        return {"directive": "reviewed", "review_receipt_id": receipt_id,
                "candidate_id": binding["artifact_id"], "basis_valid": valid,
                "host_task_id": binding["task_id"], "host_work_ready": False}

    def _accepted_validation(self, c, binding):
        """The newest domain validation, when it accepted exactly this candidate and basis.

        Publication and batch integration require it, and a review starts only once it exists.
        """
        domain = c.execute("SELECT * FROM evidence WHERE artifact_id=? AND policy=? ORDER BY sequence DESC LIMIT 1",
                           (binding["artifact_id"], binding["policy"])).fetchone()
        if domain is None or domain["verdict"] != "accepted" or json.loads(domain["qualifications_json"]):
            return None
        if json.loads(domain["details_json"]).get("candidate_binding") != self._review_basis(binding):
            return None
        return domain

    def _promote_candidate(self, c, artifact_id):
        binding = self._candidate_binding(c, artifact_id)
        if not self._basis_valid(c, binding) or not self._merge_published(c, artifact_id):
            return False
        receipt = c.execute("SELECT * FROM peer_review_receipts WHERE candidate_id=? ORDER BY created_at DESC,id DESC LIMIT 1",
                            (artifact_id,)).fetchone()
        eligible = (receipt is not None and self._independent(c, binding, receipt["reviewer_id"])
                    and receipt["verdict"] == "approve" and json.loads(receipt["qualifications_json"]) == []
                    and json.loads(receipt["binding_json"]) == self._review_basis(binding))
        domain = self._accepted_validation(c, binding) if eligible else None
        if domain is None:
            return False
        artifact = self._get(c, "artifacts", artifact_id)
        if artifact["status"] == "accepted":
            return True
        c.execute("UPDATE artifacts SET status='accepted' WHERE id=?", (artifact_id,))
        c.execute("INSERT INTO search_index VALUES(?,?,?,?)",
                  (artifact_id, artifact["kind"], artifact["kind"], canonical(self._record_body(c, artifact))))
        self._event(c, "artifact.published", artifact_id,
                    {"review_receipt_id": receipt["id"], "domain_evidence_id": domain["id"]}, binding["task_id"])
        self._wake(c)
        self._refresh_waits(c)
        self._recompute_collaborative(c, binding["task_id"])
        return True

    def _drain_task(self, c, task_id, reason):
        for attempt in self._active_attempts(c, task_id):
            if attempt["state"] == "recovery_required":
                continue
            if reason == "project_shutdown" and (
                    c.execute("SELECT 1 FROM attempt_stop_requests WHERE attempt_id=?", (attempt["id"],)).fetchone()
                    or (attempt["state"] == "draining" and (attempt["submission_json"] or attempt["continuation_id"]))):
                # Finish the already committed quiescent transition. Global run
                # controls stop new inference without discarding a submission,
                # exact review decision, voluntary wait, or departure intent.
                continue
            c.execute("UPDATE attempts SET state='draining' WHERE id=?", (attempt["id"],))
            c.execute("UPDATE workers SET state='draining' WHERE id=?", (attempt["worker_id"],))
            c.execute("INSERT OR REPLACE INTO attempt_stop_requests VALUES(?,?,?)",
                      (attempt["id"], reason, self.clock()))
        self._event(c, "task.drain_requested", task_id, {"reason": reason}, task_id)

    def _recompute_collaborative(self, c, task_id):
        task = self._task(c, task_id)
        attempts = self._refresh_aggregate(c, task_id)
        if task["state"] == "retired":
            return
        kind = task["contract"]["kind"]
        if kind == "integration":
            c.execute("UPDATE tasks SET state=?,state_version=state_version+1 WHERE id=?",
                      ("claimed" if attempts else "waiting", task_id))
            return
        if kind == "review":
            status = c.execute("SELECT state FROM review_requests WHERE task_id=?", (task_id,)).fetchone()
        else:
            raise HubError("REVIEW_PROTOCOL", "Ordinary task completion requires its explicit lifecycle declaration")
        terminal = status and status["state"] not in {"pending", "integrating"}
        state = "draining" if attempts and terminal else ("claimed" if attempts else ("done" if terminal else "ready"))
        c.execute("UPDATE tasks SET state=?,state_version=state_version+1 WHERE id=?", (state, task_id))
        if state == "done":
            self._wake(c)
            c.execute("UPDATE task_members SET runnable=0,work_reason='task_complete' WHERE task_id=?", (task_id,))
            self._invalidate_interruptions(c, task_id, "satisfied_externally")
            c.execute("UPDATE continuations SET state='satisfied_externally' WHERE task_id=? "
                      "AND state IN ('waiting','eligible','reconsider')", (task_id,))

    def _expected_revision(self, task, p):
        if type(p.get("revision")) is not int or p["revision"] != task["revision"]:
            raise HubError("STALE_REVISION", "An exact expected current task revision is required")

    def _apply_revision(self, c, task, contract):
        revision = task["revision"] + 1
        c.execute("INSERT INTO task_revisions VALUES(?,?,?,?)",
                  (task["id"], revision, canonical(contract), digest(contract)))
        self._persist_task_content(c, task["id"], revision, contract)
        c.execute("UPDATE tasks SET current_revision=?,state='ready',state_version=state_version+1 WHERE id=?",
                  (revision, task["id"]))
        c.execute("UPDATE search_index SET title=?,body=? WHERE object_id=?",
                  (contract["title"], contract["objective"], task["id"]))
        if contract_substance(contract) != contract_substance(task["contract"]):
            self._invalidate_reviews(c, task["id"], "task_revision_changed")
        self._drain_task(c, task["id"], "task_revision_changed")
        self._reconsider_task_revision(c, task["id"], revision)
        c.execute("UPDATE task_members SET runnable=executor,work_reason='task_revision_changed' "
                  "WHERE task_id=? AND membership_state='member'", (task["id"],))
        self._event(c, "task.revise", task["id"], {"revision": revision, "previous_revision": task["revision"]}, task["id"])
        self._notify(c, task["id"], task["id"], "task.revise")
        self._recompute_collaborative(c, task["id"])
        return self._task(c, task["id"])

    def _invalidate_candidate_review(self, c, request, reason):
        c.execute("UPDATE review_requests SET state='invalidated' WHERE task_id=?", (request["task_id"],))
        # Supersession preserves the original files, binding and obligation. A
        # corrected candidate must explicitly replace it and pass every gate.
        changed = c.execute("UPDATE artifacts SET status='superseded' WHERE id=? "
                            "AND status NOT IN ('accepted','withdrawn','rejected','superseded')", (request["candidate_id"],)).rowcount
        if changed:
            artifact = self._get(c, "artifacts", request["candidate_id"])
            self._event(c, "artifact.superseded", artifact["id"],
                        {"reason": reason, "review_task_id": request["task_id"]}, artifact["task_id"])
            self._reconsider_artifact_waits(c, artifact["id"], reason)
        self._drain_task(c, request["task_id"], reason)
        self._recompute_collaborative(c, request["task_id"])

    def _close_rejected_candidate(self, c, artifact_id):
        """A definitive machine rejection ends the candidate as a review rejection does: its
        review closes before it starts, its author resumes the task, and a corrected candidate
        replaces it."""
        if not c.execute("UPDATE artifacts SET status='rejected' WHERE id=? AND status NOT IN "
                         "('accepted','withdrawn','rejected','superseded')", (artifact_id,)).rowcount:
            return
        for request in c.execute("SELECT task_id FROM review_requests WHERE candidate_id=? AND state='pending'",
                                 (artifact_id,)).fetchall():
            c.execute("UPDATE review_requests SET state='invalidated' WHERE task_id=?", (request["task_id"],))
            self._drain_task(c, request["task_id"], "validation_rejected")
            self._recompute_collaborative(c, request["task_id"])
        binding = self._candidate_binding(c, artifact_id)
        self._set_member_work(c, binding["task_id"], binding["author_id"], True, "validation_rejected")
        self._wake_publication_waits(c, artifact_id)  # Told the outcome first.
        c.execute("UPDATE continuations SET state='reconsider',wake_reason='validation_rejected' "
                  "WHERE task_id=? AND preferred_worker=? AND state='waiting'", (binding["task_id"], binding["author_id"]))
        self._reconsider_artifact_waits(c, artifact_id, "validation_rejected")
        self._recompute_collaborative(c, binding["task_id"])

    def _invalidate_reviews(self, c, task_id, reason):
        for request in c.execute("SELECT task_id,candidate_id FROM review_requests WHERE host_task_id=? AND state IN ('pending','integrating')", (task_id,)).fetchall():
            self._invalidate_candidate_review(c, request, reason)

    def _collaboration_command(self, c, name, p, actor):
        if name == "record.private":
            attempt, task = self._active(c, actor)
            kind = p.get("kind", "")
            if (isinstance(kind, str) and
                    (any(part in {"note", "notes", "wiki"} for part in kind.lower().split("."))
                     or (task["contract"]["domain"] == "lean" and kind != "tool.lean-development"))):
                raise HubError("NOTE_FILES_REQUIRED", "Private notes are ordinary files in .private/notes; record.private is reserved for runtime diagnostic evidence", 422)
            artifact = self._insert_artifact(c, p["kind"], p["data"], producer=attempt["worker_id"],
                                             input_refs=p.get("input_refs", []), private=True)
            c.execute("UPDATE artifacts SET status='private' WHERE id=?", (artifact["id"],))
            c.execute("INSERT INTO private_records VALUES(?,?,?)", (artifact["id"], attempt["worker_id"], attempt["id"]))
            c.execute("DELETE FROM search_index WHERE object_id=?", (artifact["id"],))
            self._event(c, "record.private", artifact["id"], worker_id=attempt["worker_id"])
            record = self._artifact(c, artifact["id"])
            return {key: record[key] for key in ("id", "kind", "category", "status", "public", "accepted",
                                                 "body_sha", "content_file") if key in record}
        if name == "artifact.stage":
            attempt, task = self._active(c, actor)
            self._normalize_worker_fields(c, p, "coauthors")
            snapshot = self._candidate_snapshot(attempt["worker_id"])
            return self._stage_candidate(c, attempt, task, p, snapshot)
        if name in {"review.accept", "review.reject"}:
            phase = name.removeprefix("review.")
            attempt, review, report = self._validate_review(c, actor, p, phase=phase)
            if phase == "accept":
                return self._accept_review(c, attempt, review, report)
            c.execute("UPDATE attempts SET state='draining',submission_json=? WHERE id=?",
                      (canonical({"review": report}), attempt["id"]))
            c.execute("UPDATE workers SET state='draining' WHERE id=?", (attempt["worker_id"],))
            return {"runtime_directive": "drain_for_review", "attempt_id": attempt["id"]}
        if name == "task.subscribe":
            task = self._task(c, p["task_id"])
            if "revision" in p:
                self._expected_revision(task, p)
            self._role(actor, Role.WORKER, Role.OPERATOR)
            if actor.role == Role.WORKER:
                self._active(c, actor)
                if p.get("worker_id", actor.worker_id) != actor.worker_id:
                    raise HubError("FORBIDDEN", "Workers may subscribe only for themselves", 403)
            worker_id = actor.worker_id if actor.role == Role.WORKER else p["worker_id"]
            self._get(c, "workers", worker_id)
            c.execute("INSERT INTO task_subscriptions VALUES(?,?,?,'explicit',?) "
                      "ON CONFLICT(task_id,worker_id) DO UPDATE SET task_revision=excluded.task_revision,reason='explicit'",
                      (task["id"], worker_id, task["revision"], self.clock()))
            return self._task(c, task["id"])
        return None

    def _new_worker(self, c, *, kind="ordinary", backend=None, number=None):
        # Without a backend the operator's current choice for fresh workers applies; without a
        # number, the next one (a task's kept number is given only to its first executor).
        payload = {"kind": kind, **({"backend": backend} if backend else {}),
                   **({"worker_number": number} if number is not None else {})}
        worker = self._dispatch(c, "worker.register", payload, Actor(role=Role.RUNNER))
        return worker["id"]

    async def subscriptions(self, worker_id: str) -> list[dict]:
        def read(c):
            self._get(c, "workers", worker_id)
            rows = c.execute("SELECT t.id,t.current_revision,t.state,s.task_revision,s.reason FROM task_subscriptions s "
                             "JOIN tasks t ON t.id=s.task_id WHERE s.worker_id=? ORDER BY s.created_at,t.id LIMIT 200", (worker_id,))
            return [dict(row) for row in rows]
        return await self.db.call(read)
