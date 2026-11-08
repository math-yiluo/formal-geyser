"""Framework tool declarations shared by real and scripted workers."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from ..integration_notes import SCHEMA as INTEGRATION_NOTES
from ..models import Checkpoint, Predicate


def _function(name: str, description: str, properties: dict, required: list[str]) -> dict:
    properties = deepcopy(properties)
    definitions = {}
    for schema in properties.values():
        if isinstance(schema, dict):
            definitions.update(schema.pop("$defs", {}))
    schema = {
        "type": "object", "properties": properties, "required": required,
        "additionalProperties": False,
    }
    if definitions:
        schema["$defs"] = definitions
    return {
        "type": "function", "name": name, "description": description,
        "inputSchema": schema,
    }


MESSAGE_ID = ("Optional. Omitted, each call sends its own message. An ID you choose makes retrying the "
              "identical send safe; reusing it for a different message fails.")


def dynamic_tools() -> list[dict[str, Any]]:
    """Return the stable native entrypoints, independent of available operations."""
    return [
        _function("taskhub_actions",
                  "Discover available operation summaries and category IDs. Optionally filter by "
                  "category, or request one operation's exact argument schema before calling it.",
                  {"category": {"type": "string", "minLength": 1, "maxLength": 80},
                   "operation": {"type": "string", "minLength": 1, "maxLength": 80}}, []),
        _function("taskhub_call",
                  "Call an available operation using the arguments described by taskhub_actions. "
                  "Current execution authority and backend eligibility are checked for every call.",
                  {"operation": {"type": "string", "minLength": 1, "maxLength": 80},
                   "arguments": {"type": "object"}}, ["operation", "arguments"]),
    ]


def operation_tools() -> list[dict[str, Any]]:
    """Return operation schemas used behind the stable native gateway."""
    contract_path = {"type": "string", "minLength": 1,
                     "description": "Workspace-relative task JSON file, optionally referencing adjacent objective/context Markdown."}
    candidate = {
        "manifest_path": {"type": "string", "minLength": 1,
                          "description": "Workspace-relative JSON file containing kind, data, introduction, and any publication fields."},
    }
    task_revision = {
        "task_id": {"type": "string"}, "revision": {"type": "integer", "minimum": 1},
    }
    result = [
        _function("taskhub_publish", "Publish a self-contained task with one initial executor. Search first to avoid "
                  "duplicating a task; the task file format is in paths.submission_guide.",
                  {"contract_path": contract_path}, ["contract_path"]),
        _function("taskhub_checkpoint", "Save progress and the concrete next action.",
                  {"checkpoint": Checkpoint.model_json_schema()}, ["checkpoint"]),
        _function("taskhub_resume_work", "Resume useful work after a waiting message turn; normal automatic continuation then applies.", {}, []),
        _function("taskhub_pause", "Checkpoint and pause the task. Pause instead of ending "
                  "turns while blocked; a normal turn end starts the next turn. The predicate names what ends the "
                  "pause; a pause without one is refused. Messages may "
                  "trigger reply turns; resume_work may supersede your own wait. The runtime drains this turn.",
                  {"checkpoint": Checkpoint.model_json_schema(), "reason": {"type": "string", "minLength": 1},
                   "predicate": {**Predicate.model_json_schema(), "description": (
                       "subscriptions [{task_id, need}] wait on another open task: after each publication from it, the "
                       "waker judges whether what it published to the Library since you paused meets need, and wakes "
                       "you to check; a subscription to your own task, or one closing a cycle of tasks waiting on each "
                       "other, is refused, and the wait also ends when the task is done or retired. validations lists "
                       "your staged candidate ids and holds once each one's machine validation has a result, whatever "
                       "the verdict; publications lists candidate ids of your own task (yours or a co-executor's) "
                       "still on their way to the Library and holds once each one is published by the Integrator, "
                       "or rejected (in review or validation), deferred by the Integrator, withdrawn or superseded: "
                       "you are told which; completion_declaration_id awaits its feedback if you declared it, else its "
                       "decision; answers lists the ids of questions you asked with talk_send, and external_response_id "
                       "is the id returned by request; a wait for an answer also ends at the project's answer deadline. "
                       "mode (all, the default, or any) combines all but subscriptions, each of which ends the wait by "
                       "itself. Pausing keeps membership and context and cancels unused recruitment.")}},
                  ["checkpoint", "reason", "predicate"]),
        _function("taskhub_leave_task", "Leave your ordinary-task executor membership with a reason. This ends your execution relationship, preserves history and work, and retires the task if all executors leave; while another executor remains, it can invite you back (invite_back). Use pause for a temporary suspension. Leaving the current assignment drains this turn.",
                  {"task_id": {"type": "string", "minLength": 1},
                   "reason": {"type": "string", "minLength": 1, "maxLength": 4000}},
                  ["task_id", "reason"]),
        _function("taskhub_invite_back", "Invite a worker that left this task back to it, with the reason it will "
                  "read. You must be a current executor; only a former executor of the task can return. It resumes "
                  "the task in its next turn, from its saved checkpoint and files. A worker busy on another task "
                  "cannot be invited: message it with talk_send instead.",
                  {"task_id": {"type": "string", "minLength": 1},
                   "worker_id": {"type": "string", "minLength": 1,
                                 "description": "The worker that left, by ID or name, such as Worker 12."},
                   "reason": {"type": "string", "minLength": 1, "maxLength": 4000}},
                  ["task_id", "worker_id", "reason"]),
        _function("taskhub_invitation_update", "Edit the already authorized, unused collaborator invitation.",
                  {**task_revision, "invitation": {"type": "string", "minLength": 1}}, ["task_id", "revision", "invitation"]),
        _function("taskhub_invitation_withdraw", "Permanently cancel an unused collaborator invitation.",
                  {**task_revision, "reason": {"type": "string", "minLength": 1}}, ["task_id", "revision", "reason"]),
        _function("taskhub_complete", "Declare whole-task completion against the unchanged acceptance conditions and accepted publications. This opens the recorded feedback/finalization process; it does not submit an artifact or close the task immediately.",
                  {"task_id": {"type": "string"}, "revision": {"type": "integer", "minimum": 1},
                   "evidence": {"type": "object", "description": "Read paths.operation_guide and the operation_schemas and operation_examples listed in paths.reference_index for the exact acceptance map."},
                   "reason": {"type": "string", "minLength": 1}},
                  ["task_id", "revision", "evidence", "reason"]),
        _function("taskhub_completion_feedback", "Record a concrete unmet acceptance condition or that no objection was found, after checkpointing current work. "
                  "conditions lists exact acceptance-condition keys from the declaration that are unmet; [] means no objection. "
                  "Give a reason when the list is not empty.",
                  {"declaration_id": {"type": "string"}, "conditions": {"type": "array", "items": {"type": "string"}},
                   "reason": {"type": "string"}, "checkpoint": Checkpoint.model_json_schema()},
                  ["declaration_id", "conditions", "reason", "checkpoint"]),
        _function("taskhub_completion_finalize", "Resolve feedback with evidence and decide whether to close the task. Task 0 closure stops autonomous project work.",
                  {"declaration_id": {"type": "string"}, "decision": {"type": "string", "enum": ["close", "continue"]},
                   "reason": {"type": "string", "minLength": 1},
                   "evidence": {"type": "object", "description": "Use the operation_schemas and operation_examples listed in paths.reference_index for the exact acceptance map."},
                   "resolution": {"type": "object", "description": "Read paths.operation_guide and the operation_schemas and operation_examples listed in paths.reference_index before resolving feedback."}}, ["declaration_id", "decision", "reason"]),
        _function("taskhub_message_send", "Send a progress note or reply to the human. Use request when a durable response is needed.",
                  {"text": {"type": "string", "minLength": 1},
                   "message_id": {"type": "string", "minLength": 1, "description": MESSAGE_ID},
                   "request_id": {"type": "string"}}, ["text"]),
        _function("taskhub_request", "Record a durable question or a deduplicated missing-source request for the human. Use the returned external response predicate with pause only when blocked.",
                  {"kind": {"type": "string", "enum": ["question", "literature"]},
                   "request_key": {"type": "string", "minLength": 1,
                                   "description": "Questions are scoped to this task/revision; literature keys identify a source/version across tasks. Reuse only with identical title and text."},
                   "title": {"type": "string", "minLength": 1},
                   "text": {"type": "string", "minLength": 1}}, ["kind", "request_key", "title", "text"]),
        _function("taskhub_literature_register", "Record a citation to a readable source file or publication folder. copy=true retains a local copy under a readable title; otherwise keep a pointer. See the literature wiki for usage.",
                  {"source_id": {"type": "string"}, "title": {"type": "string"}, "path": {"type": "string"},
                   "copy": {"type": "boolean"}, "uri": {"type": "string"}, "version": {"type": "string"},
                   "citation": {"type": "string"}, "description": {"type": "string"}}, ["source_id", "title", "path"]),
        _function("taskhub_request_resolve", "Fulfill a literature request only after the needed source is accessible and registered.",
                  {"request_id": {"type": "string"}, "outcome": {"type": "string", "enum": ["fulfilled"]},
                   "source_id": {"type": "string"}, "text": {"type": "string"},
                   "message_id": {"type": "string", "minLength": 1, "description": MESSAGE_ID}},
                  ["request_id", "outcome", "source_id", "text"]),
        _function("taskhub_contribute",
                  "Freeze the manifest's candidate for validation and independent review and end this activation; "
                  "stage is the same but lets you keep working. Neither declares task completion. Only the record's "
                  "Lean/Markdown pair and the manifest's repository_paths are integrated.",
                  candidate, ["manifest_path"]),
        _function("taskhub_withdraw", "Explicitly withdraw an unnecessary unpublished candidate with a reason. Keep its files and history, notify dependents, and recheck the original acceptance conditions. Withdrawal is not acceptance and cannot weaken the task objective or remove published Library content.",
                  {"artifact_id": {"type": "string", "minLength": 1},
                   "reason": {"type": "string", "minLength": 1, "maxLength": 4000}},
                  ["artifact_id", "reason"]),
        _function("taskhub_stage",
                  "Stage an immutable contribution and workspace snapshot for validation while "
                  "continuing this activation. Include every contributing worker in the manifest's coauthors. "
                  "The snapshot covers your workspace outside .private: every Lean file there needs a nonempty "
                  "Markdown file with the same path and name and no proof holes. Only the record's Lean/Markdown pair "
                  "and the manifest's repository_paths are integrated.",
                  candidate, ["manifest_path"]),
        _function("taskhub_wiki_submit",
                  "Submit public notes/wiki Markdown for independent review while continuing work. Update wiki/README.md with titles, links and purposes. "
                  "Private notes are ordinary files in .private/notes and need no registration.",
                  {"paths": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 200},
                   "summary": {"type": "string", "minLength": 1},
                   "input_refs": {"type": "array", "items": {"type": "string"}, "maxItems": 200},
                   "coauthors": {"type": "array", "items": {"type": "string", "minLength": 1}, "maxItems": 32,
                                 "description": "Worker N names or IDs of the task participants who contributed to these files."},
                   "replaces": {"type": "string", "minLength": 1,
                                "description": "Exact rejected, withdrawn, or review-invalidated candidate ID replaced by these files in the same task."}},
                  ["paths", "summary"]),
        _function("taskhub_dag_status",
                  "Read the current shared DAG revision and immutable file paths, verify file integrity, "
                  "and repair navigation when possible. Graph problems do not stop other task work.", {}, []),
        _function("taskhub_dag_update",
                  "Save both shared planning DAGs from .private/dags after running the supplied checker. "
                  "Workers and reviewers may maintain them. The server rechecks the complete pair "
                  "and its global revision; graphs are not acceptance evidence.",
                  {"source_dir": {"type": "string", "minLength": 1,
                                  "description": "Workspace-relative .private/dags directory containing definitions.json and proofs.json."},
                   "expected_revision": {"type": "integer", "minimum": 0,
                                         "description": "Revision of the pair read from dag_status. On conflict use current_graph_ref in error details, or refresh dag_status; merge edits and recheck."},
                   "summary": {"type": "string", "minLength": 1, "maxLength": 4000}},
                  ["source_dir", "expected_revision", "summary"]),
        _function("taskhub_draft_update",
                  "Share a complete task draft revision from .private/drafts. Lean files need "
                  "same-stem Markdown but may reference missing definitions. This does not "
                  "compile, submit an artifact, request review, or publish to the Library.",
                  {"task_id": {"type": "string", "minLength": 1},
                   "source_dir": {"type": "string", "minLength": 1,
                                  "description": "Workspace-relative .private/drafts directory, or a child containing the complete next draft revision."},
                   "expected_revision": {"type": "integer", "minimum": 0,
                                         "description": "Current draft revision from inspect or drafts/<task-id>/current.json; zero if none exists. Independent of the task contract revision."},
                   "summary": {"type": "string", "minLength": 1, "maxLength": 4000}},
                  ["task_id", "source_dir", "expected_revision", "summary"]),
        _function("taskhub_review",
                  "Independently accept the exact candidate, then end this review turn for the Integrator to handle publication; "
                  "or reject demonstrated defects with concrete suggestions. Acceptance is not Library availability. "
                  "Missing evidence or unavailable checks leave review unresolved. Copy candidate_id, candidate_hash, "
                  "task_revision, input_refs and policy verbatim from review.decision_basis. Your verdict's fields "
                  "become your review report.",
                  {"candidate_id": {"type": "string"}, "candidate_hash": {"type": "string"},
                   "task_revision": {"type": "integer", "minimum": 1},
                   "input_refs": {"type": "array", "items": {"type": "string"}},
                   "policy": {"type": "string"},
                   "verdict": {"type": "string", "enum": ["accept", "reject"]},
                   "rationale": {"type": "string", "minLength": 1},
                   "evidence": {"type": "array", "items": {"type": "string"}},
                   "qualifications": {"type": "array", "items": {"type": "string"}},
                   "suggestions": {"type": "array", "items": {"type": "string", "minLength": 1}, "minItems": 1},
                   "semantic_checks": {"type": "object", "description": "Required to accept mathematical content. Read the semantic_review_guide, semantic_review_schema, and semantic_review_example listed in paths.reference_index for exact fields and checks."},
                   "integration_notes": INTEGRATION_NOTES},
                  ["candidate_id", "candidate_hash", "task_revision", "input_refs", "policy",
                   "verdict", "rationale", "evidence"]),
        _function("taskhub_propose_revision",
                  "Maintain the task specification as an executor or original publisher. "
                  "Use the expected current revision and a concrete reason; preserve the original objective. "
                  "Pending candidates bound to the old revision lose their review basis; resubmit them with replaces.",
                  {**task_revision, "contract_path": contract_path,
                   "reason": {"type": "string", "minLength": 1}},
                  ["task_id", "revision", "reason", "contract_path"]),
        _function("taskhub_subscribe", "Subscribe to bounded updates for a task or exact revision.",
                  task_revision, ["task_id"]),
        _function("taskhub_sync", "Receive the notifications and directed messages not yet delivered, with your "
                  "task's current revision and state. It does not repeat the task text: read paths.task for that.",
                  {}, []),
        _function("taskhub_history_search",
                  "Search your own earlier conversations (messages to you, your replies and your actions) "
                  "for every word of query, newest first. Your current conversation is not searched.",
                  {"query": {"type": "string", "minLength": 1, "maxLength": 200},
                   "limit": {"type": "integer", "minimum": 1, "maximum": 20}}, ["query"]),
        _function("taskhub_search", "Search existing tasks before publishing duplicate work.",
                  {"query": {"type": "string"}, "limit": {"type": "integer",
                                                        "minimum": 1, "maximum": 20}},
                  ["query"]),
        _function("taskhub_inspect",
                  "Read a task by its full ID. Pass revision to inspect a historical contract; "
                  "omit it for the current revision. Request include_completion for authoritative "
                  "acceptance-condition keys and existing completion declarations; only the task's original "
                  "publishers and current executors may, with no revision or the current one.",
                  {"task_id": {"type": "string"},
                   "revision": {"type": "integer", "minimum": 1},
                   "include_completion": {"type": "boolean"}}, ["task_id"]),
        _function("taskhub_fetch", "Read a candidate or accepted artifact by its exact ID.",
                  {"artifact_id": {"type": "string"}}, ["artifact_id"]),
        _function("taskhub_talk_send", "Message any project worker or reviewer. Each pair of workers shares one conversation; "
                  "name any task or artifact you mean in the text. A notice (the default kind) informs; a question asks, "
                  "and you can pause on its id (answers); an answer replies to a question you were asked and goes to "
                  "its asker. A notice does not wake a recipient that paused: it reads it when it next works. An active "
                  "recipient gets a message during its turn where its runtime supports that, otherwise at its next turn; "
                  "an idle one, or a paused one asked or answered, gets one reply turn. Text is limited to 16 KiB.",
                  {"recipient_id": {"type": "string", "description": "Stable project worker name, for example Worker 1. "
                                    "An answer goes to the asker without it."},
                   "message_id": {"type": "string", "minLength": 1, "description": MESSAGE_ID},
                   "text": {"type": "string", "minLength": 1},
                   "kind": {"type": "string", "enum": ["notice", "question", "answer"],
                            "description": "notice (default), question, or answer (with answers)."},
                   "answers": {"type": "string", "minLength": 1,
                               "description": "For an answer: the id of the question it answers."}}, ["text"]),
        _function("taskhub_talk_read", "List your peer conversations, or supply conversation_id to read any project conversation's message history.",
                  {"conversation_id": {"type": "string", "minLength": 1},
                   "after_id": {"type": "string", "description": "Conversation-list cursor; omit when reading one conversation."},
                   "after_sequence": {"type": "integer", "minimum": 0, "description": "Message-history cursor when conversation_id is supplied."},
                   "offset": {"type": "integer", "minimum": 0, "description": "Message-body offset when conversation_id is supplied."}}, []),
        _function("taskhub_public_read", "Read a file from an exact published Library commit. "
                  "Prefer native file reads at the Library root supplied in context when available.",
                  {"path": {"type": "string"}, "commit": {"type": "string"},
                   "offset": {"type": "integer", "minimum": 0},
                   "limit": {"type": "integer", "minimum": 1, "maximum": 16384}}, ["path", "commit"]),
    ]
    return result


TURN_END_INSTRUCTIONS = """Before voluntarily ending a turn, or calling an operation that ends it, you MUST wait for and
collect the terminal result of every command, tool call, and background operation you started; persist needed
state within your authority. The runtime closes all execution at each turn end.
"""


WORKER_INSTRUCTIONS = """Work on the assigned task using native files and commands. Use the supplied Worker N, Reviewer N, or Integrator name.
Your required guides and the operations you can call follow below; read an on-demand guide when its subject comes up.
paths.knowledge_readme locates public knowledge. Use taskhub_call to act, and taskhub_actions for an exact schema.
A normal turn end starts the next turn at once, so never wait by ending turns. Pause instead: with a
predicate for a review, a publication, a machine validation or an answer to your request, and without one
for a peer's reply, which brings a message turn where resume_work continues the task. The Integrator
defers instead of pausing. Respect user run controls, assigned authority and independent review. Never
self-review or launch other agents.
Only trusted sender_role=operator marks a human message; read its complete referenced file.
All project-authored prose is English.
""" + TURN_END_INSTRUCTIONS

