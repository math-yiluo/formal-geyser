from __future__ import annotations

import json
from copy import deepcopy

import pytest

from formal_geyser.coordination.runtime.actions import (
    ActionCatalog,
    ActionError,
    validate_gateway,
)
from formal_geyser.coordination.runtime.files import resource_tools, workspace_tools
from formal_geyser.coordination.runtime.tools import dynamic_tools, operation_tools


@pytest.fixture
def catalog():
    return ActionCatalog(operation_tools() + workspace_tools() + resource_tools())


def names(description):
    return {item["operation"] for item in description["operations"]}


def test_native_entrypoints_do_not_depend_on_actions_or_capabilities():
    native = dynamic_tools()
    assert [item["name"] for item in native] == ["taskhub_actions", "taskhub_call"]
    specs = operation_tools()
    original = ActionCatalog(specs)
    evolved = deepcopy(specs)
    evolved[0]["inputSchema"]["properties"]["future_option"] = {"type": "boolean"}
    evolved.append({"name": "lean_future_operation", "description": "An optional domain action.",
                    "inputSchema": {"type": "object", "additionalProperties": False}})
    changed = ActionCatalog(evolved)
    assert original.describe(operation="publish") != changed.describe(operation="publish")
    assert "lean_future_operation" in names(changed.describe())
    assert dynamic_tools() == native
    assert "enum" not in json.dumps(native)
    # The outer transport must accept an operation added after a native session
    # started; the separate catalog resolves and validates its current schema.
    assert validate_gateway("taskhub_call", {"operation": "lean_future_operation", "arguments": {}})
    assert changed.resolve("lean_future_operation", {}) == ("lean_future_operation", {})


def test_discovery_returns_summaries_then_only_the_requested_schema(catalog):
    listing = catalog.describe()
    assert "inputSchema" not in json.dumps(listing)
    categories = {item["category"]: item["count"] for item in listing["categories"]}
    assert {"tasks", "progress", "contributions", "communication", "resources", "workspace"} <= categories.keys()
    assert sum(categories.values()) == len(listing["operations"])
    workspace = catalog.describe(category="workspace")
    assert names(workspace) == {"workspace_list", "workspace_read", "workspace_write", "workspace_copy"}
    schema = catalog.describe(operation="workspace_read", category="workspace")
    assert schema["inputSchema"]["required"] == ["path"]
    assert "operations" not in schema
    assert schema["operation"] == "workspace_read"
    with pytest.raises(ActionError) as failure:
        catalog.describe(operation="workspace_read", category="tasks")
    assert failure.value.code == "INVALID_ARGUMENTS"
    with pytest.raises(ActionError) as failure:
        catalog.describe(category="invented")
    assert failure.value.code == "UNKNOWN_CATEGORY"


@pytest.mark.parametrize("capability", ["recruitment", "assignment"])
def test_decision_helpers_have_no_catalog_surface(catalog, capability):
    # Decisions run without tools, so no decision kind names a catalog capability:
    # discovery and every operation, including the former read surface, fail closed.
    for describe in (lambda: catalog.describe(capability=capability),
                     lambda: catalog.describe(capability=capability, operation="search")):
        with pytest.raises(ActionError) as failure:
            describe()
        assert failure.value.code == "INVALID_CAPABILITY"
    for operation in names(catalog.describe()):
        with pytest.raises(ActionError) as failure:
            catalog.resolve(operation, {}, capability=capability)
        assert failure.value.code == "INVALID_CAPABILITY"


@pytest.mark.parametrize("capability,operation", [
    ("worker", "review"), ("reviewer", "stage"),
    ("reviewer", "contribute"), ("reviewer", "withdraw"), ("reviewer", "complete"),
    ("reviewer", "completion_feedback"), ("reviewer", "completion_finalize"),
    ("reviewer", "invitation_update"), ("reviewer", "invitation_withdraw"),
    ("reviewer", "leave_task"), ("reviewer", "invite_back"), ("reviewer", "wiki_submit"),
    ("message_only", "invite_back"),
    ("message_only", "stage"), ("message_only", "pause"), ("message_only", "review"),
])
def test_unavailable_operations_are_hidden_and_rejected(catalog, capability, operation):
    assert operation not in names(catalog.describe(capability=capability))
    for invoke in (lambda: catalog.describe(capability=capability, operation=operation),
                   lambda: catalog.resolve(operation, {}, capability=capability)):
        with pytest.raises(ActionError) as failure:
            invoke()
        assert failure.value.code == "OPERATION_NOT_ALLOWED"


def test_message_wake_surface_preserves_communication_but_rejects_domain_execution(catalog):
    permitted = names(catalog.describe(capability="message_only"))
    assert {"publish", "message_send", "request", "resume_work", "literature_register",
            "request_resolve", "workspace_read", "resource_read", "talk_read"} <= permitted
    # An original publisher may revise its task from a message turn (TaskHub checks it is one).
    assert "propose_revision" in permitted
    assert "originally published" in catalog.describe(operation="propose_revision", capability="message_only")["description"]
    schema = catalog.describe(operation="inspect", capability="message_only")["inputSchema"]
    assert "include_completion" not in schema["properties"]
    with pytest.raises(ActionError) as failure:
        catalog.resolve("inspect", {"task_id": "task-1", "include_completion": True},
                        capability="message_only")
    assert failure.value.code == "INVALID_ARGUMENTS"
    domain = ActionCatalog(catalog.specs + [{"name": "lean_check", "description": "Run a checker.",
                                           "inputSchema": {"type": "object"}}])
    assert "lean_check" in names(domain.describe(capability="reviewer"))
    with pytest.raises(ActionError) as failure:
        domain.resolve("lean_check", {}, capability="message_only")
    assert failure.value.code == "OPERATION_NOT_ALLOWED"


@pytest.mark.parametrize("operation", [
    "talk_inbox", "release", "challenge", "subscriptions", "resource_roots", "wait", "pause_task",
    "question_request", "literature_request", "completion_inspect", "talk_list", "talk_history",
    "note_submit", "taskhub_publish",
])
def test_retired_names_are_not_aliases(catalog, operation):
    assert operation not in names(catalog.describe())
    with pytest.raises(ActionError) as failure:
        catalog.resolve(operation, {})
    assert failure.value.code == "UNKNOWN_OPERATION"


def test_the_integrator_waits_by_deferring_never_by_pausing(catalog):
    checkpoint = {"summary": "Asked the reviewer.", "next_action": "Integrate after the answer."}
    with pytest.raises(ActionError) as failure:
        catalog.resolve("pause", {"reason": "Await an answer.", "checkpoint": checkpoint}, capability="integrator")
    assert failure.value.code == "OPERATION_NOT_ALLOWED"


def test_a_clarification_turn_may_ask_a_question_but_not_wait_for_a_source(catalog):
    arguments = {"kind": "literature", "request_key": "source", "title": "A source", "text": "Need it."}
    for capability in ("review_answer", "integrator_answer"):
        with pytest.raises(ActionError) as failure:
            catalog.resolve("request", arguments, capability=capability)
        assert failure.value.code == "INVALID_ARGUMENTS"
        assert catalog.resolve("request", {**arguments, "kind": "question"}, capability=capability)[0] == "taskhub_request"
    assert catalog.resolve("request", arguments, capability="reviewer")[0] == "taskhub_request"


def test_an_unknown_or_unavailable_operation_names_close_ones(catalog):
    with pytest.raises(ActionError) as failure:
        catalog.resolve("talk_sned", {})
    assert failure.value.code == "UNKNOWN_OPERATION" and "Did you mean talk_send" in str(failure.value)
    with pytest.raises(ActionError) as failure:
        catalog.resolve("review", {}, capability="worker")
    assert failure.value.code == "OPERATION_NOT_ALLOWED"
    assert "Your instructions list every operation you can call here" in str(failure.value)


def test_consolidated_operations_keep_their_required_data(catalog):
    checkpoint = {"summary": "Saved current work.", "next_action": "Check the response."}
    waiting = {"reason": "Need source", "checkpoint": checkpoint,
               "predicate": {"external_response_id": "response-1"}}
    assert catalog.resolve("pause", waiting) == ("taskhub_pause", waiting)
    for missing in ("reason", "checkpoint", "predicate"):  # A pause names what ends it.
        with pytest.raises(ActionError) as failure:
            catalog.resolve("pause", {key: value for key, value in waiting.items() if key != missing})
        assert failure.value.code == "INVALID_ARGUMENTS"
    for kind in ("question", "literature"):
        assert catalog.resolve("request", {"kind": kind, "request_key": "source-1", "title": "Source",
                                           "text": "Please supply the missing source."})
    assert catalog.resolve("inspect", {"task_id": "task-1", "revision": 2, "include_completion": True})
    assert catalog.resolve("talk_read", {"after_id": "conversation-1"})
    assert catalog.resolve("talk_read", {"conversation_id": "conversation-1", "after_sequence": 3,
                                         "offset": 2048})
    assert catalog.resolve("wiki_submit", {"paths": ["wiki/source.md"], "summary": "Source update"})
    assert {"stage", "contribute", "complete"} <= names(catalog.describe())
    assert not any(name.startswith("merge_") for name in names(catalog.describe()))


@pytest.mark.parametrize("name,arguments", [
    ("taskhub_actions", {"operation": "publish", "capability": "reviewer"}),
    ("taskhub_call", {"operation": "publish"}),
    ("taskhub_call", {"operation": "publish", "arguments": []}),
    ("taskhub_call", {"operation": "publish", "arguments": {}, "worker_id": "another-worker"}),
    ("taskhub_call", {"operation": "publish", "arguments": {}, "role": "reviewer"}),
])
def test_outer_schema_rejects_malformed_requests_and_authority_fields(name, arguments):
    with pytest.raises(ActionError) as failure:
        validate_gateway(name, arguments)
    assert failure.value.code == "INVALID_ARGUMENTS"


def test_argument_errors_are_bounded_without_dumping_payloads(catalog):
    confidential = "private-request-body-" * 10000
    with pytest.raises(ActionError) as failure:
        catalog.resolve("publish", {"contract_path": {"sensitive": confidential}})
    result = failure.value.as_result()
    assert result["code"] == "INVALID_ARGUMENTS"
    assert "contract_path" in result["error"]
    assert "private-request-body" not in json.dumps(result)
    assert len(json.dumps(result)) < 600
    with pytest.raises(ActionError) as failure:
        catalog.resolve("search", {"query": "theorem", "limit": True})
    assert failure.value.code == "INVALID_ARGUMENTS"
    with pytest.raises(ActionError) as failure:
        catalog.resolve("pause", {"reason": "Waiting", "checkpoint": {"summary": "Saved"}})
    assert "next_action" in str(failure.value)


@pytest.mark.parametrize("operation,arguments,message", [
    # Workers asked search for 30 to 100 results; the error hid its maximum of 20.
    ("search", {"query": "theorem", "limit": 30}, "Invalid limit: Value must be at most 20."),
    ("search", {"query": "theorem", "limit": 0}, "Invalid limit: Value must be at least 1."),
    ("history_search", {"query": "x" * 201}, "Invalid query: Text must be at most 200 characters."),
    ("talk_send", {"recipient_id": "Worker 1", "text": ""}, "Invalid text: Text must be at least 1 character."),
    ("wiki_submit", {"paths": [], "summary": "Notes"}, "Invalid paths: List must have at least 1 item."),
    ("wiki_submit", {"paths": ["wiki/a.md"] * 201, "summary": "Notes"},
     "Invalid paths: List must have at most 200 items."),
])
def test_a_bound_error_states_the_bound(catalog, operation, arguments, message):
    with pytest.raises(ActionError) as failure:
        catalog.resolve(operation, arguments)
    assert failure.value.code == "INVALID_ARGUMENTS" and str(failure.value) == message


def test_an_unexpected_field_error_names_the_fields(catalog):
    with pytest.raises(ActionError) as failure:
        catalog.resolve("search", {"query": "theorem", "max_results": 5, "kind": "task"})
    assert str(failure.value) == ("Invalid arguments: Unexpected field(s): max_results, kind; "
                                  "use only fields in the operation schema.")
    # A field this turn's capability removes says so: message turns cannot inspect completion.
    with pytest.raises(ActionError) as failure:
        catalog.resolve("inspect", {"task_id": "task-1", "include_completion": True}, capability="message_only")
    assert "Unexpected field(s): include_completion" in str(failure.value)
    assert "this turn cannot use include_completion" in str(failure.value)
    assert "include_completion" in catalog.describe(operation="inspect", capability="message_only")["description"]
    with pytest.raises(ActionError) as failure:
        catalog.resolve("pause", {"reason": "Waiting", "checkpoint": {
            "summary": "Saved", "next_action": "Wait", "extra": 1}})
    assert str(failure.value) == ("Invalid checkpoint: Unexpected field(s): extra; "
                                  "use only fields in the operation schema.")


def test_schemas_and_arguments_do_not_share_mutable_catalog_state(catalog):
    returned = catalog.describe(operation="publish")
    returned["inputSchema"]["required"].clear()
    specs = catalog.specs
    specs[0]["inputSchema"]["required"].clear()
    with pytest.raises(ActionError):
        catalog.resolve("publish", {})
    arguments = {"paths": ["wiki/page.md"], "summary": "Changes"}
    _, resolved = catalog.resolve("wiki_submit", arguments)
    resolved["paths"].clear()
    assert arguments["paths"] == ["wiki/page.md"]


def test_unknown_capabilities_and_gateway_names_fail_closed(catalog):
    with pytest.raises(ActionError) as failure:
        catalog.describe(capability="operator")
    assert failure.value.code == "INVALID_CAPABILITY"
    with pytest.raises(ActionError) as failure:
        validate_gateway("taskhub_stage", {"manifest_path": "candidate.json"})
    assert failure.value.code == "UNKNOWN_GATEWAY"


@pytest.mark.parametrize("operation,capability", [
    ("batch_next", "worker"),
    ("completion_next", "reviewer"), ("invitation_next", "reviewer"),
])
def test_new_restricted_operations_keep_their_context_boundary(operation, capability):
    catalog = ActionCatalog([{"name": "taskhub_" + operation,
                              "inputSchema": {"type": "object"}}])
    assert names(catalog.describe(capability=capability)) == set()
    with pytest.raises(ActionError) as failure:
        catalog.resolve(operation, {}, capability=capability)
    assert failure.value.code == "OPERATION_NOT_ALLOWED"


def test_pause_description_distinguishes_automatic_resumption_and_explicit_resume(catalog):
    description = catalog.describe(operation="pause")["description"]
    assert "The predicate names what ends the pause; a pause without one is refused" in description
    assert "a normal turn end starts the next turn" in description
    # The predicate's fields are explained in its schema.
    predicate = catalog.describe(operation="pause")["inputSchema"]["properties"]["predicate"]["description"]
    for field in ("subscriptions", "validations", "completion_declaration_id", "answers", "external_response_id"):
        assert field in predicate
    assert "the waker judges" in predicate and "answer deadline" in predicate
    assert "Messages may trigger reply turns" in description
    assert "resume_work may supersede your own wait" in description
    summary = next(item for item in catalog.describe()["operations"] if item["operation"] == "pause")
    assert summary["description"] == description


def test_message_only_resume_description_makes_worker_available_without_resuming_audit_task(catalog):
    description = catalog.describe(operation="resume_work", capability="message_only")["description"]
    assert "Become available for a new task when useful work exists." in description
    assert "does not resume the task you left" in description
    assert description != catalog.describe(operation="resume_work", capability="worker")["description"]
    summary = next(item for item in catalog.describe(capability="message_only")["operations"]
                   if item["operation"] == "resume_work")
    assert summary["description"] == description


def test_catalog_rejects_ambiguous_operation_names():
    spec = {"name": "taskhub_example", "inputSchema": {"type": "object"}}
    with pytest.raises(ValueError, match="Duplicate"):
        ActionCatalog([spec, {**spec, "name": "example"}])
    with pytest.raises(ValueError, match="reserved"):
        ActionCatalog(dynamic_tools())
