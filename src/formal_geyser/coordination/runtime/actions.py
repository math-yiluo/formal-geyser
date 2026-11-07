"""On-demand operation discovery and validation behind a stable native catalog.

Capability filtering limits this runtime surface. It does not establish worker,
task, review, publication, or resource eligibility; dispatch and TaskHub
must still check the current authoritative state on every invocation.
"""
from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from difflib import get_close_matches
from typing import Any

from jsonschema import Draft202012Validator, ValidationError

from .tools import dynamic_tools

_REVIEW_OPERATIONS = frozenset({"review"})
# After these the turn ends: the runtime drains it, or (an acceptance) the reviewer is told to end it.
ENDS_TURN = frozenset({"pause", "contribute", "leave_task", "review", "batch_publish", "batch_finish_empty"})
_REVIEWER_EXCLUDED = frozenset({
    "stage", "contribute", "complete", "withdraw",
    "invitation_update", "invitation_withdraw", "leave_task", "invite_back", "completion_feedback",
    "completion_finalize", "wiki_submit", "draft_update",
})
_READ_OPERATIONS = frozenset({
    "search", "inspect", "sync", "fetch", "public_read", "talk_read",
    "workspace_list", "workspace_read", "resource_list", "resource_read", "resource_search",
    "resource_pdf", "lean_registry_list", "dag_status", "history_search",
})
# A message-only turn may also revise a task its worker originally published (TaskHub checks that).
_MESSAGE_OPERATIONS = (_READ_OPERATIONS - _REVIEW_OPERATIONS) | frozenset({
    "publish", "propose_revision", "message_send", "request", "literature_register", "request_resolve",
    "resume_work", "dag_update", "workspace_write", "workspace_copy", "talk_send",
})
_CATEGORIES = {
    "tasks": frozenset({
        "publish", "propose_revision", "invitation_update", "invitation_withdraw", "leave_task", "invite_back",
        "search", "inspect", "subscribe", "sync",
    }),
    "progress": frozenset({"checkpoint", "resume_work", "pause", "history_search"}),
    "contributions": frozenset({"stage", "contribute", "withdraw", "wiki_submit", "fetch", "public_read"}),
    "drafts": frozenset({"draft_update"}),
    "graphs": frozenset({"dag_update", "dag_status"}),
    "completion": frozenset({"complete", "completion_feedback", "completion_finalize"}),
    "communication": frozenset({"message_send", "request", "talk_send", "talk_read"}),
    "literature": frozenset({"literature_register", "request_resolve"}),
    "integration": frozenset({"batch_status", "batch_prepare", "batch_resolve", "batch_validate", "batch_publish", "batch_defer", "batch_retry", "batch_finish_empty"}),
    "review": _REVIEW_OPERATIONS,
}


class ActionError(ValueError):
    """A bounded, user-facing gateway failure without request-body disclosure."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message[:500])

    def as_result(self) -> dict[str, str]:
        return {"error": str(self), "code": self.code}


# A bound's error states the bound: workers asked search for 30 to 100 results, beyond its 20.
_BOUNDS = {
    "maximum": "Value must be at most {}", "minimum": "Value must be at least {}",
    "exclusiveMaximum": "Value must be less than {}", "exclusiveMinimum": "Value must be greater than {}",
    "maxLength": "Text must be at most {} character", "minLength": "Text must be at least {} character",
    "maxItems": "List must have at most {} item", "minItems": "List must have at least {} item",
}


def _bound_message(validator: str, value: object) -> str:
    message = _BOUNDS[validator].format(str(value)[:40])
    return message + "s" if message.endswith(("character", "item")) and value != 1 else message


def _names(keys: Iterable[object]) -> str:
    return ", ".join(str(key)[:40] for key in list(keys)[:6])


def _validation_message(error: ValidationError, narrowed: frozenset[str] = frozenset()) -> str:
    """`narrowed` names the fields this turn's capability removed from the operation's schema."""
    parts = [str(item)[:40] for item in list(error.absolute_path)[:6]]
    location = ".".join(parts) or "arguments"
    if error.validator == "required":
        required = error.validator_value if isinstance(error.validator_value, list) else []
        instance = error.instance if isinstance(error.instance, dict) else {}
        missing = [key for key in required if key not in instance]
        detail = "Missing required field(s): " + _names(missing)
    elif error.validator == "additionalProperties":
        schema = error.schema if isinstance(error.schema, dict) else {}
        known = schema.get("properties", {})
        patterns = list(schema.get("patternProperties", {}))
        instance = error.instance if isinstance(error.instance, dict) else {}
        unexpected = [key for key in instance if key not in known
                      and not any(re.search(pattern, key) for pattern in patterns)]
        detail = "Unexpected field(s): " + _names(unexpected) + "; use only fields in the operation schema"
        unavailable = [key for key in unexpected if not error.absolute_path and key in narrowed]
        if unavailable:
            detail += "; this turn cannot use " + _names(unavailable)
    elif error.validator == "type":
        detail = "Expected type " + str(error.validator_value)[:80]
    elif error.validator == "enum":
        detail = "Value must match one of the schema's supported choices"
    elif error.validator in _BOUNDS:
        detail = _bound_message(str(error.validator), error.validator_value)
    else:
        detail = "Value does not satisfy schema constraint " + str(error.validator)[:40]
    return f"Invalid {location}: {detail}."


def _validate(validator: Draft202012Validator, arguments: object,
              narrowed: frozenset[str] = frozenset()) -> dict[str, Any]:
    error = next(validator.iter_errors(arguments), None)
    if error is not None:
        raise ActionError("INVALID_ARGUMENTS", _validation_message(error, narrowed))
    # Every registered entrypoint takes an object; copy before dispatch so that
    # a handler cannot mutate the transport's original request or catalog data.
    if not isinstance(arguments, dict):
        raise ActionError("INVALID_ARGUMENTS", "Expected an arguments object.")
    return deepcopy(arguments)


_GATEWAYS = {spec["name"]: Draft202012Validator(spec["inputSchema"]) for spec in dynamic_tools()}


def validate_gateway(name: str, arguments: object) -> dict[str, Any]:
    """Validate the outer call without exposing operation schemas natively."""
    if not isinstance(name, str) or name not in _GATEWAYS:
        raise ActionError("UNKNOWN_GATEWAY", "Use taskhub_actions or taskhub_call.")
    return _validate(_GATEWAYS[name], arguments)


def _type(schema: dict[str, Any]) -> str:
    """A field's type; a small object shows its fields, one level deep."""
    if "enum" in schema:
        return "|".join(str(value) for value in schema["enum"])
    fields = schema.get("properties")
    if isinstance(fields, dict) and 0 < len(fields) <= 6:
        required = schema.get("required", [])
        return "{" + ", ".join(name if name in required else f"[{name}]" for name in fields) + "}"
    kind = schema.get("type", "value")
    return "|".join(kind) if isinstance(kind, list) else str(kind)


def _bounds(schema: dict[str, Any]) -> str:
    """A field's bounds for the cheat sheet, such as ≤20 or ≤4000 chars. The usual minimums go
    unsaid: nonempty text and lists, and numbers from 0 or 1."""
    kind = schema.get("type")
    low, high, unit = {"string": ("minLength", "maxLength", " chars"),
                       "array": ("minItems", "maxItems", " items")}.get(str(kind), ("minimum", "maximum", ""))
    bounds = []
    for key, sign in ((low, "≥"), (high, "≤")):
        value = schema.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and (key == high or value not in (0, 1)):
            bounds.append(f"{sign}{value}")
    return ",".join(bounds) + unit if bounds else ""


def _category(operation: str) -> str:
    for category, operations in _CATEGORIES.items():
        if operation in operations:
            return category
    if operation.startswith("workspace_"):
        return "workspace"
    if operation.startswith("resource_"):
        return "resources"
    return "domain"


@dataclass(frozen=True)
class CapabilityPolicy:
    operations: frozenset[str] | None = None
    excluded: frozenset[str] = frozenset()
    excluded_prefixes: tuple[str, ...] = ()
    extra_prefixes: tuple[str, ...] = ()


_ANSWER_OPERATIONS = (_READ_OPERATIONS - _REVIEW_OPERATIONS - {"search"}) | {"talk_send", "message_send", "request"}
_POLICIES = {
    "worker": CapabilityPolicy(excluded=_REVIEW_OPERATIONS, excluded_prefixes=("batch_",)),
    "reviewer": CapabilityPolicy(excluded=_REVIEWER_EXCLUDED,
                                 excluded_prefixes=("batch_", "completion_", "invitation_")),
    "message_only": CapabilityPolicy(operations=_MESSAGE_OPERATIONS),
    # The Integrator waits for an answer by deferring the item, never by pausing.
    "integrator": CapabilityPolicy(operations=_ANSWER_OPERATIONS | {"checkpoint", "lean_check", "workspace_write", "workspace_copy"},
                                   extra_prefixes=("batch_",)),
    "review_answer": CapabilityPolicy(operations=_ANSWER_OPERATIONS),
    "integrator_answer": CapabilityPolicy(operations=_ANSWER_OPERATIONS | {"batch_retry"}),
}
_CAPABILITIES = frozenset(_POLICIES)


def _allowed(operation: str, capability: str) -> bool:
    policy = _POLICIES[capability]
    return (operation not in policy.excluded and not operation.startswith(policy.excluded_prefixes)
            and (policy.operations is None or operation in policy.operations or operation.startswith(policy.extra_prefixes)))


class ActionCatalog:
    """Own schemas separately from the invariant native tool definitions."""

    def __init__(self, specs: Iterable[dict[str, Any]]):
        self._specs: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft202012Validator] = {}
        for raw in specs:
            spec = deepcopy(raw)
            name = spec["name"]
            operation = name.removeprefix("taskhub_")
            if name in _GATEWAYS or operation in self._specs:
                raise ValueError(f"Duplicate or reserved operation name: {operation}")
            Draft202012Validator.check_schema(spec["inputSchema"])
            self._specs[operation] = spec
            self._validators[operation] = Draft202012Validator(spec["inputSchema"])

    @property
    def specs(self) -> list[dict[str, Any]]:
        """Return an isolated copy for runtime inspection and diagnostics."""
        return deepcopy(list(self._specs.values()))

    @staticmethod
    def _check_capability(capability: str) -> None:
        if not isinstance(capability, str) or capability not in _CAPABILITIES:
            raise ActionError("INVALID_CAPABILITY", "The current execution capability is unavailable.")

    def _find(self, operation: str, capability: str) -> dict[str, Any]:
        self._check_capability(capability)
        if not isinstance(operation, str) or not operation or len(operation) > 80:
            raise ActionError("INVALID_ARGUMENTS", "Operation must be a nonempty string of at most 80 characters.")
        if operation not in self._specs or not _allowed(operation, capability):
            available = [name for name in self._specs if _allowed(name, capability)]
            near = get_close_matches(operation, available, n=3, cutoff=0.5)
            hint = (" Did you mean " + " or ".join(near) + "?" if near else "") + (
                " Your instructions list every operation you can call here.")
            if operation not in self._specs:
                raise ActionError("UNKNOWN_OPERATION", "Unknown operation." + hint)
            raise ActionError("OPERATION_NOT_ALLOWED", "This operation is unavailable in the current execution context." + hint)
        return self._specs[operation]

    @staticmethod
    def _description(operation: str, spec: dict[str, Any], capability: str) -> str:
        if operation == "talk_send" and capability == "message_only":
            return ("Send a direct message to any project worker. "
                    "This does not resume your task or grant review/publication authority.")
        if operation in {"workspace_write", "workspace_copy"} and capability == "message_only":
            return "Write your proposed shared DAG files under .private/dags/ only. Other workspace writes require task execution authority."
        if operation == "resume_work" and capability == "message_only":
            return ("Become available for a new task when useful work exists. "
                    "This does not resume the task you left.")
        if operation == "inspect" and capability == "message_only":
            return ("Read a task by ID. Pass revision to inspect a historical contract; "
                    "omit it for the current revision. Completion details (include_completion) "
                    "need your own task turn.")
        if operation == "propose_revision" and capability == "message_only":
            return ("Revise a task you originally published, with its expected current revision and a concrete "
                    "reason; preserve the original objective. An executor who did not publish a task revises it "
                    "only in its own task turn.")
        return spec.get("description", "")

    @classmethod
    def _summary(cls, operation: str, spec: dict[str, Any], capability: str) -> dict[str, Any]:
        return {"operation": operation, "category": _category(operation),
                "description": cls._description(operation, spec, capability)[:320]}

    @staticmethod
    def _schema(operation: str, spec: dict[str, Any], capability: str) -> dict[str, Any]:
        schema = deepcopy(spec["inputSchema"])
        if operation in {"workspace_write", "workspace_copy"} and capability == "message_only":
            schema["properties"]["path"] = {"type": "string", "pattern": r"^\.private/dags/"}
        if operation == "inspect" and capability == "message_only":
            schema["properties"].pop("include_completion", None)
        if operation == "request" and capability in {"review_answer", "integrator_answer"}:
            # A clarification turn cannot wait for a source, so it may only ask a question.
            schema["properties"]["kind"] = {"type": "string", "enum": ["question"]}
        return schema

    def describe(self, *, capability: str = "worker", category: str | None = None,
                 operation: str | None = None) -> dict[str, Any]:
        """List summaries, or return exactly one available operation's schema."""
        self._check_capability(capability)
        request = {key: value for key, value in {"category": category, "operation": operation}.items()
                   if value is not None}
        validate_gateway("taskhub_actions", request)
        available = {name: spec for name, spec in self._specs.items() if _allowed(name, capability)}
        counts = Counter(_category(name) for name in available)
        if category is not None and category not in counts:
            raise ActionError("UNKNOWN_CATEGORY", "Unknown or unavailable category. Available categories: " + ", ".join(sorted(counts)))
        if operation is not None:
            spec = self._find(operation, capability)
            if category is not None and category != _category(operation):
                raise ActionError("INVALID_ARGUMENTS", "The operation does not belong to the requested category.")
            return {"operation": operation, "category": _category(operation),
                    "description": self._description(operation, spec, capability),
                    "inputSchema": self._schema(operation, spec, capability)}
        return {
            "categories": [{"category": name, "count": counts[name]} for name in sorted(counts)],
            "operations": [self._summary(name, spec, capability) for name, spec in available.items()
                           if category is None or _category(name) == category],
        }

    def cheat_sheet(self, capability: str) -> str:
        """One line per operation available here: its arguments and the gist of its purpose.

        It is the same for every worker in the same role, so it belongs in the shared standing
        instructions; taskhub_actions still returns an operation's exact schema.
        """
        self._check_capability(capability)
        available = sorted(((name, spec) for name, spec in self._specs.items() if _allowed(name, capability)),
                           key=lambda item: (_category(item[0]), item[0]))
        lines = ["Operations for taskhub_call (required arguments first, then [optional] ones; {fields} of "
                 "an object argument; ≤ and ≥ give bounds; taskhub_actions gives an exact schema):"]
        for operation, spec in available:
            schema = self._schema(operation, spec, capability)
            required = schema.get("required", [])
            properties = schema.get("properties", {})
            arguments = []
            for name in required:
                bounds = _bounds(properties.get(name, {}))
                arguments.append(f"{name}: {_type(properties.get(name, {}))}" + (" " + bounds if bounds else ""))
            for name in properties:
                if name not in required:
                    fields = _type(properties[name])
                    arguments.append(f"[{name}{_bounds(properties[name])}]" + (": " + fields if fields.startswith("{") else ""))
            gist = self._description(operation, spec, capability).split(". ")[0].rstrip(".")
            if len(gist) > 110:
                gist = gist[:110].rsplit(" ", 1)[0].rstrip(",;:") + "…"
            lines.append(f"- {operation}({', '.join(arguments)}): {gist}"
                         + (" (ends the turn)" if operation in ENDS_TURN else ""))
        return "\n".join(lines)

    def resolve(self, operation: str, arguments: object, *,
                capability: str = "worker") -> tuple[str, dict[str, Any]]:
        """Check surface capability and schema, then return the internal action."""
        spec = self._find(operation, capability)
        schema = self._schema(operation, spec, capability)
        if schema == spec["inputSchema"]:
            return spec["name"], _validate(self._validators[operation], arguments)
        # This capability narrows the schema; an error names a field it removed.
        removed = frozenset(spec["inputSchema"].get("properties", {})) - frozenset(schema.get("properties", {}))
        return spec["name"], _validate(Draft202012Validator(schema), arguments, removed)
