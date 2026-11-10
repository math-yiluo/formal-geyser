"""First-run setup using native task files and the existing command receipts.

Configuration and local Git preparation happen before any execution is admitted.
Task 0, its first run, and the idempotency receipt commit together. Replaying that
receipt never re-arms execution; an interrupted service needs an explicit drive.
"""
from __future__ import annotations

import json
import os
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import tomli_w
from pydantic import Field, field_validator, model_validator

from .config import load_config
from .errors import HubError
from .models import Actor, Command, Role, StrictModel, TaskContract
from .plugins import load_plugin
from .storage import canonical, digest
from .text_bounds import shorten

SETUP_COMMAND = "project.setup"
SETUP_PRINCIPAL = "operator:operator"
# Task 0's title is the goal's first line, shortened at a word boundary to this many characters.
ROOT_TITLE_CHARACTERS = 200


class SetupStart(StrictModel):
    key: str = Field(min_length=1, max_length=200)
    goal: str = Field(min_length=1, max_length=20000)
    model: str = Field(min_length=1, max_length=200)
    effort: str = Field(min_length=1, max_length=100)
    project_id: str | None = Field(default=None, min_length=1, max_length=64)
    task_id: str | None = Field(default=None, min_length=1)
    task_revision: int | None = Field(default=None, ge=1)

    @field_validator("key", "goal", "model", "effort")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Supply a nonempty goal, model, effort, and request key")
        # Preserve the user's complete goal, including its original whitespace.
        return value

    @model_validator(mode="after")
    def exact_existing_task(self):
        if (self.task_id is None) != (self.task_revision is None):
            raise ValueError("An existing Task 0 requires both its ID and exact revision")
        return self


def _request_hash(request: SetupStart) -> str:
    return digest(request.model_dump(exclude={"key"}))


def _receipt(c, request: SetupStart) -> dict | None:
    prior = c.execute("SELECT request_hash,response_json FROM receipts WHERE principal=? AND command=? AND key=?",
                      (SETUP_PRINCIPAL, SETUP_COMMAND, request.key)).fetchone()
    if prior is None:
        return None
    if prior["request_hash"] != _request_hash(request):
        raise HubError("IDEMPOTENCY_CONFLICT", "This Start request was already used with different settings")
    return json.loads(prior["response_json"])


async def existing_receipt(hub, request: SetupStart) -> dict | None:
    _check_project(hub, request)
    return await hub.db.call(lambda c: _receipt(c, request))


def _check_project(hub, request: SetupStart) -> None:
    if request.project_id is not None and request.project_id != digest(str(hub.root))[:24]:
        raise HubError("SETUP_PROJECT_CHANGED", "The connected project changed; refresh Project setup before starting")


def _status(hub, c) -> dict[str, Any]:
    project = dict(c.execute("SELECT * FROM project_lifecycle WHERE id=1").fetchone())
    root_id = project["root_task_id"]
    task = hub._task(c, root_id) if root_id else None
    counts = {table: c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
              for table in ("tasks", "workers", "runs", "attempts", "turn_intents", "helper_intents")}
    reason = None
    if any(counts[name] for name in ("workers", "runs", "attempts", "turn_intents", "helper_intents")):
        reason = "This project already has execution history. Its settings are preserved; use the existing run controls."
    elif project["state"] != "running":
        reason = "This project is stopped or finishing Task 0. Use the existing project controls."
    elif task is None and counts["tasks"]:
        reason = "Tasks exist without a recorded Task 0. Repair the project root before starting."
    elif task is not None and (task["state"] in {"done", "retired", "recovery_required", "draining"}
                              or task["collaboration"]["mode"] != "collaborative"
                              or task["contract"]["kind"] == "review"):
        reason = "The existing Task 0 is not available for first-run setup. Its history is preserved."
    run = c.execute("SELECT id,state FROM runs ORDER BY created_at DESC,rowid DESC LIMIT 1").fetchone()
    receipt = c.execute("SELECT response_json FROM receipts WHERE principal=? AND command=? ORDER BY rowid DESC LIMIT 1",
                        (SETUP_PRINCIPAL, SETUP_COMMAND)).fetchone()
    return {
        "project_id": digest(str(hub.root))[:24], "can_configure": reason is None, "reason": reason,
        "task": ({"id": task["id"], "title": task["contract"]["title"],
                  "objective": task["contract"]["objective"], "revision": task["revision"]} if task else None),
        "settings": {"model": hub.config.codex.model, "effort": hub.config.codex.effort},
        "repository": {"enabled": hub.config.repository.enabled,
                       "initialized": bool(c.execute("SELECT 1 FROM events WHERE kind='repository.initialized'").fetchone())
                       and (hub.state_dir / "repository.git").is_dir()},
        "run": dict(run) if run else None,
        "can_drive": bool(run and run["state"] == "running" and project["state"] == "running"
                          and task and task["state"] not in {"done", "retired"}
                          and not any(counts[name] for name in ("workers", "attempts", "turn_intents", "helper_intents"))),
        "receipt": json.loads(receipt["response_json"]) if receipt else None,
    }


async def setup_status(hub) -> dict:
    return await hub.db.call(lambda c: _status(hub, c))


def _check_basis(hub, c, request: SetupStart) -> dict | None:
    _check_project(hub, request)
    status = _status(hub, c)
    if not status["can_configure"]:
        raise HubError("SETUP_LOCKED", status["reason"])
    root = status["task"]
    if root is None:
        if request.task_id is not None:
            raise HubError("SETUP_BASIS_CHANGED", "Task 0 changed; refresh Project setup before starting")
        return None
    if (request.task_id, request.task_revision, request.goal) != (root["id"], root["revision"], root["objective"]):
        raise HubError("SETUP_BASIS_CHANGED", "The existing Task 0 is read-only here; refresh its exact current revision")
    return root


async def check_basis(hub, request: SetupStart) -> None:
    await hub.db.call(lambda c: _check_basis(hub, c, request))


def validate_selection(catalog: dict, request: SetupStart) -> None:
    if catalog.get("authentication_required"):
        raise HubError("CODEX_LOGIN_REQUIRED", "Sign in to the project's configured Codex profile before starting")
    model = next((item for item in catalog.get("models", []) if item["id"] == request.model), None)
    if model is None:
        raise HubError("MODEL_UNAVAILABLE", "The selected model is no longer available; refresh the model list", 422)
    if request.effort not in {item["id"] for item in model["efforts"]}:
        raise HubError("EFFORT_UNSUPPORTED", "Choose an effort supported by the selected model", 422)


def _contract(hub, request: SetupStart) -> TaskContract:
    # The goal's first line, whole when it fits. Until 2026-11-08 this was "Task 0: " and its
    # first 120 characters, cut mid-word (the pilot's read "... following Henniar"); the task's
    # name, Task 0, now comes from its number and is shown beside its title.
    title = shorten(next(line.strip() for line in request.goal.splitlines() if line.strip()), ROOT_TITLE_CHARACTERS)
    result = TaskContract(title=title, objective=request.goal, domain=hub.config.domain,
                          acceptance_policy="lean.peer@1" if hub.config.domain == "lean" else "general.peer@1")
    load_plugin(hub.config.domain).validate_contract(result)
    return result


def _atomic_write(path: Path, contents: bytes, previous: bytes) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".project-setup-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        if path.read_bytes() != previous:
            raise HubError("CONFIGURATION_CHANGED", "Project configuration changed; restart the service before setup")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


async def prepare_local_project(hub, request: SetupStart) -> None:
    """Persist choices and local repository readiness, with no task or model start.

If later preparation fails, these valid settings and the local repository can
remain for a corrected retry. Existing keys, credentials, and profile selection
are preserved. No remote repository is configured or contacted by this method.
"""
    def prepare(c):
        _check_basis(hub, c, request)
        _contract(hub, request)  # Validate the domain before touching configuration.
        path = hub.root / "project.toml"
        previous = path.read_bytes()
        if load_config(hub.root) != hub.config:
            raise HubError("CONFIGURATION_CHANGED", "Project configuration changed; restart the service before setup")
        document = tomllib.loads(previous.decode("utf-8"))
        document.setdefault("codex", {}).update(model=request.model, effort=request.effort)
        document.setdefault("repository", {})["enabled"] = True
        updated = hub.config.model_copy(update={
            "codex": hub.config.codex.model_copy(update={"model": request.model, "effort": request.effort}),
            "repository": hub.config.repository.model_copy(update={"enabled": True}),
        })
        _atomic_write(path, tomli_w.dumps(document).encode("utf-8"), previous)
        hub.config = updated

    await hub.db.call(prepare)
    # This records the same initialization evidence used by normal CLI setup.
    # A missing or changed recorded repository head is not silently repaired.
    await hub.execute(Command(name="repository.initialize", key="project-setup-repository:" + digest(request.key),
                              payload={}), Actor(role=Role.OPERATOR))


async def create_setup(hub, request: SetupStart) -> tuple[dict, bool]:
    """Create Task 0, a run, and the existing-style receipt in one transaction."""
    def create(c):
        prior = _receipt(c, request)
        if prior is not None:
            return prior, True
        root = _check_basis(hub, c, request)
        if (hub.config.codex.model, hub.config.codex.effort) != (request.model, request.effort):
            raise HubError("CONFIGURATION_CHANGED", "The prepared model settings changed before startup")
        if load_config(hub.root) != hub.config:
            raise HubError("CONFIGURATION_CHANGED", "Project configuration changed before startup")
        hub._require_recorded_repository_head(c, hub._repository())
        actor = Actor(role=Role.OPERATOR)
        if root is None:
            root = hub._dispatch(c, "task.publish", {"contract": _contract(hub, request).model_dump()}, actor)
        run = hub._dispatch(c, "run.start", {}, actor)
        # Start means drive: a service restart after this commit resumes the run.
        c.execute("UPDATE runs SET resume_on_start=1 WHERE id=?", (run["id"],))
        response = {"key": request.key, "task_id": root["id"], "run_id": run["id"],
                    "model": request.model, "effort": request.effort}
        c.execute("INSERT INTO receipts(principal,command,key,request_hash,response_json) VALUES(?,?,?,?,?)",
                  (SETUP_PRINCIPAL, SETUP_COMMAND, request.key, _request_hash(request), canonical(response)))
        hub._event(c, "project.setup", root["id"], response, root["id"])
        return response, False
    return await hub.db.call(create, write=True)
