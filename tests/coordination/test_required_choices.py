"""No role has a default model or effort, and the answer deadline has none: a run starts or resumes
only once each is chosen, in project.toml or by the operator."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    ClaudeConfig,
    CodexConfig,
    Command,
    DecisionConfig,
    Limits,
    ProjectConfig,
    Role,
)
from formal_geyser.coordination.runtime import claude as claude_runtime

pytestmark = pytest.mark.choices_required
OP = Actor(role=Role.OPERATOR)
CHOICES = [
    ("runtime.set_role_model", {"role": "worker", "backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}),
    ("runtime.set_role_model", {"role": "review", "backend": "codex", "model": "gpt-6.1-sol", "effort": "xhigh"}),
    ("runtime.set_role_model", {"role": "integrator", "backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}),
    ("runtime.set_decision_model", {"model": "gpt-6.1-sol", "effort": "high"}),
    ("runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"}),
    ("runtime.set_answer_deadline", {"minutes": 45}),
]
EVERYTHING = ["Workers on Codex: model", "Workers on Codex: effort", "Reviewers: model", "Reviewers: effort",
              "The Integrator: model", "The Integrator: effort", "The Dispatcher: model", "The Dispatcher: effort",
              "The Waker: model", "The Waker: effort", "The answer deadline"]


async def command(hub, name, payload=None, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


@pytest.fixture
async def hub(tmp_path, monkeypatch):
    # Claude workers cannot start here unless a test says they can.
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: "Not on this machine.")
    initialize(tmp_path, ProjectConfig(limits=Limits()))
    result = Hub(tmp_path)
    # Nothing chosen: the tests' projects otherwise take the efforts and Dispatcher of tests/conftest.py.
    result.config = result.config.model_copy(update={
        "codex": result.config.codex.model_copy(update={"effort": None}),
        "claude": result.config.claude.model_copy(update={"effort": None}),
        "decisions": DecisionConfig(model=None, effort=None)})
    yield result
    await result.close()


async def refused(hub, name, payload=None):
    with pytest.raises(HubError) as error:
        await command(hub, name, payload)
    assert error.value.code == "CHOICES_REQUIRED"
    return error.value.message


async def test_a_run_starts_only_once_every_choice_is_made(hub, monkeypatch):
    assert (await hub.backend_status())["missing_choices"] == EVERYTHING
    message = await refused(hub, "run.start")
    assert all(item in message for item in EVERYTHING) and "Agents and sessions" in message
    for name, payload in CHOICES[:-1]:
        await command(hub, name, payload)
    assert await refused(hub, "run.start") == (
        "Choose these before a run starts or resumes, in Project setup's Agents and sessions panel or in "
        "project.toml: The answer deadline.")
    await command(hub, *CHOICES[-1])
    # Where Claude workers can start, their model and effort must be chosen too.
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: None)
    assert (await hub.backend_status())["missing_choices"] == ["Workers on Claude: model", "Workers on Claude: effort"]
    await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "claude",
                                                  "model": "claude-opus-5-5", "effort": "high"})
    assert (await hub.backend_status())["missing_choices"] == []
    run = await command(hub, "run.start")
    assert run["state"] == "running"


async def test_project_toml_choices_count_and_no_role_takes_anothers(tmp_path, monkeypatch):
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: "Not on this machine.")
    initialize(tmp_path, ProjectConfig.model_validate({
        "codex": {"model": "gpt-6.1-sol", "effort": "high", "review_effort": "xhigh"},
        "reviewers": {"model": "gpt-6.1-sol"},
        "integrator": {"model": "gpt-6.1-sol", "effort": "high"},
        "decisions": {"model": "gpt-6.1-sol", "effort": "high"},
        "waker": {"model": "gpt-6.1-terra", "effort": "medium"},
        "waits": {"answer_deadline_minutes": 60}}))
    hub = Hub(tmp_path)
    try:
        status = await hub.backend_status()
        assert status["missing_choices"] == []
        assert status["role_models"]["review"]["codex"]["effort"] == "xhigh"  # The older review_effort.
        assert status["answer_deadline"] == {"minutes": 60, "source": "config"}
        # Without [reviewers] model, reviewers have none: they do not take the workers'.
        hub.config = hub.config.model_copy(update={"reviewers": hub.config.reviewers.model_copy(update={"model": None})})
        assert (await hub.backend_status())["missing_choices"] == ["Reviewers: model"]
    finally:
        await hub.close()


async def test_a_run_resumes_only_while_every_choice_is_made(hub):
    for name, payload in CHOICES:
        await command(hub, name, payload)
    run = await command(hub, "run.start")
    await command(hub, "run.pause", {"run_id": run["id"]})
    await hub.db.call(lambda c: c.execute("DELETE FROM waker_preferences"), write=True)
    message = await refused(hub, "run.resume", {"run_id": run["id"]})
    assert message.endswith("The Waker: model; The Waker: effort.")
    await refused(hub, "group.resume", {"run_id": run["id"], "group": "codex.workers"})
    await command(hub, *CHOICES[4])
    assert (await command(hub, "run.resume", {"run_id": run["id"]}))["state"] == "running"


async def test_the_answer_deadline_and_the_dispatcher_effort_are_chosen_explicitly(hub):
    assert (await command(hub, "runtime.set_answer_deadline", {"minutes": 90})) == {"minutes": 90, "source": "operator"}
    for minutes in (0, 43201, "60", 1.5, None):
        with pytest.raises(HubError) as error:
            await command(hub, "runtime.set_answer_deadline", {"minutes": minutes})
        assert error.value.code == "INVALID_INPUT"
    with pytest.raises(HubError) as error:  # No effort is taken for granted.
        await command(hub, "runtime.set_decision_model", {"model": "gpt-6.1-sol"})
    assert error.value.code == "INVALID_INPUT"


async def test_the_shared_reviewer_choice_becomes_the_integrators_as_well(tmp_path):
    initialize(tmp_path, ProjectConfig(codex=CodexConfig(model="gpt-6.1-sol", effort="high"),
                                       claude=ClaudeConfig(effort="high")))
    hub = Hub(tmp_path)
    try:
        # A project from before 2026-11-07 kept one choice for reviewers and the Integrator.
        def legacy(c):
            c.execute("DELETE FROM role_models")
            c.execute("INSERT INTO role_model_preferences VALUES('review','codex','gpt-6-astra','max',1)")
            c.execute("INSERT INTO role_model_preferences VALUES('worker','claude','claude-opus-5-5',NULL,1)")
        await hub.db.call(legacy, write=True)
    finally:
        await hub.close()
    hub = Hub(tmp_path)
    try:
        rows = await hub.db.call(lambda c: sorted(tuple(row) for row in c.execute(
            "SELECT role,backend,model,effort FROM role_models")))
        assert rows == [("integrator", "codex", "gpt-6-astra", "max"), ("review", "codex", "gpt-6-astra", "max"),
                        ("worker", "claude", "claude-opus-5-5", None)]
        assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM role_model_preferences").fetchone()[0]) == 0
    finally:
        await hub.close()
