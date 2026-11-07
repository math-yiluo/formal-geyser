"""The operator's model, effort, backend and concurrency choices apply to conversations that
begin after them; a conversation keeps the backend, model and effort it began with."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    AssignmentConfig,
    ClaudeConfig,
    CodexConfig,
    Command,
    ConcurrencyConfig,
    ProjectConfig,
    Role,
    RoleModelConfig,
)
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime import claude as claude_runtime
from formal_geyser.coordination.session_settings import bound_settings
from tests.coordination.publication_helpers import new_hub
from tests.coordination.test_collaborative_core import claim, publish

CONFIG = ProjectConfig(codex=CodexConfig(model="gpt-6-astra", effort="xhigh", review_effort="max", integrator_effort="max"),
                       claude=ClaudeConfig(model="claude-opus-5-5", effort="xhigh"),
                       reviewers=RoleModelConfig(model="gpt-6-astra"), integrator=RoleModelConfig(model="gpt-6-astra"),
                       assignment=AssignmentConfig(worker_backends=["codex"], reviewer_backend="codex"),
                       concurrency=ConcurrencyConfig(codex=60, claude=12))


async def command(hub, name, payload=None, *, role=Role.OPERATOR):
    return await hub.execute(Command(name=name, payload=payload or {}, key=uuid4().hex), Actor(role=role))


@pytest.fixture
async def hub(tmp_path, monkeypatch):
    # Selecting Claude checks this machine; the tests exercise the hub, not the platform.
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: None)
    initialize(tmp_path, CONFIG)
    hub = Hub(tmp_path)
    try:
        yield hub
    finally:
        await hub.close()


async def live_session(hub, worker_id):
    """A session running for this worker (only what the session count reads)."""
    run = next((item for item in await hub.list_runs() if item["state"] == "running"), None) or await command(hub, "run.start")
    task = await publish(hub, "Live " + uuid4().hex)
    await hub.db.call(lambda c: c.execute(
        "INSERT INTO attempts(id,task_id,task_revision,worker_id,run_id,fencing_token,state,created_at) "
        "VALUES(?,?,1,?,?,1,'running',0)", (uuid4().hex, task["id"], worker_id, run["id"])), write=True)


async def bind(hub, worker_id, thread_id, settings):
    await command(hub, "worker.bind_session", {"worker_id": worker_id, "thread_id": thread_id, "model": settings["model"],
                                               "codex_home": None, "codex_profile": {}, "settings": settings},
                  role=Role.RUNNER)


async def test_a_role_choice_applies_to_new_conversations_only(hub):
    worker = await command(hub, "worker.register")
    reviewer = await command(hub, "worker.register", {"kind": "review"})
    new = await hub.session_settings(worker["id"])
    assert (new["model"], new["effort"], new["new"]) == ("gpt-6-astra", "xhigh", True)
    assert (await hub.session_settings(reviewer["id"]))["effort"] == "max"
    await bind(hub, worker["id"], "thread-1", {"model": "gpt-6-astra", "effort": "xhigh"})
    models = await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex",
                                                          "model": "gpt-6.1-sol", "effort": "high",
                                                          "apply_to_existing": True})
    assert models["worker"]["codex"] == {"model": "gpt-6.1-sol", "effort": "high", "source": "operator",
                                         "kept_by_existing": 0}
    # Reviewers keep their own: [reviewers] model and the project's review_effort.
    assert models["review"] == {"codex": {"model": "gpt-6-astra", "effort": "max", "source": "config",
                                          "kept_by_existing": 0}}
    # The running conversation keeps its own; the next one starts with the choice.
    assert (await hub.session_settings(worker["id"]))["model"] == "gpt-6-astra"
    await hub.release_conversation(worker["id"], "rotation")
    fresh = await hub.session_settings(worker["id"])
    assert (fresh["model"], fresh["effort"], fresh["new"]) == ("gpt-6.1-sol", "high", True)
    assert (await hub.session_settings(reviewer["id"]))["model"] == "gpt-6-astra"
    # Null returns the role to the project's configuration.
    await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex", "model": None, "effort": None,
                                                  "apply_to_existing": True})
    assert (await hub.session_settings(worker["id"]))["model"] == "gpt-6-astra"


async def test_each_role_has_its_own_choice_and_none_takes_anothers(hub, tmp_path_factory):
    models = (await hub.backend_status())["role_models"]
    assert models["integrator"] == {"codex": {"model": "gpt-6-astra", "effort": "max", "source": "config",
                                              "kept_by_existing": 0}}
    models = await command(hub, "runtime.set_role_model", {"role": "integrator", "backend": "codex",
                                                          "model": "gpt-6.1-terra", "effort": "medium"})
    assert (models["integrator"]["codex"]["model"], models["review"]["codex"]["model"]) == ("gpt-6.1-terra", "gpt-6-astra")
    with pytest.raises(HubError):  # The Integrator runs on Codex.
        await command(hub, "runtime.set_role_model", {"role": "integrator", "backend": "claude", "model": "claude-opus-5-5"})
    # Where only the workers are chosen, reviewers and the Integrator have nothing.
    other = tmp_path_factory.mktemp("plain")
    initialize(other, ProjectConfig(codex=CodexConfig(model="gpt-6-astra", effort="xhigh")))
    plain = Hub(other)
    try:
        status = (await plain.backend_status())["role_models"]
        for role in ("review", "integrator"):
            assert status[role]["codex"] == {"model": None, "effort": None, "source": None, "kept_by_existing": 0}
    finally:
        await plain.close()


async def test_a_role_choice_reaches_existing_workers_only_when_applied_to_them(hub):
    async def started(worker):
        settings = await hub.session_settings(worker["id"])
        return settings["model"], settings["effort"]
    old = await command(hub, "worker.register")
    elsewhere = await command(hub, "worker.register", {"backend": "claude"})
    models = await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex",
                                                          "model": "gpt-6.1-sol", "effort": "high"})
    # Unticked: a worker on Codex now keeps what its new conversations started with.
    assert models["worker"]["codex"]["kept_by_existing"] == 1
    assert await started(old) == ("gpt-6-astra", "xhigh")
    newer = await command(hub, "worker.register")
    assert await started(newer) == ("gpt-6.1-sol", "high")
    # A worker that moves to Codex later starts with the current choice.
    await command(hub, "worker.select_backend", {"worker_id": elsewhere["id"], "backend": "codex"})
    assert await started(elsewhere) == ("gpt-6.1-sol", "high")
    # A second unticked change: the first worker keeps its earliest choice, the others the one before.
    models = await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex",
                                                          "model": "gpt-7", "effort": "low"})
    assert models["worker"]["codex"]["kept_by_existing"] == 3
    assert [await started(item) for item in (old, newer, elsewhere)] == [
        ("gpt-6-astra", "xhigh"), ("gpt-6.1-sol", "high"), ("gpt-6.1-sol", "high")]
    assert await started(await command(hub, "worker.register")) == ("gpt-7", "low")
    # A choice that changes nothing keeps nothing.
    assert (await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "claude",
                                                          "model": "claude-opus-5-5", "effort": "xhigh"})
            )["worker"]["claude"]["kept_by_existing"] == 0
    # Ticked: every worker's next new conversation starts with it.
    models = await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex", "model": "gpt-7",
                                                          "effort": "low", "apply_to_existing": True})
    assert models["worker"]["codex"]["kept_by_existing"] == 0
    assert {await started(item) for item in (old, newer, elsewhere)} == {("gpt-7", "low")}
    # Reviewers keep their own efforts the same way; the Integrator's choice is its own.
    reviewer = await command(hub, "worker.register", {"kind": "review"})
    integrator = await command(hub, "worker.register", {"kind": "integrator"})
    models = await command(hub, "runtime.set_role_model", {"role": "review", "backend": "codex",
                                                          "model": "gpt-6-astra", "effort": "high"})
    assert models["review"]["codex"]["kept_by_existing"] == 1
    assert await started(integrator) == ("gpt-6-astra", "max")
    assert await started(reviewer) == ("gpt-6-astra", "max")
    assert await started(await command(hub, "worker.register", {"kind": "review"})) == ("gpt-6-astra", "high")
    with pytest.raises(HubError):
        await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex", "model": "gpt-7",
                                                      "apply_to_existing": "yes"})


async def test_role_choices_are_checked(hub):
    for payload in ({"role": "review", "backend": "claude", "model": "claude-opus-5-5"},  # Reviewers run on Codex.
                    {"role": "dispatcher", "backend": "codex", "model": "gpt-6-astra"},
                    {"role": "worker", "backend": "claude", "effort": "extreme"},
                    {"role": "worker", "backend": "codex", "model": "two words"}):
        with pytest.raises(HubError):
            await command(hub, "runtime.set_role_model", payload)
    with pytest.raises(HubError):
        await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex", "model": "x"}, role=Role.RUNNER)


def test_a_conversation_bound_before_its_own_settings_keeps_the_projects_by_role():
    profile = {"model": "gpt-6-astra", "effort": "xhigh", "review_effort": "max"}
    row = {"thread_id": "t", "kind": "review", "profile_json": json.dumps({"codex_profile": profile})}
    assert bound_settings(row) == {"model": "gpt-6-astra", "effort": "max"}
    assert bound_settings({**row, "kind": "ordinary"}) == {"model": "gpt-6-astra", "effort": "xhigh"}
    assert bound_settings({**row, "thread_id": None}) is None


async def test_a_worker_moves_to_its_requested_backend_at_its_next_new_conversation(hub):
    idle = await command(hub, "worker.register")
    moved = await command(hub, "worker.select_backend", {"worker_id": idle["id"], "backend": "claude"})
    assert moved == {"worker_id": idle["id"], "backend": "claude", "requested_backend": None}  # Between conversations.
    busy = await command(hub, "worker.register")
    await bind(hub, busy["id"], "thread-2", {"model": "gpt-6-astra", "effort": "xhigh"})
    pending = await command(hub, "worker.select_backend", {"worker_id": busy["id"], "backend": "claude"})
    assert (pending["backend"], pending["requested_backend"]) == ("codex", "claude")
    listed = {item["id"]: item for item in await hub.list_workers()}
    assert listed[busy["id"]]["requested_backend"] == "claude"
    assert (await hub.session_settings(busy["id"]))["backend"] == "codex"
    await hub.release_conversation(busy["id"], "rotation")
    settings = await hub.session_settings(busy["id"])
    assert (settings["backend"], settings["model"], settings["effort"]) == ("claude", "claude-opus-5-5", "xhigh")
    assert {item["id"]: item for item in await hub.list_workers()}[busy["id"]]["requested_backend"] is None
    reviewer = await command(hub, "worker.register", {"kind": "review"})
    with pytest.raises(HubError):  # Reviewers, the Integrator and the Dispatcher stay on Codex.
        await command(hub, "worker.select_backend", {"worker_id": reviewer["id"], "backend": "claude"})


async def test_a_requested_backend_waits_while_its_account_is_full(hub):
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 1})
    busy = await command(hub, "worker.register")
    await live_session(hub, (await command(hub, "worker.register", {"backend": "claude"}))["id"])
    await bind(hub, busy["id"], "thread-3", {"model": "gpt-6-astra", "effort": "xhigh"})
    await command(hub, "worker.select_backend", {"worker_id": busy["id"], "backend": "claude"})
    await hub.release_conversation(busy["id"], "rotation")
    worker = {item["id"]: item for item in await hub.list_workers()}[busy["id"]]
    assert (worker["backend"], worker["requested_backend"]) == ("codex", "claude")


async def test_the_operators_choice_for_new_workers_overrides_the_plan(hub):
    assert (await hub.backend_status())["new_worker_source"] == "plan"
    await command(hub, "runtime.select_backend", {"backend": "claude"})
    status = await hub.backend_status()
    assert (status["new_worker_source"], status["next_worker"]["backend"]) == ("operator", "claude")
    assert (await command(hub, "worker.register"))["backend"] == "claude"
    # Workers only: reviewers and the Integrator follow the plan.
    assert (await command(hub, "worker.register", {"kind": "review"}))["backend"] == "codex"
    assert (await command(hub, "worker.register", {"kind": "integrator"}))["backend"] == "codex"


async def test_session_limits_change_while_running(hub):
    status = await hub.backend_status()
    assert status["sessions"]["codex"] == {"live": 0, "limit": 60, "source": "config"}
    view = await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": 2})
    assert view["codex"] == {"limit": 2, "source": "operator"}
    assert (await hub.backend_status())["sessions"]["codex"]["limit"] == 2
    assert not await hub.db.read(lambda c: hub._backend_full(c, "codex"))
    for _ in range(2):
        await live_session(hub, (await command(hub, "worker.register", {"kind": "review"}))["id"])
    # Reviewers count toward Codex's sessions: a worker would wait now.
    assert await hub.db.read(lambda c: hub._backend_full(c, "codex"))
    await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": -1})  # No limit.
    assert not await hub.db.read(lambda c: hub._backend_full(c, "codex"))
    assert (await hub.backend_status())["sessions"]["codex"]["limit"] is None
    await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": None})
    assert (await hub.backend_status())["sessions"]["codex"]["source"] == "config"
    for sessions in (-2, 2000, "3", True):
        with pytest.raises(HubError):
            await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": sessions})


async def test_limits_saved_when_0_set_no_limit_keep_their_meaning(tmp_path):
    initialize(tmp_path, CONFIG)
    hub = Hub(tmp_path)
    await hub.backend_status()
    # Saved before -1 meant no limit, when the table held them and 0 meant no limit.
    await hub.db.call(lambda c: c.executemany("INSERT INTO concurrency_preferences VALUES(?,?,?)",
                                              [("codex", 0, 1), ("claude", 5, 2)]), write=True)
    await hub.close()
    hub = Hub(tmp_path)
    try:
        sessions = (await hub.backend_status())["sessions"]
        assert [(sessions[name]["limit"], sessions[name]["source"]) for name in ("codex", "claude")] == [
            (None, "operator"), (5, "operator")]
        assert await hub.db.read(lambda c: c.execute("SELECT COUNT(*) FROM concurrency_preferences").fetchone()[0]) == 0
        await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": 0})
        assert (await hub.backend_status())["sessions"]["codex"]["limit"] == 0
    finally:
        await hub.close()


async def test_an_activation_opens_its_conversation_with_its_own_settings(tmp_path, monkeypatch):
    hub = await new_hub(tmp_path)
    try:
        run = await command(hub, "run.start")
        attempt = await claim(hub, await publish(hub, "Bound"), run)
        await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex",
                                                      "model": "gpt-6.1-sol", "effort": "low", "apply_to_existing": True})
        runner = WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([])]))
        opened, original = [], runner._new_harness

        async def new_harness(backend="codex", **options):
            opened.append((backend, options.get("model"), options.get("effort")))
            return await original(backend, **options)
        monkeypatch.setattr(runner, "_new_harness", new_harness)
        await runner.run_once(run["id"], assignment=attempt)
        assert opened[0] == ("codex", "gpt-6.1-sol", "low")
        worker = await hub.db.read(lambda c: hub._get(c, "workers", attempt["worker_id"]))
        assert json.loads(worker["profile_json"])["settings"] == {"model": "gpt-6.1-sol", "effort": "low"}
        # A later choice leaves the bound conversation as it began.
        await command(hub, "runtime.set_role_model", {"role": "worker", "backend": "codex", "model": "gpt-7", "effort": "high"})
        settings = await hub.session_settings(attempt["worker_id"])
        assert (settings["model"], settings["effort"], settings["new"]) == ("gpt-6.1-sol", "low", False)
    finally:
        await hub.close()
