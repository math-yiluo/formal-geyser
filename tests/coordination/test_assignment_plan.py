"""The assignment plan gives each new agent its backend and rotation mode by role and number;
concurrent sessions are limited per backend, and worker sessions start one at a time."""
from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

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
    RotationConfig,
)
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime import claude as claude_runtime
from tests.coordination.lifecycle_helpers import resolve_recruitment
from tests.coordination.publication_helpers import new_hub
from tests.coordination.test_collaborative_core import claim, machine, publish, stage

PLAN = AssignmentConfig(worker_backends=["codex", "claude"],
                        worker_rotation=["conservative", "conservative", "aggressive", "aggressive"],
                        reviewer_backend="codex", integrator_backend="codex")


async def command(hub, name, payload=None):
    return await hub.execute(Command(name=name, payload=payload or {}, key=uuid4().hex), Actor(role=Role.OPERATOR))


@pytest.fixture(autouse=True)
def claude_available(monkeypatch):
    # Selecting Claude checks this machine; the tests exercise the hub, not the platform.
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: None)


async def planned(tmp_path, **settings):
    initialize(tmp_path, ProjectConfig(**settings))
    return Hub(tmp_path)


async def test_the_plan_sets_each_new_agents_backend_and_rotation_by_role_and_number(tmp_path):
    hub = await planned(tmp_path, assignment=PLAN)
    try:
        workers = [await command(hub, "worker.register") for _ in range(5)]
        assert [(w["worker_number"], w["backend"], w["rotation_mode"]) for w in workers] == [
            (0, "codex", "conservative"), (1, "claude", "conservative"), (2, "codex", "aggressive"),
            (3, "claude", "aggressive"), (4, "codex", "conservative")]
        reviewer = await command(hub, "worker.register", {"kind": "review"})
        integrator = await command(hub, "worker.register", {"kind": "integrator"})
        assert (reviewer["backend"], reviewer["rotation_mode"]) == ("codex", None)
        assert (integrator["backend"], integrator["rotation_mode"]) == ("codex", None)
        status = await hub.backend_status()
        assert status["next_worker"] == {"number": 5, "backend": "claude"}
        assert status["assignment"]["worker_backends"] == ["codex", "claude"]
        # The operator can still give one worker another mode.
        changed = await command(hub, "worker.set_rotation_mode", {"worker_id": workers[0]["id"], "mode": "aggressive"})
        assert changed["effective_mode"] == "aggressive"
    finally:
        await hub.close()


async def test_reviewers_can_rotate_conservatively_while_workers_rotate_aggressively(tmp_path):
    hub = await planned(tmp_path, rotation=RotationConfig(mode="aggressive"),
                        assignment=AssignmentConfig(reviewer_backend="codex", reviewer_rotation="conservative"))
    try:
        worker = await command(hub, "worker.register")
        reviewer = await command(hub, "worker.register", {"kind": "review"})
        assert (worker["rotation_mode"], reviewer["rotation_mode"]) == (None, "conservative")
        for agent in (worker, reviewer):
            await hub.db.call(lambda c, agent=agent: c.execute(
                "UPDATE workers SET thread_id=? WHERE id=?", ("thread-" + agent["id"], agent["id"])), write=True)
        # A review past the context limit is not asked for a handoff it would never use.
        assert await hub.request_rotation(reviewer["id"], "thread-" + reviewer["id"], 150_000, None) is None
        assert await hub.request_rotation(worker["id"], "thread-" + worker["id"], 150_000, None) is not None
    finally:
        await hub.close()


def test_reviewers_and_the_integrator_can_have_their_own_effort():
    codex = CodexConfig(effort="xhigh", review_effort="max", integrator_effort="max")
    assert [codex.effort_for(kind) for kind in ("ordinary", "review", "integrator")] == ["xhigh", "max", "max"]
    # Unset is unchosen: a reviewer no longer takes the workers' effort.
    assert ClaudeConfig(effort="xhigh").effort_for("review") is None


async def test_an_activation_opens_its_harness_for_the_agents_role(tmp_path, monkeypatch):
    hub = await new_hub(tmp_path)
    try:
        run = await command(hub, "run.start")
        author = await claim(hub, await publish(hub, "Bound"), run)
        await machine(hub, await stage(hub, author))
        await resolve_recruitment(hub, run["id"])
        review = await hub.next_assignment(run["id"])
        runner = WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([])]))
        roles, original = [], runner._new_harness

        async def new_harness(backend="codex", **options):
            roles.append((backend, options.get("kind")))
            return await original(backend, **options)
        monkeypatch.setattr(runner, "_new_harness", new_harness)
        await runner.run_once(run["id"], assignment=review)
        assert roles[0] == ("codex", "review")
    finally:
        await hub.close()


async def test_the_operators_choice_applies_where_the_plan_leaves_the_backend_open(tmp_path):
    hub = await planned(tmp_path)
    try:
        await command(hub, "runtime.select_backend", {"backend": "claude"})
        assert (await command(hub, "worker.register"))["backend"] == "claude"
        # The operator's choice is for workers: reviewers and the Integrator stay on Codex.
        assert (await command(hub, "worker.register", {"kind": "review"}))["backend"] == "codex"
        await command(hub, "runtime.select_backend", {"backend": "codex"})
        assert (await command(hub, "worker.register"))["backend"] == "codex"
        assert (await command(hub, "worker.register", {"kind": "integrator"}))["backend"] == "codex"
    finally:
        await hub.close()


async def test_reviewers_and_the_integrator_run_on_codex_when_workers_run_on_claude(tmp_path):
    # The Mac pilot, 2026-11-08: workers on Claude, no backend named for the other roles, and its
    # first candidate's reviewer was made on Claude, where no reviewer model is chosen.
    hub = await planned(tmp_path, new_worker_backend="claude")
    try:
        assert (await command(hub, "worker.register"))["backend"] == "claude"
        for kind in ("review", "integrator", "dispatcher", "waker"):
            assert (await command(hub, "worker.register", {"kind": kind}))["backend"] == "codex", kind
        # Nor can a caller name Claude for them.
        with pytest.raises(HubError) as refused:
            await command(hub, "worker.register", {"kind": "review", "backend": "claude"})
        assert refused.value.code == "INVALID_INPUT" and "run on Codex" in refused.value.message
    finally:
        await hub.close()


@pytest.mark.parametrize("role", ["reviewer_backend", "integrator_backend"])
def test_a_plan_cannot_put_reviewers_or_the_integrator_on_claude(role):
    with pytest.raises(ValidationError, match="run on Codex"):
        AssignmentConfig(**{role: "claude"})
    AssignmentConfig(**{role: "codex"})  # Older project files say so; that still loads.


async def test_a_full_backend_holds_new_worker_sessions_but_never_a_review(tmp_path):
    hub = await new_hub(tmp_path, config=ProjectConfig(concurrency=ConcurrencyConfig(codex=1)))
    try:
        run = await command(hub, "run.start")
        author = await claim(hub, await publish(hub, "Bound"), run)
        candidate = await stage(hub, author)
        await machine(hub, candidate)
        await publish(hub, "Lift")
        # The author's session still runs: the only Codex session the limit allows.
        assert (await hub.backend_status())["sessions"]["codex"] == {"live": 1, "limit": 1, "source": "config"}
        await resolve_recruitment(hub, run["id"])
        review = await hub.next_assignment(run["id"])
        assert (await hub.worker(review["worker_id"]))["kind"] == "review"
        # The new review counts, and no new worker starts on the full account.
        assert await hub.next_assignment(run["id"]) is None
        status = await hub.backend_status()
        assert status["sessions"]["codex"] == {"live": 2, "limit": 1, "source": "config"} and status["workers"]["codex"] == 2
    finally:
        await hub.close()


async def test_a_limit_of_zero_starts_no_worker_but_never_holds_a_review(tmp_path):
    hub = await new_hub(tmp_path)
    try:
        run = await command(hub, "run.start")
        author = await claim(hub, await publish(hub, "Bound"), run)
        await machine(hub, await stage(hub, author))
        await publish(hub, "Lift")
        assert (await hub.backend_status())["sessions"]["codex"] == {"live": 1, "limit": None, "source": "config"}
        await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": 0})
        await resolve_recruitment(hub, run["id"])
        review = await hub.next_assignment(run["id"])
        assert (await hub.worker(review["worker_id"]))["kind"] == "review"
        # Lift waits: no worker session starts, and the author's running session is not stopped.
        assert await hub.next_assignment(run["id"]) is None
        assert (await hub.backend_status())["sessions"]["codex"] == {"live": 2, "limit": 0, "source": "operator"}
    finally:
        await hub.close()


async def test_a_new_worker_skips_the_numbers_of_a_full_account(tmp_path):
    hub = await planned(tmp_path, assignment=PLAN, concurrency=ConcurrencyConfig(codex=1))
    try:
        for title in ("Bound", "Lift", "Descent"):
            await publish(hub, title)
        run = await command(hub, "run.start")
        first, second = await hub.next_assignment(run["id"]), await hub.next_assignment(run["id"])
        # Worker 2 would be on Codex, whose only session Worker 0 holds: the next new worker is 3.
        assert (await hub.backend_status())["next_worker"] == {"number": 3, "backend": "claude"}
        third = await hub.next_assignment(run["id"])
        workers = [await hub.worker(item["worker_id"]) for item in (first, second, third)]
        assert [(w["worker_number"], w["backend"]) for w in workers] == [(0, "codex"), (1, "claude"), (3, "claude")]
        # Number 2 stays unused, and while Codex stays full the next new worker is 5.
        assert (await hub.backend_status())["next_worker"] == {"number": 5, "backend": "claude"}
    finally:
        await hub.close()


async def test_saved_work_on_a_full_account_is_not_claimed_until_a_session_frees(tmp_path, monkeypatch):
    hub = await new_hub(tmp_path, config=ProjectConfig(concurrency=ConcurrencyConfig(codex=1)))
    try:
        run = await command(hub, "run.start")
        await publish(hub, "Bound")
        first = await hub.next_assignment(run["id"])
        await hub.ordinary_turn_completed(first["worker_id"], first["id"])
        await resolve_recruitment(hub, run["id"])
        # Another worker holds the account's only session; the first worker's saved work waits.
        other = await claim(hub, await publish(hub, "Lift"), run)
        claimed, original = [], hub._claim
        monkeypatch.setattr(hub, "_claim", lambda c, *args, **kwargs: claimed.append(args[2]) or original(c, *args, **kwargs))
        assert await hub.next_assignment(run["id"]) is None
        assert claimed == []  # Skipped, not claimed and rolled back on every pass.
        await hub.ordinary_turn_completed(other["worker_id"], other["id"])
        resumed = await hub.next_assignment(run["id"])
        assert resumed["worker_id"] == first["worker_id"] and claimed == [first["worker_id"]]
    finally:
        await hub.close()


async def test_saved_work_is_not_claimed_while_launch_spacing_holds_the_next_session(tmp_path, monkeypatch):
    initialize(tmp_path, ProjectConfig(concurrency=ConcurrencyConfig(codex=3, launch_seconds=30)))
    hub = Hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    try:
        await publish(hub, "Bound")
        run = await command(hub, "run.start")
        first = await hub.next_assignment(run["id"])
        await hub.ordinary_turn_completed(first["worker_id"], first["id"])
        await resolve_recruitment(hub, run["id"])
        claimed, original = [], hub._claim
        monkeypatch.setattr(hub, "_claim", lambda c, *args, **kwargs: claimed.append(args[2]) or original(c, *args, **kwargs))
        now[0] += 10_000  # The last worker session started 10 s ago.
        assert await hub.next_assignment(run["id"]) is None
        assert claimed == []  # Skipped, not claimed and rolled back.
        now[0] += 21_000
        resumed = await hub.next_assignment(run["id"])
        assert resumed["worker_id"] == first["worker_id"] and claimed == [first["worker_id"]]
    finally:
        await hub.close()


async def test_worker_sessions_start_apart_until_the_backend_first_fills(tmp_path):
    initialize(tmp_path, ProjectConfig(concurrency=ConcurrencyConfig(codex=2, launch_seconds=30)))
    hub = Hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    try:
        for title in ("Bound", "Lift", "Descent"):
            await publish(hub, title)
        run = await command(hub, "run.start")
        first = await hub.next_assignment(run["id"])
        assert first is not None
        now[0] += 10_000
        assert await hub.next_assignment(run["id"]) is None  # 10 s after the first session started.
        now[0] += 21_000
        second = await hub.next_assignment(run["id"])
        assert second is not None and second["worker_id"] != first["worker_id"]
        # Both sessions in use: the account has filled once, and the third task waits for a place.
        assert hub._saturated == {"codex"}
        now[0] += 60_000
        assert await hub.next_assignment(run["id"]) is None
    finally:
        await hub.close()


async def test_launch_spacing_counts_only_the_same_accounts_sessions(tmp_path):
    initialize(tmp_path, ProjectConfig(assignment=AssignmentConfig(worker_backends=["codex", "claude"]),
                                       concurrency=ConcurrencyConfig(codex=3, claude=3, launch_seconds=30)))
    hub = Hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    try:
        for title in ("Bound", "Lift", "Descent"):
            await publish(hub, title)
        run = await command(hub, "run.start")
        first = await hub.next_assignment(run["id"])
        now[0] += 5_000
        second = await hub.next_assignment(run["id"])  # Claude's first session: the Codex start does not hold it.
        now[0] += 5_000
        assert await hub.next_assignment(run["id"]) is None  # Codex's own previous session started 10 s ago.
        now[0] += 21_000
        third = await hub.next_assignment(run["id"])
        workers = [await hub.worker(item["worker_id"]) for item in (first, second, third)]
        assert [w["backend"] for w in workers] == ["codex", "claude", "codex"]
    finally:
        await hub.close()


async def test_launch_spacing_ends_with_the_ramp_and_applies_again_after_a_resume(tmp_path):
    initialize(tmp_path, ProjectConfig(concurrency=ConcurrencyConfig(codex=9, launch_seconds=30, launch_ramp_minutes=1)))
    hub = Hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    try:
        for title in ("Bound", "Lift", "Descent", "Ascent", "Limit"):
            await publish(hub, title)
        run = await command(hub, "run.start")
        assert await hub.next_assignment(run["id"]) is not None
        now[0] += 61_000  # A minute after the run started, the ramp is over.
        assert await hub.next_assignment(run["id"]) is not None
        now[0] += 1_000
        assert await hub.next_assignment(run["id"]) is not None  # No spacing after the ramp.
        await command(hub, "run.resume", {"run_id": run["id"]})
        now[0] += 1_000
        assert await hub.next_assignment(run["id"]) is None  # Resuming ramps up again, one start at a time.
        now[0] += 30_000
        assert await hub.next_assignment(run["id"]) is not None
    finally:
        await hub.close()
