"""Pausing after turn, or resuming, a group: Codex's or Claude's workers and reviewers, or
Codex's workers or reviewers alone, without model execution."""
from __future__ import annotations

import sqlite3

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import control_applies, group_control, upgrade_group_controls
from formal_geyser.coordination.models import ProjectConfig
from tests.coordination.publication_helpers import new_hub
from tests.coordination.test_collaborative_core import claim, machine, publish, stage, start
from tests.coordination.test_pause_after_turn import dispatch, finish, turn_under_way
from tests.coordination.test_worker_controls import active, command, finish_first_turn

# New workers alternate between the accounts by number: Worker 0 on Codex, Worker 1 on Claude.
PLANNED = ProjectConfig(assignment={"worker_backends": ["codex", "claude"]})


@pytest.fixture
async def case(tmp_path):
    hub = await new_hub(tmp_path, config=PLANNED, clock=lambda: 10_000)
    await command(hub, "task.publish", {"contract": {"title": "Task 0", "objective": "Satisfy the original goal."}})
    run = await command(hub, "run.start")
    yield hub, run
    await hub.close()


async def ready(hub, run, title, backend):
    """A worker on this account with a task of its own, ready to continue it."""
    task = await command(hub, "task.publish", {"contract": {"title": title, "objective": "Produce the stated result."}})
    worker = await command(hub, "worker.register", {"backend": backend})
    attempt = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1, "run_id": run["id"],
                                                "worker_id": worker["id"]})
    await finish_first_turn(hub, attempt)
    return attempt


async def codex_and_claude(hub, run):
    """Task 0's worker on Codex and a second on Claude, each ready to continue its own task."""
    codex = await hub.next_assignment(run["id"])
    assert (await hub.worker(codex["worker_id"]))["backend"] == "codex"
    await finish_first_turn(hub, codex)
    return codex, await ready(hub, run, "Claude's task", "claude")


async def test_pausing_codex_workers_after_turn_leaves_claude_running(case):
    hub, run = case
    codex, claude = await codex_and_claude(hub, run)
    paused = await command(hub, "group.pause_after_turn", {"group": "codex.workers"})
    assert paused["groups"]["codex.workers"] == {"control": "paused", "state": "paused", "pause_mode": "after_turn"}
    assert paused["groups"]["codex"]["state"] == "mixed"  # Its reviewers still run.
    assert paused["groups"]["claude"] == {"control": None, "state": "running", "pause_mode": None}
    control = await hub.run(run["id"], worker_id=codex["worker_id"])
    assert (control["state"], control["run_state"], control["pause_mode"]) == ("paused", "running", "after_turn")
    assert (await hub.run(run["id"], worker_id=claude["worker_id"]))["state"] == "running"
    assert (await hub.run(run["id"]))["paused_workers"] == [codex["worker_id"]]
    listed = {item["id"]: item for item in await hub.list_workers()}
    assert (listed[codex["worker_id"]]["group_control"], listed[codex["worker_id"]]["run_control"]) == ("paused", None)
    assert listed[claude["worker_id"]]["group_control"] is None
    assert (await hub.worker(codex["worker_id"]))["group_control"] == "paused"
    assert (await hub.next_assignment(run["id"]))["worker_id"] == claude["worker_id"]
    assert await hub.next_assignment(run["id"]) is None  # Codex's worker waits.
    resumed = await command(hub, "group.resume", {"group": "codex.workers"})
    assert resumed["groups"]["codex.workers"] == {"control": None, "state": "running", "pause_mode": None}
    assert (await hub.next_assignment(run["id"]))["worker_id"] == codex["worker_id"]


async def test_a_paused_group_lets_its_turns_under_way_finish(case):
    hub, run = case
    codex = await hub.next_assignment(run["id"])
    intent = await turn_under_way(hub, codex)
    await command(hub, "group.pause_after_turn", {"group": "codex"})
    view = (await hub.activation_controls([(run["id"], codex["worker_id"])]))[run["id"], codex["worker_id"]]
    assert view["run"] == {"state": "paused", "pause_mode": "after_turn", "stop_reason": "operator_pause"}
    await active(hub, codex)  # Its turn under way still acts.
    with pytest.raises(HubError):
        await dispatch(hub, codex)  # No input is sent for another turn.
    await finish(hub, intent)
    with pytest.raises(HubError):
        await active(hub, codex)


async def test_new_workers_start_only_on_an_account_whose_workers_run(case):
    hub, run = case
    await command(hub, "group.pause_after_turn", {"group": "codex"})
    first = await hub.next_assignment(run["id"])  # Task 0 goes to a new worker on Claude.
    worker = await hub.worker(first["worker_id"])
    assert (worker["backend"], worker["worker_number"]) == ("claude", 1)  # Codex's number 0 is skipped.
    await command(hub, "group.pause_after_turn", {"group": "claude"})
    await command(hub, "task.publish", {"contract": {"title": "Later task", "objective": "Produce the stated result."}})
    assert await hub.next_assignment(run["id"]) is None  # No account has its workers running.
    await command(hub, "group.resume", {"group": "codex"})
    fresh = await hub.next_assignment(run["id"])
    assert (await hub.worker(fresh["worker_id"]))["backend"] == "codex"


async def test_a_group_resumed_under_pause_all_runs_in_full_until_resume_all(case):
    hub, run = case
    codex, claude = await codex_and_claude(hub, run)
    await command(hub, "run.pause_after_turn", {"run_id": run["id"]})
    assert await hub.next_assignment(run["id"]) is None
    resumed = await command(hub, "group.resume", {"group": "claude"})
    assert resumed["run_state"] == "paused"
    assert resumed["groups"]["claude"] == {"control": "resumed", "state": "running", "pause_mode": None}
    assert resumed["groups"]["codex"] == {"control": None, "state": "paused", "pause_mode": "after_turn"}
    control = await hub.run(run["id"])
    assert (control["state"], control["resumed_groups"]) == ("paused", ["claude"])
    assert control["resumed_workers"] == [claude["worker_id"]]
    assert (await hub.next_assignment(run["id"]))["worker_id"] == claude["worker_id"]
    # New work goes to a new worker on Claude, not to Codex.
    await command(hub, "task.publish", {"contract": {"title": "Later task", "objective": "Produce the stated result."}})
    fresh = await hub.next_assignment(run["id"])
    assert (await hub.worker(fresh["worker_id"]))["backend"] == "claude"
    assert await hub.next_assignment(run["id"]) is None
    # Resume all, like Pause all, ends every group's control.
    await command(hub, "run.resume", {"run_id": run["id"]})
    after = await hub.run(run["id"])
    assert after["resumed_groups"] == [] and after["groups"]["claude"]["control"] is None
    assert (await hub.next_assignment(run["id"]))["worker_id"] == codex["worker_id"]


async def test_a_person_runs_while_its_group_is_paused_and_a_new_group_control_ends_that(case):
    hub, run = case
    first = await hub.next_assignment(run["id"])
    await finish_first_turn(hub, first)
    second = await ready(hub, run, "Second task", "codex")
    await command(hub, "group.pause_after_turn", {"group": "codex.workers"})
    one = await command(hub, "worker.resume", {"worker_id": second["worker_id"], "run_id": run["id"]})
    assert (one["control"], one["state"]) == ("resumed", "running")
    assert (await hub.next_assignment(run["id"]))["worker_id"] == second["worker_id"]
    assert await hub.next_assignment(run["id"]) is None
    # A person's own pause that interrupts differs from the group's after turns, and stays when
    # the group pauses again; a person's own resume gives way to it.
    now = await command(hub, "worker.pause", {"worker_id": first["worker_id"], "run_id": run["id"]})
    assert (now["control"], now["pause_mode"]) == ("paused", "now")
    await command(hub, "group.pause_after_turn", {"group": "codex"})
    assert (await hub.worker(first["worker_id"]))["run_control"] == "paused"
    assert (await hub.worker(second["worker_id"]))["run_control"] is None
    groups = (await hub.run(run["id"]))["groups"]
    assert groups["codex.workers"]["control"] is None and groups["codex"]["control"] == "paused"


async def test_resuming_codex_reviewers_alone_while_codex_is_paused(case):
    hub, run = case
    await command(hub, "group.pause_after_turn", {"group": "codex"})
    narrow = await command(hub, "group.resume", {"group": "codex.reviewers"})
    assert narrow["groups"]["codex.reviewers"] == {"control": "resumed", "state": "running", "pause_mode": None}
    assert narrow["groups"]["codex.workers"]["state"] == "paused" and narrow["groups"]["codex"]["state"] == "mixed"
    # Resuming all of Codex ends its narrower groups' controls.
    wide = await command(hub, "group.resume", {"group": "codex"})
    assert {name: view["control"] for name, view in wide["groups"].items()} == dict.fromkeys(wide["groups"])


async def test_paused_codex_reviewers_start_no_new_review(tmp_path):
    hub = await new_hub(tmp_path, clock=lambda: 10_000)
    try:
        run = await start(hub)
        author = await claim(hub, await publish(hub, "Reviewed contribution"), run)
        candidate = await stage(hub, author)
        await machine(hub, candidate)
        await command(hub, "group.pause_after_turn", {"group": "codex.reviewers"})
        held = await hub.next_assignment(run["id"])
        assert held is None or held["worker_kind"] != "review"
        await command(hub, "group.resume", {"group": "codex.reviewers"})
        review = await hub.next_assignment(run["id"])
        assert review["worker_kind"] == "review" and review["task_id"] == candidate["review_task_id"]
    finally:
        await hub.close()


async def test_group_controls_are_operator_commands_on_a_live_run(case):
    hub, run = case
    with pytest.raises(HubError) as unknown:
        await command(hub, "group.pause_after_turn", {"group": "claude.workers"})
    assert unknown.value.code == "INVALID_INPUT"
    await command(hub, "run.stop", {"run_id": run["id"]})
    with pytest.raises(HubError) as ended:
        await command(hub, "group.resume", {"group": "codex", "run_id": run["id"]})
    assert ended.value.code == "INVALID_RUN_TRANSITION"


def _controlled_per_person(hub, c, run_id, control):
    """The controlled people as worked out per person before: the reference for the faster read."""
    group = group_control("w", "r.id")
    return {row["id"] for row in c.execute(
        "SELECT w.id FROM workers w JOIN runs r ON r.id=? LEFT JOIN worker_controls wc ON wc.worker_id=w.id "
        "AND wc.run_id=r.id WHERE (wc.worker_id IS NOT NULL OR (w.work_intention<>'retired' AND w.state<>'retired')) "
        f"AND CASE WHEN wc.worker_id IS NOT NULL AND {control_applies('wc', group)} THEN wc.control "
        f"ELSE {group} END=?", (run_id, control))}


async def test_group_controls_are_read_per_group_with_the_same_people_as_per_person(case):
    hub, run = case
    people, made_on_claude = [], []
    for backend in ("codex", "claude"):
        for kind in ("ordinary", "review"):
            for _ in range(3):
                # Reviewers are made on Codex now; those made on Claude before 2026-11-08 remain.
                made = await command(hub, "worker.register",
                                     {"backend": "codex" if kind == "review" else backend, "kind": kind})
                people.append(made["id"])
                if kind == "review" and backend == "claude":
                    made_on_claude.append(made["id"])
    await hub.db.call(lambda c: c.executemany("UPDATE workers SET backend='claude' WHERE id=?",
                                              [(worker_id,) for worker_id in made_on_claude]), write=True)
    retired, own = people[::5], people[1::3]

    def arrange(c, groups, controls, run_state, after_turn):
        c.execute("DELETE FROM group_controls")
        c.execute("DELETE FROM worker_controls")
        c.execute("UPDATE workers SET state='retired',work_intention='retired' WHERE id IN (%s)"
                  % ",".join("?" * len(retired)), retired)
        c.execute("UPDATE runs SET state=?,pause_after_turn=? WHERE id=?", (run_state, after_turn, run["id"]))
        c.executemany("INSERT INTO group_controls(scope,run_id,control,created_at) VALUES(?,?,?,0)",
                      [(scope, run["id"], control) for scope, control in groups])
        c.executemany("INSERT INTO worker_controls(worker_id,run_id,control,created_at,after_turn) VALUES(?,?,?,0,?)",
                      [(worker_id, run["id"], control, index % 2)
                       for index, (worker_id, control) in enumerate(zip(own, controls))])

    def compare(c):
        return [(hub._controlled_workers(c, run["id"], control), _controlled_per_person(hub, c, run["id"], control))
                for control in ("paused", "resumed")]
    layouts = [[("codex", "paused")], [("codex", "resumed"), ("codex.reviewers", "paused")],
               [("codex.workers", "paused"), ("claude", "resumed")], [("claude", "paused"), ("codex", "paused")]]
    seen = set()
    for groups in layouts:
        for controls in (["paused"] * len(own), ["resumed"] * len(own), ["paused", "resumed"] * len(own)):
            for run_state, after_turn in (("running", 0), ("paused", 0), ("paused", 1)):
                await hub.db.call(lambda c: arrange(c, groups, controls, run_state, after_turn), write=True)
                for found, expected in await hub.db.call(compare):
                    assert found == expected
                    seen.update(found)
    # Every unretired person was controlled in some arrangement, and no retired one but by its own control.
    assert set(people) - set(retired) <= seen and not (set(retired) - set(own)) & seen


def test_an_older_group_controls_table_learns_the_integrators_group():
    # Before 2026-11-08 the scope check named only the Codex and Claude groups.
    c = sqlite3.connect(":memory:", isolation_level=None)
    c.execute("CREATE TABLE runs(id TEXT PRIMARY KEY)")
    c.execute("INSERT INTO runs VALUES('r')")
    c.execute("CREATE TABLE group_controls(scope TEXT PRIMARY KEY CHECK(scope IN ('codex','codex.workers',"
              "'codex.reviewers','claude')),run_id TEXT NOT NULL REFERENCES runs(id),"
              "control TEXT NOT NULL CHECK(control IN ('paused','resumed')),created_at INTEGER NOT NULL)")
    c.execute("INSERT INTO group_controls VALUES('codex.reviewers','r','paused',1)")
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO group_controls VALUES('integrator','r','paused',2)")
    upgrade_group_controls(c)
    c.execute("INSERT INTO group_controls VALUES('integrator','r','paused',2)")
    assert c.execute("SELECT scope,control FROM group_controls ORDER BY scope").fetchall() == [
        ("codex.reviewers", "paused"), ("integrator", "paused")]
    upgrade_group_controls(c)  # Once is enough; again it changes nothing.
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO group_controls VALUES('everyone','r','paused',3)")
