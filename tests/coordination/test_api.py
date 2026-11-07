import asyncio
import json
import time
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
import pytest
from fastapi.testclient import TestClient

from formal_geyser.coordination.api import ProjectOwner, create_app
from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import ProjectConfig
from formal_geyser.coordination.storage import digest
from formal_geyser.coordination.transcripts import TranscriptStore


@pytest.fixture
def project(tmp_path):
    initialize(tmp_path, ProjectConfig())
    return tmp_path


async def test_timer_retries_failed_coordination_without_disarming_or_pausing(project):
    attempts = []
    resumed = asyncio.Event()

    class Runner:
        def __init__(self, hub):
            self.hub = hub

        async def run_until_idle(self, run_id, **_):
            attempts.append(run_id)
            if len(attempts) == 1:
                raise OSError("Synthetic temporary coordinator storage error")
            resumed.set()
            return {"state": "idle"}

    app = create_app(project, runner_factory=Runner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                base_url="http://testserver") as client:
            run = await api_command(client, "run.start")
            await client.post(f"/v1/runs/{run['id']}/drive")
            await finish_driver(app, run["id"])
            assert run["id"] in app.state.driver.armed
            assert app.state.driver.results[run["id"]]["state"] == "retrying"
            assert (await app.state.hub.run(run["id"]))["state"] == "running"
            app.state.driver.retry_after[run["id"]] = 0
            await asyncio.wait_for(resumed.wait(), 3)
            assert len(attempts) >= 2


def command(name, payload=None):
    return {"name": name, "key": str(uuid4()), "payload": payload or {}}


def test_api_refuses_requests_without_the_operator_token(project, monkeypatch):
    with TestClient(create_app(project)) as client:
        monkeypatch.setattr("formal_geyser.coordination.api.operator_request", lambda request, token: False)
        assert client.get("/v1/status").status_code == 401
        assert client.post("/v1/commands", json=command("run.start")).status_code == 401
        monkeypatch.setattr("formal_geyser.coordination.api.operator_request", lambda request, token: True)
        result = client.get("/v1/status")
        assert result.status_code == 200
        assert result.json()["domain"] == "general"
        assert result.json()["running_drivers"] == []


def test_api_publishes_and_reads_contract_and_rejects_role_injection(project):
    with TestClient(create_app(project)) as client:
        published = client.post("/v1/commands", json=command(
            "task.publish",
            {"contract": {"title": "Review record", "objective": "Review the supplied record"}},
        ))
        assert published.status_code == 200, published.text
        task_id = published.json()["id"]
        assert client.get(f"/v1/tasks/{task_id}").json()["id"] == task_id
        assert len(client.get("/v1/tasks").json()) == 1
        forged = {**command("run.start"), "role": "validator"}
        assert client.post("/v1/commands", json=forged).status_code == 422
        forged = {**command("run.start"), "actor": {"role": "runner"}}
        assert client.post("/v1/commands", json=forged).status_code == 422


def test_api_rejects_malformed_json_and_declared_oversize_body(project):
    with TestClient(create_app(project, max_command_bytes=512)) as client:
        invalid = client.post(
            "/v1/commands", headers={"Content-Type": "application/json"}, content="{"
        )
        assert invalid.status_code == 422
        oversized = client.post(
            "/v1/commands", content=b"x" * 513
        )
        assert oversized.status_code == 413
        nonfinite = client.post(
            "/v1/commands",
            headers={"Content-Type": "application/json"},
            content='{"name":"run.start","key":"finite","payload":{"value":NaN}}',
        )
        assert nonfinite.status_code == 422


@pytest.mark.asyncio
async def test_api_caps_streamed_body_without_content_length(project):
    app = create_app(project, max_command_bytes=128)

    async def chunks():
        yield b"x" * 70
        yield b"x" * 70

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            result = await client.post("/v1/commands", content=chunks())
    assert result.status_code == 413


def test_api_run_start_uses_default_controls_and_requires_explicit_drive(project):
    created = []
    cancelled = []

    class Runner:
        def __init__(self, hub):
            created.append(hub)

        async def run_until_idle(self, run_id, **_):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(run_id)

    with TestClient(create_app(project, runner_factory=Runner)) as client:
        result = client.post("/v1/commands", json=command("run.start"))
        assert result.status_code == 200, result.text
        run_id = result.json()["id"]
        assert created == []
        first = client.post(f"/v1/runs/{run_id}/drive")
        second = client.post(f"/v1/runs/{run_id}/drive")
        assert first.status_code == 202
        assert second.json()["already_driving"] is True
        deadline = time.monotonic() + 10
        while not created and time.monotonic() < deadline:
            time.sleep(0.02)  # The accepted drive constructs its runner in the background.
        assert len(created) == 1
        assert client.get(f"/v1/runs/{run_id}").json()["driving"] is True
    assert cancelled == [run_id]


def test_project_owner_rejects_second_owner_and_releases_lock(project):
    with ProjectOwner(project):
        with pytest.raises(HubError, match="Another process"):
            with ProjectOwner(project):
                pass
    with ProjectOwner(project):
        pass


def test_api_always_constructs_operator_actor_even_with_nested_role_fields(project):
    class CaptureHub:
        async def execute(self, supplied, actor):
            return actor.model_dump(mode="json")

    with TestClient(create_app(project, hub=CaptureHub())) as client:
        response = client.post(
            "/v1/commands",
            json=command("worker.register", {
                "role": "validator", "actor": {"role": "runner"}, "worker_id": "forged",
            }),
        )
    assert response.status_code == 200
    assert response.json() == {
        "role": "operator", "worker_id": None, "attempt_id": None, "fencing_token": None,
    }


def test_api_reads_exact_historical_task_revision(project):
    with TestClient(create_app(project)) as client:
        created = client.post("/v1/commands", json=command(
            "task.publish", {"contract": {
                "title": "Original record", "objective": "Review the original software record",
            }}
        ))
        assert created.status_code == 200, created.text
        task_id = created.json()["id"]
        revised = client.post("/v1/commands", json=command(
            "task.revise", {"task_id": task_id, "revision": 1, "reason": "Clarify the software record", "contract": {
                "title": "Revised record", "objective": "Review the revised software record",
            }}
        ))
        assert revised.status_code == 200, revised.text
        current = client.get(f"/v1/tasks/{task_id}")
        historical = client.get(f"/v1/tasks/{task_id}?revision=1")
        assert current.json()["revision"] == 2
        assert historical.status_code == 200, historical.text
        assert historical.json()["revision"] == 1
        assert historical.json()["contract"]["title"] == "Original record"
        assert client.get(f"/v1/tasks/{task_id}?revision=0").status_code == 422
        assert client.get(f"/v1/tasks/{task_id}?revision=99").status_code == 404


async def api_command(client, name, payload=None):
    response = await client.post("/v1/commands", json=command(name, payload))
    assert response.status_code == 200, response.text
    return response.json()


async def finish_driver(app, run_id):
    await asyncio.wait_for(asyncio.shield(app.state.driver.passes[run_id]), timeout=5)


async def wait_for_initial_driver_pass(app, run_id, finishing):
    """Allow native-file startup before measuring the continuation wake race."""
    driver = app.state.driver.passes[run_id]
    ready = asyncio.create_task(finishing.wait())
    try:
        completed, _ = await asyncio.wait({driver, ready}, timeout=30,
                                          return_when=asyncio.FIRST_COMPLETED)
        if driver in completed:
            await driver
            assert finishing.is_set(), (
                "Initial driver ended before reaching the fixture boundary: "
                + repr(app.state.driver.results.get(run_id)))
        assert finishing.is_set(), "Initial driver preparation did not finish within 30 seconds"
    except BaseException:
        if not driver.done():
            driver.cancel()
        await asyncio.gather(driver, return_exceptions=True)
        raise
    finally:
        if not ready.done():
            ready.cancel()
        await asyncio.gather(ready, return_exceptions=True)


@pytest.mark.asyncio
async def test_committed_operator_commands_do_not_start_an_unarmed_run(project):
    starts = []

    class Runner:
        def __init__(self, hub):
            starts.append(hub)

        async def run_until_idle(self, run_id, **_):
            return {"state": "idle"}

    app = create_app(project, runner_factory=Runner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            run = await api_command(client, "run.start")
            await api_command(client, "task.publish", {"contract": {
                "title": "Unarmed record", "objective": "Review a software record",
            }})
            artifact = await api_command(client, "artifact.register", {
                "kind": "software.source", "data": {"ready": True},
            })
            await api_command(client, "artifact.accept", {
                "artifact_id": artifact["id"], "policy": "general.manual@1",
                "verdict": "accepted",
            })
            status = (await client.get(f"/v1/runs/{run['id']}")).json()
            assert not status["armed"] and not status["driving"]
            assert starts == []
            assert app.state.driver.passes == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("during_finish", [False, True], ids=["idle", "finishing"])
async def test_operator_receipt_wakes_previously_driven_continuation(project, during_finish):
    from formal_geyser.coordination.runtime import (
        ScriptedAction,
        ScriptedHarness,
        ScriptedTurn,
        WorkerRunner,
    )

    request_id = None
    activations = []
    passes = []
    finishing = asyncio.Event()
    release_finish = asyncio.Event()

    def harness_factory():
        if not activations:
            actions = [ScriptedAction("taskhub_call", {"operation": "request", "arguments": {
                "kind": "question", "request_key": "approved-source", "title": "Approve the source",
                "text": "Is the software source approved?",
            }}), ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {
                "reason": "Wait for the operator's approval.",
                "predicate": {"external_response_id": request_id},
                "checkpoint": {
                    "summary": "Prepared the software record",
                    "next_action": "Read the operator-approved source and finish",
                },
            }})]
        else:
            actions = [ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {
                "predicate": {"external_response_id": "unanswered-request"},
                "reason": "The operator receipt resumed the original conversation.",
                "checkpoint": {"summary": "Read the operator-approved source.",
                               "next_action": "Await another explicit request."},
            }})]
        harness = ScriptedHarness([
            ScriptedTurn(actions, usage={"totalTokens": 10 * (len(activations) + 1)})
        ])
        activations.append(harness)
        return harness

    class Runner(WorkerRunner):
        async def run_until_idle(self, run_id, **_):
            passes.append(run_id)
            result = await super().run_until_idle(run_id, **_)
            if len(passes) == 1:
                finishing.set()
                if during_finish:
                    await release_finish.wait()
            return result

    app = create_app(
        project, runner_factory=lambda hub: Runner(hub, harness_factory=harness_factory)
    )
    async with app.router.lifespan_context(app):
        # This test isolates command-driven wake races. Periodic due-work
        # checks have their own integration test and may legally add empty passes.
        app.state.driver.timer_task.cancel()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            task = await api_command(client, "task.publish", {"contract": {
                "title": "Finish approved record", "objective": "Use an approved software source",
                "acceptance_policy": "general.json@1",
                "payload": {"output_schema": {
                    "type": "object", "properties": {"ready": {"const": True}},
                    "required": ["ready"], "additionalProperties": False,
                }},
            }})
            # The id the worker's question receives (communications._on_request).
            request_id = uuid5(NAMESPACE_URL, "external-request:" + digest({
                "kind": "question", "key": "approved-source", "task_id": task["id"], "task_revision": 1})).hex
            run = await api_command(client, "run.start", {
                "limits": {},
            })
            run_id = run["id"]
            assert (await client.post(f"/v1/runs/{run_id}/drive")).status_code == 202
            await wait_for_initial_driver_pass(app, run_id, finishing)
            if not during_finish:
                await finish_driver(app, run_id)
            assert (await app.state.hub.task(task["id"]))["state"] == "waiting"
            original_worker = (await app.state.hub.list_workers())[0]["id"]
            await api_command(client, "request.resolve", {
                "request_id": request_id, "text": "The source is approved.", "message_id": "approval",
            })
            if during_finish:
                assert app.state.driver.pending_wakes == {run_id}
                release_finish.set()
            await finish_driver(app, run_id)
            assert len(passes) >= 2 and set(passes) == {run_id}
            # The deterministic timer may perform an additional empty pass;
            # inspecting due work must not create another model activation.
            assert len(activations) == 2
            assert (await app.state.hub.task(task["id"]))["state"] == "waiting"
            continuations = await app.state.hub.list_continuations()
            assert any(item["checkpoint"]["summary"] == "Read the operator-approved source."
                       for item in continuations)
            workers = await app.state.hub.list_workers()
            assert len(workers) == 1 and workers[0]["id"] == original_worker
            assert activations[1].opened_threads == [activations[0].session.thread_id]
            assert not app.state.driver.pending_wakes
            status = (await client.get(f"/v1/runs/{run_id}")).json()
            assert status["armed"] and not status["driving"]


@pytest.mark.asyncio
async def test_armed_wakes_respect_pause_and_stop(project):
    starts = []

    class Runner:
        def __init__(self, hub):
            self.hub = hub

        async def run_until_idle(self, run_id, **_):
            starts.append(run_id)
            return {"state": "idle"}

    app = create_app(project, runner_factory=Runner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            run = await api_command(client, "run.start")
            run_id = run["id"]
            await client.post(f"/v1/runs/{run_id}/drive")
            await finish_driver(app, run_id)
            await api_command(client, "run.pause", {"run_id": run_id})
            await api_command(client, "task.publish", {"contract": {
                "title": "Paused record", "objective": "Review a software record",
            }})
            await finish_driver(app, run_id)
            assert starts == [run_id]
            await api_command(client, "run.resume", {"run_id": run_id})
            await finish_driver(app, run_id)
            assert starts == [run_id, run_id]
            await api_command(client, "run.stop", {"run_id": run_id})
            await api_command(client, "artifact.register", {
                "kind": "software.record", "data": {"ready": True},
            })
            assert starts == [run_id, run_id]
            status = (await client.get(f"/v1/runs/{run_id}")).json()
            assert not status["armed"]
            assert status["state"] == "stopped"


@pytest.mark.asyncio
async def test_paused_run_drives_only_while_someone_is_resumed(project):
    starts = []

    class Runner:
        def __init__(self, hub):
            self.hub = hub

        async def run_until_idle(self, run_id, **_):
            starts.append(run_id)
            return {"state": "idle"}

    app = create_app(project, runner_factory=Runner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            run = await api_command(client, "run.start")
            run_id = run["id"]
            worker = await api_command(client, "worker.register", {})
            await api_command(client, "run.pause", {"run_id": run_id})
            assert (await client.post(f"/v1/runs/{run_id}/drive")).status_code >= 400
            resumed = await api_command(client, "worker.resume", {"worker_id": worker["id"], "run_id": run_id})
            assert resumed["control"] == "resumed"
            assert (await client.post(f"/v1/runs/{run_id}/drive")).status_code == 202
            await finish_driver(app, run_id)
            assert starts == [run_id]
            status = (await client.get(f"/v1/runs/{run_id}")).json()
            assert status["state"] == "paused" and status["resumed_workers"] == [worker["id"]]
            # Pausing that person again leaves nothing to drive.
            await api_command(client, "worker.pause", {"worker_id": worker["id"], "run_id": run_id})
            assert (await client.get(f"/v1/runs/{run_id}")).json()["resumed_workers"] == []
            assert (await client.post(f"/v1/runs/{run_id}/drive")).status_code >= 400


class CountingRunner:
    starts: list[str] = []

    def __init__(self, hub):
        self.hub = hub

    async def run_until_idle(self, run_id, **_):
        self.starts.append(run_id)
        return {"state": "idle"}


async def wait_until(predicate, timeout=10):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


async def drive_once(app, client, run_id):
    response = await client.post(f"/v1/runs/{run_id}/drive")
    assert response.status_code == 202, response.text
    await finish_driver(app, run_id)


@pytest.mark.asyncio
async def test_service_restart_resumes_driven_runs_and_respects_run_control(project, monkeypatch):
    monkeypatch.setattr(CountingRunner, "starts", [])
    starts = CountingRunner.starts
    app = create_app(project, runner_factory=CountingRunner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            paused = (await api_command(client, "run.start"))["id"]
            await drive_once(app, client, paused)
            await api_command(client, "run.pause", {"run_id": paused})
            stopped = (await api_command(client, "run.start"))["id"]
            await drive_once(app, client, stopped)
            await api_command(client, "run.stop", {"run_id": stopped})
            undriven = (await api_command(client, "run.start"))["id"]
            await api_command(client, "run.pause", {"run_id": undriven})
            running = (await api_command(client, "run.start"))["id"]
            await drive_once(app, client, running)
    starts.clear()
    restarted = create_app(project, runner_factory=CountingRunner)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://testserver",
        ) as client:
            status = (await client.get("/v1/status")).json()
            assert set(status["startup_resume"]["runs"]) == {running, paused}
            assert status["startup_resume"]["skipped"] is None
            assert set(status["armed_runs"]) == {running, paused}
            # The running run continues without a drive; the paused run waits for resume.
            await wait_until(lambda: running in starts)
            assert paused not in starts
            await api_command(client, "run.pause", {"run_id": running})
            await api_command(client, "run.resume", {"run_id": paused})
            await finish_driver(restarted, paused)
            assert paused in starts
            assert stopped not in starts and undriven not in starts
            assert {stopped, undriven}.isdisjoint(restarted.state.driver.armed)


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["disabled", "delivery_recovery_failed"])
async def test_service_restart_can_start_for_inspection_only(project, monkeypatch, cause):
    monkeypatch.setattr(CountingRunner, "starts", [])
    app = create_app(project, runner_factory=CountingRunner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            run_id = (await api_command(client, "run.start"))["id"]
            await drive_once(app, client, run_id)
    CountingRunner.starts.clear()
    if cause == "delivery_recovery_failed":
        from formal_geyser.coordination.hub import Hub

        async def fail(self):
            raise OSError("synthetic transcript failure")
        monkeypatch.setattr(Hub, "reconcile_delivery_transcripts", fail)
    restarted = create_app(project, runner_factory=CountingRunner, resume_runs=cause != "disabled")
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://testserver",
        ) as client:
            status = (await client.get("/v1/status")).json()
            assert {key: status["startup_resume"][key] for key in ("runs", "skipped")} == {
                "runs": [], "skipped": cause}
            assert status["armed_runs"] == [] and restarted.state.driver.passes == {}
            if cause == "disabled":
                await drive_once(restarted, client, run_id)
                assert CountingRunner.starts == [run_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [PermissionError("synthetic transcript lock"),
                                   json.JSONDecodeError("Expecting value", "", 0)])
async def test_transient_startup_fault_is_retried_until_driven_runs_resume(project, monkeypatch, fault):
    monkeypatch.setattr(CountingRunner, "starts", [])
    app = create_app(project, runner_factory=CountingRunner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            run_id = (await api_command(client, "run.start"))["id"]
            await drive_once(app, client, run_id)
    CountingRunner.starts.clear()
    import formal_geyser.coordination.run_driver as run_driver

    monkeypatch.setattr(run_driver, "STARTUP_RETRY_SECONDS", 0.01)
    failures = [fault] * 2  # The first fails service startup's attempt, the second a retry.
    original = Hub.reconcile_delivery_transcripts

    async def flaky(self):
        if failures:
            raise failures.pop()
        return await original(self)
    monkeypatch.setattr(Hub, "reconcile_delivery_transcripts", flaky)
    restarted = create_app(project, runner_factory=CountingRunner)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://testserver",
        ) as client:
            # The first attempt failed; the run must still resume once the lock clears.
            await wait_until(lambda: run_id in CountingRunner.starts)
            status = (await client.get("/v1/status")).json()
            assert status["startup_resume"] == {"runs": [run_id], "skipped": None}
            assert failures == []


@pytest.mark.asyncio
async def test_unapplied_history_records_are_retried_and_their_notice_clears(project, monkeypatch):
    import formal_geyser.coordination.run_driver as run_driver

    monkeypatch.setattr(run_driver, "HISTORY_RETRY_SECONDS", 0.05)
    store = TranscriptStore(project)
    input_id = store.prepare_input("worker-a", "thread-a", "runner", "Prompt")
    path = store.root / "inputs" / (input_id + ".json")
    hidden = path.with_suffix(".hidden")
    path.rename(hidden)  # Temporarily unreadable, as during an antivirus quarantine.
    hub = Hub(project)
    try:
        await hub.db.call(lambda c: c.execute(
            "INSERT INTO delivery_transcript_updates(kind,delivery_id,input_id,state,turn_id,reason) "
            "VALUES('peer','d1',?,'not_delivered',NULL,'Transport closed')", (input_id,)), write=True)
    finally:
        await hub.close()
    app = create_app(project, runner_factory=CountingRunner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            overview = (await client.get("/v1/overview?compact=true")).json()
            assert "could not be updated" in overview["delivery_recovery_error"]
            hidden.rename(path)
            await wait_until(lambda: not getattr(app.state.hub, "unapplied_transcript_updates", None))
            overview = (await client.get("/v1/overview?compact=true")).json()
            assert overview["delivery_recovery_error"] is None


@pytest.mark.asyncio
async def test_interruption_listing_is_authenticated_bounded_and_import_wakes_only_armed_run(project):
    class Hub:
        def __init__(self):
            self.reads = 0
            self.commands = []

        async def list_interruptions(self):
            self.reads += 1
            return [{"id": f"interruption-{number}", "state": "waiting"} for number in range(105)]

        async def run(self, run_id):
            return {"id": run_id, "state": "running"}

        async def mark_run_resumable(self, run_id):
            pass

        async def execute(self, supplied, actor):
            assert actor.role == "operator"
            self.commands.append(supplied.name)
            return {"id": "imported", "state": "waiting"}

    starts = []

    class Runner:
        def __init__(self, hub):
            self.hub = hub

        async def run_until_idle(self, run_id, **_):
            starts.append(run_id)
            return {"state": "idle"}

    hub = Hub()
    app = create_app(project, hub=hub, runner_factory=Runner)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            assert len((await client.get("/v1/interruptions")).json()) == 100
            page = await client.get("/v1/interruptions?offset=3&limit=2")
            assert [row["id"] for row in page.json()] == ["interruption-3", "interruption-4"]
            assert (await client.get("/v1/interruptions?limit=1001")).status_code == 422
            assert hub.commands == []
            await api_command(client, "interruption.import", {"attempt_id": "prior-attempt"})
            assert starts == []
            await client.post("/v1/runs/run/drive")
            await finish_driver(app, "run")
            await api_command(client, "interruption.import", {"attempt_id": "another-attempt"})
            await finish_driver(app, "run")
            assert starts == ["run", "run"]


def test_api_empty_limits_records_controls_without_driving(project):
    def forbidden(hub):
        raise AssertionError("Recording a new run must not invoke a driver")

    with TestClient(create_app(project, runner_factory=forbidden)) as client:
        result = client.post("/v1/commands", json=command(
            "run.start", {"limits": {}},
        ))
        assert result.status_code == 200, result.text
        run_id = result.json()["id"]
        state = client.get(f"/v1/runs/{run_id}").json()
        assert "max_active" not in state["limits"]
        assert set(state["limits"]) == {"packet_bytes", "delta_bytes"}
        assert "turn_seconds" not in state["limits"]
        assert state["driving"] is False
        assert state["armed"] is False


def test_repository_status_does_not_start_runtime(project):
    class RepositoryHub:
        async def repository_status(self):
            return {"enabled": True, "initialized": True, "head": "commit-1", "branch": "main"}

    def forbid_runner(hub):
        raise AssertionError("Repository inspection must not start model work")

    with TestClient(create_app(project, hub=RepositoryHub(), runner_factory=forbid_runner)) as client:
        response = client.get("/v1/repository")
        assert response.status_code == 200, response.text
        assert response.json() == {
            "enabled": True, "initialized": True, "head": "commit-1", "branch": "main",
        }


def test_status_does_not_materialize_every_task_or_worker(project, monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("A health count must not load task or worker bodies")
    app = create_app(project)
    with TestClient(app) as client:
        monkeypatch.setattr(app.state.hub, "list_tasks", forbidden)
        monkeypatch.setattr(app.state.hub, "list_workers", forbidden)
        result = client.get("/v1/status")
        assert result.status_code == 200
        assert result.json()["tasks"] == 0 and result.json()["workers"] == 0
