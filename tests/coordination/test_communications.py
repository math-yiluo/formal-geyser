"""Delivery, native literature, and read-only operator page integration tests."""
from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from formal_geyser.coordination.api import create_app
from formal_geyser.coordination.communications import (
    COMMUNICATION_SCHEMA,
    Communications,
)
from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.faults import FaultIsolation
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.lifecycle_schema import (
    LIFECYCLE_SCHEMA,
)
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from formal_geyser.coordination.schema import SCHEMA
from formal_geyser.coordination.storage import Database
from formal_geyser.coordination.talk import PeerTalk
from formal_geyser.coordination.transcripts import TranscriptStore

OPERATOR = Actor(role=Role.OPERATOR)
WORKER = Actor(role=Role.WORKER, worker_id="alice", attempt_id="attempt-alice", fencing_token=1)


class CommunicationHarness(FaultIsolation, Communications, PeerTalk):
    _get = Hub._get
    _role = Hub._role
    _active = Hub._active
    _effective_run_state = Hub._effective_run_state
    _worker_control = Hub._worker_control
    _control_of = Hub._control_of

    def _execution_environment(self, _connection):
        # This isolated correspondence ledger has no compiler or native runtime.
        return None

    def __init__(self, root):
        self.root = root
        self.state_dir = root / ".task-based"
        self.clock = lambda: 1000
        self.db = Database(self.state_dir / "state.sqlite3", SCHEMA + COMMUNICATION_SCHEMA + LIFECYCLE_SCHEMA)
        self.wakes = []

    def _task(self, c, task_id):
        task = self._get(c, "tasks", task_id)
        return {**task, "revision": task["current_revision"]}

    def _wake_for_message(self, c, worker_id, message_id):
        self.wakes.append((worker_id, message_id))

    def _wake(self, c):
        pass

    async def execute(self, name, payload, actor=OPERATOR):
        return await self.db.call(lambda c: self._communication_command(c, name, payload, actor), write=True)


@pytest.fixture
async def comm(tmp_path):
    hub = CommunicationHarness(tmp_path)
    def seed(c):
        c.execute("INSERT INTO runs(id,state,limits_json,created_at) VALUES('run','running','{}',0)")
        c.execute("INSERT INTO tasks(id,current_revision,state,created_at) VALUES('task',1,'claimed',0)")
        for worker in ("alice", "bob"):
            c.execute("INSERT INTO workers(id,state,active_attempt,created_at) VALUES(?,'running',?,0)",
                      (worker, "attempt-" + worker))
            c.execute("INSERT INTO attempts(id,task_id,task_revision,worker_id,run_id,fencing_token,state,"
                      "created_at) VALUES(?,'task',1,?,'run',1,'running',0)",
                      ("attempt-" + worker, worker))
    await hub.db.call(seed, write=True)
    yield hub
    await hub.db.close()


async def test_correspondence_is_native_and_operator_read_does_not_ack(comm):
    text = "Literal human text: \u4f60\u597d\nDo not translate this message."
    sent = await comm.execute("message.send", {"worker_id": "alice", "text": text, "message_id": "one"})
    assert text in Path(sent["path"]).read_text(encoding="utf-8")
    assert len(comm.wakes) == 1
    assert (await comm.human_unread(OPERATOR, worker_id="alice"))["delivery_id"] is None
    assert (await comm.human_history("alice"))["messages"][0]["acknowledged_at"] is None
    sent_again = await comm.execute("message.send", {"worker_id": "alice", "text": text, "message_id": "one"})
    assert sent_again["id"] == sent["id"] and len(comm.wakes) == 1
    with pytest.raises(HubError, match="different content"):
        await comm.execute("message.send", {"worker_id": "alice", "text": "Changed", "message_id": "one"})
    Path(sent["path"]).write_text("Tampered body", encoding="utf-8")
    with pytest.raises(HubError, match="differs"):
        await comm.human_history("alice")


async def test_partial_delivery_and_uncertain_outcome_never_silently_replay(comm):
    text = "Read this exact sentence. 位 " * 120
    await comm.execute("message.send", {"worker_id": "alice", "text": text, "message_id": "long"})
    packet = await comm.human_unread(WORKER, max_bytes=1024)
    assert not packet["messages"][0]["complete"]
    await comm.mark_delivery_uncertain(WORKER, "human", packet["delivery_id"], "thread", "turn", "input")
    assert not (await comm.human_unread(WORKER))["messages"]
    assert len(await comm.delivery_holds()) == 1
    await comm.execute("message.delivery.resolve", {"kind": "human", "delivery_id": packet["delivery_id"],
                       "outcome": "delivered", "reason": "Native stored user item confirms the exact input."})
    received = packet["messages"][0]["text"]
    while (packet := await comm.human_unread(WORKER, max_bytes=1024))["messages"]:
        received += "".join(m["text"] for m in packet["messages"])
        await comm.execute("message.ack", {"delivery_id": packet["delivery_id"]}, WORKER)
    assert received == text
    assert (await comm.human_history("alice"))["messages"][0]["acknowledged_at"] == 1000


async def test_request_deduplication_and_literature_requires_exact_readable_file(comm, tmp_path):
    payload = {"request_key": "source-one:edition-a", "title": "Source one", "text": "Please provide edition A."}
    request = await comm.execute("literature.request", payload, WORKER)
    bob = Actor(role=Role.WORKER, worker_id="bob", attempt_id="attempt-bob", fencing_token=1)
    assert (await comm.execute("literature.request", payload, bob))["id"] == request["id"]
    assert len(await comm.list_human_requests()) == 1
    resolve = {"request_id": request["id"], "outcome": "fulfilled", "text": "Here is the reference.", "message_id": "response"}
    with pytest.raises(HubError, match="Register an accessible"):
        await comm.execute("request.resolve", resolve)
    source = tmp_path / "reference.txt"
    source.write_text("A technical source fixture.", encoding="utf-8")
    registered = await comm.execute("literature.register", {"source_id": "source-one-a", "title": "Source one", "path": str(source)})
    assert registered["source_path"] == str(source)
    await comm.execute("request.resolve", {**resolve, "source_id": "source-one-a"})
    assert await comm.db.call(lambda c: comm._external_wait_satisfied(c, request["wait_predicate"]))
    assert {w for w, _ in comm.wakes} == {"alice", "bob"}
    assert (await comm.accessible_literature("source-one-a"))["source_path"] == str(source)
    source.write_text("Different edition", encoding="utf-8")
    # A wake-up reads only the ledger: the request is answered. Reading the source, off the
    # coordination thread, finds that it changed.
    assert await comm.db.call(lambda c: comm._external_wait_satisfied(c, request["wait_predicate"]))
    with pytest.raises(HubError, match="changed"):
        await comm.accessible_literature("source-one-a")


async def test_worker_cannot_impersonate_user_or_resolve_human_question(comm):
    with pytest.raises(HubError, match="own human"):
        await comm.execute("message.send", {"worker_id": "bob", "text": "Pretend", "message_id": "bad"}, WORKER)
    request = await comm.execute("message.request", {"request_key": "scope", "title": "Clarify scope", "text": "Which scope?"}, WORKER)
    with pytest.raises(HubError, match="literature only"):
        await comm.execute("request.resolve", {"request_id": request["id"], "outcome": "fulfilled", "text": "Self-answer", "message_id": "bad"}, WORKER)


def test_project_page_and_transcript_reads_never_run_models(tmp_path):
    initialize(tmp_path, ProjectConfig())
    def forbidden_runner(_):
        raise AssertionError("Read-only dashboard started a runner")
    with TestClient(create_app(tmp_path, runner_factory=forbidden_runner)) as client:
        page = client.get("/")
        assert page.status_code == 200 and 'id="turn-select"' in page.text
        assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
        created = client.post("/v1/commands", json=Command(name="worker.register", key=uuid4().hex,
                              payload={"worker_id": "alice"}).model_dump(mode="json"))
        assert created.status_code == 200, created.text
        store = TranscriptStore(tmp_path)
        input_id = store.prepare_input("alice", "thread", "initial", "Exact initial prompt")
        store.delivered(input_id, "turn")
        store.capture_native("thread", "alice", "item/completed", {"item": {"type": "agentMessage", "id": "answer", "text": "Visible reply"}})
        view = client.get("/v1/workers/alice/transcript").json()
        assert any(e.get("text") == "Exact initial prompt" for e in view["events"])
        assert any(e.get("text") == "Visible reply" for e in view["events"])
        assert client.get("/v1/overview").json()["workers"][0]["id"] == "alice"


def test_transcript_preserves_full_text_and_filters_reasoning(tmp_path):
    store = TranscriptStore(tmp_path)
    text = "Complete prompt\n" * 30000
    input_id = store.prepare_input("worker/unsafe-path", "native", "continue", text, helper_id="selector")
    store.ambiguous(input_id, "Transport lost after send")
    store.capture_native("native", "worker/unsafe-path", "item/reasoning/textDelta", {"delta": "Not visible"})
    first = store.page("worker/unsafe-path", limit=1)
    assert first["events"][0]["text"] == text and first["has_more"]
    second = store.page("worker/unsafe-path", cursor=first["next_cursor"])
    assert second["events"][0]["status"] == "uncertain"
    assert "Not visible" not in str(first) + str(second)
    with pytest.raises(HubError, match="boundary"):
        store.page("worker/unsafe-path", cursor=3)


def test_transcript_recovers_torn_tail_without_losing_committed_history(tmp_path):
    store = TranscriptStore(tmp_path)
    store.prepare_input("alice", "thread", "initial", "First prompt")
    path = next((store.root / "workers").glob("*/events.jsonl"))
    with path.open("ab") as stream:
        stream.write(b'{"unfinished":')
    assert len(store.page("alice")["events"]) == 1
    store.capture_native("thread", "alice", "item/completed", {
        "item": {"type": "agentMessage", "id": "answer", "text": "Complete answer"}})
    events = store.page("alice")["events"]
    assert [event["text"] for event in events] == ["First prompt", "Complete answer"]
    assert next(path.parent.glob("torn-tail-*.bin")).read_bytes() == b'{"unfinished":'


async def test_service_rechecks_due_work_only_while_explicitly_armed_and_running(tmp_path):
    initialize(tmp_path, ProjectConfig())
    calls = []
    second_pass = asyncio.Event()
    class NoModelRunner:
        async def run_until_idle(self, run_id, **_):
            calls.append(run_id)
            if len(calls) >= 2:
                second_pass.set()
            return {"state": "idle"}
    app = create_app(tmp_path, runner_factory=lambda _: NoModelRunner())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
            run = (await client.post("/v1/commands", json={"name": "run.start", "key": "start", "payload": {}})).json()
            assert not calls
            await client.post(f"/v1/runs/{run['id']}/drive")
            await asyncio.wait_for(second_pass.wait(), 5)
            await client.post("/v1/commands", json={"name": "run.pause", "key": "pause", "payload": {"run_id": run["id"]}})
            count = len(calls)
            await asyncio.sleep(1.2)
            assert len(calls) == count


@pytest.mark.parametrize("outcome", ["not_delivered", "delivered"])
async def test_reconciled_message_hold_wakes_remaining_text_without_a_new_message(tmp_path, outcome):
    hub = Hub(tmp_path, config=ProjectConfig())
    async def command(name, payload=None, actor=OPERATOR):
        return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)
    runner = Actor(role=Role.RUNNER)
    try:
        await command("task.publish", {"contract": {"title": "Source fixture", "objective": "Inspect the software fixture."}})
        run = await command("run.start")
        attempt = await hub.next_assignment(run["id"])
        worker = Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                       fencing_token=attempt["fencing_token"])
        await command("task.pause", {"reason": "Waiting for direction",
                                     "predicate": {"external_response_id": "unanswered-request"}}, worker)
        await command("runtime.quiescent", {"worker_id": worker.worker_id,
                      "snapshot": hub.blobs.snapshot(hub.workspace(worker.worker_id))}, runner)
        await command("message.send", {"worker_id": worker.worker_id, "message_id": "one", "text": "Long guidance. " * 500})
        packet = await hub.human_unread(runner, worker_id=worker.worker_id, max_bytes=1024)
        assert not packet["messages"][0]["complete"]
        input_id = TranscriptStore(tmp_path).prepare_input(worker.worker_id, "thread", "directed_messages",
                                                         packet["messages"][0]["text"])
        await hub.mark_delivery_uncertain(runner, "human", packet["delivery_id"], "thread", "turn", input_id)
        await hub.db.call(lambda c: hub._refresh_message_wake(c, worker.worker_id), write=True)
        assert (await hub.worker(worker.worker_id))["pending_wake"] == 0
        await command("message.delivery.resolve", {"kind": "human", "delivery_id": packet["delivery_id"],
                      "outcome": outcome, "reason": "Confirmed the exact native input outcome."})
        wake = await hub.next_assignment(run["id"])
        assert wake["worker_id"] == worker.worker_id and wake["execution_mode"] == "message"
        events = TranscriptStore(tmp_path).page(worker.worker_id)["events"]
        assert events[-1]["status"] == outcome
        remaining = await hub.human_unread(runner, worker_id=worker.worker_id, max_bytes=1024)
        assert remaining["messages"][0]["offset"] == (packet["messages"][0]["next_offset"] if outcome == "delivered" else 0)
    finally:
        await hub.close()


async def test_presend_guard_survives_crash_until_definite_native_response(comm):
    sent = await comm.execute("message.send", {"worker_id": "alice", "message_id": "guarded", "text": "One exact message"})
    packet = await comm.human_unread(WORKER)
    transcript = TranscriptStore(comm.root)
    input_id = transcript.prepare_input("alice", "thread", "initial", "One exact message")
    runner = Actor(role=Role.RUNNER)
    await comm.mark_delivery_uncertain(runner, "human", packet["delivery_id"], "thread", None, input_id)
    assert not (await comm.human_unread(WORKER))["messages"]
    await comm.db.close()
    reopened = CommunicationHarness(comm.root)
    try:
        assert not (await reopened.human_unread(WORKER))["messages"]
        with pytest.raises(HubError, match="another prepared input"):
            await reopened.confirm_delivery_transport(runner, "human", packet["delivery_id"], True, input_id="wrong")
        await reopened.confirm_delivery_transport(runner, "human", packet["delivery_id"], True,
                                                  thread_id="thread", turn_id="turn", input_id=input_id)
        assert (await reopened.human_history("alice"))["messages"][0]["id"] == sent["id"]
        assert (await reopened.human_history("alice"))["messages"][0]["acknowledged_at"] == 1000
        assert transcript.page("alice")["events"][-1]["status"] == "delivered"
    finally:
        await reopened.db.close()
