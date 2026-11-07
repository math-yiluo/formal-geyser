"""Native runtime boundaries using scripted turns and synthetic local documents."""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.content import FileReference, PinnedPublicView
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    RepositoryConfig,
    Role,
)
from formal_geyser.coordination.repository import RepositoryStore
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ToolResult,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.handoff import HANDOFF_SECTIONS, handoff_path
from formal_geyser.coordination.runtime.model_view import project_packet
from formal_geyser.coordination.storage import canonical
from tests.coordination.communication_helpers import native_text
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.publication_helpers import publish_batch

OPERATOR = Actor(role=Role.OPERATOR)
CHECKPOINT = {"summary": "Synthetic native runtime check.", "next_action": "Await useful team work."}


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


async def tool(on_tool, name, arguments=None, *, delivered=True):
    result = await on_tool("taskhub_call", {"operation": name, "arguments": arguments or {}}, uuid4().hex)
    callback = getattr(result, "after_send", None)
    if delivered and callback is not None:
        await callback()
    return result


async def call(runner, attempt, thread_id, operation, arguments, call_id):
    return await runner._tool(attempt, thread_id, "taskhub_call", {
        "operation": operation, "arguments": arguments}, call_id)


def write(workspace, path, text):
    target = workspace / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8", newline="")
    return target





def referenced_message(workspace, message):
    path = workspace / message["file"]["path"]
    body = path.read_text(encoding="utf-8")
    return json.loads(body)["text"] if path.suffix == ".json" else body.partition("\n```\n\n")[2].removesuffix("\n")


@pytest.fixture
async def native_runtime(tmp_path_factory):
    hubs = []

    async def create(*, repository=False, packet_bytes=32768):
        if repository and shutil.which("git") is None:
            pytest.skip("Local Git is required")
        root = tmp_path_factory.mktemp("nr")
        config = ProjectConfig(repository=RepositoryConfig(enabled=repository),
                               limits=Limits(packet_bytes=packet_bytes))
        initialize(root, config)
        hub = Hub(root)
        hubs.append(hub)
        case = SimpleNamespace(hub=hub, root=root)
        if repository:
            snapshot = hub.blobs.put_json({"files": [{
                "path": "wiki/guide.md", "sha256": hub.blobs.put(b"# Accepted initial guide\n")} ]})
            case.initial = await command(hub, "repository.initialize", {"snapshot": snapshot})
        case.host = await command(hub, "task.publish", {"contract": {
            "title": "Native runtime fixture", "objective": "Inspect synthetic document workflow.",
            "acceptance_policy": "general.json@1",
            "payload": {"output_schema": {"type": "object"}},
        }})
        case.run = await command(hub, "run.start")
        return case
    yield create
    for hub in hubs:
        await hub.close()


async def claim(case, task=None):
    task = task or case.host
    current = await case.hub.task(task["id"])
    initial_worker = current["collaboration"].get("recruitment", {}).get("initial_worker_id")
    if initial_worker and current["collaboration"]["recruitment"]["recruitment_decision"] == "pending":
        intent = await case.hub.record_first_turn(initial_worker, task["id"])
        await case.hub.decide_recruitment(intent["id"], True, "Inspect the independent native boundary.")
    registered = await command(case.hub, "worker.register")
    return await command(case.hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"],
        "worker_id": registered["id"], "run_id": case.run["id"], "plan": "Inspect the native boundary."})


async def selected_review(hub, run_id, review_task_id):
    return await select_review(hub, review_task_id, run_id)


def public_fixture(root, version, pages):
    """Provide a trusted pinned view; these fixtures claim no mathematical acceptance."""
    directory = root / ("public-" + version)
    for path, text in pages.items():
        write(directory, path, text)
    commit = version * 40
    files = tuple(FileReference(path, hashlib.sha256(body).hexdigest(), len(body), commit)
                  for path, body in ((path, (directory / path).read_bytes()) for path in sorted(pages)))
    return PinnedPublicView(directory, files, "f" * 64, commit, "e" * 64)





@pytest.mark.parametrize("remove_public_view", [False, True])
async def test_knowledge_paths_need_no_page_delivery(native_runtime, monkeypatch, remove_public_view):
    case = await native_runtime()
    attempt = await claim(case)
    current = public_fixture(case.root, "a", {"wiki/README.md": "# Public knowledge\n"})
    async def view():
        return current
    monkeypatch.setattr(case.hub, "public_content", view)
    first = await case.hub.prepare_native_context(attempt["worker_id"])
    assert Path(first["directories"]["knowledge_readme"]) == current.root / "wiki/README.md"
    current = None if remove_public_view else public_fixture(case.root, "b", {"document.md": "Unrelated.\n"})
    next_input = await case.hub.prepare_native_context(attempt["worker_id"])
    assert Path(next_input["directories"]["knowledge_readme"]).is_file()
    assert not {"pages", "removed", "full_read", "required_wiki"} & next_input.keys()


async def test_actual_prompt_has_readme_without_wiki_body_or_page_list(native_runtime, monkeypatch):
    case = await native_runtime()
    public = public_fixture(case.root, "a", {
        "wiki/README.md": "# Public knowledge\n## Essential\nRead the short guidance.\n",
        "notes/long.md": "NeverInlinePublicNote " * 20000})
    async def view():
        return public
    monkeypatch.setattr(case.hub, "public_content", view)
    observed = []
    class KnowledgeHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, **kwargs):
            packet = json.loads(prompt)
            observed.append(packet)
            assert "wiki" not in packet and "NeverInlinePublicNote" not in prompt
            assert Path(packet["paths"]["knowledge_readme"]).is_file()
            turn = uuid4().hex
            await on_started(turn)
            await self.close()
            return TurnOutcome(turn, "completed", True, True)
    await WorkerRunner(case.hub, KnowledgeHarness).run_once(case.run["id"])
    assert len(observed) == 1


async def test_first_activation_starts_the_handoff_file_and_never_replaces_one(native_runtime):
    case = await native_runtime()
    prompts = []

    class FirstTurn(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, **kwargs):
            prompts.append(json.loads(prompt))
            turn = uuid4().hex
            await on_started(turn)
            await self.close()
            return TurnOutcome(turn, "completed", True, True)
    runner = WorkerRunner(case.hub, FirstTurn)
    await runner.run_once(case.run["id"])
    assert len(prompts) == 1
    worker_id = (await case.hub.task(case.host["id"]))["collaboration"]["recruitment"]["initial_worker_id"]
    handoff = case.hub.workspace(worker_id) / handoff_path(case.host["id"])
    text = handoff.read_text(encoding="utf-8")
    assert text.startswith("# Handoff\n\n## Goal\n\nNative runtime fixture")
    assert f"`{prompts[0]['paths']['task']}`" in text
    assert [line[3:] for line in text.splitlines() if line.startswith("## ")] == list(HANDOFF_SECTIONS)
    handoff.write_text("# Handoff\n\nThe worker's own record.\n", encoding="utf-8")
    runner._start_handoff(worker_id, case.host["id"], "Another title", None)
    assert handoff.read_text(encoding="utf-8") == "# Handoff\n\nThe worker's own record.\n"





async def test_large_public_notes_do_not_expand_native_input(native_runtime, monkeypatch):
    case = await native_runtime(packet_bytes=32768)
    body = "LargePublicNoteSentinel\n" * 20000
    public = public_fixture(case.root, "a", {"wiki/README.md": "# Public knowledge\n", "notes/long.md": body})
    async def view():
        return public
    monkeypatch.setattr(case.hub, "public_content", view)
    attempt = await claim(case)
    runner = WorkerRunner(case.hub)
    packet, _ = await runner._native_packet(await case.hub.context(attempt["worker_id"]), attempt)
    projected = project_packet(await runner._present(packet))
    assert len(canonical(projected).encode()) < 32768
    assert "LargePublicNoteSentinel" not in canonical(projected) and "wiki" not in projected
    assert Path(projected["paths"]["public_notes"]).joinpath("long.md").read_text(encoding="utf-8") == body


async def test_initial_and_followup_private_delivery_are_recipient_only_and_explicitly_acked(
    native_runtime, monkeypatch,
):
    case = await native_runtime()
    hub = case.hub
    sender = await claim(case)
    recruitment = await hub.record_first_turn(sender["worker_id"], case.host["id"])
    await hub.decide_recruitment(recruitment["id"], True, "Receive private collaboration messages.")
    receiver = await command(hub, "worker.register")
    recipient_attempt = await command(hub, "task.join", {"task_id": case.host["id"], "revision": 1,
        "worker_id": receiver["id"], "run_id": case.run["id"], "plan": "Read collaboration messages."})
    other_task = await command(hub, "task.publish", {"contract": {
        "title": "Separate team", "objective": "No access to the peer conversation."}})
    outsider = await claim(case, other_task)
    secret = "PrivateRuntimeSentinel"
    sent = await command(hub, "talk.send", {
        "recipient_id": receiver["id"], "message_id": "initial", "text": secret + " initial"}, worker(sender))
    runner = WorkerRunner(hub)
    outsider_packet, _ = await runner._native_packet(await hub.context(outsider["worker_id"]), outsider)
    assert secret not in canonical(outsider_packet) and "private_talk" not in outsider_packet
    put = hub.blobs.put

    def no_shared_secret(body):
        assert secret.encode() not in body, "Private peer content reached the shared blob store"
        return put(body)
    monkeypatch.setattr(hub.blobs, "put", no_shared_secret)

    async def acknowledged(message_id):
        return await hub.db.call(lambda c: c.execute(
            "SELECT acknowledged_at FROM peer_messages WHERE id=?", (message_id,)).fetchone()[0])

    class PeerHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            packet = json.loads(prompt)
            assert packet["identity"] == receiver["display_name"]
            assert referenced_message(self.workspaces[-1], packet["messages"][0]) == secret + " initial"
            assert "private_talk" not in packet and "packet_id" not in packet
            assert await acknowledged(sent["id"]) is None
            turn = uuid4().hex
            await on_started(turn)
            await on_usage({"totalTokens": 1})
            assert await acknowledged(sent["id"]) is not None
            late = await command(hub, "talk.send", {
                "recipient_id": receiver["id"], "message_id": "late", "text": secret + " late"}, worker(sender))
            followup, inbox = await runner._native_packet(
                await hub.context(receiver["id"]), recipient_attempt)
            unread = followup["private_talk"]
            assert native_text(unread["messages"][0]) == secret + " late"
            assert await acknowledged(late["id"]) is None
            retried, _ = await runner._native_packet(await hub.context(receiver["id"]), recipient_attempt)
            assert retried["private_talk"]["messages"] == unread["messages"]
            await runner._ack_native(followup, recipient_attempt, inbox)
            assert await acknowledged(late["id"]) is not None
            delivered, _ = await runner._native_packet(await hub.context(receiver["id"]), recipient_attempt)
            assert "private_talk" not in delivered
            result = await tool(on_tool, "pause", {"predicate": {"external_response_id": "unanswered-request"}, "checkpoint": CHECKPOINT, "reason": "Private message check is complete."})
            await self.close()
            return TurnOutcome(turn, "interrupted", True, True, result["runtime_directive"])

    result = await WorkerRunner(hub, PeerHarness).run_once(case.run["id"], assignment=recipient_attempt)
    assert result["directive"] == "waiting", result


async def test_private_automatic_delivery_never_uses_shared_material_fallback(native_runtime, monkeypatch):
    case = await native_runtime()
    sender, recipient = await claim(case), await claim(case)
    secret = "PrivateSmallBudgetλ" * 600
    await command(case.hub, "talk.send", {
        "recipient_id": recipient["worker_id"], "message_id": "chunked", "text": secret}, worker(sender))
    runner = WorkerRunner(case.hub)

    put = case.hub.blobs.put

    def no_private_blob(body):
        assert b"PrivateSmallBudget" not in body, "Private pages must not write shared material blobs"
        return put(body)
    monkeypatch.setattr(case.hub.blobs, "put", no_private_blob)
    pieces = []
    for _ in range(200):
        packet, inbox = await runner._native_packet(await case.hub.context(recipient["worker_id"]), recipient)
        result = packet.get("private_talk", {})
        if not result.get("messages"):
            break
        assert len(canonical(result).encode()) <= 4096 and "material_sha256" not in result
        assert all("text" not in item for item in result["messages"])
        pieces.extend(native_text(item) for item in result["messages"])
        await runner._ack_native(packet, recipient, inbox)
    else:
        pytest.fail("Private reference delivery did not complete")
    assert pieces == [secret]


async def test_small_private_history_pages_never_use_shared_material_fallback_or_ack(native_runtime, monkeypatch):
    case = await native_runtime(packet_bytes=1024)
    sender, recipient = await claim(case), await claim(case)
    sent = await command(case.hub, "talk.send", {
        "recipient_id": recipient["worker_id"], "message_id": "chunked", "text": "PrivateSmallBudgetλ" * 180}, worker(sender))
    runner = WorkerRunner(case.hub)

    def no_blob(*args, **kwargs):
        raise AssertionError("Private history pages must not write shared material blobs")
    monkeypatch.setattr(case.hub.blobs, "put", no_blob)
    monkeypatch.setattr(case.hub.blobs, "put_json", no_blob)
    history = await call(runner, recipient, "recipient-thread", "talk_read", {
        "conversation_id": sent["conversation_id"]}, uuid4().hex)
    assert isinstance(history, ToolResult) and len(canonical(history).encode()) <= 1024
    assert history["has_more"] and "material_sha256" not in history
    await history.after_send()
    assert await case.hub.db.call(lambda c: c.execute(
        "SELECT acknowledged_at FROM peer_messages WHERE id=?", (sent["id"],)).fetchone()[0]) is None


@pytest.mark.parametrize("name", ["stage", "contribute"])
async def test_native_manifest_preserves_exact_metadata_and_source_snapshot(native_runtime, name):
    case = await native_runtime(repository=True)
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    body = {"kind": "json", "data": {"summary": "Exact λ metadata", "versions": [3, 8]},
            "introduction": "Start with document.md and check the exact source record versions.",
            "repository_paths": ["document.md"], "input_refs": []}
    source = "# Exact source\nNo mathematical acceptance claim.\n"
    write(workspace, "document.md", source)
    write(workspace, "submission.json", json.dumps(body, ensure_ascii=False))
    write(workspace, ".private/notes/scratch.md", "Unpublished private source\n")
    runner = WorkerRunner(case.hub)
    result = await call(runner, attempt, "manifest-thread", name, {"manifest_path": "submission.json"}, uuid4().hex)
    if name == "stage":
        record = await case.hub.artifact(result["artifact_id"])
        assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"
    else:
        assert result["runtime_directive"] == "drain_for_submission"
        finished = await command(case.hub, "runtime.quiescent", {
            "worker_id": attempt["worker_id"], "snapshot": case.hub.blobs.snapshot(workspace)}, Actor(role=Role.RUNNER))
        record = await case.hub.artifact(finished["artifact_id"])
    assert record["data"] == body["data"]
    review = await case.hub.task(record["review_task_id"])
    assert review["contract"]["context"] == body["introduction"]
    assert review["contract"]["payload"]["introduction_source"] == "author_introduction"
    files = {item["path"]: item["sha256"] for item in case.hub.blobs.get_json(record["snapshot"])["files"]}
    assert files["document.md"] == hashlib.sha256(source.encode()).hexdigest()
    assert case.hub.blobs.get(files["document.md"]) == source.encode()
    assert all(not path.startswith(".private/") for path in files)


@pytest.mark.parametrize("mode", ["traversal", "absolute", "mixed", "extra", "private-source", "unsafe-source", "invalid-json"])
async def test_native_manifest_rejects_invalid_boundaries_without_staging(native_runtime, mode):
    case = await native_runtime()
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    body = {"kind": "json", "data": {"exact": True}}
    if mode == "extra":
        body["invented_authority"] = "accepted"
    if mode in {"private-source", "unsafe-source"}:
        body["repository_paths"] = [".private/notes/secret.md" if mode == "private-source" else "../escape.md"]
    path = write(workspace, "submission.json", "{" if mode == "invalid-json" else json.dumps(body))
    arguments = {"manifest_path": str(path) if mode == "absolute" else "submission.json"}
    if mode == "traversal":
        arguments["manifest_path"] = "../submission.json"
    if mode == "mixed":
        arguments["data"] = {"different": True}
    if mode == "mixed":
        rejected = await call(WorkerRunner(case.hub), attempt, "manifest-thread", "stage", arguments, uuid4().hex)
        assert rejected["code"] == "INVALID_ARGUMENTS"
    else:
        with pytest.raises((HubError, ValueError)):
            await call(WorkerRunner(case.hub), attempt, "manifest-thread", "stage", arguments, uuid4().hex)
    assert await case.hub.list_artifacts() == []
    assert len(await case.hub.list_tasks()) == 1
    assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"


async def test_native_wiki_note_is_reviewed_while_author_stays_active(native_runtime):
    case = await native_runtime(repository=True)
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    write(workspace, "wiki/progress.md", "# Selected public note\nExact research explanation.\n")
    write(workspace, "unknown.txt", "Unselected user file.\n")
    write(workspace, ".private/notes/scratch.md", "Private scratch.\n")
    result = await call(WorkerRunner(case.hub), attempt, "note-thread", "wiki_submit", {
        "paths": ["wiki/progress.md"], "summary": "A selected research explanation."}, uuid4().hex)
    assert result["note_id"] == result["artifact_id"] and result["category"] == "note"
    assert result["review_task_id"] and "runtime_directive" not in result
    note = await case.hub.artifact(result["note_id"])
    assert note["kind"] == "note.wiki" and not note["accepted"]
    assert note["data"] == {"summary": "A selected research explanation.", "paths": ["wiki/progress.md"]}
    assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"


@pytest.mark.parametrize("mode", ["outside-wiki", "traversal", "private", "empty"])
async def test_native_wiki_submission_rejects_unsafe_or_empty_pages_without_staging(native_runtime, mode):
    case = await native_runtime()
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    path = {"outside-wiki": "notes/progress.md", "traversal": "wiki/../escape.md",
            "private": "wiki/.private/secret.md", "empty": "wiki/empty.md"}[mode]
    if mode != "traversal":
        write(workspace, path, " \n" if mode == "empty" else "Private or unselected text.\n")
    with pytest.raises((HubError, ValueError)):
        await call(WorkerRunner(case.hub), attempt, "note-thread", "wiki_submit", {
            "paths": [path], "summary": "Rejected unsafe note."}, uuid4().hex)
    assert await case.hub.list_artifacts() == []
    assert len(await case.hub.list_tasks()) == 1
    assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"


async def test_new_reviewer_accepts_then_batch_publishes_without_private_files(native_runtime):
    case = await native_runtime(repository=True)
    hub = case.hub
    author_id = reviewer_id = review_id = None
    accepted = resumed = False
    candidate_id = None
    seen = []

    class ReviewHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            nonlocal author_id, reviewer_id, review_id, accepted, resumed, candidate_id
            packet = json.loads(prompt)
            wid = packet["identity"]
            seen.append((wid, packet["task"]["id"]))
            turn = uuid4().hex
            await on_started(turn)
            await on_usage({"totalTokens": len(seen), "lastTokenUsage": {"totalTokens": len(seen)}})
            if author_id is None:
                author_id = wid
                workspace = self.workspaces[-1]
                write(workspace, "document.md", "Author's original document.\n")
                write(workspace, "unknown-user-file.txt", "Do not publish this.\n")
                write(workspace, ".private/notes/scratch.md", "Never publish private notes.\n")
                write(workspace, "submission.json", json.dumps({"kind": "json", "data": {"ready": True},
                                                               "repository_paths": ["document.md"]}))
                result = await tool(on_tool, "contribute", {"manifest_path": "submission.json"})
            elif packet["task"]["kind"] == "review":
                reviewer_id, review_id = wid, packet["task"]["id"]
                assert reviewer_id != author_id
                assert "wiki" not in packet
                assert Path(packet["paths"]["knowledge_readme"]).is_file()
                assert "native" not in packet
                refreshed = await tool(on_tool, "sync")
                assert "wiki" not in refreshed and "native" not in refreshed
                detail = await tool(on_tool, "inspect", {"task_id": review_id})
                binding = detail["collaboration"]["review"]
                candidate_id = binding["candidate_id"]
                basis = {key: binding[key] for key in (
                    "candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")}
                accepted_result = await tool(on_tool, "review", {**basis, "verdict": "accept",
                    "rationale": "Read the exact candidate.",
                    "evidence": ["Compared the frozen document and exact task revision."]})
                assert accepted_result["state"] == "integrating" and "runtime_directive" not in accepted_result
                assert (await hub.repository_status())["head"] == case.initial["head"]
                assert not (await hub.artifact(candidate_id))["accepted"]
                accepted = True
                await self.close()
                return TurnOutcome(turn, "completed", True, True)
            else:
                resumed = True
                result = await tool(on_tool, "pause", {"predicate": {"external_response_id": "unanswered-request"}, "checkpoint": CHECKPOINT, "reason": "Await further useful work."})
            await self.close()
            return TurnOutcome(turn, "interrupted", True, True, result["runtime_directive"])

    runner = WorkerRunner(hub, ReviewHarness)
    submitted = await runner.run_once(case.run["id"])
    reviews = [task for task in await hub.list_tasks() if task["contract"]["kind"] == "review"]
    assignment = await selected_review(hub, case.run["id"], reviews[0]["id"])
    await runner.run_once(case.run["id"], assignment=assignment)
    await publish_batch(hub, case.run["id"])
    assert submitted["artifact_id"] and accepted and not resumed
    assert (await hub.worker(reviewer_id))["work_intention"] == "retired"
    assert (await hub.task(case.host["id"]))["state"] != "done"
    assert (await hub.artifact(candidate_id))["accepted"]
    assert (await hub.task(review_id))["state"] == "done"
    status = await hub.repository_status()
    assert status["head"] != case.initial["head"]
    repository = RepositoryStore(hub.state_dir)
    tree = {item["path"]: item["sha256"] for item in hub.blobs.get_json(repository.snapshot(status["head"], hub.blobs))["files"]}
    assert set(tree) == {"document.md", "wiki/guide.md"}
    assert hub.blobs.get(tree["document.md"]) == b"Author's original document.\n"


@pytest.mark.parametrize("name", ["publish", "propose_revision"])
@pytest.mark.parametrize("shape", ["inline", "wrapped", "named files"])
async def test_native_contract_files_publish_and_propose_exact_contracts(native_runtime, name, shape):
    case = await native_runtime()
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    expected = {"title": "Task from native files", "objective": "# Exact objective λ\n\nInspect these documents.\n",
                "context": "# Exact context\nRetain the original sources.\n",
                "acceptance_policy": "general.json@1", "constraints": ["Keep original evidence"],
                "payload": {"output_schema": {"type": "object", "required": ["result"]}}}
    if shape != "inline":
        write(workspace, "spec/objective.md", expected["objective"])
        write(workspace, "spec/context.md", expected["context"])
    if shape == "wrapped":
        body = {"contract": {key: value for key, value in expected.items() if key not in {"objective", "context"}},
                "objective": "objective.md", "context": "context.md"}
    elif shape == "named files":
        # The contract itself names its adjacent files, as the stored task.json does.
        body = {**expected, "objective": "objective.md", "context": "context.md"}
    else:
        body = expected
    source = write(workspace, "spec/task.json", json.dumps(body, ensure_ascii=False))
    original = source.read_bytes()
    arguments = {"contract_path": "spec/task.json"}
    if name == "propose_revision":
        arguments.update(task_id=case.host["id"], revision=1, reason="A concrete native specification refinement.")
    result = await call(WorkerRunner(case.hub), attempt, "native-contract", name, arguments, uuid4().hex)
    if name == "publish":
        saved = await case.hub.task(result["id"])
        actual = saved["contract"]
        root = Path(saved["files"]["root"])
        assert (root / "objective.md").read_bytes() == expected["objective"].encode()
        assert (root / "context.md").read_bytes() == expected["context"].encode()
    else:
        revised = await case.hub.task(case.host["id"])
        actual = revised["contract"]
        assert revised["revision"] == 2
        history = await case.hub.db.call(lambda c: dict(c.execute(
            "SELECT * FROM task_revision_history WHERE task_id=? AND revision=2", (case.host["id"],)).fetchone()))
        assert history["previous_revision"] == 1 and history["reason"] == arguments["reason"]
    assert {key: actual[key] for key in expected} == expected
    assert source.read_bytes() == original
    if name == "publish":
        assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"
    assert (await case.hub.worker(attempt["worker_id"]))["current_task_id"] == case.host["id"]


@pytest.mark.parametrize("mode", ["contract-traversal", "contract-absolute", "objective-traversal",
                                   "context-traversal", "objective-absolute", "reference-type",
                                   "mixed-inline", "invalid-json", "named-missing", "named-traversal"])
async def test_native_contract_file_boundaries_reject_without_publishing(native_runtime, mode):
    case = await native_runtime()
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    write(workspace, "spec/objective.md", "Native objective.\n")
    write(workspace, "spec/context.md", "Native context.\n")
    external = write(case.root, "outside.md", "Must not become task content.\n")
    body = {"contract": {"title": "Must not publish"}, "objective": "objective.md", "context": "context.md"}
    if mode == "named-missing":
        # A file name in place of the text, with no such file: refused, never stored as the text.
        body = {"title": "Must not publish", "objective": "missing.md", "context": "context.md"}
    elif mode == "named-traversal":
        body = {"title": "Must not publish", "objective": "../../outside.md"}
    if mode == "objective-traversal":
        body["objective"] = "../../outside.md"
    elif mode == "context-traversal":
        body["context"] = "../../outside.md"
    elif mode == "objective-absolute":
        body["objective"] = str(external)
    elif mode == "reference-type":
        body["objective"] = {"path": "objective.md"}
    path = write(workspace, "spec/task.json", "{" if mode == "invalid-json" else json.dumps(body))
    arguments = {"contract_path": "spec/task.json"}
    if mode == "contract-traversal":
        arguments["contract_path"] = "../task.json"
    elif mode == "contract-absolute":
        arguments["contract_path"] = str(path)
    elif mode == "mixed-inline":
        arguments["contract"] = {"title": "Inline replacement", "objective": "Conflicting inline source"}
    if mode == "mixed-inline":
        rejected = await call(WorkerRunner(case.hub), attempt, "native-contract", "publish", arguments, uuid4().hex)
        assert rejected["code"] == "INVALID_ARGUMENTS"
    else:
        with pytest.raises((HubError, ValueError)):
            await call(WorkerRunner(case.hub), attempt, "native-contract", "publish", arguments, uuid4().hex)
    assert len(await case.hub.list_tasks()) == 1
    assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"
    assert external.read_text() == "Must not become task content.\n"


async def test_native_revision_contract_rejects_mixed_inline_and_file_without_proposal(native_runtime):
    case = await native_runtime()
    attempt = await claim(case)
    write(case.hub.workspace(attempt["worker_id"]), "task.json", json.dumps({
        "title": "File revision", "objective": "Read exact source"}))
    rejected = await call(WorkerRunner(case.hub), attempt, "native-contract", "propose_revision", {
        "task_id": case.host["id"], "revision": 1, "reason": "Mixed source must fail.",
        "contract_path": "task.json", "contract": {"title": "Inline", "objective": "Conflicting source"}},
        uuid4().hex)
    assert rejected["code"] == "INVALID_ARGUMENTS"
    assert len(await case.hub.list_tasks()) == 1
    assert (await case.hub.task(case.host["id"]))["revision"] == 1


@pytest.mark.parametrize("name", ["stage", "complete"])
async def test_alternate_wiki_note_policy_cannot_complete_json_host(native_runtime, name):
    case = await native_runtime(repository=True)
    attempt = await claim(case)
    workspace = case.hub.workspace(attempt["worker_id"])
    write(workspace, "wiki/note.md", "# A partial note\nThis does not complete the JSON host.\n")
    runner = WorkerRunner(case.hub)
    arguments = {"kind": "note.wiki", "data": {"summary": "A partial wiki explanation.", "paths": ["wiki/note.md"]},
                 "policy": "general.peer@1", "repository_paths": ["wiki/note.md"], "completion": True}
    write(workspace, "submission.json", json.dumps(arguments))
    arguments = {"manifest_path": "submission.json"}
    if name == "stage":
        with pytest.raises(HubError) as rejected:
            await call(runner, attempt, "note-policy", name, arguments, uuid4().hex)
        assert rejected.value.code == "INVALID_INPUT"
    else:
        result = await call(runner, attempt, "note-policy", name, arguments, uuid4().hex)
        assert result["code"] == "INVALID_ARGUMENTS"
    assert (await case.hub.authorize_worker(worker(attempt)))["state"] == "running"
    assert await case.hub.list_artifacts() == []
    assert len(await case.hub.list_tasks()) == 1
    host = await case.hub.task(case.host["id"])
    assert host["state"] != "done" and host["contract"]["acceptance_policy"] == "general.json@1"
