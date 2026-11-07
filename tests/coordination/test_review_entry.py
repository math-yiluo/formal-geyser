"""Synthetic exact review inputs; no model turns or mathematical validation."""
from __future__ import annotations

import hashlib
import json
import stat
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.content import ContentError
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.material_files import MaterialFiles
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Role,
)
from formal_geyser.coordination.runtime import ScriptedHarness
from formal_geyser.coordination.runtime.model_view import review_view
from formal_geyser.coordination.runtime.review_entry import (
    _author_introduction,
    prepare_review_entry,
)
from formal_geyser.coordination.runtime.runner import WorkerRunner
from formal_geyser.coordination.storage import canonical, digest
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.publication_helpers import file_payload, new_hub

from .wait_helpers import legacy_wait


async def command(hub, name, payload=None, actor=None):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}),
                             actor or Actor(role=Role.OPERATOR))


def actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


def material(case, reference):
    body = (case.hub.workspace(case.reviewer["worker_id"])
                     / reference["material_path"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == reference["material_sha256"]
    return json.loads(body)


@pytest.fixture
async def review_case(tmp_path):
    hubs = []

    async def create(*, kind="software.result", long_contract=False, revised=False, data_extra=None):
        mathematical = kind.startswith("lean.")
        hub = await new_hub(tmp_path / str(len(hubs)))
        hubs.append(hub)
        contract = {"title": "Exact synthetic candidate review", "objective": "Preserve the original goal.",
                    "acceptance_policy": "general.peer@1", "constraints": ["Preserve the original constraint."],
                    "context": ("Exact acceptance context. " * 700 if long_contract else "Read every cited source."),
                    "payload": {"acceptance_conditions": ["Meet the original acceptance condition."]}}
        host = await command(hub, "task.publish", {"contract": contract})
        if revised:
            host = await command(hub, "task.revise", {
                "task_id": host["id"], "revision": 1,
                "contract": {**host["contract"], "objective": "Also meet the strengthened goal.",
                             "constraints": ["Meet the current constraint."],
                             "payload": {"acceptance_conditions": ["Meet the strengthened acceptance condition."]}},
                "reason": "Clarify execution while preserving the original obligations.",
            })
        run = await command(hub, "run.start")
        author = await hub.next_assignment(run["id"])
        workspace = hub.workspace(author["worker_id"])
        introduction = "A brief author introduction, included once."
        data = {"summary": "Synthetic exact record.", "citations": [
            {"source_id": "storage-fixture", "locator": "Section 2, page 7", "sha256": "c" * 64}]}
        data.update(data_extra or {})
        paths = []
        if mathematical:
            sources = {"Fixture.lean": b"-- Storage fixture only; no mathematical validation.\n",
                       "Fixture.md": b"Storage fixture, source location: Section 2, page 7.\n"}
            for name, body in sources.items():
                (workspace / name).write_bytes(body)
            data["bundle"] = {"lean_path": "Fixture.lean", "markdown_path": "Fixture.md",
                              "lean_sha256": hub.blobs.put(sources["Fixture.lean"]),
                              "markdown_sha256": hub.blobs.put(sources["Fixture.md"])}
            paths = list(sources)
        elif kind == "note.wiki":
            data["paths"] = ["wiki/progress.md"]
            (workspace / "wiki").mkdir(exist_ok=True)
            (workspace / "wiki/progress.md").write_text("Exact wiki progress.\n", encoding="utf-8")
            paths = data["paths"]
        else:
            paths = file_payload(hub, author, data=data)["repository_paths"]
        candidate = await command(hub, "artifact.stage", {
            "kind": kind, "data": data, "introduction": introduction, "repository_paths": paths,
        }, actor(author))
        reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
        return SimpleNamespace(hub=hub, host=host, run=run, author=author, reviewer=reviewer,
                               candidate=candidate, introduction=introduction, data=data)

    yield create
    for hub in hubs:
        await hub.close()


@pytest.mark.parametrize("kind", ["software.result", "note.wiki"])
async def test_ordinary_entry_preserves_exact_basis_record_and_author_without_internal_objects(review_case, kind):
    case = await review_case(kind=kind)
    before = await case.hub.db.call(lambda c: list(c.iterdump()))
    entry = await prepare_review_entry(case.hub, case.reviewer)
    assert canonical(entry).count(case.introduction) == 1
    assert entry["candidate"]["id"] == case.candidate["id"]
    assert entry["candidate"]["hash"] == case.candidate["body_sha"]
    assert entry["candidate"]["snapshot"] == case.candidate["snapshot"]
    assert json.loads(Path(entry["candidate"]["record_file"]["path"]).read_bytes()) == case.data
    assert entry["review_request"]["author_id"] == case.author["worker_id"]
    assert entry["review_request"]["coauthors"] == []
    assert entry["review_request"]["basis"] == {
        "candidate_id": case.candidate["id"], "candidate_hash": case.candidate["body_sha"],
        "task_revision": case.host["revision"], "input_refs": [], "policy": "general.peer@1"}
    assert "data" not in entry["candidate"] and "contract" not in entry["host_task"]
    assert len(canonical(entry)) < 6000
    assert entry["exact_details_file"]["requires_read"] is False
    details = material(case, entry["exact_details_file"])
    assert details["candidate"]["data"]["citations"] == case.data["citations"]
    assert details["review_task"]["contract"] == case.reviewer["task"]["contract"]
    assert await case.hub.db.call(lambda c: list(c.iterdump())) == before


async def test_a_review_entry_writes_its_files_after_its_transaction_off_the_coordination_thread(
        review_case, monkeypatch):
    case = await review_case(long_contract=True, revised=True)
    threads = []
    original = MaterialFiles.write

    def observed(files, body):
        threads.append(threading.current_thread().name)
        return original(files, body)
    monkeypatch.setattr(MaterialFiles, "write", observed)
    entry = await prepare_review_entry(case.hub, case.reviewer)
    assert threads and not any(name.startswith("taskhub-db") for name in threads)
    # Each reference the entry gives is a file that now exists with its bytes.
    details = material(case, entry["exact_details_file"])
    assert details["review_task"]["contract"] == case.reviewer["task"]["contract"]


async def test_pinned_host_keeps_original_and_strengthened_acceptance_with_exact_long_file(review_case):
    case = await review_case(long_contract=True, revised=True)
    entry = await prepare_review_entry(case.hub, case.reviewer)
    host = entry["host_task"]
    assert (host["id"], host["revision"], host["current_revision"], host["contract_hash"]) == (
        case.host["id"], 2, 2, case.host["contract_hash"])
    assert host["acceptance_policy"] == "general.peer@1"
    conditions = host["acceptance_conditions"]
    assert conditions == {
        "objective": "Preserve the original goal.",
        "constraint:0": "Preserve the original constraint.",
        "current-objective": "Also meet the strengthened goal.",
        "current-constraint:0": "Meet the current constraint.",
        "acceptance:0": "Meet the original acceptance condition.",
        "current-acceptance:0": "Meet the strengthened acceptance condition.",
    }
    instructions = material(case, host["acceptance_instructions"])
    assert host["acceptance_instructions"]["requires_read"] is True
    assert entry["exact_details_file"]["requires_read"] is False
    assert instructions["context"] == case.host["contract"]["context"]
    assert instructions["requirements"] == case.host["contract"]["requirements"]
    pinned = json.loads(Path(host["files"]["path"]).read_bytes())
    assert pinned["revision"] == 2 and pinned["contract_sha256"] == case.host["contract_hash"]
    assert len(canonical(entry)) < 6000


async def test_the_review_input_shows_the_validation_and_long_acceptance_texts_as_files(review_case):
    case = await review_case(long_contract=True)
    entry = await prepare_review_entry(case.hub, case.reviewer)
    assert entry["validation"] == {"verdict": "accepted", "policy": "general.peer@1"}
    view = review_view(entry, case.hub.workspace(case.reviewer["worker_id"]))
    assert view["validation"] == entry["validation"]
    # A long text is a file reference that says whether reading it is required.
    instructions = view["acceptance"]["acceptance_instructions"]
    assert set(instructions) == {"path", "required"} and instructions["required"] is True


async def test_a_reviewer_receives_a_peer_reply_before_its_verdict(review_case):
    case = await review_case()
    reviewer_id = case.reviewer["worker_id"]
    await command(case.hub, "talk.send", {"recipient_id": reviewer_id, "message_id": "reply",
                                          "text": "The bound comes from Lemma 2."}, actor(case.author))
    assert await case.hub.pending_message_signal(reviewer_id)  # So it can be steered into a turn.
    runner = WorkerRunner(case.hub, ScriptedHarness)
    packet, _ = await runner._native_packet(await case.hub.context(reviewer_id), case.reviewer)
    assert [message["sender_id"] for message in packet["private_talk"]["messages"]] == [case.author["worker_id"]]


@pytest.mark.parametrize("kind", ["lean.definition", "lean.statement", "lean.proof"])
async def test_mathematical_entry_uses_frozen_pair_and_rejects_changed_native_sources(review_case, kind):
    case = await review_case(kind=kind)
    (case.hub.workspace(case.author["worker_id"]) / "Fixture.lean").write_text(
        "-- Later author edits are not the candidate.\n", encoding="utf-8")
    entry = await prepare_review_entry(case.hub, case.reviewer)
    candidate = entry["candidate"]
    assert entry["exact_details_file"]["requires_read"] is False
    record = Path(candidate["record_file"]["path"])
    assert record.is_relative_to(case.hub.root / "staging")
    assert json.loads(record.read_bytes())["citations"] == case.data["citations"]
    for name in ("lean", "markdown"):
        source = candidate["primary_pair"][name]
        path = Path(source["path"])
        assert path.is_relative_to(record.parent / "files")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == source["sha256"]
    frozen = Path(candidate["primary_pair"]["lean"]["path"])
    frozen.chmod(stat.S_IREAD | stat.S_IWRITE)
    frozen.write_bytes(b"Changed frozen evidence.\n")
    with pytest.raises((ContentError, HubError)):
        await prepare_review_entry(case.hub, case.reviewer)
    assert frozen.read_bytes() == b"Changed frozen evidence.\n"


async def test_acceptance_remains_usable_across_turns_without_integration_state(review_case):
    case = await review_case()
    before = await prepare_review_entry(case.hub, case.reviewer)
    await command(case.hub, "review.accept", {**before["review_request"]["basis"],
        "rationale": "Exact synthetic source inspection.", "evidence": ["Checked the bound record."],
    }, actor(case.reviewer))
    before_database = await case.hub.db.call(lambda c: list(c.iterdump()))
    entry = await prepare_review_entry(case.hub, case.reviewer)
    assert entry["exact_details_file"] != before["exact_details_file"]
    assert entry["review_request"]["phase"] == "integrating"
    assert entry["review_request"]["basis"] == before["review_request"]["basis"]
    assert entry["review_request"]["acceptance"]["attempt_id"] == case.reviewer["id"]
    assert "integration" not in entry and "prior_integrations_file" not in entry
    assert await case.hub.db.call(lambda c: list(c.iterdump())) == before_database


async def test_exact_details_stay_stable_across_new_reviewer_attempts_but_track_new_evidence(review_case):
    case = await review_case()
    initial = await prepare_review_entry(case.hub, case.reviewer)
    await command(case.hub, "review.accept", {**initial["review_request"]["basis"],
        "rationale": "Inspect the immutable fixture.", "evidence": ["Checked the exact record."],
    }, actor(case.reviewer))
    await legacy_wait(case.hub, {"checkpoint": {
        "summary": "The accepted candidate is unchanged.", "next_action": "Resume this review."}},
        actor(case.reviewer))
    await command(case.hub, "runtime.quiescent", {
        "worker_id": case.reviewer["worker_id"],
        "snapshot": case.hub.blobs.snapshot(case.hub.workspace(case.reviewer["worker_id"])),
    }, Actor(role=Role.RUNNER))
    await case.hub.db.call(lambda c: case.hub._wake_for_message(
        c, case.reviewer["worker_id"], "synthetic-answer-turn"), write=True)
    original_attempt = await case.hub.next_assignment(case.run["id"])
    assert original_attempt["execution_mode"] == "message_only"
    before = await prepare_review_entry(case.hub, original_attempt)
    await case.hub.ordinary_turn_completed(original_attempt["worker_id"], original_attempt["id"])
    await case.hub.db.call(lambda c: case.hub._wake_for_message(
        c, original_attempt["worker_id"], "synthetic-second-answer-turn"), write=True)
    case.reviewer = await case.hub.next_assignment(case.run["id"])
    assert case.reviewer["id"] != original_attempt["id"]
    resumed = await prepare_review_entry(case.hub, case.reviewer)
    assert resumed["exact_details_file"] == before["exact_details_file"]
    assert resumed == before
    details = material(case, resumed["exact_details_file"])
    for name in ("review_task", "host_task", "original_host_task"):
        assert not {"active_attempt", "active_attempts", "fencing_counter", "state_version"} & details[name].keys()
    await command(case.hub, "artifact.accept", {
        "artifact_id": case.candidate["id"], "policy": "general.peer@1", "verdict": "accepted",
        "details": {"observation": "New exact synthetic validation evidence."},
    }, Actor(role=Role.VALIDATOR))
    validated = await prepare_review_entry(case.hub, case.reviewer)
    assert validated["exact_details_file"] != resumed["exact_details_file"]
    assert material(case, validated["exact_details_file"])["candidate"]["evidence"][-1]["details"][
        "observation"] == "New exact synthetic validation evidence."


async def test_host_revision_requires_new_bound_review_and_changes_exact_details(review_case):
    case = await review_case()
    before = await prepare_review_entry(case.hub, case.reviewer)
    revised = await command(case.hub, "task.revise", {
        "task_id": case.host["id"], "revision": 1,
        "contract": {**case.host["contract"], "payload": {"scope": "A changed current host revision."}},
        "reason": "The new review must bind the changed host requirements.",
    })
    with pytest.raises(HubError) as error:
        await prepare_review_entry(case.hub, case.reviewer)
    assert error.value.code == "WORKER_DRAINING"
    for attempt in (case.author, case.reviewer):
        await command(case.hub, "runtime.quiescent", {
            "worker_id": attempt["worker_id"],
            "snapshot": case.hub.blobs.snapshot(case.hub.workspace(attempt["worker_id"])),
        }, Actor(role=Role.RUNNER))
    author = await case.hub.next_assignment(case.run["id"])
    assert author["task_id"] == case.host["id"] and author["task_revision"] == 2
    candidate = await command(case.hub, "artifact.stage", file_payload(
        case.hub, author, data=case.data, introduction=case.introduction), actor(author))
    case.reviewer = await select_review(case.hub, candidate["review_task_id"], case.run["id"])
    changed = await prepare_review_entry(case.hub, case.reviewer)
    assert changed["exact_details_file"] != before["exact_details_file"]
    assert changed["candidate"]["hash"] == before["candidate"]["hash"]
    assert changed["host_task"]["id"] == before["host_task"]["id"]
    assert changed["host_task"]["revision"] == changed["review_request"]["basis"]["task_revision"] == 2
    details = material(case, changed["exact_details_file"])
    assert details["host_task"]["contract_hash"] == revised["contract_hash"]
    assert details["host_task"]["contract"] == revised["contract"]
    assert details["original_host_task"]["contract"] == case.host["contract"]


@pytest.mark.parametrize("kind", ["software.result", "lean.proof"])
async def test_missing_native_record_reference_rejects_blob_only_compatibility(review_case, kind):
    case = await review_case(kind=kind)
    case.hub.blobs.put_json(case.data)
    await case.hub.db.call(lambda c: c.execute("DELETE FROM native_record_files WHERE record_id=?",
                                              (case.candidate["id"],)), write=True)
    await case.hub.db.call(lambda c: c.execute("DELETE FROM native_task_files WHERE task_id=?",
                                              (case.host["id"],)), write=True)
    before = await case.hub.db.call(lambda c: list(c.iterdump()))
    with pytest.raises(HubError, match="native|Native|file"):
        await prepare_review_entry(case.hub, case.reviewer)
    assert case.hub.blobs.get_json(case.candidate["body_sha"]) == case.data
    assert await case.hub.db.call(lambda c: list(c.iterdump())) == before


async def test_stale_actor_cannot_materialize_another_review_entry(review_case):
    case = await review_case()
    stale = {**case.reviewer, "fencing_token": case.reviewer["fencing_token"] + 1}
    with pytest.raises(HubError) as error:
        await prepare_review_entry(case.hub, stale)
    assert error.value.code == "STALE_BINDING"


async def test_linked_review_files_are_business_views_without_changing_exact_records(review_case):
    quoted = {"attempt_id": "an authored example", "helper_id": "a quoted source variable",
              "created_at": "an authored date", "nested": {"fencing_token": "quoted terminology"}}
    case = await review_case(data_extra={"quoted_example": quoted})
    source = Path(case.candidate["content_file"]["path"])
    original_body = source.read_bytes()
    original_hash = case.candidate["body_sha"]
    entry = await prepare_review_entry(case.hub, case.reviewer)
    await command(case.hub, "review.accept", {**entry["review_request"]["basis"],
        "rationale": "Check the exact fixture without interpreting its quoted field names.",
        "evidence": ["Read the frozen source."]}, actor(case.reviewer))
    before = await case.hub.db.call(lambda c: list(c.iterdump()))
    result = await prepare_review_entry(case.hub, case.reviewer)
    details = material(case, result["exact_details_file"])
    text = canonical(details)
    assert case.reviewer["id"] not in text
    assert case.author["id"] not in text
    assert details["candidate"]["data"] == case.data
    assert details["candidate"]["body_sha"] == original_hash
    assert details["acceptance"]["candidate_id"] == case.candidate["id"]
    assert "attempt_id" not in details["acceptance"]
    assert source.read_bytes() == original_body
    assert digest(json.loads(original_body)) == original_hash
    assert await case.hub.db.call(lambda c: list(c.iterdump())) == before


def test_long_author_introduction_keeps_one_complete_readable_reference(tmp_path):
    introduction = "Exact author prose with a cited condition.\n" * 200
    reference = _author_introduction(MaterialFiles(tmp_path), {
        "contract": {"payload": {}, "context": introduction, "title": "A retained review"}})
    view = review_view({"author_introduction": reference}, tmp_path)
    assert view["author_introduction"]["path"] == reference["material_path"]
    body = (tmp_path / view["author_introduction"]["path"]).read_bytes()
    assert hashlib.sha256(body).hexdigest() == reference["material_sha256"]
    assert json.loads(body) == introduction


async def access_call(case, runner, operation, arguments):
    return await runner._dispatch_tool(case.reviewer, "fixture", "taskhub_call",
        {"operation": operation.removeprefix("taskhub_"), "arguments": arguments},
        uuid4().hex, packet_bytes=32768)


@pytest.mark.parametrize("phase", ["pending", "integrating"])
async def test_assigned_reviewer_fetches_and_reads_exact_candidate_files(review_case, phase):
    case = await review_case(kind="lean.definition")
    entry = await prepare_review_entry(case.hub, case.reviewer)
    if phase == "integrating":
        await command(case.hub, "review.accept", {**entry["review_request"]["basis"],
            "rationale": "Read the synthetic frozen pair.", "evidence": ["Exact fixture bytes."]},
            actor(case.reviewer))
    runner = WorkerRunner(case.hub)
    before = await case.hub.db.call(lambda c: list(c.iterdump()))
    fetched = await access_call(case, runner, "fetch", {"artifact_id": case.candidate["id"]})
    assert fetched["id"] == case.candidate["id"]
    for reference in [entry["candidate"]["record_file"], *entry["candidate"]["primary_pair"].values()]:
        path = Path(reference["path"])
        expected = path.read_bytes()
        for operation, arguments in (
                ("taskhub_workspace_read", {"path": str(path)}),
                ("taskhub_resource_read", {"root": "staging",
                    "path": path.relative_to(case.hub.root / "staging").as_posix()})):
            result = await access_call(case, runner, operation, arguments)
            assert result["path"] == arguments["path"]
            assert result["text"].encode() == expected
            assert result["source_sha256"] == reference["sha256"]
    assert await case.hub.db.call(lambda c: list(c.iterdump())) == before


async def test_review_reads_include_exact_input_closure_but_not_unrelated_or_private_records(review_case):
    case = await review_case(kind="lean.definition")
    def add_evidence(c):
        def record(name, refs=()):
            return case.hub._insert_artifact(c, "lean.definition", {"name": name},
                task=case.host, producer=case.author["worker_id"], input_refs=list(refs))
        nested = record("Nested retained input")
        evidence = record("Direct retained input", [nested["id"]])
        unrelated = record("Unrelated candidate")
        private = case.hub._insert_artifact(c, "lean.definition", {"name": "Private evidence"},
            task=case.host, producer=case.author["worker_id"], private=True)
        # Synthetic historical references exercise read authority only, never
        # candidate validity, mathematical acceptance or publication authority.
        c.execute("UPDATE candidate_bindings SET input_refs_json=? WHERE artifact_id=?",
                  (canonical([evidence["id"], private["id"]]), case.candidate["id"]))
        return nested, evidence, unrelated, private
    nested, evidence, unrelated, private = await case.hub.db.call(add_evidence, write=True)
    reviewer = actor(case.reviewer)
    for record in (nested, evidence):
        assert (await case.hub.native_record(record["id"], reviewer))["id"] == record["id"]
    runner = WorkerRunner(case.hub)
    for record in (unrelated, private):
        assert (await case.hub.native_record(record["id"], reviewer))["id"] == record["id"]
        assert (await access_call(case, runner, "workspace_read", {"path": record["content_file"]["path"]}))["text"]


async def test_native_fallback_preserves_private_workspace_and_public_resources_only(review_case):
    case = await review_case(kind="lean.definition")
    runner = WorkerRunner(case.hub)
    own = case.hub.workspace(case.reviewer["worker_id"]) / ".private/notes/read.txt"
    own.parent.mkdir(parents=True, exist_ok=True)
    own.write_text("Own private reading note", encoding="utf-8")
    for path in (str(own), ".private/notes/read.txt"):
        result = await access_call(case, runner, "workspace_read", {"path": path})
        assert result["text"] == "Own private reading note"
    literature = case.hub.root / "literature/exact-source.txt"
    literature.parent.mkdir(parents=True, exist_ok=True)
    literature.write_text("Original cited source", encoding="utf-8")
    result = await access_call(case, runner, "resource_read", {"root": "literature", "path": str(literature)})
    assert result["text"] == "Original cited source"
    for root in ("literature", "tasks", "wiki", "library"):
        (case.hub.root / root).mkdir(exist_ok=True)
        listing = await access_call(case, runner, "resource_list", {"root": root})
        assert listing["path"] == ""
    foreign = case.hub.workspace(case.author["worker_id"]) / ".private/notes/secret.txt"
    foreign.parent.mkdir(parents=True, exist_ok=True)
    foreign.write_text("Foreign private note", encoding="utf-8")
    assert (await access_call(case, runner, "workspace_read", {"path": str(foreign)}))["text"] == "Foreign private note"
    with pytest.raises(ValueError, match="parent traversal"):
        await access_call(case, runner, "workspace_read",
            {"path": str(literature.parent / ".." / ".task-based/state.sqlite3")})
    assert (await access_call(case, runner, "resource_list", {"root": "staging"}))["entries"]
    with pytest.raises(ValueError, match="Available roots:.*literature.*staging"):
        await access_call(case, runner, "resource_read", {"root": "missing", "path": "file.txt"})


async def test_native_fallback_follows_only_verified_relocation_and_rejects_changed_pair(review_case):
    case = await review_case(kind="lean.definition")
    entry = await prepare_review_entry(case.hub, case.reviewer)
    originals = [entry["candidate"]["record_file"], *entry["candidate"]["primary_pair"].values()]
    before = {reference["path"]: Path(reference["path"]).read_bytes() for reference in originals}
    await case.hub.db.call(lambda c: case.hub._relocate_artifact(c, case.candidate["id"],
        "submission-history", {"disposition": "rejected", "reason": "Synthetic relocation fixture only."}), write=True)
    runner = WorkerRunner(case.hub)
    for reference in originals:
        old = Path(reference["path"])
        assert not old.is_file()
        for operation, arguments in (("workspace_read", {"path": str(old)}),
                ("resource_read", {"root": "staging",
                    "path": old.relative_to(case.hub.root / "staging").as_posix()})):
            result = await access_call(case, runner, operation, arguments)
            assert result["path"] == arguments["path"]
            assert result["text"].encode() == before[str(old)]
            assert result["source_sha256"] == reference["sha256"]
    current = await case.hub.native_record(case.candidate["id"], actor(case.reviewer))
    pair = Path(current["content_file"]["root"]) / "files/Fixture.lean"
    pair.chmod(stat.S_IREAD | stat.S_IWRITE)
    pair.write_bytes(b"Changed frozen pair")
    with pytest.raises((ContentError, HubError)):
        await access_call(case, runner, "workspace_read", {"path": originals[1]["path"]})


async def test_native_fallback_cannot_follow_literature_link_outside_its_root(review_case):
    case = await review_case(kind="lean.definition")
    secret = case.hub.state_dir / "outside.txt"
    secret.write_text("Never disclose", encoding="utf-8")
    link = case.hub.root / "literature/linked-source.txt"
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(secret)
    except OSError:
        pytest.skip("Creating symbolic links is unavailable on this host")
    with pytest.raises(ContentError, match="Linked native content paths"):
        await access_call(case, WorkerRunner(case.hub), "resource_read",
                          {"root": "literature", "path": "linked-source.txt"})
