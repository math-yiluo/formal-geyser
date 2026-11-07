"""File-backed instruction delivery without a model or a separate reading ledger."""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

import formal_geyser
from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import ProjectConfig
from formal_geyser.coordination.plugins import GeneralPlugin
from formal_geyser.coordination.runtime import ScriptedHarness, WorkerRunner
from formal_geyser.coordination.runtime import instructions as module
from formal_geyser.coordination.runtime.actions import _CAPABILITIES
from formal_geyser.coordination.runtime.continuation import ContinuationPromptStore
from formal_geyser.coordination.runtime.files import ScopedFiles, workspace_tools
from formal_geyser.coordination.runtime.helpers import DECISION_INSTRUCTIONS
from formal_geyser.coordination.runtime.instructions import (
    GUIDE_DIRECTORY,
    continuation_guidance,
    guidance_anchor,
    guide_texts,
    prepare_guidance,
    standing_guides,
)
from formal_geyser.coordination.runtime.model_view import reading_view
from formal_geyser.coordination.runtime.tools import (
    WORKER_INSTRUCTIONS,
    dynamic_tools,
    operation_tools,
)
from formal_geyser.coordination.storage import BlobStore
from formal_geyser.plugin import LeanPlugin
from formal_geyser.submission_reference import prepare_reference
from formal_geyser.worker_tools import CheckArguments


def domain_guides(texts):
    return SimpleNamespace(worker_guides=lambda **_: dict(texts))


def packet(guidance):
    return {
        "worker_id": "Worker 1", "context_epoch": guidance["context_epoch"],
        "task": {"id": "task-a", "revision": 1, "title": "Inspect local files",
                 "kind": "work", "objective": "Read the exact synthetic input."},
        "instruction_guidance": guidance,
    }


def prepare_prompt(store, guidance, *, worker_id="worker-a", thread_id="thread-a"):
    return store.prepare(packet(guidance), worker_id=worker_id, thread_id=thread_id)


def confirm_prompt(store, prepared):
    assert store.confirm(prepared, current_worker={
        "id": prepared.worker_id, "thread_id": prepared.thread_id,
        "current_task_id": prepared.task_id, "context_epoch": prepared.context_epoch,
    })


def digests(guidance):
    return [*guidance["files"].values(), *guidance["optional_files"].values()]


@pytest.mark.parametrize("plugin,review,required,optional", [
    (GeneralPlugin(), False, {"worker.md"}, {"shared-dags.md", "worker-operations.md"}),
    (GeneralPlugin(), True, {"worker.md", "review.md"},
     {"shared-dags.md", "worker-operations.md", "review-operations.md"}),
    (LeanPlugin(), False, {"worker.md", "lean.md"},
     {"shared-dags.md", "worker-operations.md", "lean-operations.md"}),
    # A reviewer has its own Lean note, not the authoring guide.
    (LeanPlugin(), True, {"worker.md", "review.md", "lean-review.md"},
     {"shared-dags.md", "worker-operations.md", "review-operations.md",
      "lean-operations.md", "lean-review-operations.md"}),
])
def test_packaged_guides_are_exact_native_and_scoped_files(tmp_path, plugin, review, required, optional):
    workspace = tmp_path / "workspace"
    source = guide_texts(plugin, review=review)
    guidance = prepare_guidance(workspace, plugin, review=review, context_epoch=2)
    assert set(source) == required | optional
    assert set(guidance["files"]) == required
    assert set(guidance["optional_files"]) == optional
    assert guidance["mode"] == ("review" if review else "work")
    assert guidance["directory"] == GUIDE_DIRECTORY
    assert "read_before_work" not in guidance
    scoped = ScopedFiles(workspace)
    anchor = guidance_anchor(guidance)
    # The required guides' text is part of the standing instructions; the anchor names the
    # on-demand guides, which stay files read when their subject comes up.
    standing = standing_guides(plugin, review=review)
    versions = guidance["files"] | guidance["optional_files"]
    for name, text in source.items():
        assert text.startswith("# ") and text.strip()
        relative = f"{guidance['directory']}/{name}"
        body = (workspace / relative).read_bytes()
        assert body == text.encode("utf-8")
        assert versions[name] == hashlib.sha256(body).hexdigest()
        assert (relative in anchor) == (name in optional)
        assert (text.strip() in standing) == (name in required)
        parts, offset = [], 0
        while True:
            page = scoped.read(relative, offset=offset, limit=317)
            assert page["source_sha256"] == versions[name]
            parts.append(page["text"])
            if not page["has_more"]:
                break
            assert page["next_offset"] > offset
            offset = page["next_offset"]
        assert "".join(parts) == text
        assert text not in json.dumps(guidance)
        assert text not in anchor
    assert {path.relative_to(workspace).as_posix() for path in workspace.rglob("*")
            if path.is_file()} == {f"{GUIDE_DIRECTORY}/{name}" for name in source}
    reading = reading_view(guidance)
    assert set(reading["on_demand"]) == {f"{GUIDE_DIRECTORY}/{name}" for name in optional}
    assert set(reading) == {"on_demand", "instruction"}
    assert not any(sha in json.dumps(reading) for sha in versions.values())
    assert sum(len(source[name]) for name in required) < (7500 if review else 4500)
    for name, text in source.items():
        for target in re.findall(r"\]\(([^)]+\.md)\)", text):
            assert target in source, f"{name} links to an unavailable guide: {target}"


def test_core_and_domain_sources_are_reloaded_and_only_changed_copies_are_written(tmp_path, monkeypatch):
    sources = tmp_path / "source" / "guides"
    sources.mkdir(parents=True)
    core = sources / "worker.md"
    core.write_text("# Original core\nKeep exact evidence.\n", encoding="utf-8", newline="")
    graph = sources / "shared-dags.md"
    graph.write_text("# Shared DAGs\nCheck before saving.\n", encoding="utf-8", newline="")
    (sources / "worker-operations.md").write_text(
        "# Worker operations\nRead only when needed.\n", encoding="utf-8", newline="")
    domain = tmp_path / "domain.md"
    domain.write_text("# Original domain\nSynthetic α guidance.\n", encoding="utf-8", newline="")
    monkeypatch.setattr(module, "__file__", str(sources.parent / "instructions.py"))
    plugin = SimpleNamespace(worker_guides=lambda **_: {
        "domain.md": domain.read_text(encoding="utf-8")})
    writes = []
    original_write = ScopedFiles.write

    def write(self, path, text, expected_sha256=None):
        writes.append(path)
        return original_write(self, path, text, expected_sha256)

    monkeypatch.setattr(ScopedFiles, "write", write)
    workspace = tmp_path / "workspace"
    first = prepare_guidance(workspace, plugin, review=False, context_epoch=1)
    assert set(writes) == {f"{GUIDE_DIRECTORY}/worker.md", f"{GUIDE_DIRECTORY}/domain.md",
                           f"{GUIDE_DIRECTORY}/shared-dags.md", f"{GUIDE_DIRECTORY}/worker-operations.md"}
    writes.clear()
    assert prepare_guidance(workspace, plugin, review=False, context_epoch=1) == first
    assert not writes

    core.write_text("# Revised core\nKeep exact evidence and original locations.\n",
                    encoding="utf-8", newline="")
    domain.write_text("# Revised domain\nSynthetic β guidance.\n", encoding="utf-8", newline="")
    revised = prepare_guidance(workspace, plugin, review=False, context_epoch=1)
    assert set(writes) == {f"{GUIDE_DIRECTORY}/worker.md", f"{GUIDE_DIRECTORY}/domain.md"}
    assert all(revised["files"][name] != first["files"][name] for name in ("worker.md", "domain.md"))
    assert revised["optional_files"] == first["optional_files"]
    for name, path in (("worker.md", core), ("domain.md", domain)):
        assert (workspace / GUIDE_DIRECTORY / name).read_bytes() == path.read_bytes()

    writes.clear()
    (workspace / GUIDE_DIRECTORY / "domain.md").write_text("An obsolete local copy.", encoding="utf-8")
    assert prepare_guidance(workspace, plugin, review=False, context_epoch=1) == revised
    assert writes == [f"{GUIDE_DIRECTORY}/domain.md"]


def test_guides_preserve_user_files_and_stay_out_of_submission_snapshot(tmp_path):
    workspace = tmp_path / "workspace"
    protected = {
        "AGENTS.md": "# User-owned project directions\n",
        ".private/notes/scratch.md": "Private research notes.\n",
        f"{GUIDE_DIRECTORY}/personal.md": "User file outside the supplied guide list.\n",
        "library/source.md": "Accepted source.\n",
        "draft.md": "Current authored work.\n",
    }
    for relative, text in protected.items():
        target = workspace / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="")
    guidance = prepare_guidance(workspace, LeanPlugin(), review=True, context_epoch=0)
    assert "personal.md" not in guidance["files"]
    for relative, text in protected.items():
        assert (workspace / relative).read_bytes() == text.encode("utf-8")
    blobs = BlobStore(tmp_path / "blobs")
    published = blobs.get_json(blobs.snapshot(workspace))["files"]
    assert {item["path"] for item in published} == {"AGENTS.md", "library/source.md", "draft.md"}


@pytest.mark.parametrize("name", [
    "../escape.md", "sub/guide.md", r"sub\guide.md", "/absolute.md", "C:/outside.md",
    "alternate:stream.md", "guide.txt", "guide.md\x00", "guide.md\n", "UPPER.md",
    "worker.md", "review.md", "worker-operations.md", "shared-dags.md",
    "con.md", "nul.md", "com1.md", "lpt9.md", 5,
])
def test_domain_guides_reject_unsafe_or_reserved_names_before_writing(tmp_path, name):
    workspace = tmp_path / "workspace"
    with pytest.raises(ValueError):
        prepare_guidance(workspace, domain_guides({name: "# Must not escape\n"}),
                         review=False, context_epoch=0)
    assert not list(workspace.rglob("*.md"))
    assert not (tmp_path / "escape.md").exists()


@pytest.mark.parametrize("value", [None, ["guide.md"], {"domain.md": ""},
                                    {"domain.md": " \n\t"}, {"domain.md": 3}])
def test_invalid_domain_guide_content_cannot_partially_publish_guides(tmp_path, value):
    plugin = SimpleNamespace(worker_guides=lambda **_: value)
    with pytest.raises(ValueError):
        prepare_guidance(tmp_path, plugin, review=False, context_epoch=0)
    assert not list(tmp_path.rglob("*.md"))


@pytest.mark.parametrize("relative", [".", ".private", GUIDE_DIRECTORY])
def test_workspace_or_guide_directory_links_cannot_redirect_refresh(tmp_path, relative):
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "worker.md"
    sentinel.write_bytes(b"Outside data must be unchanged.\n")
    workspace = tmp_path / "workspace"
    linked = workspace if relative == "." else workspace / relative
    linked.parent.mkdir(parents=True, exist_ok=True)
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("The host does not allow directory symlinks")
    with pytest.raises(ValueError, match="links"):
        prepare_guidance(workspace, GeneralPlugin(), review=False, context_epoch=0)
    assert sentinel.read_bytes() == b"Outside data must be unchanged.\n"
    assert list(outside.iterdir()) == [sentinel]


@pytest.mark.parametrize("mode", ["message", "message_only"])
def test_correspondence_modes_have_their_own_short_anchor(tmp_path, mode):
    guidance = prepare_guidance(tmp_path, GeneralPlugin(), review=False, context_epoch=0,
                                execution_mode=mode)
    assert guidance["mode"] == mode
    anchor = guidance_anchor(guidance)
    assert f"Current mode: {mode}." in anchor
    # The required guide is standing text already; the anchor names the on-demand guides.
    assert f"{GUIDE_DIRECTORY}/worker-operations.md" in anchor
    assert f"{GUIDE_DIRECTORY}/worker.md" not in anchor
    assert "one-off correspondence" in anchor


def test_unchanged_continuations_omit_reading_and_reuse_existing_baseline(tmp_path):
    workspace = tmp_path / "workspace"
    guidance = prepare_guidance(workspace, LeanPlugin(), review=True, context_epoch=1)
    store = ContinuationPromptStore(tmp_path / "state")
    first = prepare_prompt(store, guidance)
    assert first.mode == "full"
    assert json.loads(first.text)["reading"] == reading_view(guidance)
    confirm_prompt(store, first)
    # The on-demand paths stay in the standing anchor, so an unchanged continuation repeats nothing.
    anchor = guidance_anchor(guidance)
    assert all(f"{GUIDE_DIRECTORY}/{name}" in anchor for name in guidance["optional_files"])
    for _ in range(3):
        store = ContinuationPromptStore(tmp_path / "state")
        prepared = prepare_prompt(store, guidance)
        body = json.loads(prepared.text)
        assert prepared.mode == "delta"
        assert "reading" not in body
        assert body["instruction"].endswith("No new context changes or deliveries.")
        assert not body.get("changes") and not body.get("removed_fields")
        assert GUIDE_DIRECTORY not in prepared.text
        assert not any(sha in prepared.text for sha in digests(guidance))
        assert len(prepared.text) < len(first.text)
        confirm_prompt(store, prepared)

    baseline_files = list(store.root.glob("*.json"))
    assert len(baseline_files) == 1
    baseline = json.loads(baseline_files[0].read_text(encoding="utf-8"))
    assert baseline["snapshot"]["instruction_guidance"] == guidance
    assert {path.parent for path in (tmp_path / "state").rglob("*")
            if path.is_file()} == {store.root}


def test_changed_and_new_required_guides_arrive_in_standing_instructions_not_in_prompts(tmp_path):
    texts = {"domain.md": "# Domain\nVersion one.\n"}
    plugin = domain_guides(texts)
    workspace = tmp_path / "workspace"
    previous = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    assert "Version one." in standing_guides(plugin, review=False)
    store = ContinuationPromptStore(tmp_path / "state")
    confirm_prompt(store, prepare_prompt(store, previous))

    texts["domain.md"] = "# Domain\nVersion two.\n"
    texts["new-workflow.md"] = "# Newly applicable workflow\nRead the current source.\n"
    current = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    assert set(current["files"]) == {"worker.md", "domain.md", "new-workflow.md"}
    assert current["files"]["domain.md"] != previous["files"]["domain.md"]
    # The next activation's standing instructions carry every required guide's current text.
    standing = standing_guides(plugin, review=False)
    assert "Version two." in standing and "Read the current source." in standing
    assert "Version one." not in standing
    prepared = prepare_prompt(store, current)
    body = json.loads(prepared.text)
    assert prepared.mode == "delta"
    # So the prompt neither repeats nor requests them: it has nothing new of its own.
    assert "reading" not in body
    assert body["instruction"].endswith("No new context changes or deliveries.")
    assert not any(change["path"][0] == "instruction_guidance"
                   for change in body.get("changes", []))
    assert "Version two." not in prepared.text and "new-workflow.md" not in prepared.text
    confirm_prompt(store, prepared)
    assert "reading" not in json.loads(prepare_prompt(store, current).text)


def test_removed_guide_is_explicit_and_not_requested_again(tmp_path):
    retired = {"retired.md": "# Retired workflow\nFormer workflow.\n",
               "retired-operations.md": "# Retired operations\nFormer procedure.\n"}
    texts = {"domain.md": "# Domain\nCurrent workflow.\n", **retired}
    plugin = domain_guides(texts)
    workspace = tmp_path / "workspace"
    previous = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    assert "Former workflow." in standing_guides(plugin, review=False)
    assert f"{GUIDE_DIRECTORY}/retired-operations.md" in guidance_anchor(previous)
    store = ContinuationPromptStore(tmp_path / "state")
    confirm_prompt(store, prepare_prompt(store, previous))
    for name in retired:
        del texts[name]
    current = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    prepared = prepare_prompt(store, current)
    body = json.loads(prepared.text)
    reminder = body["reading"]
    # A removed on-demand guide is named; a removed required guide leaves the standing instructions.
    assert reminder["removed"] == [f"{GUIDE_DIRECTORY}/retired-operations.md"]
    assert f"{GUIDE_DIRECTORY}/retired-operations.md" not in reminder["on_demand"]
    assert "required" not in reminder and f"{GUIDE_DIRECTORY}/retired.md" not in prepared.text
    assert "Removed guides no longer apply." in reminder["instruction"]
    assert "No new context changes" not in body["instruction"]
    assert "Former workflow." not in standing_guides(plugin, review=False)
    assert "retired" not in guidance_anchor(current)
    for name, text in retired.items():
        assert (workspace / GUIDE_DIRECTORY / name).read_text(encoding="utf-8") == text
    confirm_prompt(store, prepared)
    assert "reading" not in json.loads(prepare_prompt(store, current).text)


def test_optional_procedures_refresh_without_becoming_required_or_exposing_hashes(tmp_path):
    texts = {"domain-operations.md": "# Domain operations\nOriginal local procedure.\n"}
    workspace = tmp_path / "workspace"
    plugin = domain_guides(texts)
    before = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    store = ContinuationPromptStore(tmp_path / "state")
    confirm_prompt(store, prepare_prompt(store, before))
    texts["domain-operations.md"] = "# Domain operations\nUpdated exact local procedure.\n"
    current = prepare_guidance(workspace, plugin, review=False, context_epoch=0)
    prepared = prepare_prompt(store, current)
    reading = json.loads(prepared.text)["reading"]
    assert current["files"] == before["files"]
    assert current["optional_files"] != before["optional_files"]
    relative = f"{GUIDE_DIRECTORY}/domain-operations.md"
    assert (workspace / relative).read_text(encoding="utf-8") == texts["domain-operations.md"]
    assert "required" not in reading and relative in reading["on_demand"]
    assert texts["domain-operations.md"].strip() not in standing_guides(plugin, review=False)
    assert texts["domain-operations.md"] not in prepared.text
    assert not any(sha in prepared.text for sha in current["optional_files"].values())
    confirm_prompt(store, prepared)
    assert "reading" not in json.loads(prepare_prompt(store, current).text)


def test_compaction_requests_no_reread_and_does_not_create_a_reading_receipt(tmp_path):
    workspace = tmp_path / "workspace"
    before = prepare_guidance(workspace, LeanPlugin(), review=True, context_epoch=2)
    # Compaction keeps the epoch, and the required guides are standing instructions that
    # compaction never removes: there is nothing to reread.
    current = prepare_guidance(workspace, LeanPlugin(), review=True, context_epoch=2, compacted=True)
    assert current["reason"] == "context_compacted" and "read_before_work" not in current
    assert current["files"] == before["files"]
    assert continuation_guidance(current, before) is None
    store = ContinuationPromptStore(tmp_path / "state")
    confirm_prompt(store, prepare_prompt(store, before))
    # A backend whose compaction drops inputs invalidates the baseline; its next input is full.
    store.invalidate("worker-a", reason="context_compacted")
    assert store.compaction_pending("worker-a")
    compacted = prepare_prompt(store, current)
    assert compacted.mode == "full"
    reading = json.loads(compacted.text)["reading"]
    assert reading == reading_view(current) == reading_view(before)
    assert set(reading) == {"on_demand", "instruction"}
    assert not set(reading["on_demand"]) & {f"{GUIDE_DIRECTORY}/{name}" for name in current["files"]}
    confirm_prompt(store, compacted)
    assert not store.compaction_pending("worker-a")
    normal = prepare_guidance(workspace, LeanPlugin(), review=True, context_epoch=2)
    later = prepare_prompt(store, normal)
    assert later.mode == "delta" and "reading" not in json.loads(later.text)


def test_another_conversation_gets_the_full_reading_list_without_inventing_a_read_receipt(tmp_path):
    guidance = prepare_guidance(tmp_path / "first-work", GeneralPlugin(),
                                review=False, context_epoch=0)
    store = ContinuationPromptStore(tmp_path / "state")
    confirm_prompt(store, prepare_prompt(store, guidance))
    other_guidance = prepare_guidance(tmp_path / "other-work", GeneralPlugin(),
                                      review=False, context_epoch=0)
    other = prepare_prompt(store, other_guidance, worker_id="other-worker", thread_id="other-thread")
    delivered = json.loads(other.text)["reading"]
    assert other.mode == "full"
    assert delivered == reading_view(guidance)
    assert delivered["on_demand"] == [f"{GUIDE_DIRECTORY}/{name}" for name in guidance["optional_files"]]
    assert "part of your standing instructions" in delivered["instruction"]
    assert "required" not in delivered and "read_before_work" not in delivered
    source = prepare_prompt(store, guidance)
    assert source.mode == "delta"


def test_missing_or_relocated_guidance_baseline_requests_current_files(tmp_path):
    current = prepare_guidance(tmp_path, GeneralPlugin(), review=False, context_epoch=0)
    for previous in (None, {**deepcopy(current), "directory": ".private/old-instructions"}):
        result = continuation_guidance(current, previous)
        assert result == current
        assert reading_view(result)["on_demand"] == [
            f"{GUIDE_DIRECTORY}/{name}" for name in current["optional_files"]]
        assert "read_before_work" not in result and "removed_guides" not in result


def test_instruction_delivery_uses_existing_file_tools_and_small_separate_prompts():
    assert {tool["name"] for tool in dynamic_tools()} == {"taskhub_actions", "taskhub_call"}
    names = {tool["name"] for tool in operation_tools() + workspace_tools()}
    assert {"taskhub_workspace_list", "taskhub_workspace_read"} <= names
    assert not any("instruction" in name or "guide" in name for name in names)
    # The required guides follow these rules in the standing instructions; nothing asks for a reread.
    assert "required guides" in WORKER_INSTRUCTIONS and "on-demand guide" in WORKER_INSTRUCTIONS
    assert "reading.required" not in WORKER_INSTRUCTIONS and "reread" not in WORKER_INSTRUCTIONS.lower()
    assert "instruction_guidance" not in WORKER_INSTRUCTIONS
    assert "taskhub_actions" in WORKER_INSTRUCTIONS and "taskhub_call" in WORKER_INSTRUCTIONS
    assert "instruction_guidance" not in DECISION_INSTRUCTIONS
    assert GUIDE_DIRECTORY not in DECISION_INSTRUCTIONS
    guides = guide_texts(LeanPlugin(), review=True)
    standing = WORKER_INSTRUCTIONS + LeanPlugin().worker_instructions()
    assert sum(len(text) for text in guides.values()) > 4 * len(standing)
    assert all(text not in standing and text not in DECISION_INSTRUCTIONS for text in guides.values())


@pytest.mark.parametrize("domain", ["general", "lean"])
async def test_standing_cheat_sheet_lists_each_available_operation_once_and_stays_bounded(tmp_path, domain):
    initialize(tmp_path, ProjectConfig(domain=domain))
    hub = Hub(tmp_path)
    try:
        catalog = WorkerRunner(hub, ScriptedHarness).actions
        for capability in sorted(_CAPABILITIES):
            sheet = catalog.cheat_sheet(capability)
            header, *lines = sheet.splitlines()
            assert header.startswith("Operations for taskhub_call") and "taskhub_actions" in header
            available = [item["operation"] for item in catalog.describe(capability=capability)["operations"]]
            listed = [line.removeprefix("- ").partition("(")[0] for line in lines]
            assert all(line.startswith("- ") for line in lines)
            assert sorted(listed) == sorted(available) and len(set(listed)) == len(listed)
            for operation, line in zip(listed, lines):
                schema = catalog.describe(capability=capability, operation=operation)["inputSchema"]
                signature, _, gist = line.partition("): ")
                required = schema.get("required", [])
                assert all(f"{name}: " in signature for name in required)
                assert all(re.search(rf"\[{name}(?:[≤≥][^\]]*)?\]", signature) for name in schema.get("properties", {})
                           if name not in required)
                # Every maximum is shown with its argument.
                for name, field in schema.get("properties", {}).items():
                    for key in ("maximum", "maxLength", "maxItems"):
                        if key in field:
                            assert re.search(rf"\b{name}(?:: \S+ )?≤{field[key]}\b", signature), (operation, name)
                assert gist.strip()
            assert "inputSchema" not in sheet and '"properties"' not in sheet
            assert len(sheet) < 7000, (capability, len(sheet))
            line_of = dict(zip(listed, lines))
            if capability == "worker":
                # A small object argument shows its fields, and an operation that ends the turn says so.
                assert "checkpoint: {summary, next_action, [input_refs]}" in line_of["pause"]
                assert "predicate: {[mode], [subscriptions], [validations], [answers]" in line_of["pause"]
                assert all(line_of[name].endswith("(ends the turn)") for name in ("pause", "contribute", "leave_task"))
                assert not line_of["stage"].endswith("(ends the turn)")
                # Bounds are shown, so a worker does not ask search for 30 results; usual minimums are not.
                assert line_of["search"].startswith("- search(query: string, [limit≤20]): ")
                assert "query: string ≤200 chars, [limit≤20]" in line_of["history_search"]
                assert "reason: string ≤4000 chars" in line_of["leave_task"]
                assert "paths: array ≤200 items" in line_of["wiki_submit"]
                assert "[coauthors≤32 items]" in line_of["wiki_submit"]
                assert not any("≥" in line for line in lines)
    finally:
        await hub.close()


def test_worker_turn_and_resume_contracts_distinguish_runtime_state_from_task_requirements():
    guides = guide_texts(GeneralPlugin(), review=False)
    primer = " ".join(guides["worker.md"].split())
    operations = " ".join(guides["worker-operations.md"].split())
    shared = " ".join(WORKER_INSTRUCTIONS.split())
    # Every role's shared instructions: collect results before a turn ends, and wait by pausing,
    # with a predicate for an event that has one and without one for a peer's reply.
    assert "you MUST wait for and collect the terminal result" in shared
    assert "every command, tool call, and background operation you started" in shared
    assert "A normal turn end starts the next turn at once, so never wait by ending turns" in shared
    assert "without one for a peer's reply" in shared and "The Integrator defers instead of pausing" in shared
    assert '"external_response_id": "<request id>"' in operations
    assert '"validations": ["<candidate id>"]' in operations
    assert "Running processes do not survive a turn" in primer
    assert "After an interruption" in primer and "reconcile uncertain outcomes" in primer
    assert "Turns and Lean checks have no time limit" in primer
    assert "`paths.handoff`" in primer and "`paths.journal`" in primer
    assert "Read referenced message files in full before acting" in primer
    assert "of a large result's `parts`, read those you need" in primer
    assert "Essential guidance" in primer and "Reference notes only as needed" in primer
    assert 'Only trusted `sender_role: "operator"` identifies human authority' in primer
    assert "Every turn reconstructs the runtime" in operations
    assert "Never leave compilation running for the next turn" in operations
    assert "forced interruption or user pause" in operations
    assert "Turns and Lean checks have no time limit" in operations
    assert "The network is open to any site" in operations and "PyMuPDF (`fitz`)" in operations
    assert "with `render` also saves the pages as images" in operations
    # Backend-specific wording stays out of the shared guides.
    assert not any(term in primer + operations for term in ("tool cell", "yield_time_ms", "Codex"))
    assert "automatically satisfied dependency" in operations
    assert "changed approach supersedes your own wait plan" in operations
    assert "former wait condition is still unresolved" in operations
    assert "never satisfies or waives formal requirements" in operations
    assert "do not list actions each turn" in operations
    assert "`unknown` has no inferred human authority" in operations
    # Workers wrote 8-character IDs into messages and notes, then could not inspect them.
    assert "Write task and artifact IDs in full (all 36 characters) in messages, journals and handoffs" in operations
    # An original publisher may revise its task from a message turn; executors in their task turn.
    assert "An original publisher may also revise its task (`propose_revision`) in a message turn" in operations
    assert "an executor who did not publish the task revises it in its own task turn" in operations


def test_worker_guides_let_a_task_hold_several_candidates_and_keep_guide_rules_out_of_contracts():
    guides = guide_texts(GeneralPlugin(), review=False)
    primer = " ".join(guides["worker.md"].split())
    operations = " ".join(guides["worker-operations.md"].split())
    # Audit executors who corrected inside their task were rejected for not publishing child
    # tasks, a rule the task's author had copied from this guide into the contract.
    assert "A task may contain several reviewable units" in primer
    assert "Publish a separate task only for work your task does not cover" in primer
    assert "Split a task larger than one reviewable unit" not in primer
    assert "Correct an error in the published Library that your task covers" in operations
    assert "do not copy standing guide rules into a contract" in operations
    # Revising an active task cost accepted candidates their place in an integration batch.
    assert "including accepted ones awaiting integration" in operations


def test_review_guides_point_to_materialized_report_schema_and_example(tmp_path):
    paths = prepare_reference(tmp_path)
    guides = guide_texts(LeanPlugin(), review=True)
    lean_review = guides["lean-review-operations.md"]
    assert "ORIGINAL literature" in guides["lean-review.md"]
    assert "[Lean review operations](lean-review-operations.md)" in guides["lean-review.md"]
    assert "paths.reference_index" in lean_review
    for name in ("semantic_review_guide", "semantic_review_schema", "semantic_review_example"):
        assert name in lean_review
        assert (tmp_path / paths[name]).is_file()
    assert not {"final_semantic_schema", "final_semantic_example"} & paths.keys()
    assert "path:line" in lean_review and "path:start-end" in lean_review
    # Reviewers cited imported modules, or several ranges in one location.
    reference = " ".join((tmp_path / paths["semantic_review_guide"]).read_text(encoding="utf-8").split())
    schema = json.loads((tmp_path / paths["semantic_review_schema"]).read_text(encoding="utf-8"))
    rule = schema["$defs"]["CorrespondenceFinding"]["properties"]["lean_location"]["description"]
    for text in (" ".join(lean_review.split()), reference):
        assert "is in the candidate's primary pair (`review.candidate.lean`" in text
        assert "never another file or an imported module, which you name in the finding text instead" in text
        assert "each further range needs another finding" in text
    assert "never another file or an imported module" in rule and "another finding for each further range" in rule


def test_statement_recipe_is_a_valid_inspecting_check(tmp_path):
    paths = prepare_reference(tmp_path)
    guide = (tmp_path / paths["submission_guide"]).read_text(encoding="utf-8")
    examples = [json.loads(text) for text in re.findall(r"```json\s*(.*?)\s*```", guide, re.DOTALL)]
    check = next(value for value in examples if "environment_artifact" in value)
    parsed = CheckArguments.model_validate(check)
    assert parsed.declaration == "Project.selectedStatement"
    assert parsed.modules[-1] == "Project.Statement"
    flattened = " ".join(guide.split())
    assert "returned `ok` must be `true`" in flattened
    assert "`inspection.statement_returns_prop` must be `true`" in flattened


def test_task_format_names_the_project_domain(tmp_path):
    paths = prepare_reference(tmp_path)
    guide = " ".join((tmp_path / paths["submission_guide"]).read_text(encoding="utf-8").split())
    assert "A contract has `title`, `objective`, `domain`, `kind`," in guide
    assert "`domain` is the project's, `\"lean\"`; a task file without it gets it." in guide


def test_new_note_guidance_requires_source_files_and_universal_publication(tmp_path):
    paths = prepare_reference(tmp_path)
    submission = (tmp_path / paths["submission_guide"]).read_text(encoding="utf-8")
    assert "wiki/README.md" in submission and "wiki_submit" in submission
    assert "edited Markdown `paths`" in submission
    assert "No inline note bodies" in submission
    assert "independent review" in submission.lower()
    review = " ".join(guide_texts(GeneralPlugin(), review=True)["review-operations.md"].split())
    assert "reviewed contributions have source files to review and integrate" in review
    assert "shared planning DAGs" in review and "need no extra review" in review


REMOVED_FORMAT_NAMES = ("source_artifact", "source_version", "binding_artifact", "statement-binding",
                        "lean_stage_record", "lean_import_source", "lean_snapshot", "target_type_sha256",
                        "statement_artifact", "statement_version", "native_binding", "library_membership",
                        "merge_prepare", "merge_validate", "merge_publish")


def test_packaged_guides_name_no_removed_record_formats():
    """Workers and reviewers follow these files; a removed field name makes their submission fail."""
    package = Path(formal_geyser.__file__).parent
    texts = {str(path.relative_to(package)): path.read_text(encoding="utf-8") for path in package.rglob("*.md")}
    texts.update(worker_instructions=WORKER_INSTRUCTIONS, decision_instructions=DECISION_INSTRUCTIONS)
    assert "semantic-review-guide.md" in texts and "submission-guide.md" in texts
    assert [(name, term) for name, text in texts.items() for term in REMOVED_FORMAT_NAMES if term in text] == []
