"""Carrying an instance's core to a fresh one: open tasks keep their ids; no worker comes along."""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from formal_geyser.coordination.cli import app
from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.core_transfer import (
    CONTEXT_LIMIT,
    NOTE_HEADER,
    STARTER_PAGES,
    _check_library,
    carried_contract,
    export_core,
    import_core,
)
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import ProjectConfig
from formal_geyser.coordination.storage import canonical
from tests.coordination.publication_helpers import _command, new_hub

LIBRARY = {"Notes/Intro.md": b"# The Library\n", "results/seed.json": b"{}\n"}
DRAFT = {"Draft/Plan.lean": b"def plan : Nat := 1\n", "Draft/Plan.md": b"# Plan\nThe first step.\n"}
DAGS = {name: (canonical({"nodes": [], "edges": []}) + "\n").encode("utf-8")
        for name in ("definitions.json", "proofs.json")}


async def publish(hub, title, **contract):
    return await _command(hub, "task.publish", {"contract": {
        "title": title, "objective": f"Do {title.lower()}.", **contract}})


async def install_draft(hub, task_id):
    reference = hub.draft_files.install(task_id, 1, 1, "worker-1", "Worker 1", "First plan", DRAFT)
    await hub.db.call(lambda c: c.execute("INSERT INTO native_draft_heads VALUES(?,?,?,?)", (
        task_id, 1, reference["content_key"], reference["manifest_sha256"])), write=True)
    hub.draft_files.point(reference)


async def install_dags(hub):
    await hub.reconcile_dags()
    reference = hub.dag_files.install(1, DAGS, summary="Planned.")
    await hub.db.call(lambda c: c.execute(
        "UPDATE native_dag_head SET revision=?,content_key=?,manifest_sha256=? WHERE id=1",
        (1, reference["content_key"], reference["manifest_sha256"])), write=True)
    hub.dag_files.point(reference)


@pytest.fixture
async def exported(tmp_path):
    source = await new_hub(tmp_path / "old", files=LIBRARY)
    try:
        root = await publish(source, "Program root")
        child = await publish(source, "Child", context=f"Builds on {root['id']}.", requirements=[
            {"key": "root-result", "task_id": root["id"], "task_revision": 1}])
        finished = await publish(source, "Finished")
        waiting = await publish(source, "Waits on the finished task", requirements=[
            {"key": "done-result", "task_id": finished["id"], "task_revision": 1}])
        review = await publish(source, "Looks like a review")

        def history(c):
            c.execute("UPDATE tasks SET state='done' WHERE id=?", (finished["id"],))
            # A review goes with its candidate, which stays behind.
            row = c.execute("SELECT contract_json FROM task_revisions WHERE task_id=?", (review["id"],)).fetchone()
            c.execute("UPDATE task_revisions SET contract_json=? WHERE task_id=?",
                      (json.dumps({**json.loads(row[0]), "kind": "review"}), review["id"]))
            c.execute("UPDATE task_origins SET source_task_id=? WHERE task_id=?", (root["id"], child["id"]))
        await source.db.call(history, write=True)
        await _command(source, "task.set_priority", {"task_id": child["id"], "priority": 0})
        await _command(source, "task.set_pinned", {"task_id": waiting["id"], "pinned": True})
        await install_draft(source, root["id"])
        await install_dags(source)
        local = tmp_path / "scan.pdf"
        local.write_bytes(b"%PDF a source the project keeps a copy of")
        central = tmp_path / "central" / "book.pdf"
        central.parent.mkdir()
        central.write_bytes(b"%PDF a source in the central collection")
        await _command(source, "literature.register", {"source_id": "local-scan", "path": str(local),
                                                      "title": "Local scan", "copy": True})
        await _command(source, "literature.register", {"source_id": "central-book", "path": str(central),
                                                      "title": "Central book"})
        await source.public_content()
        summary = export_core(tmp_path / "old", tmp_path / "bundle")
    finally:
        await source.close()
    return tmp_path / "bundle", summary, {"root": root, "child": child, "finished": finished,
                                          "waiting": waiting, "review": review}


async def test_the_export_holds_the_open_work_tasks_and_what_they_name(exported):
    bundle, summary, tasks = exported
    assert summary["tasks"] == 3 and summary["drafts"] == 1 and summary["dag_revision"] == 1
    assert summary["literature"] == 2 and summary["literature_bundled"] == 1
    assert summary["root_task_id"] == tasks["root"]["id"] and summary["history_tasks"] == 5
    carried = {entry["id"] for entry in json.loads((bundle / "tasks.json").read_text(encoding="utf-8"))}
    assert carried == {tasks[name]["id"] for name in ("root", "child", "waiting")}
    references = json.loads((bundle / "references.json").read_text(encoding="utf-8"))
    assert references["tasks"][tasks["finished"]["id"]]["state"] == "done"
    library = json.loads((bundle / "library.json").read_text(encoding="utf-8"))
    assert {item["path"] for item in library["files"]} >= set(LIBRARY)
    assert (bundle / "drafts" / tasks["root"]["id"] / "Draft/Plan.lean").read_bytes() == DRAFT["Draft/Plan.lean"]


async def test_a_fresh_instance_takes_the_core_with_the_same_task_ids(exported, tmp_path, monkeypatch):
    bundle, _, tasks = exported
    target = await new_hub(tmp_path / "new", files=LIBRARY)
    try:
        summary = await import_core(target, bundle)
        assert summary["tasks"] == 3 and summary["drafts"] == 1 and summary["dag_revision"] == 1
        assert summary["literature_skipped"] == []
        root, child, waiting = [await target.task(tasks[name]["id"]) for name in ("root", "child", "waiting")]
        assert {root["revision"], child["revision"], waiting["revision"]} == {1}
        # A carried requirement names the carried task at its new revision; its words still hold.
        assert child["contract"]["requirements"] == [{"key": "root-result", "task_id": root["id"], "task_revision": 1,
                                                      "artifact_id": None}]
        assert f"Builds on {root['id']}." in child["contract"]["context"]
        # A requirement on a finished task is met: removed, and the context says what it was.
        assert waiting["contract"]["requirements"] == []
        assert NOTE_HEADER in waiting["contract"]["context"] and "done in the previous instance" in waiting["contract"]["context"]

        def state(c):
            return {"root": c.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()[0],
                    "origin": c.execute("SELECT source_task_id FROM task_origins WHERE task_id=?", (child["id"],)).fetchone()[0],
                    "admission": {r[0]: (r[1], r[2] is not None) for r in c.execute("SELECT * FROM task_admission")},
                    "draft": c.execute("SELECT revision FROM native_draft_heads WHERE task_id=?", (root["id"],)).fetchone()[0],
                    "dag": c.execute("SELECT revision FROM native_dag_head WHERE id=1").fetchone()[0],
                    "literature": sorted(r[0] for r in c.execute("SELECT id FROM literature_entries")),
                    "workers": c.execute("SELECT count(*) FROM workers").fetchone()[0],
                    "runs": c.execute("SELECT count(*) FROM runs").fetchone()[0]}
        found = await target.db.call(state)
        assert found["root"] == root["id"] and found["origin"] == root["id"]
        assert found["admission"][child["id"]] == (0, False) and found["admission"][waiting["id"]] == (None, True)
        assert found["draft"] == 1 and found["dag"] == 1
        assert found["literature"] == ["central-book", "local-scan"]
        assert found["workers"] == 0 and found["runs"] == 0  # No worker comes along; no run starts.
        draft = json.loads((target.root / "drafts" / root["id"] / "current.json").read_text(encoding="utf-8"))
        assert draft["revision"] == 1
        assert {name: (Path(draft["root"]) / name).read_bytes() for name in DRAFT} == DRAFT
        assert (target.state_dir / "core-transfer" / summary["bundle_id"] / "history-tasks.jsonl").is_file()
        # Run again after a failure part-way, even on a later day, it finishes without doing anything twice.
        real = time.strftime
        monkeypatch.setattr(time, "strftime", lambda form, *rest: "2099-01-01" if form == "%Y-%m-%d" else real(form, *rest))
        again = await import_core(target, bundle)
        assert again["tasks"] == 3 and again["drafts"] == 0
        assert await target.db.call(lambda c: c.execute("SELECT count(*) FROM tasks").fetchone()[0]) == 3
    finally:
        await target.close()


async def test_an_instance_with_records_of_its_own_is_refused(exported, tmp_path):
    bundle, _, _ = exported
    target = await new_hub(tmp_path / "busy", files=LIBRARY)
    try:
        await publish(target, "Its own task")
        with pytest.raises(HubError) as caught:
            await import_core(target, bundle)
        assert caught.value.code == "CORE_TARGET_NOT_EMPTY"
    finally:
        await target.close()


async def test_only_the_operator_chooses_a_new_tasks_id(tmp_path):
    hub = await new_hub(tmp_path / "ids", files=LIBRARY)
    try:
        kept = "0f1e2d3c-4b5a-4678-9abc-def012345678"
        task = await _command(hub, "task.publish", {"task_id": kept, "contract": {"title": "Kept", "objective": "Keep."}})
        assert task["id"] == kept
        for task_id, code in ((kept, "TASK_EXISTS"), ("Not-A-UUID", "INVALID_INPUT")):
            with pytest.raises(HubError) as caught:
                await _command(hub, "task.publish", {"task_id": task_id, "deduplicate": False,
                                                    "contract": {"title": "Other", "objective": "Other."}})
            assert caught.value.code == code
    finally:
        await hub.close()


def test_a_carried_contract_names_what_stayed_behind_and_binds_the_new_environment():
    carried = {"task-1", "parent-1"}
    references = {
        "artifacts": {
            "env-old": {"kind": "lean.environment", "status": "accepted", "title": None, "published_commit": None, "paths": []},
            "stmt-1": {"kind": "lean.statement", "status": "accepted", "title": "State it",
                       "published_commit": "abc", "paths": ["Langlands/A.lean", "Langlands/A.md"]},
            "def-2": {"kind": "lean.definition", "status": "staged", "title": "Define it",
                      "published_commit": None, "paths": []}},
        "tasks": {"retired-1": {"state": "retired", "title": "Old route", "revision": 2}}}
    entry = {"id": "task-1", "revision": 3, "state": "waiting", "contract": {
        "title": "Prove it", "objective": "Prove it.", "context": "Earlier notes.",
        "inputs": ["env-old", "stmt-1"],
        "requirements": [{"key": "statement", "artifact_id": "stmt-1", "policy": "lean.statement@1"},
                         {"key": "definition", "artifact_id": "def-2"},
                         {"key": "route", "task_id": "retired-1", "task_revision": 2}],
        "payload": {"environment_artifact": "env-old", "parent_task_id": "parent-1", "parent_task_revision": 4}}}
    contract = carried_contract(entry, carried, references, {"env-old"}, "2026-11-08")
    assert contract["inputs"] == [] and contract["requirements"] == []
    assert contract["payload"] == {"parent_task_id": "parent-1", "parent_task_revision": 1}
    context = contract["context"]
    assert context.startswith("Earlier notes.") and "revision 3, waiting" in context
    assert "published in the Library this instance starts from (Langlands/A.lean, Langlands/A.md)" in context
    assert "staged and not carried over" in context and "retired in the previous instance" in context
    crowded = carried_contract({**entry, "contract": {**entry["contract"], "context": "x" * (CONTEXT_LIMIT - 300)}},
                               carried, references, {"env-old"}, "2026-11-08")
    assert len(crowded["context"]) <= CONTEXT_LIMIT


async def test_a_lean_instance_must_start_from_the_bundles_library(exported, tmp_path):
    bundle, _, _ = exported
    hub = await new_hub(tmp_path / "lean", files=LIBRARY)
    try:
        with pytest.raises(HubError) as caught:
            _check_library(hub, bundle)
        assert caught.value.code == "CORE_TARGET_NOT_READY"
        library = json.loads((bundle / "library.json").read_text(encoding="utf-8"))["files"]
        marker = hub.state_dir / "initialization.json"

        def initialized(files):
            # What `taskctl init --from-library` records: the sources it took, the starter pages it added.
            entries = [{"path": path, "sha256": hub.blobs.put(body)} for path, body in files.items()]
            snapshot = hub.blobs.put_json({"files": sorted(entries, key=lambda item: item["path"])})
            marker.write_text(json.dumps({"format": 1, "state": "ready", "initial_snapshot": snapshot,
                                          "environment_artifact": "env-new"}), encoding="utf-8")
        sources = {item["path"]: (bundle / "library" / item["path"]).read_bytes() for item in library
                   if item["path"].endswith((".lean", ".md"))}
        starter = {path: b"# Starter page\n" for path in STARTER_PAGES if path not in sources}
        initialized({**sources, **starter, "lean-toolchain": b"leanprover/lean4:v4.34.0\n"})
        assert _check_library(hub, bundle) == "env-new"
        initialized({**sources, "Notes/Intro.md": b"# Another Library\n"})
        with pytest.raises(HubError) as caught:
            _check_library(hub, bundle)
        assert caught.value.code == "CORE_LIBRARY_MISMATCH" and caught.value.details["changed"] == ["Notes/Intro.md"]
    finally:
        await hub.close()


def test_taskctl_exports_and_imports_a_core(tmp_path):
    runner = CliRunner()
    old, new, bundle = tmp_path / "old", tmp_path / "new", tmp_path / "bundle"
    initialize(old, ProjectConfig())
    contract = tmp_path / "contract.json"
    contract.write_text(json.dumps({"title": "Open work", "objective": "Do the open work."}), encoding="utf-8")
    published = runner.invoke(app, ["--project", str(old), "tasks", "publish", str(contract)])
    assert published.exit_code == 0, published.output
    task_id = json.loads(published.stdout)["id"]
    exported = runner.invoke(app, ["--project", str(old), "export-core", str(bundle)])
    assert exported.exit_code == 0, exported.output
    assert json.loads(exported.stdout)["tasks"] == 1 and json.loads(exported.stdout)["library_commit"] is None
    initialize(new, ProjectConfig())
    imported = runner.invoke(app, ["--project", str(new), "import-core", str(bundle)])
    assert imported.exit_code == 0, imported.output
    listed = runner.invoke(app, ["--project", str(new), "tasks", "list"])
    assert [task["id"] for task in json.loads(listed.stdout)] == [task_id]
    # The bundle is written once, outside the project it describes.
    for destination in (bundle, old / "bundle"):
        refused = runner.invoke(app, ["--project", str(old), "export-core", str(destination)])
        assert refused.exit_code == 1
