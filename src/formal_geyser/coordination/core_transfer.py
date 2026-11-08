"""Carry an instance's core to a fresh instance: its open tasks, its current Library, their drafts.

`export_core` reads an instance (it may be serving) and writes a bundle directory:

- `library/` and `library.json`: the files of the current published Library, with their hashes;
- `tasks.json`: every unfinished work task, its current contract, its operator priority and
  pin, and the task it came from (reviews and the Integrator's task go with their candidates and
  batches, which stay behind);
- `drafts/<task>/`: each carried task's current shared draft;
- `dags/`: the current shared planning DAGs;
- `literature.json` and `literature/`: every registered source, with the project's own copies;
- `references.json`: the artifacts and unfinished-elsewhere tasks the carried contracts name, so
  the import can say what each was;
- `history/tasks.jsonl`: every task of the old instance, read-only, for the operator;
- `project.toml`: the old settings, for reference only; `core.json`: what the bundle holds.

`import_core` seeds a fresh, empty instance from a bundle, with the service stopped: the
instance must have been initialized from the bundle's `library/` (`taskctl init --from-library`).
Each task keeps its id, so what its words, the DAGs and its draft say of other carried tasks
holds; it starts again at revision 1 with the new instance's environment. A requirement or
input that names something the new instance does not have (an artifact, a finished or
unfinished-elsewhere task) is removed, and the task's context says what it was. Workers,
conversations, workspaces, candidates, reviews, evidence and history stay with the old instance;
no run is started.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..registry import read_snapshot
from .errors import HubError
from .models import Actor, Command, Role
from .wiki import _TEMPLATES as STARTER_PAGES

FORMAT = 1
OPERATOR = Actor(role=Role.OPERATOR)
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
CONTEXT_LIMIT = 40000  # TaskContract.context
NOTE_HEADER = "Carried over from the previous FormalGeyser instance"


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _copy_tree(source: Path, target: Path) -> list[dict[str, str]]:
    """Copy a tree's regular files (no links); their relative paths and hashes."""
    files = []
    for path in sorted(source.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        relative = path.relative_to(source).as_posix()
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        body = path.read_bytes()
        destination.write_bytes(body)
        files.append({"path": relative, "sha256": _sha(body)})
    return files


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def export_core(project: Path, destination: Path) -> dict:
    """Write `project`'s core to the new directory `destination` (see the module)."""
    project = project.resolve()
    state = project / ".task-based"
    destination = destination.resolve()
    if destination.exists() and any(destination.iterdir()):
        raise ValueError(f"{destination} is not empty")
    if destination.is_relative_to(project):
        raise ValueError("Write the bundle outside the project")
    destination.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(f"file:{state / 'state.sqlite3'}?mode=ro", uri=True, timeout=60)
    db.row_factory = sqlite3.Row
    db.execute("BEGIN")  # One snapshot of the ledger for the whole export.
    try:
        summary = _export(project, state, destination, db)
    finally:
        db.rollback()
        db.close()
    _write_json(destination / "core.json", summary)
    return summary


def _export(project: Path, state: Path, destination: Path, db: sqlite3.Connection) -> dict:
    current = {row["id"]: dict(row) for row in db.execute(
        "SELECT t.id,t.state,t.current_revision,t.created_at,r.contract_json FROM tasks t JOIN task_revisions r "
        "ON r.task_id=t.id AND r.revision=t.current_revision ORDER BY t.created_at,t.id")}
    origins = {row[0]: row[1] for row in db.execute("SELECT task_id,source_task_id FROM task_origins")}
    contracts = {tid: json.loads(row["contract_json"]) for tid, row in current.items()}
    carried = [tid for tid, row in current.items()
               if row["state"] not in ("done", "retired") and contracts[tid].get("kind", "work") == "work"]
    carried_set = set(carried)
    admission = {row["task_id"]: dict(row) for row in db.execute("SELECT * FROM task_admission")}
    root = db.execute("SELECT root_task_id FROM project_lifecycle WHERE id=1").fetchone()

    # What the carried contracts name: artifacts, and tasks that are not carried.
    named_artifacts: set[str] = set()
    named_tasks: set[str] = set()
    for tid in carried:
        contract = contracts[tid]
        named_artifacts.update(contract.get("inputs", []))
        for requirement in contract.get("requirements", []):
            if requirement.get("artifact_id"):
                named_artifacts.add(requirement["artifact_id"])
            if requirement.get("task_id"):
                named_tasks.add(requirement["task_id"])
        payload = contract.get("payload") or {}
        for key in ("parent_task_id",):
            if payload.get(key):
                named_tasks.add(payload[key])
        for key in ("environment_artifact", "subject_artifact"):
            if payload.get(key):
                named_artifacts.add(payload[key])
    artifacts: dict[str, dict[str, Any]] = {}
    for row in db.execute("SELECT id,kind,status,task_id FROM artifacts"):
        if row["id"] in named_artifacts:
            artifacts[row["id"]] = dict(row)
    published = {row[0]: row[1] for row in db.execute("SELECT candidate_id,commit_sha FROM repository_publications")}
    paths = {row[0]: json.loads(row[1]) for row in db.execute("SELECT artifact_id,paths_json FROM candidate_repositories")}
    for artifact_id, record in artifacts.items():
        record["title"] = contracts.get(record["task_id"], {}).get("title")
        record["published_commit"] = published.get(artifact_id)
        record["paths"] = paths.get(artifact_id, [])
    tasks_named = {tid: {"state": current[tid]["state"], "title": contracts[tid].get("title"),
                         "revision": current[tid]["current_revision"]}
                   for tid in sorted(named_tasks - carried_set) if tid in current}
    environments = sorted(aid for aid, record in artifacts.items() if record["kind"] == "lean.environment")
    _write_json(destination / "references.json", {"artifacts": artifacts, "tasks": tasks_named})

    # The carried tasks, with their drafts.
    heads = {row["task_id"]: dict(row) for row in db.execute("SELECT * FROM native_draft_heads")}
    records: list[dict[str, Any]] = []
    drafts = 0
    for tid in carried:
        entry = {"id": tid, "state": current[tid]["state"], "revision": current[tid]["current_revision"],
                 "created_at": current[tid]["created_at"], "contract": contracts[tid],
                 "origin_task_id": origins.get(tid), "priority": None, "pinned_at": None, "draft": None}
        if tid in admission:
            entry["priority"] = admission[tid]["priority"]
            entry["pinned_at"] = admission[tid]["pinned_at"]
        head = heads.get(tid)
        if head is not None:
            source = project / "drafts" / tid / f"revision-{head['revision']}-{head['content_key']}"
            manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
            files = _copy_tree(source / "files", destination / "drafts" / tid)
            entry["draft"] = {"revision": head["revision"], "summary": manifest.get("summary") or "",
                              "files": files}
            drafts += 1
        records.append(entry)
    _write_json(destination / "tasks.json", records)

    # The shared planning DAGs.
    dag = db.execute("SELECT revision,content_key FROM native_dag_head WHERE id=1").fetchone()
    dag_revision = None
    if dag is not None:
        source = project / "graphs" / f"revision-{dag['revision']}-{dag['content_key']}" / "files"
        _copy_tree(source, destination / "dags")
        dag_revision = dag["revision"]

    # Literature: the project's own copies travel; the rest is named by its path.
    literature: list[dict[str, Any]] = []
    bundled = 0
    for row in db.execute("SELECT id,path FROM literature_entries ORDER BY id"):
        record = json.loads((project / row["path"]).read_text(encoding="utf-8"))
        source = Path(record["source_path"])
        if not source.is_absolute():
            source = project / source
        local: str | None = None
        if source.resolve().is_relative_to(project):
            local = source.resolve().relative_to(project).as_posix()
            target = destination / "literature" / local
            target.parent.mkdir(parents=True, exist_ok=True)
            if source.is_dir():
                shutil.copytree(source, target, dirs_exist_ok=True)
            elif source.is_file():
                shutil.copyfile(source, target)
            else:
                local = None
            bundled += local is not None
        literature.append({"id": record["id"], "title": record["title"], "citation": record.get("citation", ""),
                           "text": record.get("text") or record["title"], "uri": record.get("uri"),
                           "version": record.get("version"), "source_sha256": record.get("source_sha256"),
                           "source_path": record["source_path"], "bundled_path": local})
    _write_json(destination / "literature.json", literature)

    # The current published Library, if any was published.
    current_path = project / "library" / "current.json"
    pointer: dict[str, Any] = (json.loads(current_path.read_text(encoding="utf-8")) if current_path.is_file()
                               else {"commit": None})
    library = _copy_tree(project / "library" / pointer["root"], destination / "library") if "root" in pointer else []
    (destination / "library").mkdir(exist_ok=True)
    _write_json(destination / "library.json", {"commit": pointer["commit"], "files": library})

    # Every task, read-only, for the operator; and the old settings.
    history = destination / "history" / "tasks.jsonl"
    history.parent.mkdir(parents=True, exist_ok=True)
    with history.open("w", encoding="utf-8") as stream:
        for tid, row in current.items():
            stream.write(json.dumps({"id": tid, "state": row["state"], "revision": row["current_revision"],
                                     "created_at": row["created_at"], "kind": contracts[tid].get("kind", "work"),
                                     "title": contracts[tid].get("title"), "origin_task_id": origins.get(tid)},
                                    ensure_ascii=False) + "\n")
    if (project / "project.toml").is_file():
        shutil.copyfile(project / "project.toml", destination / "project.toml")
    return {"format": FORMAT, "bundle_id": uuid4().hex, "exported_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "source": str(project), "library_commit": pointer["commit"], "library_files": len(library),
            "tasks": len(records), "drafts": drafts, "dag_revision": dag_revision,
            "literature": len(literature), "literature_bundled": bundled,
            "root_task_id": root[0] if root is not None else None, "environments": environments,
            "history_tasks": len(current)}


def _describe_artifact(artifact_id: str, references: dict) -> str:
    record = references["artifacts"].get(artifact_id)
    if record is None:
        return f"artifact {artifact_id} (unknown to the previous instance's ledger)"
    title = f" from task \"{record['title']}\"" if record.get("title") else ""
    if record.get("published_commit"):
        where = ", ".join(record["paths"]) or "the Library"
        return (f"{record['kind']} {artifact_id}{title}, published in the Library this instance starts from "
                f"({where})")
    return f"{record['kind']} {artifact_id}{title}, {record['status']} and not carried over (it was never published)"


def _describe_task(task_id: str, references: dict) -> str:
    record = references["tasks"].get(task_id)
    if record is None:
        return f"task {task_id} (not in the previous instance)"
    if record["state"] == "done":
        return (f"task {task_id} \"{record['title']}\", done in the previous instance (what it published "
                f"is in the Library this instance starts from)")
    return f"task {task_id} \"{record['title']}\", {record['state']} in the previous instance and not carried over"


def carried_contract(entry: dict, carried: set[str], references: dict, environments: set[str],
                     moved_on: str) -> dict:
    """The contract a carried task is published with in the new instance (see the module)."""
    contract = dict(entry["contract"])
    notes: list[str] = []
    payload = dict(contract.get("payload") or {})
    if payload.get("environment_artifact") in environments:
        del payload["environment_artifact"]  # The new instance binds its own.
    for key in ("subject_artifact",):
        if payload.get(key):
            notes.append(f"Its {key.replace('_', ' ')} was {_describe_artifact(payload.pop(key), references)}.")
    if payload.get("parent_task_id"):
        if payload["parent_task_id"] in carried:
            payload["parent_task_revision"] = 1
        else:
            notes.append(f"Its parent was {_describe_task(payload.pop('parent_task_id'), references)}.")
            payload.pop("parent_task_revision", None)
    inputs: list[str] = []  # Every input names a record of the previous instance.
    for item in contract.get("inputs", []):
        if item in environments:
            continue
        notes.append(f"Input {_describe_artifact(item, references)}.")
    requirements: list[dict[str, Any]] = []
    for requirement in contract.get("requirements", []):
        requirement = {key: value for key, value in requirement.items() if key != "policy"}
        if requirement.get("task_id"):
            if requirement["task_id"] in carried:
                requirements.append({**requirement, "task_revision": 1})
            else:
                notes.append(f"Requirement {requirement['key']}: "
                             f"{_describe_task(requirement['task_id'], references)}; removed.")
        elif requirement.get("artifact_id"):
            notes.append(f"Requirement {requirement['key']}: "
                         f"{_describe_artifact(requirement['artifact_id'], references)}; removed.")
    contract.update(payload=payload, inputs=inputs, requirements=requirements)
    header = (f"{NOTE_HEADER} on {moved_on}: this task keeps its id and starts again at revision 1 "
              f"(it was revision {entry['revision']}, {entry['state']}). Its workers, their conversations and "
              f"workspaces, its candidates and reviews stayed behind; its shared draft and the shared DAGs "
              f"came along. Ids of other carried tasks still name them here; any other id names a record "
              f"of the previous instance.")
    context = contract.get("context") or ""
    block = "\n\n" + header + ("\n" + "\n".join("- " + note for note in notes) if notes else "")
    room = CONTEXT_LIMIT - len(context)
    if len(block) > room:
        kept: list[str] = []
        for note in notes:
            candidate = "\n\n" + header + "\n" + "\n".join("- " + n for n in [*kept, note])
            if len(candidate) + 80 > room:
                break
            kept.append(note)
        block = "\n\n" + header + "\n" + "\n".join("- " + n for n in kept)
        block += f"\n- and {len(notes) - len(kept)} more (see references.json in the transfer bundle)"
        if len(block) > room:
            block = block[:max(0, room)]
    contract["context"] = context + block
    return contract


def _order(entries: list[dict], carried: set[str], root: str | None) -> list[dict]:
    """The root first, then each task after the carried tasks it requires or descends from."""
    by_id = {entry["id"]: entry for entry in entries}
    needs: dict[str, set[str]] = {}
    for entry in entries:
        contract = entry["contract"]
        needed = {r["task_id"] for r in contract.get("requirements", []) if r.get("task_id") in carried}
        parent = (contract.get("payload") or {}).get("parent_task_id")
        if parent in carried:
            needed.add(parent)
        needed.discard(entry["id"])
        needs[entry["id"]] = needed
    ordered: list[dict] = []
    done: set[str] = set()
    queue = sorted(by_id, key=lambda tid: (tid != root, by_id[tid]["created_at"], tid))
    while queue:
        progressed = False
        for tid in list(queue):
            if needs[tid] <= done:
                ordered.append(by_id[tid])
                done.add(tid)
                queue.remove(tid)
                progressed = True
                break
        if not progressed:  # A cycle: take the earliest, its requirements follow it.
            tid = queue.pop(0)
            ordered.append(by_id[tid])
            done.add(tid)
    return ordered


async def import_core(hub, bundle: Path) -> dict:
    """Seed `hub`, a fresh instance with its service stopped, from `bundle` (see the module)."""
    bundle = bundle.resolve()
    core = json.loads((bundle / "core.json").read_text(encoding="utf-8"))
    if core.get("format") != FORMAT:
        raise HubError("CORE_FORMAT", f"This bundle has format {core.get('format')!r}; import format {FORMAT}", 422)
    record_dir = hub.state_dir / "core-transfer" / core["bundle_id"]
    resuming = (record_dir / "started.json").is_file()
    counts = await hub.db.call(lambda c: {name: c.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                                          for name in ("tasks", "workers", "runs")})
    if not resuming and any(counts.values()):
        raise HubError("CORE_TARGET_NOT_EMPTY", f"Import into a fresh instance; this one has {counts}", 409)
    environment = None
    if hub.config.domain == "lean":
        environment = await asyncio.to_thread(_check_library, hub, bundle)
    record_dir.mkdir(parents=True, exist_ok=True)
    if not resuming:
        _write_json(record_dir / "started.json", {"bundle": str(bundle), "started_at": time.time(),
                                                  "moved_on": time.strftime("%Y-%m-%d")})
    # A resumed import repeats the first attempt's commands exactly, so they are retries, not new ones.
    moved_on = json.loads((record_dir / "started.json").read_text(encoding="utf-8"))["moved_on"]
    for name in ("core.json", "references.json", "library.json"):
        shutil.copyfile(bundle / name, record_dir / name)
    shutil.copyfile(bundle / "history" / "tasks.jsonl", record_dir / "history-tasks.jsonl")
    key = "core-transfer:" + core["bundle_id"] + ":"

    async def command(name: str, suffix: str, payload: dict):
        return await hub.execute(Command(name=name, key=key + suffix, payload=payload), OPERATOR)

    # Literature first: carried contexts may cite it.
    skipped_literature = []
    for record in json.loads((bundle / "literature.json").read_text(encoding="utf-8")):
        path = (bundle / "literature" / record["bundled_path"]) if record["bundled_path"] else Path(record["source_path"])
        if not path.exists():
            skipped_literature.append({"id": record["id"], "reason": f"{path} is not on this machine"})
            continue
        payload = {"source_id": record["id"], "path": str(path), "title": record["title"],
                   "citation": record["citation"], "description": record["text"], "copy": bool(record["bundled_path"])}
        payload.update({k: record[k] for k in ("uri", "version") if record.get(k)})
        try:
            await command("literature.register", "literature:" + record["id"], payload)
        except HubError as error:
            skipped_literature.append({"id": record["id"], "reason": f"{error.code}: {error.message}"})

    entries = json.loads((bundle / "tasks.json").read_text(encoding="utf-8"))
    references = json.loads((bundle / "references.json").read_text(encoding="utf-8"))
    carried = {entry["id"] for entry in entries}
    environments = set(core.get("environments", []))
    root = core.get("root_task_id") if core.get("root_task_id") in carried else None
    for entry in _order(entries, carried, root):
        contract = carried_contract(entry, carried, references, environments, moved_on)
        await command("task.publish", "task:" + entry["id"],
                      {"contract": contract, "task_id": entry["id"], "deduplicate": False})
    # Where each task came from, when that task came along too (display only).
    origins = [(entry["origin_task_id"], entry["id"]) for entry in entries if entry.get("origin_task_id") in carried]

    def keep_origins(c):
        c.executemany("UPDATE task_origins SET source_task_id=? WHERE task_id=?", origins)
    if origins:
        await hub.db.call(keep_origins, write=True)
    for entry in entries:
        if entry.get("priority") is not None:
            await command("task.set_priority", "priority:" + entry["id"],
                          {"task_id": entry["id"], "priority": entry["priority"]})
    for entry in sorted((e for e in entries if e.get("pinned_at") is not None), key=lambda e: e["pinned_at"]):
        await command("task.set_pinned", "pinned:" + entry["id"], {"task_id": entry["id"], "pinned": True})

    drafts = 0
    for entry in entries:
        if entry.get("draft"):
            drafts += await _import_draft(hub, bundle, entry)
    dag_revision = await _import_dags(hub, bundle) if (bundle / "dags").is_dir() else None
    summary = {"bundle_id": core["bundle_id"], "tasks": len(entries), "drafts": drafts,
               "dag_revision": dag_revision, "literature_skipped": skipped_literature,
               "environment": environment, "library_commit": core["library_commit"],
               "record": str(record_dir)}
    _write_json(record_dir / "import.json", {**summary, "finished_at": time.time()})
    return summary


# What `taskctl init --from-library` takes from a directory (bootstrap.capture_library).
_SKIPPED_DIRECTORIES = frozenset({".git", ".lake", ".dependencies", ".task-based", ".private", ".venv",
                                  "__pycache__", "node_modules", ".taskhub-integration"})
_SKIPPED_FILES = frozenset({"lakefile.lean", "lakefile.md"})


def _source(path: str) -> bool:
    parts = path.split("/")
    return (path.lower().endswith((".lean", ".md")) and path.casefold() not in _SKIPPED_FILES
            and not any(part.casefold() in _SKIPPED_DIRECTORIES for part in parts[:-1]))


def _check_library(hub, bundle: Path) -> str:
    """The new instance's prepared environment, once its initial Library is the bundle's."""
    path = hub.state_dir / "initialization.json"
    marker = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    if marker.get("state") != "ready":
        raise HubError("CORE_TARGET_NOT_READY", "Initialize the instance from the bundle's library/ first "
                       "(taskctl init --domain lean --from-library BUNDLE/library)", 409)
    imported = {item["path"]: item["sha256"] for item in read_snapshot(hub.blobs, marker["initial_snapshot"])
                if _source(item["path"])}
    expected = {item["path"]: item["sha256"]
                for item in json.loads((bundle / "library.json").read_text(encoding="utf-8"))["files"]
                if _source(item["path"])}
    # Initialization adds the starter pages a Library lacks.
    extra = sorted(set(imported) - set(expected) - set(STARTER_PAGES))
    missing = sorted(set(expected) - set(imported))
    changed = sorted(path for path in set(expected) & set(imported) if expected[path] != imported[path])
    if missing or extra or changed:
        raise HubError("CORE_LIBRARY_MISMATCH", "The instance was not initialized from this bundle's Library", 409,
                       details={"missing": missing[:20], "extra": extra[:20], "changed": changed[:20],
                                "counts": [len(missing), len(extra), len(changed)]})
    return marker["environment_artifact"]


async def _import_draft(hub, bundle: Path, entry: dict) -> int:
    task_id = entry["id"]
    exists = await hub.db.call(lambda c: c.execute("SELECT 1 FROM native_draft_heads WHERE task_id=?",
                                                   (task_id,)).fetchone())
    if exists:
        return 0
    source = bundle / "drafts" / task_id
    files = {item["path"]: (source / item["path"]).read_bytes() for item in entry["draft"]["files"]}
    summary = (entry["draft"]["summary"] or "Shared draft") + f" (carried over at revision {entry['draft']['revision']})"
    reference = await asyncio.to_thread(hub.draft_files.install, task_id, 1, 1, "operator",
                                        "Core transfer", summary, files)

    def head(c):
        c.execute("INSERT INTO native_draft_heads VALUES(?,?,?,?)",
                  (task_id, reference["revision"], reference["content_key"], reference["manifest_sha256"]))
        hub._event(c, "draft.updated", task_id, {"draft": reference, "summary": summary}, task_id)
    await hub.db.call(head, write=True)
    await asyncio.to_thread(hub.draft_files.point, reference)
    return 1


async def _import_dags(hub, bundle: Path) -> int:
    current = await hub.reconcile_dags()
    if current is not None and current["revision"] > 0:
        return current["revision"]  # Imported already.
    files = {name: (bundle / "dags" / name).read_bytes() for name in ("definitions.json", "proofs.json")}
    summary = "The previous instance's shared planning DAGs, carried over with its unfinished tasks."
    reference = await asyncio.to_thread(hub.dag_files.install, 1, files, summary=summary)

    def head(c):
        c.execute("UPDATE native_dag_head SET revision=?,content_key=?,manifest_sha256=? WHERE id=1",
                  (reference["revision"], reference["content_key"], reference["manifest_sha256"]))
        hub._event(c, "dags.updated", "shared-dags", {"graphs": reference, "summary": summary})
    await hub.db.call(head, write=True)
    await asyncio.to_thread(hub.dag_files.point, reference)
    return reference["revision"]
