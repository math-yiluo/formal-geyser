"""Stopped-service maintenance that preserves conversations and historical evidence."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from .coordination.api import ProjectOwner
from .coordination.config import load_config
from .coordination.hub import Hub
from .coordination.library_rest import RECORD, restore_workspace
from .coordination.models import Actor, Command, Role
from .coordination.runtime.ownership import tree_quiet
from .lean_service import client as lean_service
from .plugin import LeanPlugin


def require_quiet_project(root: Path) -> None:
    with sqlite3.connect((root / ".task-based/state.sqlite3").as_uri() + "?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        if db.execute("SELECT 1 FROM runs WHERE state='running'").fetchone():
            raise ValueError("Pause all and stop the service before maintenance.")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "worker_controls" in tables and db.execute("SELECT 1 FROM worker_controls WHERE control='resumed'").fetchone():
            raise ValueError("Pause all individually resumed workers before maintenance.")
        if (db.execute("SELECT 1 FROM attempts WHERE ended_at IS NULL").fetchone()
                or db.execute("SELECT 1 FROM turn_intents WHERE state IN ('prepared','sending','bound','uncertain')").fetchone()
                or db.execute("SELECT 1 FROM helper_intents WHERE state IN ('running','uncertain') "
                              "OR (state='prepared' AND (thread_id IS NOT NULL OR remote_turn_id IS NOT NULL "
                              "OR raw_result_json IS NOT NULL OR result_json IS NOT NULL OR usage_json IS NOT NULL "
                              "OR inference_uncertain<>0))").fetchone()):
            raise ValueError("Finish or reconcile retained executions before maintenance.")
    for path in (root / ".task-based/owned-processes").glob("*.json"):
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if receipt.get("process_quiet") is not True and not tree_quiet(receipt, end_orphan=True):
            raise ValueError("An owned process still lacks shutdown confirmation: " + path.name)


async def revalidate(hub, artifact_id: str, *, record: bool) -> dict:
    artifact = await hub.artifact(artifact_id)
    if artifact["kind"] not in {"lean.definition", "lean.statement", "lean.proof"} or artifact["status"] != "staged":
        raise ValueError("Select an unpublished staged definition, statement, or proof candidate.")
    task = await hub.task(artifact["task_id"], revision=artifact["task_revision"])
    task = {**task, "contract": {**task["contract"], "acceptance_policy": artifact["publication_policy"]}}
    # A new check of the original bytes, not a replay of a cached native tool call; the Lean
    # service reuses an earlier identical official check's definitive result.
    with lean_service.priority(blocking=False):
        report = await LeanPlugin().validate_candidate(hub, task, artifact)
    if record:
        await hub.execute(Command(name="artifact.accept", key="maintenance-revalidation:" + uuid4().hex,
            payload={"artifact_id": artifact_id, **report.model_dump(mode="json")}), Actor(role=Role.VALIDATOR))
    return {"artifact_id": artifact_id, "recorded": record, "report": report.model_dump(mode="json")}


async def project(root: Path, function):
    root = root.resolve()
    config = load_config(root)
    if config.domain != "lean":
        raise ValueError("Select a Lean project instance.")
    with ProjectOwner(root):
        require_quiet_project(root)
        hub = Hub(root, config=config)
        try:
            return await function(hub)
        finally:
            await hub.close()


async def run(root: Path, artifact_id: str, *, record: bool) -> dict:
    return await project(root, lambda hub: revalidate(hub, artifact_id, record=record))


async def restore_all_library_copies(hub) -> dict:
    """Write back every resting worker's Library copy (library_rest), as its next reader would."""
    restored: dict[str, int] = {}
    for record in sorted((hub.state_dir / "workers").glob("*/" + RECORD)):
        written = await asyncio.to_thread(restore_workspace, hub.blobs, (record.parent / "work").resolve())
        if written is not None:
            restored[record.parent.name] = written
    return {"workers": len(restored), "files": sum(restored.values()), "restored": restored}


async def restore_library_copies(root: Path) -> dict:
    """With the service stopped: before running a version that does not know rested workspaces,
    which would read one as its worker having deleted those Library files."""
    return await project(root, restore_all_library_copies)
