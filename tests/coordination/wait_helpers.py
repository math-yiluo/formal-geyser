"""Waits recorded before 2026-11-07: with requirements, or with no condition at all.

A pause no longer takes either (hub._on_task_wait), but such stored waits stay in a running project
and must wake as they did. `legacy_wait` records one the way the pause command did, without its
newer checks, for tests of that behaviour.
"""
from __future__ import annotations

from formal_geyser.coordination.models import Checkpoint


async def legacy_wait(hub, payload: dict, actor) -> dict:
    """What `task.pause` or `task.wait` with `payload` recorded for `actor` before 2026-11-07."""
    checkpoint = payload.get("checkpoint") or {"summary": payload.get("reason", "Paused"),
                                               "next_action": "Reconsider saved work when actionable"}
    predicate = {"mode": "all", "requirements": [], "validations": [], "external_response_id": None,
                 "completion_declaration_id": None, **(payload.get("predicate") or {})}

    def record(c):
        attempt, task = hub._active(c, actor)
        hub._cancel_recruitment(c, task["id"], "explicit_task_pause")
        c.execute("UPDATE workers SET work_intention='waiting' WHERE id=?", (attempt["worker_id"],))
        return hub._record_wait(c, attempt, task, Checkpoint.model_validate(checkpoint).model_dump(), predicate)
    return await hub.db.call(record, write=True)
