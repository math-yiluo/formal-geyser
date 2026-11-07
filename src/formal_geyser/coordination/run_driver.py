"""The runs this service drives: which are armed, the pass driving each, and what each pass
found. Committed operator commands and a local timer wake them; nothing polls a model."""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections import deque
from collections.abc import Callable
from time import monotonic
from typing import Any

from .errors import HubError
from .models import Command

logger = logging.getLogger(__name__)
# Startup recovery retries transient faults (such as a locked file) with backoff.
STARTUP_RETRY_SECONDS = 5.0
STARTUP_RETRY_MAX_SECONDS = 300.0
STARTUP_FAULTS = (HubError, OSError, sqlite3.Error)
# Visible-history records that could not be updated are retried this often.
HISTORY_RETRY_SECONDS = 30.0
# Retired reviewers' workspaces are reclaimed this often, in the background (workspace_retention).
RECLAIM_SECONDS = 600.0
# A run whose passes start nothing (paused, or every account held or full) is looked at again
# after 1, 2, 4 and 8 s, then every IDLE_MAX_SECONDS, until a command wakes it. Every second, as
# before, an idle run read its controls three times and checked all due work (Linux pilot).
IDLE_MAX_SECONDS = 10.0
WAKE_COMMANDS = frozenset({
    "task.publish", "task.revise", "task.retire", "task.pause", "task.wait",
    "task.join", "artifact.register", "artifact.stage", "artifact.submit",
    "artifact.accept", "artifact.withdraw", "artifact.record_replacement", "review.accept", "review.reject",
    "task.set_priority", "task.set_pinned",
    "worker.recover", "worker.resume", "group.resume", "run.resume", "run.reconcile_usage", "interruption.import",
    "validation.recover", "validation.retry", "integrator.retry",
    "talk.send", "message.send", "message.request", "literature.request", "literature.register",
    "request.resolve", "message.delivery.resolve", "worker.resume_work", "task.resume_work",
    "task.invitation.update", "task.invitation.withdraw", "task.complete.declare",
    "task.complete.feedback", "task.complete.feedback_unavailable", "task.complete.finalize", "task.complete.cancel",
    "task.leave", "task.invite_back", "helper.reconcile", "task.claim",
})
# The operator's commands that claim work for a worker. Only a pass launches the claim: in the
# Linux pilot a task.join left Worker 303's claim running for eight hours without an activation.
CLAIM_COMMANDS = frozenset({"task.join", "task.claim"})


def admits_work(run: dict) -> bool:
    # A paused run still drives the people the operator resumed, individually or as a group.
    return run["state"] == "running" or bool(run.get("resumed_workers")) or bool(run.get("resumed_groups"))


class RunDriver:
    """Drives each armed run, one pass at a time, from startup recovery to shutdown."""

    def __init__(self, hub: Any, make_runner: Callable, *, resume_runs: bool):
        self.hub = hub
        self.make_runner = make_runner
        self.resume_runs = resume_runs
        self.armed: set[str] = set()
        # The pass driving each run, and what each run's last pass found.
        self.passes: dict[str, asyncio.Task] = {}
        self.results: dict[str, dict] = {}
        # Runs woken since their pass began, which need another pass before idling.
        self.pending_wakes: set[str] = set()
        # Each run's operator claims (CLAIM_COMMANDS) not launched yet, shared with its passes.
        self.claims: dict[str, deque[dict]] = {}
        self.retry_after: dict[str, float] = {}
        # Runs whose last passes started nothing: how many in a row, and when the timer looks again.
        self.idle_passes: dict[str, int] = {}
        self.idle_until: dict[str, float] = {}
        self.history_retry_at = 0.0
        self.shutting_down = False
        self.delivery_recovery_error: str | None = None
        self.startup_resume: dict | None = None
        self.startup_retry_task: asyncio.Task | None = None
        self.timer_task: asyncio.Task | None = None
        self.reclaim_at = monotonic() + RECLAIM_SECONDS
        self.reclaim_task: asyncio.Task | None = None

    async def recover(self) -> None:
        """Recover what a restart left and re-arm the driven runs, retrying a transient fault."""
        if not await self._startup_recovery():
            # A transient fault must not leave driven runs idle until the next restart.
            self.startup_retry_task = asyncio.create_task(
                self._retry_startup_recovery(), name="taskhub-startup-recovery")

    def start_timer(self) -> None:
        self.timer_task = asyncio.create_task(self._timer_tick(), name="taskhub-local-timers")

    def stop(self) -> list[asyncio.Task]:
        """Arm and wake nothing more; the tasks still to cancel."""
        self.shutting_down = True
        self.armed.clear()
        self.pending_wakes.clear()
        tasks = list(self.passes.values())
        if self.timer_task is not None:
            tasks.append(self.timer_task)
        if self.startup_retry_task is not None:
            tasks.append(self.startup_retry_task)
        if self.reclaim_task is not None:
            tasks.append(self.reclaim_task)
        return tasks

    async def _startup_recovery(self) -> bool:
        """Apply committed delivery outcomes, then re-arm driven runs; report whether both finished."""
        reconcile = getattr(self.hub, "reconcile_delivery_transcripts", None)
        if reconcile is not None:
            try:
                await reconcile()
            except Exception as error:
                if not isinstance(error, STARTUP_FAULTS):
                    logger.exception("Startup delivery recovery failed unexpectedly; it will be retried")
                # Only committed outcomes are applied, so a retry never re-sends a message;
                # uncertain deliveries keep their own holds. No run resumes meanwhile.
                self.delivery_recovery_error = str(error)
                self.startup_resume = {"runs": [], "skipped": "delivery_recovery_failed",
                                       "error": str(error)[:1000], "retrying": True}
                return False
        self.delivery_recovery_error = None
        for name in ("reconcile_task_trees", "reconcile_archive_relocations"):
            reconcile = getattr(self.hub, name, None)
            if reconcile is not None:
                try:
                    await reconcile()  # File work committed before a restart; failures stay recorded.
                except Exception:
                    logger.exception("Startup %s will be retried after the next command", name)
        self.startup_resume = await self._resume_on_start()
        return self.startup_resume["skipped"] != "read_failed"

    async def _retry_startup_recovery(self) -> None:
        delay = STARTUP_RETRY_SECONDS
        while not self.shutting_down:
            await asyncio.sleep(delay)
            try:
                if await self._startup_recovery():
                    return
            except Exception:
                # The retry itself must never stop while it reports that it is retrying.
                logger.exception("Startup recovery attempt failed; it will be retried")
            delay = min(delay * 2, STARTUP_RETRY_MAX_SECONDS)

    async def _resume_on_start(self) -> dict:
        """Re-arm runs that were driven before the restart; the timer then drives running ones."""
        if not self.resume_runs:
            return {"runs": [], "skipped": "disabled"}
        resumable = getattr(self.hub, "resumable_runs", None)
        if resumable is None:
            return {"runs": [], "skipped": "unsupported"}
        try:
            runs = await resumable()
        except Exception as error:
            return {"runs": [], "skipped": "read_failed", "error": str(error)[:1000], "retrying": True}
        self.armed.update(runs)
        return {"runs": runs, "skipped": None}

    def driving(self, run_id: str) -> bool:
        """Whether this service is driving the run now."""
        task = self.passes.get(run_id)
        return task is not None and not task.done()

    def driving_runs(self) -> list[str]:
        return [run_id for run_id, task in self.passes.items() if not task.done()]

    def arm(self, run_id: str) -> bool:
        """Arm the run and queue a pass; whether one was already driving it."""
        self.armed.add(run_id)
        self._woken(run_id)
        return self.queue(run_id)

    def queue(self, run_id: str) -> bool:
        """Record demand before checking task completion, so a finishing pass cannot lose it."""
        if self.shutting_down or run_id not in self.armed:
            return False
        self.pending_wakes.add(run_id)
        already_driving = self.driving(run_id)
        if not already_driving:
            self.results.pop(run_id, None)
            self.passes[run_id] = asyncio.create_task(self._drive(run_id), name=f"formal-geyser-run-{run_id}")
        return already_driving

    def committed(self, command: Command, result: Any = None) -> None:
        """Wake the runs a committed command may give work; a stopped or completed run disarms.
        A claim the operator made (`result` of a CLAIM_COMMANDS command) waits for its run's pass."""
        if command.name in CLAIM_COMMANDS and isinstance(result, dict) and result.get("id") and result.get("run_id"):
            self.claims.setdefault(result["run_id"], deque()).append(result)
        if command.name in {"run.stop", "run.complete"}:
            run_id = command.payload.get("run_id")
            self.armed.discard(run_id)
            self.pending_wakes.discard(run_id)
            return
        if command.name not in WAKE_COMMANDS:
            return
        targets = set(self.armed)
        if command.name.startswith("run."):
            targets.intersection_update({command.payload.get("run_id")})
        for run_id in targets:
            self._woken(run_id)
            self.queue(run_id)

    def _woken(self, run_id: str) -> None:
        """The run started work or a command woke it: the timer looks at it every second again."""
        self.idle_passes.pop(run_id, None)
        self.idle_until.pop(run_id, None)

    def _idle(self, run_id: str) -> None:
        """A pass started nothing: the timer looks again after twice as long, up to IDLE_MAX_SECONDS."""
        passes = self.idle_passes[run_id] = self.idle_passes.get(run_id, 0) + 1
        self.idle_until[run_id] = monotonic() + min(IDLE_MAX_SECONDS, 2.0 ** (passes - 1))

    def history_notice(self) -> str | None:
        """Current, not startup-time: it clears as soon as a retry applies the records."""
        unapplied = getattr(self.hub, "unapplied_transcript_updates", None) or []
        return (str(len(unapplied)) + " visible-history record(s) could not be updated; their delivery outcomes"
                " are recorded and work continues. They are retried automatically." if unapplied else None)

    async def _retry_history(self) -> None:
        if (not getattr(self.hub, "unapplied_transcript_updates", None)
                or (self.startup_retry_task and not self.startup_retry_task.done())
                or monotonic() < self.history_retry_at):
            return
        self.history_retry_at = monotonic() + HISTORY_RETRY_SECONDS
        try:
            await self.hub.reconcile_delivery_transcripts()
        except Exception as error:
            logger.warning("Visible-history retry deferred: %s", type(error).__name__)

    async def _timer_tick(self) -> None:
        """Check persisted due work while explicitly armed, without a model poll."""
        while not self.shutting_down:
            await asyncio.sleep(1)
            await self._retry_history()
            maintenance = getattr(getattr(self.hub, "db", None), "maintain_if_idle", None)
            if maintenance is not None:
                try:
                    await maintenance()
                except Exception as error:
                    logger.warning("Bounded database maintenance deferred: %s", type(error).__name__)
            self._reclaim_retired()
            for run_id in tuple(self.armed):
                if self.driving(run_id):
                    continue
                if monotonic() < max(self.retry_after.get(run_id, 0), self.idle_until.get(run_id, 0)):
                    continue
                # The pass reads the run's controls itself, and ends at once if it admits nothing.
                self.queue(run_id)

    def _reclaim_retired(self) -> None:
        """Start a background pass retiring long-idle workers and reclaiming retired workers' workspaces when one is due."""
        reclaim = getattr(self.hub, "reclaim_retired_workspaces", None)
        if (reclaim is None or monotonic() < self.reclaim_at
                or (self.reclaim_task is not None and not self.reclaim_task.done())):
            return
        self.reclaim_at = monotonic() + RECLAIM_SECONDS
        self.reclaim_task = asyncio.create_task(self._reclaim(reclaim), name="taskhub-workspace-reclaim")

    @staticmethod
    async def _reclaim(reclaim: Callable) -> None:
        try:
            done = await reclaim()
        except Exception as error:
            logger.warning("Retiring idle workers and reclaiming workspaces deferred: %s", type(error).__name__)
            return
        if done:
            logger.info("Reclaimed %d retired workers' workspaces, %.0f MiB",
                        len(done), sum(freed for _, freed in done) / 2**20)

    async def _drive(self, run_id: str) -> None:
        while run_id in self.armed and not self.shutting_down:
            # This pass observes all earlier committed commands. Commands arriving
            # during it leave the flag set and require another pass before idle.
            self.pending_wakes.discard(run_id)
            try:
                record = await self.hub.run(run_id)
                if not admits_work(record):
                    self.results[run_id] = {"state": "not_running", "run_state": record["state"]}
                    if record["state"] in {"stopped", "complete"}:
                        self.armed.discard(run_id)
                        return
                    if run_id in self.pending_wakes:
                        continue
                    self._idle(run_id)
                    return
                runner = self.make_runner(self.hub)
                result = await runner.run_until_idle(run_id, claims=self.claims.setdefault(run_id, deque()))
                self.results[run_id] = {"state": "finished", "result": result}
                if result.get("failure_observed"):
                    self.retry_after[run_id] = monotonic() + 5
                else:
                    self.retry_after.pop(run_id, None)
                if result.get("steps"):
                    self._woken(run_id)
                else:
                    self._idle(run_id)
            except asyncio.CancelledError:
                self.results[run_id] = {"state": "cancelled"}
                raise
            except Exception as error:
                self.results[run_id] = {
                    "state": "retrying",
                    "error": error.as_dict() if isinstance(error, HubError) else {
                        "code": "driver_failed",
                        "message": str(error),
                    },
                }
                self.retry_after[run_id] = monotonic() + 5
                return
            # There is no await between this check and returning. A command
            # arriving later sees a completed task and queues its own pass.
            if run_id not in self.pending_wakes:
                return
