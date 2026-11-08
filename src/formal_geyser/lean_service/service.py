"""The Lean service's core: its jobs, the scheduling loop, execution and measurement.

Clients submit jobs and wait for them. A development check (interactive lane) belongs to
its client: when nobody waits for it any more it is abandoned. An official check (job
lane) is the shared, durable record of one frozen input: the same input under the same
environment is one job, its definitive result is reused, and it survives pauses, client
restarts and the service's own restarts. Every second the scheduler decides what runs;
the service carries that out and measures each job's processes, so that the next similar
job is sized from what this one really used.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import shutil
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from .. import offload
from ..coordination.models import LeanServiceConfig
from ..coordination.storage import collected_thread_call, digest
from ..lake_dedupe import SlotDeduper
from ..process import ProcessRunner, process_environment
from . import machine as machine_module
from .executor import JobSpec, content_key, memory_limited, run_check, stopped_reason
from .osjobs import JobGroup
from .sandbox import Unsandboxed
from .scheduler import CLASSES, GIB, Job, Machine, Policy, plan, rank
from .sessions import SessionPool
from .store import Store
from .warm import WarmSet

logger = logging.getLogger(__name__)
# Per Lean process, until jobs like it have been measured. In the Linux pilot a measured check's
# peak was 1.3 GiB private and 1.4 to 1.7 GiB committed, nearly whatever it checked: these are
# its 90th percentiles, and the least a Lean process is assumed to take is below them.
DEFAULT_MEMORY, DEFAULT_COMMIT = 27 * GIB // 20, 33 * GIB // 20  # 1.35 and 1.65 GiB
MIN_MEMORY, MIN_COMMIT = GIB, 5 * GIB // 4
DEFAULT_SECONDS = {"interactive": 60.0, "official": 180.0, "integrator": 900.0}
SAFETY = 1.15  # Estimates are the last observed peak with this margin.
CLIENT_SECONDS = 60.0  # A development check nobody has asked about for this long is abandoned.
SAMPLE_SECONDS = 10.0
TICK_SECONDS = 1.0
# A validation's build keeps the threads its CPU over this long shows it uses, plus one
# (_release_idle_threads): its Lake build may start no more processes than it was given, so this
# overcommits the machine by at most a validation's few threads (validation_threads).
RELEASE_WINDOW_SECONDS = 120.0
# A reading of free memory serves this long while the next ones fail; past it free memory is
# unknown and nothing starts or resumes: taken as unlimited, it would start every queued check.
MEASUREMENT_SECONDS = 30.0
# A job's cgroup that could not be removed is tried again after this long, doubling to the most.
CLEANUP_SECONDS, CLEANUP_MAX_SECONDS = 5.0, 300.0
# A job that could not start (its scratch directory could not be made: on 2026-11-05 the Linux
# pilot's disk was full) stays queued and is tried again after this long, doubling to the most;
# meanwhile other jobs may start.
START_RETRY_SECONDS, START_RETRY_MAX_SECONDS = 1.0, 60.0
# bwrap, which the Codex sandbox runs, refuses a command of more than 9,000 arguments in all. On
# 2026-11-01 a validation in the Linux pilot passed one Lake target for each of 9,429 modules (the
# Library's 8,674 and a candidate's 755) and failed before Lean ran. A longer command reads its
# arguments from a file instead (argument_file_command).
ARGUMENT_FILE_LIMIT = 1000


def argument_file_command(argv, directory: Path) -> tuple[list[str], Path]:
    """`argv` as a short command whose shell, inside the sandbox, reads all its arguments but the
    executable from a file in `directory`: the returned path, for the caller to remove. The shell
    splits the file at whitespace, so no argument may contain any."""
    arguments = [str(item) for item in argv[1:]]
    if any(not item or any(character.isspace() for character in item) for item in arguments):
        raise ValueError("An argument file holds only nonempty arguments without whitespace")
    path = directory / f"arguments-{uuid4().hex}"
    path.write_text("\n".join(arguments) + "\n", encoding="utf-8")
    # `set -f`: the arguments are not file name patterns.
    return ["/bin/sh", "-c", 'set -f; exec "$0" $(cat "$1")', str(argv[0]), str(path)], path


async def carried_out(awaitable):
    """`awaitable` awaited to its end even when the caller is cancelled meanwhile, which is then
    raised. Ending a job is not left halfway: a cgroup taken from the job and never removed, a
    job left running with no run, or its end never recorded."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    if cancelled:
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


def estimate_key(spec: JobSpec) -> str:
    """Jobs on the same modules; a job nothing like has run yet takes the defaults."""
    request = spec.request
    return digest({"kind": spec.kind, "modules": sorted(request.modules), "replay": request.kernel_replay,
                   "inspections": len(request.inspections), "official": request.snapshot is not None})


def threads_in_use(samples, assigned: int, window: float = RELEASE_WINDOW_SECONDS) -> int | None:
    """The threads a running build needs, from its CPU over the last `window` seconds (`samples`:
    monotonic time and CPU seconds): the cores it used, rounded up, plus one, and never more than
    it holds; None until its samples span the window."""
    if len(samples) < 2 or samples[-1][0] - samples[0][0] < window:
        return None
    cores = (samples[-1][1] - samples[0][1]) / (samples[-1][0] - samples[0][0])
    return min(assigned, max(1, math.ceil(cores) + 1))


class ServiceJob:
    def __init__(self, row: dict[str, Any], spec: JobSpec, sched: Job):
        self.id = row["id"]
        self.row = row
        self.spec = spec
        self.sched = sched
        self.group: JobGroup | None = None
        self.task: asyncio.Task | None = None
        self.scratch: Path | None = None
        self.done = asyncio.Event()
        self.runnable = asyncio.Event()
        self.runnable.set()
        self.cancel_requested = False
        self.abandoned = False
        self.memory_limited: int | None = None  # What its largest process held when it was stopped for it.
        self.borrowed: list[JobGroup] = []  # A resident session's processes, while this job uses them.
        self.suspended_since: float | None = None
        self.run_paused_base = 0.0  # Suspended seconds before the current run started.
        self.peak_committed = 0
        self.cpu_seconds = 0.0
        # Its current run's (monotonic time, CPU seconds), over RELEASE_WINDOW_SECONDS.
        self.cpu_samples: deque[tuple[float, float]] = deque()
        self.start_failures = 0  # In a row; a start resets it.
        self.start_retry_at = 0.0  # A queued job whose start failed is not started before then.

    def paused_total(self, now: float) -> float:
        return self.row["suspended_seconds"] + (now - self.suspended_since if self.suspended_since else 0.0)


@dataclass
class Lingering:
    """A job's cgroup that could not be removed. Something of the job may still run in it, so until
    it is removed (`_keep_removing`, without a time limit) its threads stay taken, its scratch stays,
    and the job, if it was queued again, does not start a second run beside the first."""
    group: JobGroup
    threads: int
    scratch: Path | None
    since: float
    task: asyncio.Task | None = None


class JobRunner:
    """Starts a job's Lean and Lake processes: in its cgroup, in the sandbox, never while it is
    suspended, and with its suspended time left out of any timeout."""

    def __init__(self, service: LeanService, job: ServiceJob):
        self.service = service
        self.job = job

    def paused_seconds(self) -> float:
        """How long the job has been suspended in all, which a time limit leaves out."""
        return self.job.paused_total(self.service.clock())

    def memory_stop(self) -> tuple[str, float, int] | None:
        """When the service stopped the job for its memory: its lane, the limit in GiB and what
        its largest process held (executor.memory_limited)."""
        if self.job.memory_limited is None:
            return None
        return self.job.sched.lane, self.service.config.process_memory_gib, self.job.memory_limited

    def borrow(self, group: JobGroup) -> None:
        """While the job uses a resident session, suspending the job suspends the session too."""
        self.job.borrowed.append(group)
        if self.job.sched.state == "suspended":
            group.suspend()

    def give_back(self, group: JobGroup) -> None:
        if group in self.job.borrowed:
            self.job.borrowed.remove(group)
            if group.suspended:
                group.resume()

    async def run(self, argv, *, cwd: Path, timeout: float | None = None, output_limit: int = 1048576,
                  env=None, stdin: bytes | None = None):
        job, service = self.job, self.service
        await job.runnable.wait()
        build = Path(argv[0]).name == "lake" and "build" in argv[1:2]
        if build:
            job.sched.shape = "build"
        assert job.scratch is not None
        # The sandbox lets a process write its working directory, so a process that needs no
        # particular one, such as a version probe, runs in the job's scratch directory.
        lake = Path(job.spec.request.blobs).parent / "lake"
        if not any(Path(cwd).resolve().is_relative_to(root.resolve()) for root in (lake / "workspaces", job.scratch)):
            cwd = job.scratch
        temporary = job.scratch / "tmp"
        temporary.mkdir(parents=True, exist_ok=True)
        environment = dict(env if env is not None else process_environment())
        environment["TMPDIR"] = str(temporary)
        writable = [Path(cwd), job.scratch, *([Path(environment["LAKE_CACHE_DIR"])]
                                              if environment.get("LAKE_CACHE_DIR") else [])]
        for directory in writable:
            # Created here, so that the sandbox can grant them.
            directory.mkdir(parents=True, exist_ok=True)
        arguments = None
        if len(argv) > ARGUMENT_FILE_LIMIT:
            argv, arguments = argument_file_command(argv, temporary)
        try:
            command, environment = service.sandbox.wrap(argv, cwd=Path(cwd), env=environment, writable=writable)
            base = job.paused_total(service.clock())
            return await service.process_runner.run(
                command, cwd=cwd, timeout=timeout, output_limit=output_limit, env=environment, stdin=stdin,
                group=job.group, register=False, paused=lambda: job.paused_total(service.clock()) - base)
        finally:
            if arguments is not None:
                arguments.unlink(missing_ok=True)
            if build:
                # The rest of the check runs one Lean process at a time.
                job.sched.shape = "single"
                if job.sched.assigned > 1:
                    job.sched.assigned = 1
                    service.wake()


class LeanService:
    def __init__(self, root: Path, config: LeanServiceConfig | None = None, *, sandbox=None, runner=None,
                 measure=None, threads: int | None = None, clock=time.time, session_command=None):
        self.root = Path(root).resolve()
        self.config = config or LeanServiceConfig()
        self.state_dir = self.root / ".task-based" / "lean-service"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.state_dir / "service.sqlite3", result_days=self.config.result_days)
        self.deduper = SlotDeduper(self.root / ".task-based" / "objects" / "lake")
        self.sandbox = sandbox or Unsandboxed()
        self.process_runner = runner or ProcessRunner()
        self.measure = measure or machine_module.memory
        self.threads = threads or self.config.threads or machine_module.logical_cores()
        self.clock = clock
        self.instance = uuid4().hex
        self.jobs: dict[str, ServiceJob] = {}
        self.finished: dict[str, ServiceJob] = {}  # Recent, for clients still waiting.
        self.lingering: dict[str, Lingering] = {}  # By job ID.
        # Job records whose write failed, by job ID: the job's row and the fields to write from it
        # again each second (_write), and since when writes have been failing.
        self._unwritten: dict[str, tuple[dict[str, Any], set[str]]] = {}
        self._unwritten_since: float | None = None
        self._measured: tuple[float, tuple[int, int]] | None = None  # The last reading of free memory.
        self._memory_unknown = False
        self.policy = Policy(per_owner=self.config.per_owner, aging_seconds=self.config.aging_minutes * 60,
                             validation_slots=self.config.validation_slots,
                             validation_threads=self.config.validation_threads)
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._closing = False
        self._cpu = machine_module.CpuMeter()
        self._last: dict[str, float | int | None] = {"memory_available": None, "commit_available": None,
                                                    "cpu_busy": None}
        self.warm: WarmSet | None = None  # Set by the server: the library pages it keeps resident.
        # Seconds the event loop ran the sampling timer late, in all: blocking work on the loop
        # delays every running job, and each job's result gives its share (`event_loop_lag`).
        self.loop_lag = 0.0
        self.sessions = SessionPool(sandbox=self.sandbox, limit=self.config.sessions,
                                    idle_seconds=self.config.session_idle_minutes * 60, command=session_command)
        self.warm_changed = asyncio.Event()

    # Lifecycle

    async def start(self) -> None:
        # Each check's reading, syncing and hashing of the Library runs in these processes, so
        # checks use the machine's cores instead of sharing this process's one interpreter.
        await asyncio.to_thread(offload.configure, self.config.bookkeeping_processes)
        now = self.clock()
        for row in await asyncio.to_thread(self.store.live):
            spec = JobSpec.model_validate_json(row["spec_json"])
            restarted = row["state"] != "queued"
            row.update(state="queued", requeues=row["requeues"] + int(restarted),
                       last_polled_at=now if spec.lane == "interactive" else row["last_polled_at"])
            estimate = await self._estimate(spec)
            job = ServiceJob(row, spec, self._sched(row["id"], spec, estimate, row["submitted_at"]))
            job.sched.requeues = row["requeues"]
            self.jobs[job.id] = job
            await self._write(job, "state", "requeues", "last_polled_at")
        self._tasks = [asyncio.create_task(self._schedule_loop(), name="lean-service-schedule"),
                       asyncio.create_task(self._sample_loop(), name="lean-service-sample")]
        self.wake()

    async def close(self) -> None:
        """Stop: running jobs are queued again for the next start."""
        self._closing = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.sessions.close()
        running = [job.task for job in self.jobs.values() if job.task is not None and not job.task.done()]
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        removing = [item.task for item in self.lingering.values() if item.task is not None]
        for task in removing:
            task.cancel()
        await asyncio.gather(*removing, return_exceptions=True)
        for group in [*(job.group for job in self.jobs.values() if job.group is not None),
                      *(item.group for item in self.lingering.values())]:
            try:
                await asyncio.to_thread(group.close)
            except Exception:
                logger.exception("A Lean job's cgroup cannot be removed; the service's unit ends what is left in it")
        await self._flush()
        if self._unwritten:
            # Their jobs are queued again at the next start, as the store still shows them live.
            logger.error("The records of %d Lean jobs could not be written before the service stopped",
                         len(self._unwritten))
        await asyncio.to_thread(offload.shutdown)
        self.store.close()

    def wake(self) -> None:
        self._wake.set()

    # Client operations

    async def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        spec = JobSpec.model_validate(body)
        key = await asyncio.to_thread(content_key, spec)
        now = self.clock()
        if key is not None:
            for job in self.jobs.values():
                if job.row["content_key"] == key:
                    await self._upgrade(job, spec)
                    return {**self.view(job), "deduplicated": True}
            stored = await asyncio.to_thread(self.store.reusable, key)
            if stored is not None:
                return {**self._row_view(stored), "deduplicated": True}
        job_id = uuid4().hex
        row = {"id": job_id, "content_key": key, "estimate_key": estimate_key(spec), "lane": spec.lane,
               "kind": spec.kind, "owner": spec.owner, "label": spec.label, "blocking": int(spec.blocking),
               "spec_json": spec.model_dump_json(), "state": "queued", "submitted_at": now,
               "last_polled_at": now, "requeues": 0, "suspensions": 0, "suspended_seconds": 0.0, "threads": 0}
        await asyncio.to_thread(self.store.insert, row)
        estimate = await self._estimate(spec)
        job = ServiceJob(row, spec, self._sched(job_id, spec, estimate, now))
        self.jobs[job_id] = job
        self.wake()
        return {**self.view(job), "deduplicated": False}

    async def wait(self, job_id: str, timeout: float) -> dict[str, Any]:
        job = self.jobs.get(job_id) or self.finished.get(job_id)
        if job is None:
            row = await asyncio.to_thread(self.store.get, job_id)
            if row is None:
                raise KeyError(job_id)
            return self._row_view(row)
        job.row["last_polled_at"] = self.clock()
        if not job.done.is_set():
            try:
                await asyncio.wait_for(job.done.wait(), max(0.0, timeout))
            except TimeoutError:
                pass
        job.row["last_polled_at"] = self.clock()
        return self.view(job)

    async def cancel(self, job_id: str) -> dict[str, Any]:
        job = self.jobs.get(job_id)
        if job is None:
            return await self.wait(job_id, 0)
        if job.task is None:
            await self._finish(job, None, "cancelled")
        else:
            job.cancel_requested = True
            job.task.cancel()
            await job.done.wait()
        return self.view(job)

    def view(self, job: ServiceJob) -> dict[str, Any]:
        state = "done" if job.done.is_set() else job.sched.state
        view = {"id": job.id, "lane": job.sched.lane, "owner": job.row["owner"], "label": job.row["label"],
                "blocking": job.sched.blocking, "state": state, "outcome": job.row.get("outcome"),
                "submitted_at": job.row["submitted_at"], "started_at": job.sched.started_at,
                "threads": job.sched.assigned, "requeues": job.row["requeues"],
                "suspensions": job.row["suspensions"]}
        if state == "queued":
            queued = sorted((item.sched for item in self.jobs.values() if item.sched.state == "queued"),
                            key=lambda item: rank(item, self.clock(), self.policy))
            view["position"] = 1 + [item.id for item in queued].index(job.id)
        if state == "done":
            view["result"] = job.row.get("result")
        return view

    def _row_view(self, row: dict[str, Any]) -> dict[str, Any]:
        import json

        return {"id": row["id"], "lane": row["lane"], "owner": row["owner"], "label": row["label"],
                "blocking": bool(row["blocking"]), "state": row["state"], "outcome": row["outcome"],
                "submitted_at": row["submitted_at"], "started_at": row["started_at"], "threads": row["threads"],
                "requeues": row["requeues"], "suspensions": row["suspensions"],
                **({"result": json.loads(row["result_json"])} if row.get("result_json") else {})}

    def status(self) -> dict[str, Any]:
        now = self.clock()
        jobs = sorted(self.jobs.values(), key=lambda job: rank(job.sched, now, self.policy))
        counts = {state: sum(job.sched.state == state for job in jobs) for state in ("running", "queued", "suspended")}
        return {"instance": self.instance, "pid": os.getpid(), "sandbox": self.sandbox.name,
                "threads": self.threads, "threads_in_use": sum(job.sched.assigned for job in jobs
                                                               if job.sched.state == "running"),
                **self._last, "counts": counts, "sessions": self.sessions.status(),
                "jobs": [{"id": job.id, "lane": job.sched.lane, "owner": job.row["owner"], "label": job.row["label"],
                          "state": job.sched.state, "threads": job.sched.assigned, "blocking": job.sched.blocking,
                          "seconds": now - (job.sched.started_at if job.sched.state != "queued" and job.sched.started_at
                                            else job.row["submitted_at"]),
                          "private_memory": job.sched.observed_memory} for job in jobs],
                "cleanup": [{"id": job_id, "threads": item.threads, "seconds": now - item.since}
                            for job_id, item in self.lingering.items()],
                "unwritten": len(self._unwritten),
                "warm": self.warm.status() if self.warm is not None else None}

    async def telemetry(self, hours: float) -> dict[str, Any]:
        return await asyncio.to_thread(self.store.telemetry, self.clock() - hours * 3600)

    # Scheduling

    async def _estimate(self, spec: JobSpec) -> dict | None:
        # Jobs on the same modules only. The lane's last job is no estimate: in the Linux pilot a
        # cached check left 16 MB for every new interactive check, and 14 started at once.
        return await asyncio.to_thread(self.store.estimate, estimate_key(spec))

    def _sched(self, job_id: str, spec: JobSpec, estimate: dict | None, submitted_at: float) -> Job:
        modules = len(spec.request.modules)
        if estimate is not None:
            processes = max(1, estimate["processes"])
            # A check that Lake answered from its cache measures next to nothing; the next one on
            # the same modules may rebuild them.
            memory = max(MIN_MEMORY, int(estimate["memory"] * SAFETY))
            commit = max(MIN_COMMIT, int(estimate["committed"] * SAFETY))
            seconds = max(1.0, estimate["seconds"])
        else:
            processes, memory, commit = modules, DEFAULT_MEMORY, DEFAULT_COMMIT
            seconds = DEFAULT_SECONDS[spec.lane]
        processes = max(1, min(self.threads, processes))
        if spec.lane == "integrator":
            threads = self.policy.integrator_threads
        else:
            threads = max(self.policy.single_threads, processes) if modules > 1 else self.policy.single_threads
        if spec.lane == "official":
            # Lake restores nearly all of it from its cache (LeanServiceConfig.validation_threads).
            threads = min(threads, self.policy.validation_threads)
        threads = min(self.threads, threads)
        return Job(id=job_id, lane=spec.lane, owner=spec.owner, shape="build", threads=threads,
                   min_threads=max(1, min(threads, self.threads // 4)) if processes > 1 and spec.lane != "official"
                   else 1,
                   memory=memory, commit=commit, processes=processes, seconds=seconds, blocking=spec.blocking,
                   submitted_at=submitted_at)

    async def _upgrade(self, job: ServiceJob, spec: JobSpec) -> None:
        """A second request for the same input may need it sooner."""
        changed = False
        if CLASSES[spec.lane] < CLASSES[job.sched.lane]:
            job.sched.lane = spec.lane
            changed = True
        if spec.blocking and not job.sched.blocking:
            job.sched.blocking = True
            changed = True
        if changed:
            job.row.update(lane=job.sched.lane, blocking=int(job.sched.blocking))
            await self._write(job, "lane", "blocking")
            self.wake()

    def _machine(self) -> Machine | None:
        """What the machine has free now; None while free memory is unknown (MEASUREMENT_SECONDS).
        The threads a cgroup that could not be removed still holds are not free."""
        now = self.clock()
        measured = self.measure()
        if measured is not None:
            self._measured = (now, measured)
        elif self._measured is not None and now - self._measured[0] <= MEASUREMENT_SECONDS:
            measured = self._measured[1]
        if (measured is None) != self._memory_unknown:
            self._memory_unknown = measured is None
            logger.warning("Free memory cannot be measured: no Lean job starts or resumes until it can"
                           if measured is None else "Free memory is measured again")
        memory, commit = measured if measured is not None else (None, None)
        self._last.update(memory_available=memory, commit_available=commit)
        if memory is None or commit is None:
            return None
        config = self.config
        held = sum(item.threads for item in self.lingering.values())
        return Machine(threads=max(0, self.threads - held), memory=memory, commit=commit,
                       reserve=int(config.reserve_gib * GIB),
                       commit_reserve=int(config.commit_reserve_gib * GIB),
                       low_watermark=int(config.low_watermark_gib * GIB),
                       critical_watermark=int(config.critical_watermark_gib * GIB))

    async def _schedule_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), TICK_SECONDS)
            except TimeoutError:
                pass
            self._wake.clear()
            try:
                await self.schedule()
            except Exception:
                logger.exception("Lean scheduling failed; it is retried")

    async def schedule(self) -> None:
        """One scheduling decision, carried out."""
        if self._closing:
            return
        await self._recover()
        machine = self._machine()
        if machine is None:
            return
        now = self.clock()
        # A job queued again waits while its previous run's cgroup is lingering, and one that could
        # not start waits until it is tried again.
        actions = plan([job.sched for job in self.jobs.values()
                        if job.id not in self.lingering and job.start_retry_at <= now], machine, now, self.policy)
        for action in actions:
            job = self.jobs.get(action.job_id)
            if job is None:
                continue
            try:
                if action.kind == "start":
                    await self._start(job, action.threads)
                elif action.kind == "suspend":
                    await self._suspend(job, action.reason)
                elif action.kind == "resume":
                    await self._resume(job)
                elif action.kind == "requeue":
                    await self._requeue(job, action.reason)
            except Exception:
                # The rest of the decision is still carried out; the next one tries this again.
                logger.exception("Lean job %s: %s failed; it is retried", job.id, action.kind)

    async def _start(self, job: ServiceJob, threads: int) -> None:
        """Start the job's run. Its state changes only once its cgroup and scratch directory exist,
        together with the task of its run; until then a failure leaves it queued, holding nothing."""
        now = self.clock()
        try:
            group = await asyncio.to_thread(JobGroup)
        except OSError as error:
            # No process of the job may run outside its cgroup: it ends, unavailable.
            logger.error("Lean job %s could not start: %s", job.id, error)
            await self._finish(job, {"error": {"type": "EnvironmentUnavailable", "message": str(error)},
                                     "definitive": False}, "failed")
            return
        scratch = self.state_dir / "scratch" / job.id
        if self.jobs.get(job.id) is not job:
            await self._not_started(job, group, None)  # It ended meanwhile: cancelled by its client, say.
            return
        try:
            scratch.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            await self._not_started(job, group, error)
            return
        job.group, job.scratch, job.start_failures, job.start_retry_at = group, scratch, 0, 0.0
        job.sched.state, job.sched.assigned, job.sched.started_at = "running", threads, now
        job.sched.observed_memory = job.sched.observed_commit = 0
        job.cpu_samples.clear()
        job.run_paused_base = job.paused_total(now)
        job.runnable.set()
        job.task = asyncio.create_task(self._run(job, threads), name="lean-job-" + job.id)
        # However the run ends, the next decision looks at it (_recover).
        job.task.add_done_callback(lambda _: self.wake())
        job.row.update(state="running", started_at=now, threads=max(job.row["threads"], threads))
        names = ["state", "started_at", "threads"]
        if job.row.get("queue_seconds") is None:
            job.row["queue_seconds"] = now - job.row["submitted_at"]
            names.append("queue_seconds")
        await self._write(job, *names)

    async def _not_started(self, job: ServiceJob, group: JobGroup, error: OSError | None) -> None:
        """Undo a start that failed before the job's run began: its cgroup, empty, is removed and
        the job, if still queued, is tried again later (START_RETRY_SECONDS)."""
        try:
            await asyncio.to_thread(group.close)
        except Exception:
            logger.exception("Lean job %s: its unused cgroup cannot be removed", job.id)
        if error is None or self.jobs.get(job.id) is not job or job.sched.state != "queued":
            return
        job.start_failures += 1
        delay = min(START_RETRY_SECONDS * 2 ** (job.start_failures - 1), START_RETRY_MAX_SECONDS)
        job.start_retry_at = self.clock() + delay
        logger.warning("Lean job %s could not start (%s: %s); it stays queued and is tried again in %.0f s",
                       job.id, type(error).__name__, error, delay)

    @staticmethod
    async def _signal(job: ServiceJob, action: str) -> bool:
        """Freeze or thaw a job's processes. False when the job ended before or meanwhile: a plan is
        carried out one action at a time, its release took the cgroup, and its state stays as it ended."""
        group = job.group
        if group is None:
            return False
        try:
            for each in (group, *job.borrowed):
                await asyncio.to_thread(getattr(each, action))
        except OSError:
            if job.group is group:
                raise
            return False
        return job.group is group

    async def _suspend(self, job: ServiceJob, reason: str) -> None:
        """Freeze a running job. Its next process waits (`runnable`) only while it is suspended: a
        freeze that fails, even part-way, is undone and the job runs on (_undo_freeze)."""
        job.runnable.clear()
        try:
            frozen = await self._signal(job, "suspend")
        except OSError as error:
            await self._undo_freeze(job, error)
            raise  # The decision logs it; a later one may suspend the job again.
        if not frozen:
            job.runnable.set()  # It ended meanwhile: nothing of it was suspended.
            return
        now = self.clock()
        job.sched.state, job.sched.suspended_at, job.sched.suspended_for = "suspended", now, reason
        job.suspended_since = now
        job.row.update(state="suspended", suspensions=job.row["suspensions"] + 1)
        await self._write(job, "state", "suspensions")

    async def _undo_freeze(self, job: ServiceJob, error: OSError) -> None:
        """A freeze that failed may have frozen part of the job (its cgroup, not a session it borrows,
        say) while its state stays running, which nothing would thaw. Thaw all of it, so the job runs
        on as its state says. If that fails too, its run is stopped and it is queued again, so it
        never waits with nothing to resume it."""
        thawed = True
        for each in [each for each in (job.group, *job.borrowed) if each is not None]:
            try:
                await asyncio.to_thread(each.resume)
            except OSError:
                thawed = False
                logger.exception("Lean job %s: a group its failed freeze may have frozen cannot be thawed", job.id)
        job.runnable.set()
        if not thawed and job.group is not None:
            logger.error("Lean job %s could not be frozen (%s) nor thawed; its run is stopped and queued again",
                         job.id, error)
            await self._requeue(job, "a failed freeze could not be undone")

    async def _requeue(self, job: ServiceJob, reason: str) -> None:
        """Stop a suspended job, which holds memory it cannot use, and queue it again (_stopped)."""
        task = job.task
        if task is None or task.done():
            return
        logger.warning("Lean job %s is stopped and queued again: %s", job.id, reason)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _resume(self, job: ServiceJob) -> None:
        if not await self._signal(job, "resume"):
            return
        now = self.clock()
        job.row["suspended_seconds"] = job.paused_total(now)
        job.suspended_since = None
        job.sched.state, job.sched.suspended_at, job.sched.suspended_for = "running", None, None
        job.row["state"] = "running"
        job.runnable.set()
        await self._write(job, "state", "suspended_seconds")

    # Execution

    async def _run(self, job: ServiceJob, threads: int) -> None:
        # Development checks and validations end by time; the Integrator's job does not, since a
        # legitimate rebuild of much of the Library may take long and a timeout holds up its batch.
        lane = job.sched.lane
        minutes = {"interactive": self.config.check_minutes, "official": self.config.validation_minutes}.get(lane, 0)
        lag = self.loop_lag
        try:
            result = await run_check(job.spec.request, runner=JobRunner(self, job), threads=threads,
                                     scratch=job.scratch, sessions=self.sessions if self.config.sessions else None,
                                     owner=job.row["owner"], time_limit=minutes * 60 if minutes else None,
                                     time_limit_reason=stopped_reason(lane, minutes * 60) if minutes else None,
                                     build_slots=self.config.build_slots or None)
            if isinstance(result.get("timings"), dict):
                result["timings"]["event_loop_lag"] = round(self.loop_lag - lag, 3)
            outcome = "completed"
        except asyncio.CancelledError:
            # A second cancellation meanwhile (a client's cancel beside a stop for memory, say) does
            # not leave the job running with no run.
            await carried_out(self._stopped(job))
            return
        except Exception as error:
            logger.exception("Lean job %s failed", job.id)
            result = {"error": {"type": "RuntimeError", "message": f"{type(error).__name__}: {error}"},
                      "definitive": False}
            outcome = "failed"
        await self._finish(job, result, outcome)

    async def _release(self, job: ServiceJob) -> bool:
        """End and measure whatever still runs in the job's cgroup, and remove it. False when it
        cannot be removed: it is then kept, lingering, with the job's threads and scratch."""
        if job.id in self.lingering:
            return False
        group, job.group = job.group, None
        if group is not None:
            try:
                usage = await asyncio.to_thread(group.usage)
                job.cpu_seconds += usage.cpu_seconds
                job.peak_committed = max(job.peak_committed, usage.peak_committed_memory)
                job.row["peak_private_memory"] = max(job.row.get("peak_private_memory") or 0, group.peak_private_memory)
            except Exception:
                logger.exception("Lean job %s: its last measurement failed", job.id)
            try:
                await asyncio.to_thread(group.close)
            except Exception:
                # The most it ran with: a build's threads are given back as it ends, cancelled too.
                threads = max(1, job.sched.assigned, job.row.get("threads") or 0)
                logger.exception("Lean job %s: its cgroup cannot be removed; it keeps %d threads until it is",
                                 job.id, threads)
                item = self.lingering[job.id] = Lingering(group, threads, job.scratch, self.clock())
                item.task = asyncio.create_task(self._keep_removing(job.id, item), name="lean-cleanup-" + job.id)
                return False
        if job.scratch is not None:
            await asyncio.to_thread(shutil.rmtree, job.scratch, True)
        return True

    async def _keep_removing(self, job_id: str, item: Lingering) -> None:
        delay = CLEANUP_SECONDS
        while True:
            await asyncio.sleep(delay)
            try:
                await asyncio.to_thread(item.group.close)
                break
            except Exception as error:
                delay = min(delay * 2, CLEANUP_MAX_SECONDS)
                logger.warning("Lean job %s: its cgroup still cannot be removed (%s); tried again in %.0f s",
                               job_id, error, delay)
        if item.scratch is not None:
            await asyncio.to_thread(shutil.rmtree, item.scratch, True)
        del self.lingering[job_id]
        logger.warning("Lean job %s: its cgroup was removed after %.0f s", job_id, self.clock() - item.since)
        self.wake()

    async def _recover(self) -> None:
        """A job whose run ended without ending the job (its task cancelled before it began, say)
        keeps no threads: it is stopped as its cancelled run would have been (_stopped), and so,
        unless its client cancelled it or it was stopped for its memory, queued again."""
        for job in list(self.jobs.values()):
            if job.sched.state == "queued" or (job.task is not None and not job.task.done()):
                continue
            task = job.task
            if task is None:
                how = "no run"
            elif task.cancelled():
                how = "its run cancelled"
            else:
                error = task.exception()
                how = "its run ended" + (f": {error!r}" if error is not None else "")
            logger.error("Lean job %s was left %s with %s; it is stopped as a cancelled run is", job.id,
                         job.sched.state, how)
            await self._stopped(job)

    async def _stopped(self, job: ServiceJob) -> None:
        """A cancelled run: cancelled by its client, stopped for its memory (it ends unavailable), or
        stopped with the service and queued again (to start once its cgroup is gone)."""
        await self._release(job)
        if job.cancel_requested:
            await self._finish(job, None, "cancelled")
            return
        if job.memory_limited is not None:
            await self._finish(job, memory_limited(job.spec.request, job.sched.lane, self.config.process_memory_gib,
                                                   job.memory_limited), "completed")
            return
        job.task = None
        job.sched.state, job.sched.assigned, job.sched.started_at = "queued", 0, None
        job.sched.requeues += 1
        job.suspended_since = None
        job.runnable.set()
        job.row.update(state="queued", requeues=job.row["requeues"] + 1)
        await self._write(job, "state", "requeues", "suspended_seconds")
        self.wake()

    async def _finish(self, job: ServiceJob, result: dict | None, outcome: str) -> None:
        try:
            await carried_out(self._record(job, result, outcome))
        except Exception:
            logger.exception("Lean job %s: its end was not recorded", job.id)
        finally:
            # Waiting clients learn the end even when recording it failed.
            self.jobs.pop(job.id, None)
            self.finished[job.id] = job
            while len(self.finished) > 500:
                self.finished.pop(next(iter(self.finished)))
            job.done.set()
            self.wake()

    async def _record(self, job: ServiceJob, result: dict | None, outcome: str) -> None:
        import json

        # Every reader sees the result as stored: JSON.
        result = None if result is None else json.loads(json.dumps(result))
        job.row.update(outcome=outcome, result=result)
        now = self.clock()
        # The result stands when the cgroup lingers: the processes that gave it were confirmed
        # ended (ProcessRunner), and the cgroup keeps its threads until it is removed.
        await self._release(job)
        run_seconds =((now - job.sched.started_at - (job.paused_total(now) - job.run_paused_base))
                       if job.sched.started_at else None)
        definitive = bool(result and result.get("definitive")) and outcome == "completed"
        fields = {"state": "done", "outcome": outcome, "result": result, "definitive": int(definitive),
                  "finished_at": now, "run_seconds": run_seconds, "cpu_seconds": job.cpu_seconds,
                  "peak_private_memory": job.row.get("peak_private_memory"),
                  "peak_committed_memory": job.peak_committed or None,
                  "suspended_seconds": job.paused_total(now)}
        job.row.update(fields)
        await self._write(job, *fields)
        if self.warm is not None and result and result.get("library_imports"):
            roots = [Path(root["path"]) for root in job.spec.request.environment.get("library_roots", [])]
            if self.warm.note(roots, set(result["library_imports"])):
                self.warm_changed.set()
        # A run stopped for its memory sizes nothing: it did not end on its own.
        if (outcome == "completed" and run_seconds is not None and job.row.get("peak_private_memory")
                and job.memory_limited is None):
            compilation = (result or {}).get("compilation") or {}
            processes = max(1, min(job.row["threads"] or 1, len(compilation.get("built_modules") or []) or 1))
            try:
                await asyncio.to_thread(
                    self.store.record_estimate, job.row["estimate_key"],
                    memory=job.row["peak_private_memory"] // processes,
                    committed=(job.peak_committed or job.row["peak_private_memory"]) // processes,
                    processes=processes, seconds=run_seconds)
            except Exception as error:
                # An estimate only sizes later jobs; the next similar job measures again.
                logger.warning("Lean job %s: its measurements were not kept (%s: %s)", job.id,
                               type(error).__name__, error)

    # Records

    async def _write(self, job: ServiceJob, *names: str) -> None:
        """Record these fields of the job's row in the store. Each write takes the row's values as
        they are when it is made (Store.write_row). One that fails, as when the disk is full, is
        kept and made again every second, with the job's later fields (_flush), so the store never
        stays behind the job: a job whose end was not written would otherwise be shown running
        and run again at the next start."""
        held = self._unwritten.pop(job.id, None)
        await self._store(job.id, job.row, set(names) | (held[1] if held is not None else set()))

    async def _store(self, job_id: str, row: dict[str, Any], names: set[str]) -> bool:
        written = False
        try:
            # A cancelled caller waits for the write: one still running then is not taken as failed.
            await collected_thread_call(self.store.write_row, job_id, row, names)
            written = True
        except Exception as error:
            if self._unwritten_since is None:
                self._unwritten_since = self.clock()
                logger.warning("Lean job records cannot be written (%s: %s); they are written again every second",
                               type(error).__name__, error)
        finally:
            if not written:
                held = self._unwritten.get(job_id)
                self._unwritten[job_id] = (row, (held[1] | names) if held is not None else names)
        return written

    async def _flush(self) -> None:
        """Write again the job records whose write failed; once one fails again, the rest wait."""
        for job_id in list(self._unwritten):
            if job_id not in self._unwritten:
                continue  # Written meanwhile with the job's later fields.
            row, names = self._unwritten.pop(job_id)
            if not await self._store(job_id, row, names):
                return
        if self._unwritten_since is not None and not self._unwritten:
            logger.warning("Lean job records are written again, after %.0f s", self.clock() - self._unwritten_since)
            self._unwritten_since = None

    # Measurement

    @staticmethod
    async def _sample_job(job: ServiceJob) -> None:
        """Measure a running job (what it uses now, and its peaks), or freeze a process that was
        starting in a suspended one. A job that ends meanwhile was measured by its release."""
        group = job.group
        if group is None:
            return
        if job.sched.state == "suspended":
            job.cpu_samples.clear()  # Its CPU while frozen says nothing of what it uses.
            await asyncio.to_thread(group.suspend)
            return
        if job.sched.state != "running":
            return
        try:
            usage = await asyncio.to_thread(group.usage)
        except OSError:
            if job.group is group:
                raise
            return  # It ended meanwhile, and its cgroup went with it.
        if job.group is not group:
            return
        job.sched.observed_memory, job.sched.observed_commit = usage.private_memory, usage.committed_memory
        for session_group in list(job.borrowed):
            borrowed = await asyncio.to_thread(session_group.usage)
            job.sched.observed_memory += borrowed.private_memory
            job.sched.observed_commit += borrowed.committed_memory
        job.peak_committed = max(job.peak_committed, usage.peak_committed_memory)
        job.row["peak_private_memory"] = max(job.row.get("peak_private_memory") or 0, group.peak_private_memory)
        samples, now = job.cpu_samples, time.monotonic()
        if samples and usage.cpu_seconds < samples[-1][1]:
            samples.clear()  # A new run, whose cgroup counts from zero.
        samples.append((now, usage.cpu_seconds))
        while len(samples) > 2 and now - samples[1][0] >= RELEASE_WINDOW_SECONDS:
            samples.popleft()  # The first stays the latest one at least a window old.

    def _release_idle_threads(self, job: ServiceJob) -> None:
        """A validation's build keeps only the threads it uses, as every job keeps one once its build
        has ended: its threads go back to the queue. In the Linux pilot a validation whose one new
        module took more than half an hour to compile held 31 of the 32 threads while it used one
        core, and 131 jobs waited."""
        sched = job.sched
        if sched.lane != "official" or sched.shape != "build" or sched.state != "running" or sched.assigned <= 1:
            return
        keep = threads_in_use(job.cpu_samples, sched.assigned)
        if keep is not None and keep < sched.assigned:
            logger.info("Lean job %s keeps %d of its %d threads: its build used %d or fewer for %.0f s",
                        job.id, keep, sched.assigned, keep - 1, RELEASE_WINDOW_SECONDS)
            sched.assigned = keep
            self.wake()

    async def _limit_memory(self, job: ServiceJob) -> None:
        """Stop a running job one of whose Lean processes holds more than `process_memory_gib`: it
        ends unavailable (`_stopped`) instead of taking the machine's memory, which stops every
        other check and queues it again, itself included at the head. A resident session that
        holds such a process ends with it: its next check starts a new one."""
        limit = int(self.config.process_memory_gib * GIB)
        group, task = job.group, job.task
        if (not limit or group is None or task is None or task.done() or job.memory_limited is not None
                or job.sched.state != "running"):
            return
        held = {each: await asyncio.to_thread(each.largest_process) for each in (group, *job.borrowed)}
        largest = max(held.values())
        if largest <= limit or job.group is not group:
            return
        logger.warning("Lean job %s is stopped: one of its processes holds %.1f GiB, past the limit of %g GiB",
                       job.id, largest / GIB, self.config.process_memory_gib)
        job.memory_limited = largest
        for session_group in job.borrowed:
            if held.get(session_group, 0) > limit:
                await asyncio.to_thread(session_group.terminate)
        task.cancel()

    async def _sample_loop(self) -> None:
        last_sample = last_prune = 0.0
        while True:
            slept = time.monotonic()
            await asyncio.sleep(TICK_SECONDS)
            self.loop_lag += max(0.0, time.monotonic() - slept - TICK_SECONDS)
            try:
                now = self.clock()
                await self._flush()
                for job in list(self.jobs.values()):
                    try:
                        await self._sample_job(job)
                        self._release_idle_threads(job)
                        await self._limit_memory(job)
                    except Exception:
                        # One job's failed measurement costs neither the others' nor the rest
                        # of this tick: in the Linux pilot half the checks were never measured.
                        logger.exception("Measuring Lean job %s failed; it is retried", job.id)
                    if (job.sched.lane == "interactive" and not job.abandoned and not job.done.is_set()
                            and now - (job.row.get("last_polled_at") or now) > CLIENT_SECONDS):
                        job.abandoned = True  # Its client has gone.
                        asyncio.create_task(self.cancel(job.id))
                await self.sessions.evict()
                low = self._last.get("memory_available")
                if low is not None and low < self.config.low_watermark_gib * GIB:
                    await self.sessions.evict(one=True)  # Memory comes first: an idle session can start again.
                if now - last_sample >= SAMPLE_SECONDS:
                    last_sample = now
                    self._last["cpu_busy"] = self._cpu.busy()
                    jobs = list(self.jobs.values())
                    await asyncio.to_thread(
                        self.store.sample, at=now, cpu_busy=self._last["cpu_busy"],
                        memory_available=self._last["memory_available"],
                        commit_available=self._last["commit_available"],
                        threads_in_use=sum(job.sched.assigned for job in jobs if job.sched.state == "running"),
                        running=sum(job.sched.state == "running" for job in jobs),
                        queued=sum(job.sched.state == "queued" for job in jobs),
                        suspended=sum(job.sched.state == "suspended" for job in jobs))
                if now - last_prune >= 3600:
                    last_prune = now
                    await asyncio.to_thread(self.store.prune, now)
                    if self.config.share_outputs_gib:
                        shared = await asyncio.to_thread(
                            self.deduper.run, int(self.config.share_outputs_gib * 1024**3))
                        if shared["shared"]:
                            logger.info("Shared %.1f GB of identical build outputs on disk", int(shared["shared"]) / 1e9)
            except Exception:
                logger.exception("Lean service measurement failed; it is retried")
