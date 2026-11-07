"""Which Lean jobs run now, with how many threads, and which wait or are suspended.

A batch scheduler in the manner of HPC systems such as Slurm: strict priority classes, CPU
and memory packed together, EASY backfilling, and memory-pressure feedback. It is pure: it
reads a snapshot of the jobs and the machine and returns actions; the service carries them
out and measures what the jobs really use.

- Classes, in strict order: the Integrator's jobs, interactive development checks, then
  official validations. Official validations that block a review or a publication go
  first; the others count as blocking once they have waited long enough.
  Within a class it is first come, first served, and a job that was started over keeps
  its place at the head. Blocking validations still run a few at a time ahead of
  interactive checks (`validation_slots`), so a steady interactive load never starves the
  reviews and publications that wait for them.
- Running work is never stopped for a higher class: an Integrator job at the head of the
  queue waits for room like any job, and meanwhile only work that cannot delay it starts.
  It takes at most `integrator_threads`, and other work shares the machine while it runs.
- Jobs share the thread capacity (the logical cores) and the memory: what is
  available now, less a reserve, less the room running jobs are still expected to grow
  by. Each job's need comes from the history of similar jobs. If the job at the head
  cannot start, later jobs may start only when they cannot delay it (EASY backfilling).
- Below the low memory watermark nothing new starts; below the critical watermark the
  newest job of the lowest class is suspended, one per decision. A suspended job keeps its
  memory, so once nothing runs and memory is still below the low watermark, the newest
  suspended job of the lowest class is stopped and queued again, one per decision, past its
  cooldown: in the Linux pilot, with swap full, every check stayed suspended for 17 minutes.
- Within a class owners take turns, each with at most a few running jobs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

GIB = 2**30
Lane = Literal["integrator", "interactive", "official"]
CLASSES: dict[str, int] = {"integrator": 0, "interactive": 1, "official": 2}


@dataclass
class Job:
    id: str
    lane: Lane
    owner: str
    # What it runs now: a Lake build may use many threads, a single Lean process a few.
    shape: Literal["build", "single"] = "single"
    threads: int = 1  # The most threads it can use.
    min_threads: int = 1
    memory: int = 2 * GIB  # Estimated peak private memory of one of its Lean processes.
    commit: int = 3 * GIB  # Estimated peak committed memory of one of its Lean processes.
    processes: int = 1  # The most Lean processes it runs at once.
    seconds: float = 60.0  # Estimated running time.
    blocking: bool = False  # An official validation that a review or a publication waits for.
    submitted_at: float = 0.0
    state: Literal["queued", "running", "suspended"] = "queued"
    started_at: float | None = None
    assigned: int = 0  # Threads it holds while running or suspended.
    observed_memory: int = 0  # Private memory it uses now.
    observed_commit: int = 0
    requeues: int = 0
    suspended_at: float | None = None
    suspended_for: str | None = None

    def need(self, threads: int) -> tuple[int, int]:
        """Peak private and committed memory with this many threads."""
        parallel = max(1, min(threads, self.processes))
        return self.memory * parallel, self.commit * parallel

    def growth(self) -> tuple[int, int]:
        """How much more it is still expected to take."""
        memory, commit = self.need(self.assigned)
        return max(0, memory - self.observed_memory), max(0, commit - self.observed_commit)


@dataclass(frozen=True)
class Machine:
    threads: int  # Thread capacity: the logical cores.
    memory: int  # Available physical memory, measured now.
    commit: int  # Available committed memory, measured now.
    reserve: int = 4 * GIB  # For the system, the agents' processes and shared .olean pages.
    commit_reserve: int = 8 * GIB
    low_watermark: int = 3 * GIB
    critical_watermark: int = 3 * GIB // 2


@dataclass(frozen=True)
class Policy:
    per_owner: int = 2  # Running jobs per owner within a class.
    aging_seconds: float = 20 * 60  # A waiting official job then counts as blocking.
    # Official validations that a review or publication waits for (or that aged) may run this
    # many at once ahead of interactive checks, so a steady interactive load never starves them.
    validation_slots: int = 4
    single_threads: int = 2  # The most threads for a job that runs one Lean process.
    # The most threads for an Integrator job: it mostly restores Lake's cached modules (in the Linux
    # pilot a median 0.06 cores, at most 1.35), so the rest of the machine stays with other work.
    integrator_threads: int = 6
    # The most threads for an official validation, for the same reason (LeanServiceConfig).
    validation_threads: int = 4
    pressure_cooldown_seconds: float = 30.0  # Before a job suspended for memory resumes.


@dataclass(frozen=True)
class Action:
    kind: Literal["start", "suspend", "resume", "requeue"]
    job_id: str
    threads: int = 0
    reason: str = ""


@dataclass
class _Free:
    threads: int
    memory: int
    commit: int

    def fits(self, threads: int, memory: int, commit: int) -> bool:
        return threads <= self.threads and memory <= self.memory and commit <= self.commit

    def take(self, threads: int, memory: int, commit: int) -> None:
        self.threads -= threads
        self.memory -= memory
        self.commit -= commit


def _urgent(job: Job, now: float, policy: Policy) -> bool:
    return job.lane != "official" or job.blocking or now - job.submitted_at >= policy.aging_seconds


def rank(job: Job, now: float, policy: Policy = Policy()) -> tuple:
    """Position in the queue: class, started over, blocking or aged, arrival."""
    return CLASSES[job.lane], 0 if job.requeues else 1, 0 if _urgent(job, now, policy) else 1, job.submitted_at, job.id


def plan(jobs: list[Job], machine: Machine, now: float, policy: Policy = Policy()) -> list[Action]:
    """The actions to take now."""
    running = [job for job in jobs if job.state == "running"]
    suspended = sorted((job for job in jobs if job.state == "suspended"), key=lambda job: rank(job, now, policy))
    queued = [job for job in jobs if job.state == "queued"]
    growth = [job.growth() for job in running]
    free = _Free(machine.threads - sum(job.assigned for job in running),
                 machine.memory - machine.reserve - sum(item[0] for item in growth),
                 machine.commit - machine.commit_reserve - sum(item[1] for item in growth))

    if running and machine.memory < machine.critical_watermark:
        # Give memory back first: the newest job of the lowest class stops growing and the
        # system may page it out; the shared .olean pages stay resident.
        victim = max(running, key=lambda job: (CLASSES[job.lane], job.started_at or 0.0, job.id))
        return [Action("suspend", victim.id, reason="memory_critical")]
    if machine.memory < machine.low_watermark:
        stoppable = [job for job in suspended if job.suspended_at is None
                     or now - job.suspended_at >= policy.pressure_cooldown_seconds]
        if not running and stoppable:
            # Nothing runs to give memory back, and the suspended jobs hold theirs: the newest
            # of the lowest class stops and keeps its place at the head of its class.
            victim = max(stoppable, key=lambda job: (CLASSES[job.lane], job.started_at or 0.0, job.id))
            return [Action("requeue", victim.id, reason="memory_exhausted")]
        return []

    actions: list[Action] = []
    waiting: list[Job] = []  # Suspended jobs that do not resume now.
    resumed: list[Job] = []  # They run once this plan is carried out: they count for owners and places.
    for job in suspended:
        if (job.suspended_for == "memory_critical" and job.suspended_at is not None
                and now - job.suspended_at < policy.pressure_cooldown_seconds):
            waiting.append(job)
            continue
        memory, commit = job.growth()
        threads = max(job.assigned, 1)
        if free.fits(threads, memory, commit):
            actions.append(Action("resume", job.id, threads, "capacity_recovered"))
            free.take(threads, memory, commit)
            resumed.append(job)
        else:
            waiting.append(job)
    if not waiting:
        actions.extend(_admit(queued, running + resumed, free, now, policy, busy=bool(running or actions)))
        return actions
    # Suspended work resumes before anything new of its class or a lower one starts; a higher
    # class goes first, as it would have preempted it.
    bar = min(CLASSES[job.lane] for job in waiting)
    actions.extend(_admit([job for job in queued if CLASSES[job.lane] < bar], running + resumed, free, now, policy,
                          busy=True))
    if not actions and not running:
        # Nothing runs and none of them fits: the first one past its cooldown runs alone, rather
        # than all work waiting for memory that only finished work gives back.
        alone = next((job for job in waiting if job.suspended_for != "memory_critical"
                      or job.suspended_at is None or now - job.suspended_at >= policy.pressure_cooldown_seconds),
                     None)
        if alone is not None:
            actions.append(Action("resume", alone.id, max(alone.assigned, 1), "runs_alone"))
    return actions


def _threads(job: Job, limit: int, free: _Free) -> int:
    """The most threads, up to `limit` and the job's own, whose memory fits; 0 if none."""
    threads = min(job.threads, limit, free.threads)
    while threads >= job.min_threads:
        if free.fits(threads, *job.need(threads)):
            return threads
        threads -= 1
    return 0


def _admit(queued: list[Job], running: list[Job], free: _Free, now: float, policy: Policy, *,
           busy: bool) -> list[Action]:
    actions: list[Action] = []
    owners: dict[tuple[int, str], int] = {}
    for job in running:
        key = (CLASSES[job.lane], job.owner)
        owners[key] = owners.get(key, 0) + 1
    remaining = sorted(queued, key=lambda job: rank(job, now, policy))
    reservation: tuple[float, _Free] | None = None  # The head's start time and what is spare then.
    validations = sum(1 for job in running if job.lane == "official")

    def load(job: Job) -> int:
        return owners.get((CLASSES[job.lane], job.owner), 0)

    while remaining:
        # Owners take turns within a class: fewest running first, then queue order.
        eligible = [job for job in remaining if load(job) < policy.per_owner]
        if not eligible:
            break
        official = [job for job in eligible if job.lane == "official"]
        if (validations < policy.validation_slots and not any(job.lane == "integrator" for job in eligible)
                and any(job.requeues or _urgent(job, now, policy) for job in official)):
            # Ahead of interactive checks, in the class's own order: restarted work first.
            job = min(official, key=lambda job: (load(job), rank(job, now, policy)))
        else:
            best_class = min(CLASSES[job.lane] for job in eligible)
            job = min((job for job in eligible if CLASSES[job.lane] == best_class),
                      key=lambda job: (load(job), rank(job, now, policy)))
        remaining.remove(job)
        if job.lane == "official":
            validations += 1  # Started, backfilled or the head: it takes a place either way.
        limit = job.threads if job.shape == "build" else min(job.threads, policy.single_threads)
        threads = _threads(job, limit, free)
        if reservation is None:
            if not threads and not busy and not actions:
                threads = max(1, min(job.min_threads, free.threads))  # A job that never fits runs alone.
            if threads:
                actions.append(Action("start", job.id, threads, "fits"))
                free.take(threads, *job.need(threads))
                owners[(CLASSES[job.lane], job.owner)] = load(job) + 1
                continue
            reservation = _reserve(job, running, free, now)
            continue
        if not threads:
            continue
        memory, commit = job.need(threads)
        shadow, spare = reservation
        if now + job.seconds <= shadow or spare.fits(threads, memory, commit):
            actions.append(Action("start", job.id, threads, "backfill"))
            free.take(threads, memory, commit)
            if now + job.seconds > shadow:
                spare.take(threads, memory, commit)
            owners[(CLASSES[job.lane], job.owner)] = load(job) + 1
    return actions


def _reserve(head: Job, running: list[Job], free: _Free, now: float) -> tuple[float, _Free]:
    """When the head job can start, from the running jobs' expected ends, and what is spare then."""
    threads = max(head.min_threads, 1)
    memory, commit = head.need(threads)
    available = _Free(free.threads, free.memory, free.commit)
    ends = sorted(running, key=lambda job: max(now, (job.started_at or now) + job.seconds))
    shadow = now
    for job in ends:
        if available.fits(threads, memory, commit):
            break
        shadow = max(now, (job.started_at or now) + job.seconds)
        held_memory, held_commit = job.need(job.assigned)
        available.take(-job.assigned, -max(held_memory, job.observed_memory), -max(held_commit, job.observed_commit))
    if not available.fits(threads, memory, commit):
        # It runs alone once everything has ended; only work that ends before then may start.
        last = max((max(now, (job.started_at or now) + job.seconds) for job in running), default=now)
        return last, _Free(0, 0, 0)
    return shadow, _Free(available.threads - threads, available.memory - memory, available.commit - commit)
