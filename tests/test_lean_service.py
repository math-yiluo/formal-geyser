"""The Lean service core: jobs, deduplication, persistence, preemption and measurement.

The checks are synthetic (FakeRunner): no Lean runs. One test runs a small Python process
inside a job to measure it, and one uses the real Codex sandbox when it is installed.
"""
from __future__ import annotations

import asyncio
import collections
import errno
import functools
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from formal_geyser import cgroups
from formal_geyser.coordination.models import LeanServiceConfig
from formal_geyser.coordination.runtime.executable import native_candidates
from formal_geyser.lean_service import client, executor
from formal_geyser.lean_service import service as service_module
from formal_geyser.lean_service.osjobs import JobGroup, Usage
from formal_geyser.lean_service.sandbox import CodexSandbox, Unsandboxed
from formal_geyser.lean_service.scheduler import GIB
from formal_geyser.lean_service.service import LeanService
from formal_geyser.process import ProcessResult, ProcessRunner
from tests.conftest import FakeRunner
from tests.lean_service_helpers import LocalClient

# The audited Codex as scripts/setup-linux.sh installs it, if it is installed.
CODEX = next((path for path in native_candidates(
    Path(__file__).resolve().parents[1] / ".tools/codex-0.160.0/node_modules/@openai") if path.is_file()), None)


class Gated(FakeRunner):
    """Holds the processes a predicate selects until released."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.holds: list[tuple] = []

    def hold(self, predicate):
        entered, release = asyncio.Event(), asyncio.Event()
        self.holds.append((predicate, entered, release))
        return entered, release

    async def run(self, argv, **kwargs):
        for predicate, entered, release in self.holds:
            if predicate(argv):
                entered.set()
                await release.wait()
        return await super().run(argv, **kwargs)


def builds(module):
    return lambda argv: "build" in argv and f"+{module}:olean" in argv


def inspects(module):
    return lambda argv: "--output" in argv and len(argv) > 4 and argv[4] == module


def spec(hub, *, lane="official", owner="w1", snapshot=None, directory=None, modules=("Baseline", "Candidate"),
         replay=None, inspections=(), blocking=True, label="check"):
    return {"kind": "check", "lane": lane, "owner": owner, "label": label, "blocking": blocking,
            "request": {"environment": hub.artifacts["environment-1"]["data"], "blobs": str(hub.blobs.root),
                        "snapshot": snapshot, "directory": None if directory is None else str(directory),
                        "modules": list(modules), "kernel_replay": snapshot is not None if replay is None else replay,
                        "inspections": list(inspections)}}


async def started(hub, runner=None, config=None, **kwargs):
    service = LeanService(hub.root, config or LeanServiceConfig(), runner=runner or FakeRunner(),
                          measure=lambda: (64 * GIB, 128 * GIB), threads=8, **kwargs)
    await service.start()
    return service


def workspace(tmp_path, name="work", files=None):
    directory = tmp_path / name
    for relative, body in (files or {"Other.lean": "def Other.value : Nat := 1\n"}).items():
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return directory


async def test_official_checks_of_one_input_are_one_job_whose_definitive_result_is_reused(proof_case):
    hub, _, artifact = proof_case
    runner = Gated()
    entered, release = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    try:
        first = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        second = await service.submit(spec(hub, snapshot=artifact["snapshot"], owner="w2"))
        assert second["id"] == first["id"] and second["deduplicated"] and second["state"] == "running"
        release.set()
        done = await service.wait(first["id"], 30)
        assert done["state"] == "done" and done["result"]["compilation"]["ok"] and done["result"]["definitive"]
        assert done["result"]["compilation"]["kernel_replay"]["ok"]
        again = await service.submit(spec(hub, snapshot=artifact["snapshot"], owner="w3"))
        assert (again["id"], again["state"], again["deduplicated"]) == (first["id"], "done", True)
        assert again["result"] == done["result"]
        other = await service.submit(spec(hub, snapshot=artifact["snapshot"], inspections=[
            {"module": "Candidate", "declaration": "Candidate.target", "statement": "Baseline.target"}]))
        assert other["id"] != first["id"]
        inspected = await service.wait(other["id"], 30)
        assert inspected["result"]["inspections"][0]["ok"]
    finally:
        await service.close()


async def test_an_unavailable_result_is_not_reused(proof_case):
    hub, _, artifact = proof_case
    service = await started(hub, FakeRunner(exit_code=1))  # Exit 1 without a finding: unavailable.
    try:
        first = await service.wait((await service.submit(spec(hub, snapshot=artifact["snapshot"])))["id"], 30)
        assert not first["result"]["compilation"]["ok"] and not first["result"]["definitive"]
        again = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        assert again["id"] != first["id"] and not again["deduplicated"]
    finally:
        await service.close()


async def test_a_development_check_reads_working_files_in_place_and_ends_with_its_caller(proof_case, tmp_path):
    hub, _, _ = proof_case
    work = workspace(tmp_path, files={"Other.lean": "def Other.value : Nat := 1\n", "Other.md": "Prose.",
                                      ".private/Secret.lean": "-- private\n", "drafts/Draft.lean": "-- shared\n"})
    blobs_before = sorted(path.name for path in Path(hub.blobs.root).rglob("*") if path.is_file())
    runner = Gated()
    service = await started(hub, runner)
    try:
        result = await client.run(LocalClient(service), spec(hub, lane="interactive", directory=work, modules=["Other"]))
        compilation = result["compilation"]
        assert compilation["ok"] and "kernel_replay" not in compilation
        # Where the check's time went, step by step.
        assert {"doctor", "sources", "digest", "heartbeats", "compilation", "release", "library_check",
                "event_loop_lag"} <= result["timings"].keys()
        assert {"library_check", "sources", "slot", "recover", "synchronize", "outputs", "build", "unchanged",
                "release_check"} <= compilation["timings"].keys()
        assert all(seconds >= 0 for seconds in [*result["timings"].values(), *compilation["timings"].values()])
        slot = Path(compilation["workspace"]) / "source"
        assert sorted(path.relative_to(slot).as_posix() for path in slot.rglob("*.lean")) == ["Other.lean"]
        assert not (work / ".lake").exists() and not list(work.rglob("*.olean"))
        assert sorted(path.name for path in Path(hub.blobs.root).rglob("*") if path.is_file()) == blobs_before
        entered, _ = runner.hold(builds("Other"))
        waiting = asyncio.create_task(client.run(LocalClient(service), spec(hub, lane="interactive", directory=work,
                                                                            modules=["Other"])))
        await asyncio.wait_for(entered.wait(), 30)
        [job_id] = list(service.jobs)
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        assert (await service.wait(job_id, 30))["outcome"] == "cancelled"
    finally:
        await service.close()


def test_working_sources_skip_private_and_build_directories_and_refuse_links(tmp_path):
    from formal_geyser.registry import RegistryError

    work = workspace(tmp_path, files={"A.lean": "a", "Sub/B.lean": "b", "Sub/Notes.md": "prose",
                                      "lakefile.lean": "config", "Sub/lakefile.lean": "nested",
                                      ".private/C.lean": "private", "drafts/D.lean": "draft",
                                      "Sub/drafts/E.lean": "nested draft", "__pycache__/F.lean": "cache"})
    read = executor.working_sources(work, max_files=10, max_bytes=1000)
    # A draft directory is reserved only at the top; a lakefile only at the top is configuration.
    assert read == {"A.lean": b"a", "Sub/B.lean": b"b", "Sub/lakefile.lean": b"nested",
                    "Sub/drafts/E.lean": b"nested draft"}
    with pytest.raises(RegistryError, match="more Lean files"):
        executor.working_sources(work, max_files=3, max_bytes=1000)
    with pytest.raises(RegistryError, match="byte limit"):
        executor.working_sources(work, max_files=10, max_bytes=5)
    outside = workspace(tmp_path, "outside", {"G.lean": "outside"})
    os.mkfifo(work / "Sub" / "Pipe.lean")  # Refused without waiting for a writer.
    with pytest.raises(RegistryError, match="byte limit"):
        executor.working_sources(work, max_files=10, max_bytes=1000)
    (work / "Sub" / "Pipe.lean").unlink()
    os.symlink(outside / "G.lean", work / "Sub" / "G.lean")
    with pytest.raises(ValueError, match="Linked"):
        executor.working_sources(work, max_files=10, max_bytes=1000)
    (work / "Sub" / "G.lean").unlink()
    os.symlink(outside, work / "Linked", target_is_directory=True)
    with pytest.raises(RegistryError, match="Linked directories"):
        executor.working_sources(work, max_files=10, max_bytes=1000)


async def test_an_official_check_continues_when_its_caller_stops_waiting(proof_case):
    hub, _, artifact = proof_case
    runner = Gated()
    entered, release = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    try:
        with client.waiting(0.2), pytest.raises(client.LeanJobPending) as pending:
            await client.run(LocalClient(service), spec(hub, snapshot=artifact["snapshot"]), wait=0.2)
        job_id = pending.value.job["id"]
        assert service.jobs[job_id].sched.state == "running"
        release.set()
        result = await client.run(LocalClient(service), spec(hub, snapshot=artifact["snapshot"]))
        assert result["compilation"]["ok"] and (await service.wait(job_id, 1))["state"] == "done"
    finally:
        await service.close()


async def test_a_check_the_service_lost_is_asked_again_not_rejected(proof_case):
    hub, _, artifact = proof_case
    service = await started(hub)

    class Forgetful(LocalClient):
        forgot = False

        async def wait(self, job_id, timeout):
            if not self.forgot:
                self.forgot = True
                raise KeyError(job_id)
            return await super().wait(job_id, timeout)

    try:
        runner = Gated()
        entered, release = runner.hold(builds("Candidate"))
        service.process_runner = runner
        forgetful = Forgetful(service)
        waiting = asyncio.create_task(client.run(forgetful, spec(hub, snapshot=artifact["snapshot"])))
        await asyncio.wait_for(entered.wait(), 30)
        release.set()
        result = await asyncio.wait_for(waiting, 30)
        assert forgetful.forgot and result["compilation"]["ok"]
        assert len(service.finished) == 1  # Asking again attached to the same job.
    finally:
        await service.close()


async def test_an_official_check_whose_request_times_out_continues_in_the_service():
    import httpx

    from formal_geyser.coordination.errors import ValidationContinues

    class Slow:
        def __init__(self, failing):
            self.failing = failing

        async def submit(self, spec):
            if self.failing == "submit":
                raise httpx.ReadTimeout("timed out")
            return {"id": "job-1", "state": "queued"}

        async def wait(self, job_id, timeout):
            raise httpx.ReadTimeout("timed out")

    official = {"lane": "official", "label": "Candidate c-1"}
    with pytest.raises(client.LeanServiceSlow) as slow:
        await client.run(Slow("submit"), official)
    assert isinstance(slow.value, ValidationContinues)
    assert slow.value.details == {"id": None, "state": "unknown", "lane": "official", "label": "Candidate c-1"}
    with pytest.raises(client.LeanServiceSlow) as slow:
        await client.run(Slow("wait"), official)
    assert slow.value.details["id"] == "job-1" and slow.value.details["state"] == "queued"
    with pytest.raises(httpx.ReadTimeout):  # A development check reports its timeout as before.
        await client.run(Slow("wait"), {"lane": "interactive", "label": "Check"})


async def test_jobs_survive_a_service_restart(proof_case):
    hub, _, artifact = proof_case
    runner = Gated()
    entered, _ = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
    await asyncio.wait_for(entered.wait(), 30)
    await service.close()  # Stops the running check; it is queued for the next start.
    restarted = await started(hub)
    try:
        assert restarted.jobs[job["id"]].row["requeues"] == 1
        done = await restarted.wait(job["id"], 30)
        assert done["state"] == "done" and done["result"]["compilation"]["ok"]
    finally:
        await restarted.close()


async def test_an_integrator_job_takes_at_most_a_few_threads(proof_case, tmp_path):
    hub, _, _ = proof_case
    service = await started(hub)
    try:
        batch = executor.JobSpec.model_validate(spec(hub, lane="integrator", owner="integrator", directory=workspace(tmp_path),
                                                     modules=[f"M{n}" for n in range(8)], replay=False))
        sized = service._sched("batch", batch, None, 0.0)
        assert (sized.threads, sized.min_threads) == (6, 2)  # Of the 8 threads; it mostly restores the cache.
    finally:
        await service.close()


async def test_a_validation_takes_at_most_a_few_threads(proof_case):
    # Lake restores nearly all of a validation's modules from its cache: its build gets
    # validation_threads, not a thread for each module, and may start with one.
    hub, _, artifact = proof_case
    service = await started(hub)
    try:
        official = executor.JobSpec.model_validate(spec(hub, snapshot=artifact["snapshot"],
                                                        modules=[f"M{n}" for n in range(8)]))
        sized = service._sched("validation", official, None, 0.0)
        assert (sized.threads, sized.min_threads) == (4, 1)
        estimated = service._sched("validation", official, {"processes": 8, "memory": GIB, "committed": GIB,
                                                            "seconds": 60.0}, 0.0)
        assert estimated.threads == 4
    finally:
        await service.close()
    narrow = await started(hub, config=LeanServiceConfig(validation_threads=2))
    try:
        official = executor.JobSpec.model_validate(spec(hub, snapshot=artifact["snapshot"],
                                                        modules=[f"M{n}" for n in range(8)]))
        assert narrow._sched("validation", official, None, 0.0).threads == 2
    finally:
        await narrow.close()


async def test_builds_past_the_build_slot_limit_wait_for_a_slot(proof_case):
    hub, _, artifact = proof_case
    assert LeanServiceConfig().build_slots == 8
    runner, started_builds = Gated(), []
    # Every build of Candidate is counted when it reaches Lake; the first is held there.
    entered, release = runner.hold(lambda argv: builds("Candidate")(argv) and not started_builds.append(argv))
    service = await started(hub, runner, config=LeanServiceConfig(build_slots=1))
    try:
        first = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        second = await service.submit(spec(hub, snapshot=artifact["snapshot"], owner="w2", inspections=[
            {"module": "Candidate", "declaration": "Candidate.target", "statement": "Baseline.target"}]))
        assert second["id"] != first["id"]
        await asyncio.sleep(0.5)
        assert service.jobs[second["id"]].sched.state == "running"  # Started, and waiting for the slot.
        assert len(started_builds) == 1
        release.set()
        results = [(await service.wait(job["id"], 30))["result"] for job in (first, second)]
        assert all(result["compilation"]["ok"] for result in results)
        assert [Path(result["compilation"]["workspace"]).name for result in results] == ["0", "0"]
        assert results[1]["compilation"]["timings"]["slot"] >= 0.4
    finally:
        await service.close()


def test_a_build_needs_the_cores_it_used_over_the_window_plus_one():
    window = service_module.RELEASE_WINDOW_SECONDS
    assert service_module.threads_in_use([(0.0, 0.0)], 4) is None
    assert service_module.threads_in_use([(0.0, 0.0), (window - 1, 50.0)], 4) is None  # Not a window yet.
    assert service_module.threads_in_use([(0.0, 0.0), (window, 1.2 * window)], 31) == 3
    assert service_module.threads_in_use([(0.0, 0.0), (window, 0.0)], 31) == 1
    assert service_module.threads_in_use([(0.0, 0.0), (window, 8.0 * window)], 4) == 4  # Never more than it holds.


async def test_a_validation_build_gives_back_the_threads_it_does_not_use(proof_case, monkeypatch):
    hub, _, artifact = proof_case
    service = await started(hub)
    try:
        official = executor.JobSpec.model_validate(spec(hub, snapshot=artifact["snapshot"]))
        clock, woken = [1000.0], []
        monkeypatch.setattr(service_module.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(service, "wake", lambda: woken.append(True))

        class Group:
            peak_private_memory = 0
            cpu = 0.0

            def usage(self):
                return Usage(self.cpu, 0, 0, 0, 1)

        def running(lane="official"):
            sched = service._sched("job", official, None, 0.0)
            sched.lane, sched.state, sched.shape, sched.assigned = lane, "running", "build", 4
            job = service_module.ServiceJob({"id": "job-" + lane}, official, sched)
            job.group = Group()
            return job

        job, check = running(), running("interactive")
        for _ in range(130):  # One core for 130 seconds, sampled every second.
            for each in (job, check):
                each.group.cpu += 1.0
                await LeanService._sample_job(each)
                service._release_idle_threads(each)
            clock[0] += 1.0
            if clock[0] < 1000.0 + service_module.RELEASE_WINDOW_SECONDS:
                assert job.sched.assigned == 4  # Not before a whole window.
        assert job.sched.assigned == 2 and woken
        assert check.sched.assigned == 4  # A development check keeps its own.
        assert len(job.cpu_samples) <= service_module.RELEASE_WINDOW_SECONDS + 2
        # A new run counts from zero; a frozen job's samples say nothing of what it uses.
        job.group.cpu = 0.0
        await LeanService._sample_job(job)
        assert len(job.cpu_samples) == 1
        job.sched.state = "suspended"
        job.group.suspend = lambda: None
        await LeanService._sample_job(job)
        assert not job.cpu_samples
    finally:
        await service.close()


async def test_an_integrator_job_runs_while_the_checks_already_running_go_on(proof_case, tmp_path):
    hub, _, artifact = proof_case
    runner = Gated()
    build_entered, build_release = runner.hold(builds("Candidate"))
    inspect_entered, inspect_release = runner.hold(inspects("Other"))
    seen = {}

    def integrator_build(argv):
        if "build" in argv and "+Integrated:olean" in argv:
            seen.update({job.row["label"]: job.sched.state for job in service.jobs.values()})
        return False

    runner.hold(integrator_build)
    service = await started(hub, runner)
    try:
        official = await service.submit(spec(hub, snapshot=artifact["snapshot"], label="official"))
        check = await service.submit(spec(hub, lane="interactive", directory=workspace(tmp_path), modules=["Other"],
                                          inspections=[{"module": "Other", "declaration": "Other.value"}],
                                          label="check"))
        await asyncio.wait_for(build_entered.wait(), 30)
        await asyncio.wait_for(inspect_entered.wait(), 30)
        batch = await service.submit(spec(hub, lane="integrator", owner="integrator", label="batch",
                                          directory=workspace(tmp_path, "batch", {"Integrated.lean": "-- all\n"}),
                                          modules=["Integrated"], replay=False))
        done = await service.wait(batch["id"], 30)
        assert done["result"]["compilation"]["ok"]
        assert seen == {"official": "running", "check": "running", "batch": "running"}
        build_release.set()
        inspect_release.set()
        assert (await service.wait(check["id"], 30))["result"]["inspections"][0]["ok"]
        assert (await service.wait(official["id"], 30))["result"]["compilation"]["ok"]
        rows = {row["label"]: row for row in [service.store.get(item["id"]) for item in (official, check)]}
        assert rows["official"]["requeues"] == 0 and rows["check"]["suspensions"] == 0
    finally:
        build_release.set()
        inspect_release.set()
        await service.close()


class Measured(FakeRunner):
    """Its build also runs a real process that holds 96 MiB, inside the job's cgroup."""

    async def run(self, argv, **kwargs):
        if "build" in argv:
            code = "import time; block = bytearray(96 * 2**20)\nfor i in range(0, len(block), 4096): block[i] = 1\ntime.sleep(2.5)"
            await ProcessRunner().run([sys.executable, "-c", code], cwd=kwargs["cwd"], group=kwargs.get("group"),
                                      register=False)
        return await super().run(argv, **kwargs)


async def test_each_job_is_measured_and_sizes_the_next_similar_one(proof_case):
    hub, _, artifact = proof_case
    service = await started(hub, Measured())
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await service.wait(job["id"], 60)
        row = service.store.get(job["id"])
        assert row["peak_committed_memory"] >= 90 * 2**20 and row["peak_private_memory"] >= 90 * 2**20
        assert row["cpu_seconds"] > 0 and row["run_seconds"] >= 2 and row["queue_seconds"] is not None
        estimate = service.store.estimate(row["estimate_key"])
        # Kept per Lean process: the job ran up to two at once.
        assert estimate is not None and estimate["memory"] * estimate["processes"] >= 90 * 2**20
        # The lane's last job sizes nothing else: a job nothing like has run yet takes the defaults.
        assert service.store.estimate("lane:official") is None
        # A lower peak, as of a check Lake answered from its cache, lowers it a tenth at a time.
        service.store.record_estimate(row["estimate_key"], memory=2**20, committed=2**20,
                                      processes=estimate["processes"], seconds=estimate["seconds"])
        assert service.store.estimate(row["estimate_key"])["memory"] == int(estimate["memory"] * 0.9)
        tiny = service._sched("tiny", executor.JobSpec.model_validate(spec(hub, snapshot=artifact["snapshot"])),
                              {**estimate, "memory": 2**20, "committed": 2**20}, 0.0)
        assert (tiny.memory, tiny.commit) == (GIB, 5 * GIB // 4)  # The least a Lean process is assumed to take.
        telemetry = await service.telemetry(1)
        assert telemetry["lanes"]["official"]["jobs"] == 1
        sized = service._sched("next", executor.JobSpec.model_validate(spec(hub, snapshot=artifact["snapshot"])),
                               estimate, 0.0)
        assert sized.memory >= estimate["memory"] and sized.seconds == pytest.approx(estimate["seconds"])
    finally:
        await service.close()


async def test_suspended_work_holding_the_memory_is_stopped_and_queued_again_once_nothing_runs(proof_case):
    hub, _, artifact = proof_case
    runner, memory = Gated(), [64 * GIB]
    entered, release = runner.hold(builds("Candidate"))
    service = LeanService(hub.root, LeanServiceConfig(), runner=runner, threads=8,
                          measure=lambda: (memory[0], 2 * memory[0]))
    await service.start()
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        job = service.jobs[submitted["id"]]
        await service._suspend(job, "memory_critical")
        memory[0] = 2 * GIB  # Below the low watermark, with nothing running.
        job.sched.suspended_at -= 60  # Past its cooldown.
        for _ in range(100):
            if job.sched.requeues:
                break
            await asyncio.sleep(0.1)
        assert (job.sched.state, job.sched.requeues, service.store.get(job.id)["state"]) == ("queued", 1, "queued")
        release.set()
        memory[0] = 64 * GIB
        result = await service.wait(job.id, 60)
        assert result["state"] == "done" and result["requeues"] == 1
    finally:
        await service.close()


async def test_a_job_that_ends_while_it_is_measured_is_left_to_its_release():
    class Group:
        peak_private_memory = 7

        def __init__(self, job=None, error=None):
            self.job, self.error = job, error

        def usage(self):
            if self.job is not None:
                self.job.group = None  # The job ends meanwhile: its release takes the group.
            if self.error is not None:
                raise self.error
            return Usage(1.0, 10, 12, 12, 1)

    def running(group_of):
        job = SimpleNamespace(sched=SimpleNamespace(state="running", observed_memory=0, observed_commit=0),
                              borrowed=[], peak_committed=0, peak_observed=0, row={},
                              cpu_samples=collections.deque())
        job.group = group_of(job)
        return job

    for error in (None, FileNotFoundError(2, "No such cgroup")):
        ended = running(lambda job: Group(job, error))
        await LeanService._sample_job(ended)
        assert ended.row == {} and ended.sched.observed_memory == 0
    measured = running(lambda job: Group())
    await LeanService._sample_job(measured)
    assert (measured.sched.observed_memory, measured.sched.observed_commit, measured.peak_committed,
            measured.row["peak_private_memory"]) == (10, 12, 12, 7)


async def test_a_job_that_ends_before_or_while_the_plan_freezes_or_thaws_it_stays_as_it_ended():
    # A plan is carried out one action at a time, and a job can end in between.
    class Group:
        def __init__(self, job=None, error=None):
            self.job, self.error, self.calls = job, error, []

        def signal(self, action):
            self.calls.append(action)
            if self.job is not None:
                self.job.group = None  # The job ends meanwhile: its release takes the group.
            if self.error is not None:
                raise self.error

        def suspend(self):
            self.signal("suspend")

        def resume(self):
            self.signal("resume")

    updates = []

    async def write(job, *names):
        updates.append({name: job.row[name] for name in names})

    service = SimpleNamespace(_signal=LeanService._signal, clock=lambda: 100.0, _write=write)

    def job(state, group_of=lambda job: None):
        job = SimpleNamespace(id="job", sched=SimpleNamespace(state=state, suspended_at=None, suspended_for=None),
                              row={"state": state, "suspensions": 0}, borrowed=[], runnable=asyncio.Event(),
                              suspended_since=None, paused_total=lambda now: 5.0)
        job.group = group_of(job)
        return job

    for group_of in (lambda job: None, lambda job: Group(job), lambda job: Group(job, FileNotFoundError(2, "gone"))):
        running, suspended = job("running", group_of), job("suspended", group_of)
        await LeanService._suspend(service, running, "memory")
        await LeanService._resume(service, suspended)
        assert (running.sched.state, suspended.sched.state, running.row["state"]) == ("running", "suspended", "running")
    assert updates == []
    frozen = job("running", lambda job: Group())
    await LeanService._suspend(service, frozen, "memory")
    assert frozen.group.calls == ["suspend"] and frozen.sched.state == "suspended" and not frozen.runnable.is_set()
    await LeanService._resume(service, frozen)
    assert frozen.group.calls == ["suspend", "resume"] and frozen.sched.state == "running" and frozen.runnable.is_set()
    assert updates == [{"state": "suspended", "suspensions": 1}, {"state": "running", "suspended_seconds": 5.0}]


async def test_a_freeze_that_fails_is_undone_and_the_job_runs_on():
    # A freeze that raised used to leave the job running with `runnable` cleared: its next process
    # waited for a resume that the plan, seeing it running, never gave.
    class Group:
        def __init__(self, fail=()):
            self.fail, self.calls, self.frozen = set(fail), [], False

        def suspend(self):
            self.calls.append("suspend")
            if "suspend" in self.fail:
                raise OSError(28, "No space left on device")
            self.frozen = True

        def resume(self):
            self.calls.append("resume")
            if "resume" in self.fail:
                raise OSError(28, "No space left on device")
            self.frozen = False

    updates, requeued = [], []

    async def write(job, *names):
        updates.append({name: job.row[name] for name in names})

    async def requeue(job, reason):
        requeued.append(job.id)

    service = SimpleNamespace(_signal=LeanService._signal, clock=lambda: 100.0, _write=write, _requeue=requeue)
    service._undo_freeze = functools.partial(LeanService._undo_freeze, service)

    def job(group, borrowed=()):
        return SimpleNamespace(id="job", group=group, borrowed=list(borrowed), runnable=asyncio.Event(),
                               sched=SimpleNamespace(state="running", suspended_at=None, suspended_for=None),
                               row={"state": "running", "suspensions": 0}, suspended_since=None)

    # The job's own cgroup cannot be frozen.
    failed = job(Group(fail={"suspend"}))
    with pytest.raises(OSError):
        await LeanService._suspend(service, failed, "memory")
    assert failed.group.calls == ["suspend", "resume"]
    # Part of the job froze: its cgroup did, a session it borrows did not. Both are thawed.
    session = Group(fail={"suspend"})
    partly = job(Group(), [session])
    with pytest.raises(OSError):
        await LeanService._suspend(service, partly, "memory")
    assert partly.group.calls == ["suspend", "resume"] and not partly.group.frozen
    assert session.calls == ["suspend", "resume"]
    for each in (failed, partly):
        assert each.sched.state == each.row["state"] == "running" and each.runnable.is_set()
        await asyncio.wait_for(each.runnable.wait(), 1)  # Its next process starts.
    assert updates == [] and requeued == []
    # What cannot be thawed either may stay frozen: the run is stopped and the job queued again.
    stuck = job(Group(fail={"suspend", "resume"}))
    with pytest.raises(OSError):
        await LeanService._suspend(service, stuck, "memory")
    assert stuck.runnable.is_set() and requeued == ["job"] and updates == []


async def test_a_job_without_its_cgroup_ends_unavailable_instead_of_waiting(proof_case, monkeypatch):
    hub, _, artifact = proof_case

    def unavailable(**limits):
        raise cgroups.Unavailable("The user's systemd manager is unreachable")

    monkeypatch.setattr(service_module, "JobGroup", unavailable)
    service = await started(hub)
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(job["id"], 30)
        assert done["state"] == "done" and done["outcome"] == "failed"
        assert "unreachable" in done["result"]["error"]["message"]
    finally:
        await service.close()


def stuck_groups():
    """Job cgroups whose removal fails while `state.stuck` holds, as when a process in one cannot be ended."""
    state = SimpleNamespace(stuck=True)

    class Group:
        peak_private_memory = 0

        def __init__(self, **limits):
            self.suspended = False

        def usage(self):
            return Usage(0.0, 0, 0, 0, 0)

        def oom_kills(self):
            return 0

        def largest_process(self):
            return 0

        def suspend(self):
            self.suspended = True

        def resume(self):
            self.suspended = False

        def terminate(self):
            pass

        def close(self):
            if state.stuck:
                raise OSError(errno.EBUSY, "Device or resource busy")

    return state, Group


async def test_a_job_whose_cgroup_cannot_be_removed_keeps_its_threads_and_does_not_run_twice(proof_case, monkeypatch):
    # Something of the stopped run may still run in its cgroup: the job, queued again, starts only
    # once the cgroup is gone, and meanwhile its threads go to no other work.
    hub, _, artifact = proof_case
    state, group = stuck_groups()
    monkeypatch.setattr(service_module, "JobGroup", group)
    monkeypatch.setattr(service_module, "CLEANUP_SECONDS", 0.05)
    runner = Gated()
    entered, release = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        job = service.jobs[submitted["id"]]
        threads = job.sched.assigned
        await service._requeue(job, "memory_exhausted")
        assert job.sched.state == "queued" and service.status()["cleanup"][0]["id"] == job.id
        assert service._machine().threads == 8 - threads
        release.set()
        await asyncio.sleep(0.5)  # Several decisions and removal attempts.
        assert job.sched.state == "queued" and job.sched.started_at is None
        state.stuck = False
        result = await service.wait(job.id, 60)
        assert result["state"] == "done" and result["requeues"] == 1
        assert not service.lingering and service._machine().threads == 8
    finally:
        await service.close()


async def test_a_finished_job_whose_cgroup_cannot_be_removed_keeps_its_result_and_its_threads(proof_case, monkeypatch):
    hub, _, artifact = proof_case
    state, group = stuck_groups()
    monkeypatch.setattr(service_module, "JobGroup", group)
    monkeypatch.setattr(service_module, "CLEANUP_SECONDS", 0.05)
    service = await started(hub)
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        result = await service.wait(submitted["id"], 60)
        assert result["state"] == "done" and result["outcome"] == "completed"
        item = service.lingering[submitted["id"]]
        assert service._machine().threads == 8 - item.threads and item.scratch.exists()
        state.stuck = False
        for _ in range(100):
            if not service.lingering:
                break
            await asyncio.sleep(0.05)
        assert not service.lingering and not item.scratch.exists() and service._machine().threads == 8
    finally:
        await service.close()


# On 2026-11-05 the Linux pilot's disk was full for some minutes. Starts failed at the job's scratch
# directory, runs failed, and the service's records could not always be written; seven jobs stayed
# running with no run, holding their threads, and once space returned no job started until the
# service was restarted. The next tests fill the disk at each of those places and empty it again.


def no_space(path="") -> OSError:
    return OSError(errno.ENOSPC, "No space left on device", str(path))


def counted_groups():
    """Real job cgroups, and the paths of those that exist."""
    live: set[Path] = set()

    class Group(JobGroup):
        def __init__(self, **limits):
            super().__init__(**limits)
            live.add(self.path)

        def close(self):
            super().close()
            live.discard(self.path)

    return live, Group


class FullDisk:
    """A store connection whose writes, but for new jobs, fail while `full` holds, as SQLite's do
    when the disk is full."""

    def __init__(self, db):
        self.db, self.full = db, False

    def execute(self, sql, *args):
        if self.full and not sql.startswith(("SELECT", "INSERT INTO jobs")):
            raise sqlite3.OperationalError("database or disk is full")
        return self.db.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self.db, name)


class Spaceless(FakeRunner):
    """Its processes fail for want of disk space while `full` holds."""

    full = False

    async def run(self, argv, **kwargs):
        if self.full:
            raise no_space(kwargs.get("cwd", ""))
        return await super().run(argv, **kwargs)


class GatedSpaceless(Gated, Spaceless):
    """Held as Gated, then failing as Spaceless."""


async def until(condition, seconds=10.0):
    for _ in range(int(seconds / 0.05)):
        if condition():
            return
        await asyncio.sleep(0.05)
    assert condition()


async def test_a_job_whose_scratch_directory_cannot_be_made_stays_queued_holding_nothing(proof_case, monkeypatch):
    # It holds no threads and no cgroup, is tried again later while other jobs start, and starts
    # once its directory can be made.
    hub, _, artifact = proof_case
    live, group = counted_groups()
    monkeypatch.setattr(service_module, "JobGroup", group)
    monkeypatch.setattr(service_module, "START_RETRY_SECONDS", 0.05)
    monkeypatch.setattr(service_module, "START_RETRY_MAX_SECONDS", 0.2)
    failing = {"all"}
    mkdir = Path.mkdir

    def full(path, *args, **kwargs):
        if (path.parent.name, path.parent.parent.name) == ("scratch", "lean-service") and (
                "all" in failing or path.name in failing):
            raise no_space(path)
        return mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", full)
    service = await started(hub)
    try:
        first = service.jobs[(await service.submit(spec(hub, snapshot=artifact["snapshot"])))["id"]]
        await until(lambda: first.start_failures >= 2)
        assert (first.sched.state, first.sched.assigned, first.task, first.group) == ("queued", 0, None, None)
        assert not live and service.status()["threads_in_use"] == 0
        assert service.store.get(first.id)["state"] == "queued"
        failing.clear()
        failing.add(first.id)  # Only its own directory now: other jobs start meanwhile.
        other = await service.submit(spec(hub, snapshot=artifact["snapshot"], inspections=[
            {"module": "Candidate", "declaration": "Candidate.target", "statement": "Baseline.target"}]))
        assert (await service.wait(other["id"], 30))["result"]["inspections"][0]["ok"]
        failures = first.start_failures
        await until(lambda: first.start_failures > failures)  # Still tried again.
        assert first.sched.state == "queued" and not live
        failing.clear()
        done = await service.wait(first.id, 30)
        assert done["state"] == "done" and done["result"]["compilation"]["ok"] and done["requeues"] == 0
        assert not live and service.store.get(first.id)["state"] == "done"
    finally:
        await service.close()


async def test_a_job_whose_records_cannot_be_written_runs_and_its_records_follow(proof_case):
    # Neither its start nor its end can be written: it runs and ends all the same, holds nothing
    # once it has ended, other jobs start, and its records are written once they can be.
    hub, _, artifact = proof_case
    service = await started(hub)
    disk = service.store._db = FullDisk(service.store._db)
    try:
        disk.full = True
        first = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(first["id"], 30)
        assert done["state"] == "done" and done["result"]["compilation"]["ok"] and done["result"]["definitive"]
        assert service.store.get(first["id"])["state"] == "queued"  # As submitted: nothing since was written.
        assert service.status()["unwritten"] == 1 and service.status()["threads_in_use"] == 0
        second = await service.submit(spec(hub, snapshot=artifact["snapshot"], inspections=[
            {"module": "Candidate", "declaration": "Candidate.target", "statement": "Baseline.target"}]))
        assert (await service.wait(second["id"], 30))["result"]["inspections"][0]["ok"]
        assert service.status()["unwritten"] == 2
        disk.full = False
        await until(lambda: not service._unwritten)
        rows = [service.store.get(item["id"]) for item in (first, second)]
        assert [(row["state"], row["outcome"], row["definitive"]) for row in rows] == [("done", "completed", 1)] * 2
        assert all(row["started_at"] is not None and row["queue_seconds"] is not None for row in rows)
        again = await service.submit(spec(hub, snapshot=artifact["snapshot"], owner="w2"))
        assert again["id"] == first["id"] and again["deduplicated"]  # Its definitive result, as stored.
    finally:
        await service.close()


async def test_a_run_that_fails_for_want_of_disk_space_ends_holding_nothing_and_its_end_is_written_later(
        proof_case, monkeypatch):
    # As in the pilot: a running check's processes fail when the disk fills, and its end cannot be
    # written either.
    hub, _, artifact = proof_case
    live, group = counted_groups()
    monkeypatch.setattr(service_module, "JobGroup", group)
    runner = GatedSpaceless()
    entered, release = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    disk = service.store._db = FullDisk(service.store._db)
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        assert service.store.get(submitted["id"])["state"] == "running"
        runner.full = disk.full = True
        release.set()
        done = await service.wait(submitted["id"], 30)
        assert done["state"] == "done" and not done["result"]["definitive"]
        assert "No space left on device" in json.dumps(done["result"])
        assert service.store.get(submitted["id"])["state"] == "running"  # Its end is not written yet.
        assert not service.jobs and not live and service.status()["threads_in_use"] == 0
        assert not (service.state_dir / "scratch" / submitted["id"]).exists()
        runner.full = disk.full = False
        await until(lambda: service.store.get(submitted["id"])["state"] == "done")
        again = await service.submit(spec(hub, snapshot=artifact["snapshot"]))  # Not reused: unavailable.
        assert again["id"] != submitted["id"]
        result = await service.wait(again["id"], 30)
        assert result["result"]["compilation"]["ok"] and result["result"]["definitive"]
    finally:
        release.set()
        await service.close()


@pytest.mark.parametrize("ending", [RuntimeError("lost"), asyncio.CancelledError()])
async def test_a_job_whose_run_ends_without_ending_it_is_queued_again_not_left_running(proof_case, ending):
    # A safeguard for whatever else ends a run without ending its job: a task cancelled before it
    # began, say, never runs the run's own handling.
    hub, _, artifact = proof_case
    service = await started(hub)
    run, lost = service._run, []

    async def losing(job, threads):
        if not lost:
            lost.append(job.id)
            raise ending
        await run(job, threads)

    service._run = losing
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(submitted["id"], 30)
        assert done["state"] == "done" and done["result"]["compilation"]["ok"] and done["requeues"] == 1
        assert lost == [submitted["id"]] and service.status()["threads_in_use"] == 0
    finally:
        await service.close()


async def test_a_job_whose_run_was_cancelled_twice_while_stopping_ends_cancelled(proof_case):
    # A client's cancel, and its retry, while the run is being stopped: the stop is carried out,
    # and the job ends instead of staying running with no run.
    hub, _, artifact = proof_case
    runner = Gated()
    entered, _ = runner.hold(builds("Candidate"))
    service = await started(hub, runner)
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await asyncio.wait_for(entered.wait(), 30)
        job = service.jobs[submitted["id"]]
        release = job.group.close

        def slow_close():
            time.sleep(0.3)  # The release takes a while: the second cancel arrives meanwhile.
            release()

        job.group.close = slow_close
        first = asyncio.create_task(service.cancel(job.id))
        await asyncio.sleep(0.1)
        second = asyncio.create_task(service.cancel(job.id))
        views = await asyncio.wait_for(asyncio.gather(first, second), 30)
        assert [view["outcome"] for view in views] == ["cancelled", "cancelled"]
        assert not service.jobs and service.store.get(job.id)["state"] == "done"
        assert service.status()["threads_in_use"] == 0
    finally:
        await service.close()


async def test_ending_a_job_is_carried_out_even_when_its_caller_is_cancelled_meanwhile():
    steps = []

    async def ending():
        steps.append("began")
        await asyncio.sleep(0.2)
        steps.append("ended")

    async def caller():
        await service_module.carried_out(ending())
        steps.append("after")

    task = asyncio.create_task(caller())
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert steps == ["began", "ended"]


async def test_free_memory_that_cannot_be_measured_is_not_unlimited(proof_case):
    # A reading serves a while when the next ones fail; past that nothing starts until memory is
    # measured again.
    hub, _, artifact = proof_case
    readings, now = [None], [1000.0]
    service = LeanService(hub.root, LeanServiceConfig(), runner=FakeRunner(), threads=8,
                          measure=lambda: readings[0], clock=lambda: now[0])
    await service.start()
    try:
        submitted = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await service.schedule()
        assert service.jobs[submitted["id"]].sched.state == "queued"
        assert service.status()["memory_available"] is None
        readings[0] = (64 * GIB, 128 * GIB)
        assert service._machine().memory == 64 * GIB
        readings[0] = None
        now[0] += service_module.MEASUREMENT_SECONDS
        assert service._machine().memory == 64 * GIB
        now[0] += 1
        assert service._machine() is None
        readings[0] = (64 * GIB, 128 * GIB)
        result = await service.wait(submitted["id"], 60)
        assert result["state"] == "done"
    finally:
        await service.close()


async def test_a_development_check_nobody_waits_for_is_abandoned(proof_case, tmp_path, monkeypatch):
    hub, _, _ = proof_case
    monkeypatch.setattr(service_module, "CLIENT_SECONDS", 0.5)
    runner = Gated()
    runner.hold(builds("Other"))
    service = await started(hub, runner)
    try:
        job = await service.submit(spec(hub, lane="interactive", directory=workspace(tmp_path), modules=["Other"]))
        done = await service.wait(job["id"], 10)
        assert done["state"] == "done" and done["outcome"] == "cancelled"
    finally:
        await service.close()


class Recording:
    name = "recording"

    def __init__(self):
        self.calls = []

    def wrap(self, argv, *, cwd, env, writable):
        self.calls.append({"argv": list(argv), "cwd": Path(cwd), "env": dict(env), "writable": list(writable)})
        return list(argv), dict(env)


async def test_processes_run_in_the_job_slot_or_scratch_with_their_own_temporary_directory(proof_case, monkeypatch):
    hub, _, artifact = proof_case
    monkeypatch.setattr(executor, "_VERSIONS", {})
    sandbox = Recording()
    service = await started(hub, sandbox=sandbox)
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        await service.wait(job["id"], 30)
        version, build = sandbox.calls[0], next(call for call in sandbox.calls if "build" in call["argv"])
        scratch = service.state_dir / "scratch" / job["id"]
        # A version probe needs no working directory: it does not get the toolchain's to write.
        assert "--version" in version["argv"] and version["cwd"] == scratch
        assert version["env"]["TMPDIR"] == str(scratch / "tmp")
        assert build["cwd"].is_relative_to(Path(hub.blobs.root).parent / "lake" / "workspaces")
        assert Path(build["env"]["LAKE_CACHE_DIR"]) in build["writable"] and scratch in build["writable"]
        assert not scratch.exists()  # Removed with the job.
    finally:
        await service.close()


@pytest.mark.skipif(CODEX is None, reason="the audited Codex in .tools")
async def test_the_codex_sandbox_lets_a_process_write_only_its_roots():
    # Inside the checkout, like a project, rather than below /tmp, which the sandbox keeps read-only.
    base = Path(__file__).resolve().parents[1] / ".task-based-sandbox-test"
    shutil.rmtree(base, ignore_errors=True)
    work, extra, outside = base / "work", base / "cache", base / "outside"
    for directory in (work, extra, outside):
        directory.mkdir(parents=True)
    tmp = Path("/tmp") / f"lean-sandbox-test-{uuid.uuid4().hex}.txt"
    sandbox = CodexSandbox(str(CODEX), base / "codex-home")
    try:
        results = {}
        for name, target in (("work", work / "a.txt"), ("extra", extra / "b.txt"), ("outside", outside / "c.txt"),
                             ("tmp", tmp)):
            code = f"open(r'{target}', 'w').write('x')"
            command, env = sandbox.wrap([sys.executable, "-c", code], cwd=work,
                                        env={**os.environ, "TMPDIR": str(work)}, writable=[work, extra])
            results[name] = (await ProcessRunner().run(command, cwd=work, env=env, register=False)).returncode
        assert results["work"] == 0 and results["extra"] == 0 and results["outside"] != 0 and results["tmp"] != 0
        assert not (outside / "c.txt").exists() and not tmp.exists()
    finally:
        shutil.rmtree(base, ignore_errors=True)
        tmp.unlink(missing_ok=True)


def test_the_unsandboxed_runner_changes_nothing(tmp_path):
    assert Unsandboxed().wrap(["lean"], cwd=tmp_path, env={"A": "1"}, writable=[]) == (["lean"], {"A": "1"})


@pytest.mark.skipif(os.name != "posix", reason="a POSIX shell")
def test_a_long_command_reads_its_arguments_from_a_file(tmp_path):
    # More arguments than bwrap's 9,000, as a validation of the whole pilot Library passed.
    script = tmp_path / "arguments.py"
    script.write_text("import json, sys\nprint(json.dumps(sys.argv[1:]))\n", encoding="utf-8")
    targets = [f"+Library.Module{index}:olean" for index in range(9500)] + ["*", "$HOME"]
    argv = [sys.executable, str(script), "build", *targets]
    command, path = service_module.argument_file_command(argv, tmp_path)
    assert len(command) == 5 and command[3] == sys.executable
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    # Exactly the original arguments, in order, with no pattern or variable expanded.
    assert json.loads(result.stdout) == ["build", *targets]
    path.unlink()
    with pytest.raises(ValueError):
        service_module.argument_file_command([sys.executable, "two words"], tmp_path)


@pytest.mark.skipif(CODEX is None, reason="the audited Codex in .tools")
async def test_the_codex_sandbox_runs_a_command_longer_than_bwrap_accepts():
    base = Path(__file__).resolve().parents[1] / ".task-based-sandbox-arguments"
    shutil.rmtree(base, ignore_errors=True)
    work = base / "work"
    work.mkdir(parents=True)
    sandbox = CodexSandbox(str(CODEX), base / "codex-home")
    try:
        script = work / "count.py"
        script.write_text("import sys\nprint(len(sys.argv) - 1, sys.argv[-1])\n", encoding="utf-8")
        argv = [sys.executable, str(script), *[f"+M{index}:olean" for index in range(9500)]]
        command, _ = service_module.argument_file_command(argv, work)
        wrapped, env = sandbox.wrap(command, cwd=work, env={**os.environ, "TMPDIR": str(work)}, writable=[work])
        result = await ProcessRunner().run(wrapped, cwd=work, env=env, register=False)
        assert result.returncode == 0 and result.stdout.split() == ["9500", "+M9499:olean"]
    finally:
        shutil.rmtree(base, ignore_errors=True)


async def test_the_job_runner_passes_a_long_build_through_an_argument_file(tmp_path):
    seen = {}

    class Capture:
        async def run(self, command, **kwargs):
            seen.update(command=command, arguments=Path(command[-1]).read_text().split(), shape=job.sched.shape)
            return SimpleNamespace(ok=True)

    targets = [f"+M{index}:olean" for index in range(service_module.ARGUMENT_FILE_LIMIT + 1)]
    sandbox = Recording()
    service = SimpleNamespace(sandbox=sandbox, process_runner=Capture(), clock=lambda: 0.0, wake=lambda: None)
    runnable = asyncio.Event()
    runnable.set()
    job = SimpleNamespace(runnable=runnable, sched=SimpleNamespace(shape="single", assigned=1), scratch=tmp_path / "scratch",
                          spec=SimpleNamespace(request=SimpleNamespace(blobs=str(tmp_path / "objects" / "blobs"))),
                          group=None, paused_total=lambda now: 0.0)
    slot = tmp_path / "objects" / "lake" / "workspaces" / "slot"
    await service_module.JobRunner(service, job).run(["/toolchain/bin/lake", "build", *targets], cwd=slot, env={})
    assert len(sandbox.calls[0]["argv"]) == 5 and seen["arguments"] == ["build", *targets]
    # Still scheduled as a build while it runs, and the file goes with the process.
    assert seen["shape"] == "build" and job.sched.shape == "single"
    assert not Path(seen["command"][-1]).exists()


def test_only_turning_the_heartbeat_limit_off_is_refused_where_lean_would_compile():
    sources = {"A.lean": b"import B\n-- set_option maxHeartbeats 0\n/- set_option maxHeartbeats 0 /- nested -/ -/\n"
                         b"set_option maxHeartbeats 800000 in\ntheorem a : True := trivial\n"
                         b"  set_option synthInstance.maxHeartbeats 0 in\ntheorem b : True := trivial\n",
               "B.lean": b"set_option maxHeartbeats 0\n", "C.lean": b"set_option maxHeartbeats 0\n"}
    found = executor.unlimited_heartbeats(sources, ["A"])
    # A finite raise and comments pass; B is compiled with A, C is not.
    assert [(item["fileName"], item["pos"]) for item in found] == [
        ("A.lean", {"line": 6, "column": 2}), ("B.lean", {"line": 1, "column": 0})]


async def test_a_check_that_turns_the_heartbeat_limit_off_is_refused_unrun(proof_case, tmp_path):
    hub, _, _ = proof_case
    runner = FakeRunner()
    service = await started(hub, runner)
    try:
        files = {"Other.lean": "def Other.value : Nat := 1\n",
                 "Slow.lean": "import Other\n\nset_option maxHeartbeats 0 in\ntheorem slow : True := trivial\n"}
        job = await service.submit(spec(hub, lane="interactive", directory=workspace(tmp_path, files=files),
                                        modules=["Slow"]))
        done = await service.wait(job["id"], 30)
        compilation = done["result"]["compilation"]
        assert not compilation["ok"] and compilation["reason"] == executor.UNLIMITED_HEARTBEATS
        [message] = compilation["diagnostics"]["messages"]
        assert (message["severity"], message["fileName"], message["pos"]) == ("error", "Slow.lean", {"line": 3, "column": 0})
        assert not any("build" in argv for argv, _ in runner.calls)  # Lean never ran on it.
        assert done["result"]["definitive"]  # The same input is refused again: a finding, not an outage.
    finally:
        await service.close()


async def test_checks_have_a_time_limit_by_lane_and_the_integrators_job_none(proof_case, tmp_path):
    hub, _, artifact = proof_case
    runner = FakeRunner()
    service = await started(hub, runner)

    async def timeouts(body):
        runner.calls.clear()
        job = await service.submit(body)
        assert (await service.wait(job["id"], 60))["state"] == "done"
        return [kwargs["timeout"] for argv, kwargs in runner.calls if "--version" not in argv]

    try:
        check = await timeouts(spec(hub, lane="interactive", directory=workspace(tmp_path), modules=["Other"],
                                    inspections=[{"module": "Other", "declaration": "Other.value"}]))
        validation = await timeouts(spec(hub, snapshot=artifact["snapshot"]))
        batch = await timeouts(spec(hub, lane="integrator", owner="integrator", label="batch",
                                    directory=workspace(tmp_path, "batch", {"Integrated.lean": "-- all\n"}),
                                    modules=["Integrated"], replay=False))
        # The build and the inspection share the check's 30 minutes, the build and the kernel
        # replay the validation's 60, each starting with what is left; the Integrator has none.
        assert len(check) == 2 and all(30 * 60 - 60 < value <= 30 * 60 for value in check)
        assert len(validation) == 2 and all(60 * 60 - 60 < value <= 60 * 60 for value in validation)
        assert batch and all(value is None for value in batch)
    finally:
        await service.close()


async def test_a_validation_whose_kernel_replay_does_not_end_is_stopped_unavailable(proof_case):
    hub, _, artifact = proof_case

    class EndlessReplay(FakeRunner):
        async def run(self, argv, **kwargs):
            if len(argv) > 2 and Path(argv[2]).name == "KernelReplay.lean":
                await asyncio.sleep(kwargs["timeout"])  # What the process runner does to a process that runs on.
                return ProcessResult(tuple(argv), None, "", "", kwargs["timeout"], timed_out=True)
            return await super().run(argv, **kwargs)

    service = LeanService(hub.root, LeanServiceConfig(validation_minutes=0.02), runner=EndlessReplay(),
                          measure=lambda: (64 * GIB, 128 * GIB), threads=8)
    await service.start()
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(job["id"], 60)
        compilation = done["result"]["compilation"]
        assert not compilation["ok"] and compilation["kernel_replay"]["process"]["timed_out"]
        assert compilation["reason"].startswith("The check did not end within 0.02 minutes")
        assert done["result"]["definitive"] is False  # Unavailable, not a finding, and not reused.
    finally:
        await service.close()


class Growing(FakeRunner):
    """Its build runs a real process that holds 256 MiB, inside the job's cgroup, until stopped."""

    async def run(self, argv, **kwargs):
        if "build" in argv:
            code = "import time; block = bytearray(256 * 2**20)\nfor i in range(0, len(block), 4096): block[i] = 1\ntime.sleep(60)"
            await ProcessRunner().run([sys.executable, "-c", code], cwd=kwargs["cwd"], group=kwargs.get("group"),
                                      register=False)
        return await super().run(argv, **kwargs)


async def test_a_check_whose_process_holds_more_than_its_memory_limit_ends_unavailable(proof_case):
    # In the pilot two inspections grew past 8 GB: the machine's memory ran out, every check was
    # stopped and queued again, the two started over first, and no check ended for 12 minutes.
    hub, _, artifact = proof_case
    service = LeanService(hub.root, LeanServiceConfig(process_memory_gib=0.125), runner=Growing(),
                          measure=lambda: (64 * GIB, 128 * GIB), threads=8)
    await service.start()
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(job["id"], 50)
        compilation = done["result"]["compilation"]
        assert done["state"] == "done" and done["requeues"] == 0
        assert compilation["process"]["memory_limited"] and compilation["process"]["memory"] > 0.125 * GIB
        assert compilation["reason"].startswith("The check was stopped: one of its Lean processes held")
        assert done["result"]["definitive"] is False  # Unavailable, as one past its time.
        # What every reader of a result takes for granted is there, as the check had found it.
        assert done["result"]["doctor"]["ok"] and isinstance(done["result"]["sources_sha256"], str)
        row = service.store.get(job["id"])
        assert row["outcome"] == "completed" and service.store.estimate(row["estimate_key"]) is None
    finally:
        await service.close()


async def test_a_resident_session_holding_the_large_process_ends_with_the_check(proof_case):
    hub, _, _ = proof_case

    class Group:
        def __init__(self, largest):
            self.largest, self.terminated = largest, False

        def largest_process(self):
            return self.largest

        def terminate(self):
            self.terminated = True

    service = LeanService(hub.root, LeanServiceConfig(process_memory_gib=1), runner=FakeRunner())
    own, session, other = Group(10), Group(2 * GIB), Group(GIB // 2)
    running = asyncio.create_task(asyncio.sleep(60))
    job = SimpleNamespace(id="check", group=own, task=running, memory_limited=None,
                          sched=SimpleNamespace(state="running"), borrowed=[session, other])
    await service._limit_memory(job)
    await asyncio.gather(running, return_exceptions=True)
    assert running.cancelled() and job.memory_limited == 2 * GIB
    assert session.terminated and not other.terminated and not own.terminated
    # Within the limit nothing happens.
    calm = SimpleNamespace(id="calm", group=Group(10), task=asyncio.create_task(asyncio.sleep(60)),
                           memory_limited=None, sched=SimpleNamespace(state="running"), borrowed=[])
    await service._limit_memory(calm)
    assert calm.memory_limited is None and not calm.task.cancelled()
    calm.task.cancel()


async def test_a_check_whose_process_the_kernel_ended_for_memory_ends_unavailable_saying_so(proof_case, monkeypatch):
    # On the pilot on 2026-11-09 the kernel ended a development check's Lean process at 18.6 GiB
    # when the machine's memory ran out; the check was recorded as completed with a process
    # ended by a signal, and its worker ran the same check twice more.
    hub, _, artifact = proof_case

    class Ended(JobGroup):
        def oom_kills(self):
            return 1

    monkeypatch.setattr(service_module, "JobGroup", Ended)
    service = LeanService(hub.root, LeanServiceConfig(process_memory_gib=12), runner=FakeRunner(),
                          measure=lambda: (64 * GIB, 128 * GIB), threads=8)
    await service.start()
    try:
        job = await service.submit(spec(hub, snapshot=artifact["snapshot"]))
        done = await service.wait(job["id"], 50)
        compilation = done["result"]["compilation"]
        assert done["state"] == "done" and done["requeues"] == 0
        assert compilation["process"]["memory_limited"] and compilation["process"]["ended_by_system"]
        assert compilation["reason"].startswith("The check was stopped: the machine's memory ran out")
        assert done["result"]["definitive"] is False and done["result"]["doctor"]["ok"]
        row = service.store.get(job["id"])
        assert row["outcome"] == "completed" and service.store.estimate(row["estimate_key"]) is None
    finally:
        await service.close()


def test_a_job_and_a_session_are_bounded_by_the_process_limit_for_each_thread():
    limits = service_module.job_memory_limits(LeanServiceConfig(process_memory_gib=12), 1)
    assert limits == {"memory_max": (12 + service_module.JOB_MEMORY_SLACK_GIB) * GIB, "swap_max": 12 * GIB}
    assert service_module.job_memory_limits(LeanServiceConfig(process_memory_gib=12), 4)["swap_max"] == 48 * GIB
    assert service_module.job_memory_limits(LeanServiceConfig(process_memory_gib=0), 4) == {}


async def test_resident_sessions_are_bounded_as_a_one_thread_job(proof_case):
    hub, _, _ = proof_case
    service = LeanService(hub.root, LeanServiceConfig(process_memory_gib=2), runner=FakeRunner())
    assert service.sessions.group_limits == {"memory_max": (2 + service_module.JOB_MEMORY_SLACK_GIB) * GIB,
                                             "swap_max": 2 * GIB}


def test_a_development_check_ended_by_the_system_says_what_to_do():
    request = executor.CheckRequest.model_validate({
        "environment": {}, "blobs": "/b", "directory": "/w", "modules": ["Draft"]})
    result = executor.memory_limited(request, "interactive", 12, 3 * GIB, by_system=True)
    reason = result["compilation"]["reason"]
    assert reason.startswith("The development check was stopped: the machine's memory ran out")
    assert "check again later" in reason and "3.0 GiB" in reason and result["definitive"] is False
    assert result["compilation"]["process"]["error"].startswith("Stopped: the system ended a Lean process")


def test_a_development_check_stopped_for_its_memory_says_what_to_do():
    request = executor.CheckRequest.model_validate({
        "environment": {}, "blobs": "/b", "directory": "/w", "modules": ["Draft"]})
    result = executor.memory_limited(request, "interactive", 12, 13 * GIB)
    reason = result["compilation"]["reason"]
    assert reason.startswith("The development check was stopped: one of its Lean processes held 13.0 GiB")
    assert "naming large intermediate objects" in reason and result["definitive"] is False
