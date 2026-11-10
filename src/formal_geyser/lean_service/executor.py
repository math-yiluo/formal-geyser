"""One Lean check, as the Lean service runs it.

A check verifies the registered environment, builds the requested modules in a persistent
build slot, replays the built declarations through the kernel when the check is official,
and inspects the requested declarations while the slot is still held. The source is either
a frozen snapshot from the project's blob store (official checks) or an agent's working
directory, read in place (development checks); compiled outputs stay in the slot, never in
the source tree. Every Lean and Lake process runs through the runner the service gives the
job.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import stat
import time
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, ValidationError, model_validator

from .. import offload
from ..coordination.models import StrictModel
from ..coordination.snapshot_limits import MAX_SNAPSHOT_FILES
from ..coordination.storage import SKIPPED_DIRECTORIES, BlobStore, digest
from ..lake_build import RECIPE_VERSION, REPLAY_VERSION, timed
from ..paths import module_name, module_path, relative_path
from ..registry import (
    ApprovalRequired,
    EnvironmentUnavailable,
    RegistryError,
    file_sha256,
    read_snapshot,
)
from ..schemas import EnvironmentRecord, LeanName
from ..tooling import LeanTooling, failed_check_verdict, replay_helper

EXECUTOR_VERSION = 1


class Inspection(StrictModel):
    module: LeanName
    declaration: LeanName
    statement: LeanName | None = None


class CheckRequest(StrictModel):
    environment: dict[str, Any]  # An approved EnvironmentRecord.
    blobs: str  # The project's content-addressed store.
    snapshot: str | None = None  # Frozen source: an official check.
    directory: str | None = None  # Working files, read in place: a development check.
    overlay: str | None = None  # A draft's working files, which take the place of the directory's.
    modules: Annotated[list[LeanName], Field(min_length=1, max_length=MAX_SNAPSHOT_FILES)]
    kernel_replay: bool = True
    inspections: Annotated[list[Inspection], Field(max_length=20)] = []

    @model_validator(mode="after")
    def one_source(self):
        if (self.snapshot is None) == (self.directory is None):
            raise ValueError("A check reads exactly one source: a snapshot or a directory")
        if any(value is not None and not Path(value).is_absolute() for value in (self.directory, self.overlay)):
            raise ValueError("A working directory must be an absolute path")
        if self.overlay is not None and self.directory is None:
            raise ValueError("A draft is checked over a working directory")
        return self


class JobSpec(StrictModel):
    """What a client submits."""
    kind: Literal["check"] = "check"
    lane: Literal["integrator", "interactive", "official"]
    owner: str = Field(min_length=1, max_length=200)
    label: str = Field(min_length=1, max_length=500)
    blocking: bool = False
    request: CheckRequest


def content_key(spec: JobSpec) -> str | None:
    """Official checks of the same frozen input under the same environment are one job."""
    request = spec.request
    if request.snapshot is None:
        return None
    helpers = Path(str(replay_helper())).parent
    return digest({"executor": EXECUTOR_VERSION, "recipe": RECIPE_VERSION, "replay": REPLAY_VERSION,
                   "environment": request.environment, "snapshot": request.snapshot,
                   "modules": request.modules, "kernel_replay": request.kernel_replay,
                   "inspections": [item.model_dump(mode="json") for item in request.inspections],
                   "helpers": {name: file_sha256(helpers / name) for name in ("KernelReplay.lean", "Inspect.lean")}})


def _lean_paths(paths) -> list[str]:
    return [path for path in paths if path.endswith(".lean") and path != "lakefile.lean"
            and not any(part.startswith(".") for part in path.split("/"))]


def snapshot_sources(blobs, snapshot: str, *, max_files: int, max_bytes: int) -> dict[str, bytes]:
    """The .lean files of a frozen snapshot, as Lake builds them."""
    entries = read_snapshot(blobs, snapshot, max_files=max_files, max_bytes=max_bytes)
    by_path = {item["path"]: item["sha256"] for item in entries}
    return {path: blobs.get(by_path[path]) for path in _lean_paths(sorted(by_path))}


def working_sources(directory: Path, *, max_files: int, max_bytes: int) -> dict[str, bytes]:
    """The .lean files of a working directory, read in place within the snapshot limits.

    Links, hidden directories and build or tool directories are not followed; private
    notes and shared drafts live in hidden or separate directories.

    The walk holds each directory open and opens each file in it without following a link, so
    a link swapped in meanwhile is refused, never read. It took a third of the Lean service's
    CPU in the Linux pilot to resolve each file's path component by component instead: a
    worker's directory has a thousand Lean files.
    """
    from ..coordination.draft_paths import is_draft_path
    from ..coordination.graph_paths import is_graph_path

    root = directory.resolve()
    if not root.is_dir() or directory.is_symlink():
        raise RegistryError("The source directory does not exist or is a link")
    result: dict[str, bytes] = {}
    total = 0
    top = str(root)
    for base, directories, names, folder in os.fwalk(top, follow_symlinks=False):
        prefix = "" if base == top else os.path.relpath(base, top).replace(os.sep, "/") + "/"
        for name in list(directories):
            if (name.startswith(".") or name.casefold() in SKIPPED_DIRECTORIES
                    or (not prefix and (is_draft_path(name) or is_graph_path(name)))):
                directories.remove(name)
            elif stat.S_ISLNK(os.stat(name, dir_fd=folder, follow_symlinks=False).st_mode):
                raise RegistryError("Linked directories cannot be Lean source")
        for name in sorted(names):
            relative = prefix + name
            if not _lean_paths([relative]):
                continue
            relative_path(relative)
            info = os.stat(name, dir_fd=folder, follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("Linked artifact paths are not allowed")
            if len(result) >= max_files:
                raise RegistryError("The source has more Lean files than the snapshot limit")
            remaining = max_bytes - total
            if not stat.S_ISREG(info.st_mode) or info.st_size > remaining:
                raise RegistryError("The Lean sources exceed the snapshot byte limit")
            # Without following a link or waiting on a pipe, either swapped in since.
            handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
            with open(handle, "rb") as stream:
                body = stream.read(remaining + 1)
            if len(body) > remaining:
                raise RegistryError("The Lean sources exceed the snapshot byte limit")
            total += len(body)
            result[relative] = body
    return result


def prepared_sources(blobs: str, snapshot: str | None, directory: str | None, overlay: str | None,
                     max_files: int, max_bytes: int, modules: list[str]) -> tuple[dict[str, bytes], dict[str, Any],
                                                                                 dict[str, float]]:
    """A check's sources, their digest and library imports, the places that turn the heartbeat
    limit off, and each step's seconds. It reads and scans the whole Library, so run_check runs
    it in one bookkeeping call (offload)."""
    timings: dict[str, float] = {}
    with timed(timings, "sources"):
        if snapshot is not None:
            sources = snapshot_sources(BlobStore(Path(blobs)), snapshot, max_files=max_files, max_bytes=max_bytes)
        else:
            sources = working_sources(Path(str(directory)), max_files=max_files, max_bytes=max_bytes)
            if overlay is not None:
                sources.update(working_sources(Path(overlay), max_files=max_files, max_bytes=max_bytes))
                if len(sources) > max_files or sum(len(body) for body in sources.values()) > max_bytes:
                    raise RegistryError("The Lean sources exceed the snapshot limits")
    for module in modules:
        if module_path(module) not in sources:
            raise RegistryError(f"Missing source module: {module}")
    found: dict[str, Any] = {}
    with timed(timings, "digest"):
        found["sources_sha256"] = digest({path: hashlib.sha256(body).hexdigest() for path, body in sources.items()})
        found["library_imports"] = sorted(library_imports(sources))
    with timed(timings, "heartbeats"):
        found["unlimited"] = unlimited_heartbeats(sources, modules)
    return sources, found, timings


def project_closure(sources: dict[str, bytes], modules: list[str]) -> set[str]:
    """The project modules the requested ones import, themselves included."""
    from ..mathematical_artifacts import module_imports

    project = {module_name(path): body for path, body in sources.items()}
    seen: set[str] = set()
    pending = [module for module in modules if module in project]
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        try:
            imports = module_imports(project[module].decode("utf-8-sig"))
        except (ValueError, UnicodeDecodeError):
            continue  # The build reports it.
        pending.extend(name for name in imports if name in project)
    return seen


# Lean's heartbeat limit bounds each declaration's elaboration; turned off, one that does not end
# runs without end. In the Linux pilot a development check with `maxHeartbeats 0` ran for over 20
# minutes and grew to 14 GB before it was cancelled. A finite limit, however high, still ends.
_UNLIMITED_HEARTBEATS = re.compile(r"\bset_option\s+(?:synthInstance\.)?maxHeartbeats\s+0(?![0-9_.])")
UNLIMITED_HEARTBEATS = ("`set_option maxHeartbeats 0` turns Lean's heartbeat limit off, so a declaration whose "
                        "elaboration does not end runs without end; the Lean service does not run it. A timeout "
                        "points at the proof: restructure it, or give a finite limit.")


def _without_comments(text: str) -> str:
    """The text with Lean's comments (`--` to the end of the line, nested `/- -/`) blanked out,
    keeping every line break so that positions stay."""
    blanked, depth, index = list(text), 0, 0
    while index < len(text):
        if depth == 0 and text.startswith("--", index):
            end = text.find("\n", index)
            end = len(text) if end < 0 else end
            blanked[index:end] = " " * (end - index)
            index = end
        elif text.startswith("/-", index):
            depth += 1
            blanked[index:index + 2] = "  "
            index += 2
        elif depth and text.startswith("-/", index):
            depth -= 1
            blanked[index:index + 2] = "  "
            index += 2
        else:
            if depth and text[index] != "\n":
                blanked[index] = " "
            index += 1
    return "".join(blanked)


def unlimited_heartbeats(sources: dict[str, bytes], modules: list[str]) -> list[dict]:
    """An error diagnostic for each place where a module the check compiles, or a project module
    it imports, turns Lean's heartbeat limit off."""
    messages = []
    for module in sorted(project_closure(sources, modules)):
        path = module_path(module)
        code = _without_comments(sources[path].decode("utf-8-sig", errors="replace"))
        for match in _UNLIMITED_HEARTBEATS.finditer(code):
            line = code.count("\n", 0, match.start()) + 1
            column = match.start() - code.rfind("\n", 0, match.start()) - 1
            messages.append({"severity": "error", "fileName": path, "pos": {"line": line, "column": column},
                             "endPos": {"line": line, "column": column + match.end() - match.start()},
                             "data": UNLIMITED_HEARTBEATS})
    return messages


def _refused(request: CheckRequest, sources: dict[str, bytes], messages: list[dict], *, reason: str,
             timed_out: bool = False) -> dict:
    """A compilation report for a check that was not run, or was stopped, before Lean answered."""
    report = {"ok": False, "proof_accepted": False, "validation_method": "lean-service", "reason": reason,
              "diagnostics": {"messages": messages, "other_output": [],
                              "errors": sum(item["severity"] == "error" for item in messages),
                              "warnings": sum(item["severity"] == "warning" for item in messages)},
              "modules": [{"module": module, "source_sha256": hashlib.sha256(sources[module_path(module)]).hexdigest()}
                          for module in request.modules], "built_modules": [], "reused_modules": []}
    if timed_out:
        report["process"] = {"timed_out": True}
    return report


def stopped_reason(lane: str, seconds: float) -> str:
    """Why a check that reached its lane's time limit was stopped."""
    if lane == "interactive":
        return (f"The development check did not end within {seconds / 60:g} minutes and was stopped: an "
                "elaboration or a kernel reduction in it may not end. Find the step that does not end, or "
                "check smaller pieces; a lower finite limit for the draft, such as `set_option maxHeartbeats "
                "20000`, can make such a step stop with an error at its position. Do not raise or turn off "
                "Lean's heartbeat limit to get past it.")
    return (f"The check did not end within {seconds / 60:g} minutes and was stopped. That is no finding about "
            "the candidate: the check is unavailable and is tried again later. Its kernel replay or "
            "inspection, which Lean does not bound by heartbeats, may not end.")


def stopped_at(path: str, text: str, processing: tuple[tuple[int, int], ...] | list, diagnostics: dict) -> str:
    """Where a stopped session check was, as Lean last reported its progress, and what Lean had
    reported by then; empty if it had said neither."""
    said = []
    if processing:
        spans = [str(first) if first == last else f"{first}–{last}" for first, last in processing[:3]]
        named = (f"line {spans[0]}" if len(processing) == 1 and "–" not in spans[0]
                 else "lines " + ", ".join(spans) + (", …" if len(processing) > 3 else ""))
        lines = text.splitlines()
        first = processing[0][0]
        quoted = lines[first - 1].strip()[:120] if 0 < first <= len(lines) else ""
        said.append(f"When it was stopped, Lean was still working on {named} of {path}"
                    + (f": `{quoted}`." if quoted else "."))
    errors, warnings = diagnostics.get("errors", 0), diagnostics.get("warnings", 0)
    if errors or warnings:
        said.append(f"By then it had reported {errors} error{'s' if errors != 1 else ''} and {warnings} "
                    f"warning{'s' if warnings != 1 else ''}, which are in the diagnostics.")
    return " ".join(said)


def memory_limited(request: CheckRequest, lane: str, limit_gib: float, observed: int,
                   result: dict[str, Any] | None = None, *, by_system: bool = False) -> dict:
    """The result of a check stopped because one of its Lean processes held more than `limit_gib`
    of private memory: unavailable, as one past its time (failed_check_verdict). `result` is
    what the check had found by then, such as its doctor's report and its sources' digest,
    which every reader of a result takes for granted; a check stopped before it had them
    reports them unknown. `by_system`: the kernel ended one of its processes when memory ran
    out (the Lean service's processes are the first it ends), and `observed` is the most its
    processes were seen to hold."""
    held = f"{observed / 2**30:.1f} GiB"
    if by_system:
        if lane == "interactive":
            reason = (f"The development check was stopped: the machine's memory ran out and the system ended one "
                      f"of its Lean processes, the largest then running. The check was last seen holding {held}; "
                      f"a check may use {limit_gib:g} GiB. If it held much less, other checks filled the memory: "
                      "check again later. Otherwise an elaboration, a kernel reduction or an inspection in it "
                      "grows without bound: find the step that grows by checking smaller pieces, and without "
                      "inspections; keep large terms out of statements by naming large intermediate objects as "
                      "definitions of their own.")
        else:
            reason = (f"The check was stopped: the machine's memory ran out and the system ended one of its Lean "
                      f"processes; the check was last seen holding {held}. That is no finding about the "
                      "candidate: the check is unavailable and is tried again later.")
        stopped = f"Stopped: the system ended a Lean process when memory ran out; the check held {held}"
    elif lane == "interactive":
        reason = (f"The development check was stopped: one of its Lean processes held {held} of memory, more "
                  f"than the {limit_gib:g} GiB a check may use. An elaboration, a kernel reduction or an "
                  "inspection in it grows without bound or far past what the Library's checks need. Find the "
                  "step that grows by checking smaller pieces, and without inspections; keep large terms out of "
                  "statements by naming large intermediate objects as definitions of their own.")
    else:
        reason = (f"The check was stopped: one of its Lean processes held {held} of memory, more than the "
                  f"{limit_gib:g} GiB a check may use. That is no finding about the candidate: the check is "
                  "unavailable and is tried again later. Its kernel replay or inspection, which Lean does not "
                  "bound by heartbeats, may grow without bound.")
    if not by_system:
        stopped = f"Stopped: a Lean process held {held}, past the limit of {limit_gib:g} GiB"
    # The process error makes a client reading results with earlier code take it as unavailable too.
    compilation = {"ok": False, "proof_accepted": False, "validation_method": "lean-service", "reason": reason,
                   "process": {"memory_limited": True, "memory": observed, "error": stopped,
                               **({"ended_by_system": True} if by_system else {})},
                   "diagnostics": {"messages": [], "other_output": [], "errors": 0, "warnings": 0},
                   "modules": [{"module": module} for module in request.modules],
                   "built_modules": [], "reused_modules": []}
    found = dict(result or {})
    found.setdefault("doctor", {"ok": False, "reason": "The check was stopped before its doctor answered"})
    found.setdefault("sources_sha256", None)
    found.setdefault("library_imports", [])
    found.update(compilation=compilation, inspections=[])
    found["definitive"] = definitive(found)
    return found


def _timed_out(result: dict) -> bool:
    compilation = result.get("compilation") or {}
    reports = [compilation, compilation.get("kernel_replay") or {}, *result.get("inspections", [])]
    return any((report.get("process") or {}).get("timed_out") for report in reports)


def library_imports(sources: dict[str, bytes]) -> set[str]:
    """Modules the sources import from outside the project, such as Mathlib's."""
    from ..mathematical_artifacts import module_imports

    project = {module_name(path) for path in sources}
    found: set[str] = set()
    for body in sources.values():
        try:
            found.update(module_imports(body.decode("utf-8-sig")))
        except (ValueError, UnicodeDecodeError):
            continue
    return found - project


_VERSIONS: dict[tuple[str, str, str], dict] = {}


async def _doctor(tooling: LeanTooling) -> dict:
    """The executables are the registered ones; their version is asked once per service."""
    executable = await asyncio.to_thread(tooling.executable)
    await asyncio.to_thread(tooling.lake_executable)
    key = (str(executable), tooling.environment.lean_executable_sha256, tooling.environment.lean_version)
    if key not in _VERSIONS:
        report = await tooling.doctor()
        if not report["ok"]:
            return report
        _VERSIONS[key] = report
    return _VERSIONS[key]


def definitive(result: dict) -> bool:
    """Whether the same input would give the same result again: not a process failure."""
    error = result.get("error")
    if error is not None:
        return error.get("type") not in {"EnvironmentUnavailable", "OSError", "FileNotFoundError",
                                         "PermissionError", "RuntimeError", "ApprovalRequired"}
    if not result.get("doctor", {}).get("ok"):
        return False
    compilation = result.get("compilation") or {}
    if not compilation.get("ok"):
        return failed_check_verdict(compilation) == "rejected"
    return all(item.get("ok") or failed_check_verdict(item) == "rejected" for item in result.get("inspections", []))


async def run_check(request: CheckRequest, *, runner, threads: int = 1, scratch: Path | None = None,
                    sessions=None, owner: str | None = None, time_limit: float | None = None,
                    time_limit_reason: str | None = None, build_slots: int | None = None) -> dict:
    """The check's result; a rejection of the input is a result too. A development check of one
    module goes to the owner's resident session when `sessions` can serve it. Sources that turn
    Lean's heartbeat limit off are refused unrun. `time_limit` is the seconds of the check's own
    time, its suspended time not counted: whichever of its Lean processes, or its session check,
    is running when it runs out is stopped, and the check ends unavailable with
    `time_limit_reason`. Its build waits for one of `build_slots` build slots (None: no limit).
    Cancelled because one of its processes held too much memory (the
    runner's `memory_stop`), it ends unavailable with what it had found by then. Its `timings` give
    the seconds of each step; the compilation's own steps are in `compilation.timings`."""
    paused = getattr(runner, "paused_seconds", None)
    started, paused_before = time.monotonic(), (paused() if paused else 0.0)
    timings: dict[str, float] = {}
    result: dict[str, Any] = {"timings": timings}

    def remaining() -> float:
        assert time_limit is not None
        return time_limit - (time.monotonic() - started) + ((paused() if paused else 0.0) - paused_before)

    try:
        environment = EnvironmentRecord.model_validate(request.environment)
        blobs = BlobStore(Path(request.blobs))
        tooling = LeanTooling(environment, blobs, runner, scratch=scratch,
                              budget=remaining if time_limit is not None else None,
                              build_slots=build_slots)
        with timed(timings, "doctor"):
            result["doctor"] = await _doctor(tooling)
        if not result["doctor"]["ok"]:
            result["definitive"] = False
            return result
        limits = environment.limits
        preparing = time.monotonic()
        sources, found, steps = await offload.call(
            prepared_sources, request.blobs, request.snapshot, request.directory, request.overlay,
            limits.snapshot_file_limit, limits.snapshot_bytes, list(request.modules))
        # Handing the sources back counts toward reading them.
        steps["sources"] = round(steps["sources"] + max(0.0, time.monotonic() - preparing - sum(steps.values())), 3)
        timings.update(steps)
        result["sources_sha256"], result["library_imports"] = found["sources_sha256"], found["library_imports"]
        unlimited = found["unlimited"]
        if unlimited:
            result["compilation"] = _refused(request, sources, unlimited, reason=UNLIMITED_HEARTBEATS)
            result["inspections"] = []
            result["definitive"] = definitive(result)
            return result
        if sessions is not None and owner and sessions.eligible(request):
            from .sessions import CheckTimedOut, SessionFailed

            try:
                with timed(timings, "session"):
                    result["compilation"] = await sessions.check(
                        tooling, request, sources, owner=owner, threads=threads,
                        borrow=runner.borrow, give_back=runner.give_back,
                        time_limit=None if time_limit is None else max(0.0, remaining()), paused=paused)
                result["inspections"] = []
                result["definitive"] = definitive(result)
                return result
            except CheckTimedOut as error:
                assert time_limit is not None
                found = error.diagnostics or {}
                path = module_path(request.modules[0])
                where = stopped_at(path, sources[path].decode("utf-8-sig", errors="replace"), error.processing, found)
                reason = time_limit_reason or stopped_reason("interactive", time_limit)
                result["compilation"] = _refused(request, sources, found.get("messages", []), timed_out=True,
                                                 reason=f"{reason} {where}".rstrip())
                result["inspections"] = []
                result["definitive"] = definitive(result)
                return result
            except SessionFailed as error:
                result["session_fallback"] = str(error)  # The ordinary build answers instead.
        compiling = time.monotonic()
        async with tooling.compilation(sources, request.modules, kernel_replay=request.kernel_replay,
                                       threads=threads) as compilation:
            timings["compilation"] = round(time.monotonic() - compiling, 3)
            result["compilation"] = compilation
            result["inspections"] = []
            if compilation["ok"]:
                with timed(timings, "inspections"):
                    for item in request.inspections:
                        inspection = await tooling.inspect(Path(compilation["build_directory"]), item.module,
                                                           item.declaration, statement=item.statement)
                        result["inspections"].append({"module": item.module, "declaration": item.declaration,
                                                      **inspection})
            releasing = time.monotonic()
        # Releasing the slot also waits for the cache maintenance that ran beside the build.
        timings["release"] = round(time.monotonic() - releasing, 3)
        if time_limit is not None and remaining() <= 1.0 and _timed_out(result):
            result["compilation"]["reason"] = time_limit_reason or stopped_reason("", time_limit)
        # The registered environment is unchanged after the build too.
        with timed(timings, "library_check"):
            await asyncio.to_thread(tooling.executable)
            await tooling.library_paths()
    except (ApprovalRequired, EnvironmentUnavailable, RegistryError, ValidationError, ValueError, KeyError,
            TypeError, OSError) as error:
        result = {"error": {"type": type(error).__name__, "message": str(error)}, "timings": timings}
    except asyncio.CancelledError:
        stop = getattr(runner, "memory_stop", None)
        stopped = stop() if callable(stop) else None
        if stopped is None:
            raise
        current = asyncio.current_task()
        if current is not None:
            current.uncancel()  # The service cancelled the run for this ending only.
        lane, limit_gib, observed = stopped
        return memory_limited(request, lane, limit_gib, observed, result=result)
    result["definitive"] = definitive(result)
    return result
