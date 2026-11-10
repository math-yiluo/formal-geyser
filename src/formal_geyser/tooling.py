from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
from collections import OrderedDict
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from typing import Callable, Literal, Sequence
from uuid import uuid4

from .coordination.content import ContentConflict, _handle, _normal_path, _plain, _read
from .coordination.models import ValidationReport
from .paths import lean_name
from .process import ProcessResult, ProcessRunner, process_environment
from .registry import (
    ApprovalRequired,
    EnvironmentUnavailable,
    RegistryError,
    file_sha256,
    library_key,
    validate_library,
)
from .schemas import EnvironmentRecord

# The inspector's result with --output, and the files beside it that hold its exact
# expressions (lean_tools/Inspect.lean), each written as `Expr.repr` writes it ("repr") or, too
# large written out in full, with each distinct subterm once ("shared").
INSPECT_PROTOCOL = "task-based-lean-inspect@3"
INSPECT_EXPRESSIONS = {"type": ".type_repr", "statement_target": ".statement_target_repr"}
EXPRESSION_ENCODINGS = frozenset({"repr", "shared"})

_MODULE_SETS: OrderedDict[tuple[str, str], frozenset[str]] = OrderedDict()
_MODULE_SETS_LOCK = threading.Lock()


def registered_modules(blobs, manifest_sha: str) -> frozenset[str]:
    """Cache only immutable manifest-derived names, never library integrity."""
    key = (str(Path(blobs.root).resolve()), manifest_sha)
    with _MODULE_SETS_LOCK:
        cached = _MODULE_SETS.get(key)
        if cached is None:
            manifest = blobs.get_json(manifest_sha)
            cached = frozenset(item["path"][:-6].casefold() for item in manifest["files"]
                               if item["path"].endswith(".olean"))
            _MODULE_SETS[key] = cached
        _MODULE_SETS.move_to_end(key)
        while len(_MODULE_SETS) > 16:
            _MODULE_SETS.popitem(last=False)
        return cached


def parse_diagnostics(result: ProcessResult) -> dict:
    messages: list[dict] = []
    other = []
    for stream, text in (("stdout", result.stdout), ("stderr", result.stderr)):
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line[line.index("{"):] if "{" in line else line)
            except (json.JSONDecodeError, ValueError):
                # Lake turns Lean SerialMessage JSON into positioned log entries.
                match = re.match(r"^(error|warning|info): (.+\.lean):(\d+):(\d+): ?(.*)$", line)
                if match:
                    severity, filename, row, column, message = match.groups()
                    messages.append({"severity": severity, "fileName": filename,
                                     "pos": {"line": int(row), "column": int(column)},
                                     "data": message})
                elif messages and line and not re.match(r"^(?:error|warning|info|trace):", line):
                    messages[-1]["data"] += "\n" + line
                other.append({"stream": stream, "text": line})
                continue
            if isinstance(item, dict) and item.get("severity") in {"error", "warning", "information", "info"}:
                messages.append(item)
            else:
                other.append({"stream": stream, "text": line})
    return {"messages": messages, "other_output": other,
            "errors": sum(item["severity"] == "error" for item in messages),
            "warnings": sum(item["severity"] == "warning" for item in messages)}


SORRY_REASON = "Lean reported a declaration using sorry"


def uses_sorry(diagnostics: dict) -> bool:
    """Whether Lean reported a declaration using sorry; no check that passes may have one."""
    return any("declaration uses" in str(item.get("data", "")) and "sorry" in str(item.get("data", ""))
               for item in diagnostics["messages"])


# What stops a check at a finding about the candidate or at missing resources; RegistryError
# (with ApprovalRequired and EnvironmentUnavailable) and pydantic's ValidationError are
# ValueErrors.
CHECK_ERRORS = (ValueError, KeyError, TypeError, OSError)
COMPILED_SUFFIXES = {".olean", ".ilean", ".dll", ".so", ".dylib", ".exe", ".o", ".obj"}


def error_report(error: Exception, policy: str, details: dict | None = None) -> ValidationReport:
    """The verdict of a check stopped by `error` (one of CHECK_ERRORS): a missing approval waits
    for the operator, a missing environment or file is unavailable, any other finding rejects."""
    verdict: Literal["needs_operator", "unavailable", "rejected"] = (
        "needs_operator" if isinstance(error, ApprovalRequired)
        else "unavailable" if isinstance(error, (EnvironmentUnavailable, OSError)) else "rejected")
    return ValidationReport(verdict=verdict, policy=policy, details={**(details or {}), "reason": str(error)})


def service_failure(result: dict, details: dict, policy: str,
                    inspection_key: str | None = None) -> ValidationReport | None:
    """Record the Lean service's doctor, compilation and (as `inspection_key`) first inspection
    in `details`; the verdict of the first of them that failed, or None when all passed."""
    details["doctor"] = result["doctor"]
    if not result["doctor"]["ok"]:
        return ValidationReport(verdict="unavailable", policy=policy, details=details)
    details["compilation"] = result["compilation"]
    if not result["compilation"]["ok"]:
        return ValidationReport(verdict=failed_check_verdict(result["compilation"]), policy=policy, details=details)
    if inspection_key is not None:
        inspection = details[inspection_key] = result["inspections"][0]
        if not inspection["ok"]:
            return ValidationReport(verdict=failed_check_verdict(inspection), policy=policy, details=details)
    return None


def require_plain_sources(paths, subject: str) -> None:
    """A snapshot holds source only, none of it in the toolchain's own namespaces."""
    for path in paths:
        if Path(path).suffix.lower() in COMPILED_SUFFIXES or ".olean." in path.lower():
            raise RegistryError(f"{subject} snapshots must contain source, not precompiled binaries")
        if path.split("/")[0].split(".")[0] in {"Lean", "Init", "Std"}:
            raise RegistryError(f"{subject} sources may not shadow the toolchain's namespace roots")


def failed_check_verdict(report: dict) -> Literal["unavailable", "rejected"]:
    """Process failures are unavailable; candidate compilation diagnostics are actionable findings."""
    replay = report.get("kernel_replay")
    if isinstance(replay, dict) and not replay.get("ok"):
        return failed_check_verdict(replay)  # The build succeeded; the replay decides.
    process = report.get("process", {})
    if (process.get("timed_out") or process.get("memory_limited") or process.get("output_limited")
            or process.get("error") or process.get("tree_quiet") is False
            or "budget exhausted" in report.get("reason", "").lower()):
        return "unavailable"
    # A helper's own unreadable or malformed output says nothing about the candidate.
    if report.get("unreadable_output"):
        return "unavailable"
    if process.get("returncode") not in (None, 0) and not _reports_finding(report):
        return "unavailable"
    if any(failed_check_verdict(item) == "unavailable" for item in report.get("modules", [])):
        return "unavailable"
    return "rejected"


# Lean's message for a compiled import it could not read, such as a library file
# under memory pressure. Library integrity is checked separately.
UNREADABLE_IMPORT = "failed to read file "


def _resource_exhaustion(message: str) -> bool:
    return ("(deterministic) timeout at " in message
            or "maximum recursion depth has been reached" in message
            or "(kernel) deep recursion detected" in message
            or ("maximum number of heartbeats (" in message and "has been reached" in message))


REPLAY_PROTOCOL = "task-based-lean-replay@1"


def replay_helper() -> Path:
    return Path(str(files("formal_geyser").joinpath("lean_tools/KernelReplay.lean")))


def _reports_finding(report: dict) -> bool:
    """Whether a failed Lean process positively reported a defect in its input.

    Its exit status does not: Lean also exits with 1 on an internal panic such as
    running out of memory. A compilation reports findings as error diagnostics,
    other than an unreadable compiled import; the helpers print a finding.
    """
    diagnostics = report.get("diagnostics")
    if isinstance(diagnostics, dict):
        return any(item.get("severity") == "error" and not str(item.get("data", "")).startswith(UNREADABLE_IMPORT)
                   for item in diagnostics.get("messages", []))
    try:
        finding = json.loads(report["process"]["stdout"])
    except (KeyError, TypeError, ValueError):
        return False
    return (isinstance(finding, dict) and isinstance(finding.get("finding"), str)
            and not _resource_exhaustion(finding["finding"]))


def registered_executable(location: str, expected_sha256: str) -> Path:
    executable = Path(location)
    if not executable.is_absolute() or not executable.is_file():
        raise EnvironmentUnavailable("Registered executable is missing or not an absolute path")
    if executable.is_symlink():
        raise EnvironmentUnavailable("Register the resolved executable, not a link")
    if file_sha256(executable) != expected_sha256:
        raise EnvironmentUnavailable("Registered executable content has changed")
    return executable.resolve()


# Identity digests from full checks run on behalf of this process (see registry).
_VERIFIED_LIBRARY_DIGESTS: dict[tuple[str, str], str] = {}
# Checks in flight: concurrent requests for one library share a single child process.
_LIBRARY_CHECKS: dict[tuple, asyncio.Future] = {}


async def _run_library_check(request: dict) -> dict:
    result = await ProcessRunner().run(
        [sys.executable, "-I", "-m", "formal_geyser.library_check"],
        cwd=Path(__file__).resolve().parent, stdin=json.dumps(request).encode("utf-8"))
    try:
        response = json.loads(result.stdout)
    except ValueError:
        response = None
    if not result.ok or not isinstance(response, dict):
        detail = result.error or result.stderr.strip()[-500:] or f"exit code {result.returncode}"
        raise RegistryError("The library integrity check did not complete: " + detail)
    return response


async def validate_library_isolated(blobs, location: str, manifest_sha256: str, *,
                                    reuse_unchanged: bool = False) -> Path:
    """registry.validate_library, run in a child process.

    Hashing a multi-gigabyte library on this process's threads held the GIL
    against the coordination database thread and event loop, so every request
    stalled while validations ran. The check itself is unchanged. Concurrent
    requests for the same library share one running check instead of each
    reading it again.
    """
    key = library_key(location, manifest_sha256)
    blob_root = str(Path(blobs.root).resolve())
    flight = (asyncio.get_running_loop(), key, blob_root, reuse_unchanged)
    pending = _LIBRARY_CHECKS.get(flight)
    if pending is None:
        async def check() -> Path:
            try:
                response = await _run_library_check({
                    "blobs": blob_root, "location": location, "manifest_sha256": manifest_sha256,
                    "reuse_digest": _VERIFIED_LIBRARY_DIGESTS.get(key) if reuse_unchanged else None})
                if not response.get("ok"):
                    raise RegistryError(str(response.get("error") or "Library integrity check failed"))
                _VERIFIED_LIBRARY_DIGESTS[key] = response["digest"]
                return Path(response["root"])
            finally:
                _LIBRARY_CHECKS.pop(flight, None)
        pending = _LIBRARY_CHECKS[flight] = asyncio.ensure_future(check())
        # A check whose callers were all cancelled still completes; its outcome is retrieved here.
        pending.add_done_callback(lambda done: done.cancelled() or done.exception())
    return await asyncio.shield(pending)


def _stream_sha256(path: Path, *, chunk_bytes: int = 1 << 20) -> tuple[str, int]:
    """A file's SHA-256 and size, read in chunks under _read's checks, whatever its size."""
    path = _plain(path)
    sha, size = hashlib.sha256(), 0
    with _normal_path(path).open("rb") as stream:
        _handle(stream, path)
        before = os.fstat(stream.fileno())
        while block := stream.read(chunk_bytes):
            sha.update(block)
            size += len(block)
        after = os.fstat(stream.fileno())
        _handle(stream, path)
    if size != before.st_size or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ContentConflict("Native content changed during the read")
    return sha.hexdigest(), size


def _expression_fingerprints(result: Path) -> dict[str, tuple[str, int] | None]:
    """The SHA-256 and size of each exact expression the inspector wrote beside its result,
    read as a stream; None for one it did not write."""
    found: dict[str, tuple[str, int] | None] = {}
    for name, suffix in INSPECT_EXPRESSIONS.items():
        path = result.with_name(result.name + suffix)
        found[name] = _stream_sha256(path) if os.path.lexists(path) else None
    return found


@contextmanager
def _result_directory(scratch: Path | None, prefix: str):
    """A private directory for one helper's result file, inside the scratch directory that the
    sandboxed helper can write."""
    if scratch is None:
        with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
            yield Path(temporary)
        return
    directory = scratch / (prefix + uuid4().hex)
    directory.mkdir(parents=True)
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


class _ServiceOnly:
    """Lean and Lake processes start only in the Lean service, which supplies its job runner."""

    async def run(self, argv, **kwargs):
        raise RuntimeError("Lean processes run only in the Lean service; submit a job to it")


def lean_version(executable: Path) -> str:
    """`lean --version`, for setting up an environment before a Lean service can run it."""
    import subprocess

    result = subprocess.run([str(executable), "--version"], cwd=executable.parent, capture_output=True,
                            text=True, timeout=60)
    if result.returncode != 0 or not result.stdout.strip():
        raise EnvironmentUnavailable("The Lean compiler could not report its version")
    return result.stdout.strip()


class LeanTooling:
    def __init__(self, environment: EnvironmentRecord, blobs=None, runner=None, *,
                 isolate_library_checks: bool = True, scratch: Path | None = None,
                 budget: Callable[[], float] | None = None, build_slots: int | None = None):
        self.environment = environment
        # The most Lake builds that hold a build slot at once (lake_build); None: no limit.
        self.build_slots = build_slots
        # The seconds the check has left, if its Lean service lane limits its time.
        self.budget = budget
        self.blobs = blobs
        self.runner = runner or _ServiceOnly()
        # Where helper processes write their result files.
        self.scratch = scratch or (Path(blobs.root).parent / "scratch" if blobs else None)
        # Every check reuses a verified registered library (library_paths) and a build slot's
        # recorded compiled outputs (lake_build) while their files keep their identities.
        # Keep expensive library integrity checks off the coordination event loop.
        self.isolate_library_checks = isolate_library_checks

    def process_timeout(self) -> float | None:
        """A Lean or Lake process's timeout: the environment's limit, or the check's remaining
        time if that is shorter."""
        limits = [value for value in (self.environment.limits.timeout_seconds,
                                      self.budget() if self.budget else None) if value is not None]
        return max(0.001, min(limits)) if limits else None

    def executable(self) -> Path:
        return registered_executable(self.environment.lean_executable,
                                     self.environment.lean_executable_sha256)

    def lake_executable(self) -> Path:
        expected = self.environment.lake_executable_sha256
        if expected is None:
            raise EnvironmentUnavailable("Environment has no Lake pin; explicitly capture and approve a pinned environment")
        return registered_executable(str(self.executable().with_name("lake")), expected)

    async def kernel_replay(self, build_directory: Path, modules: Sequence[str],
                            library_paths: Sequence[Path]) -> dict:
        """Send every declaration the compiled modules recorded to the kernel again.

        Compilation trusts what elaboration put in the environment: `debug.skipKernelTC`
        or an unchecked addition can record a theorem the kernel never checked, and the
        target inspection would accept it. The helper replays the modules against one
        environment of their other imports, with the algorithm of Lean's leanchecker. Several
        modules that reach one name twice are reported with `held_name` and replay nothing.
        """
        executable = self.executable()
        helper = replay_helper()
        with _result_directory(self.scratch, "replay-") as temporary:
            request, output = temporary / "request.json", temporary / "result.json"
            request.write_text(json.dumps({"modules": list(modules)}), encoding="utf-8")
            result = await self.runner.run(
                [str(executable), "--run", str(helper), str(executable.parent.parent), str(request), str(output)],
                cwd=build_directory, timeout=self.process_timeout(),
                output_limit=self.environment.limits.output_bytes,
                env={**process_environment([build_directory, *library_paths]), "LEAN_NUM_THREADS": "1"})
            report: dict = {"ok": False, "replayed_modules": list(modules), "process": result.to_dict(),
                            "validation_method": "kernel-replay", "helper_sha256": file_sha256(helper)}
            try:
                outcome = json.loads(await asyncio.to_thread(_read, output, max_bytes=16 * 1024 * 1024))
            except (OSError, ValueError):
                outcome = None
        if isinstance(outcome, dict) and outcome.get("protocol") == REPLAY_PROTOCOL:
            if result.ok and outcome.get("verdict") == "verified":
                return {**report, "ok": True, "constants": outcome.get("constants")}
            if isinstance(outcome.get("held"), str):
                # Several modules replayed together reach one name twice, which says nothing about any
                # of them: lake_build._replay_groups replays them in smaller groups.
                report.update(held_name=outcome["held"], held_module=outcome.get("module"),
                              reason="Modules replayed together reach two declarations named " + outcome["held"])
                return report
            if isinstance(outcome.get("finding"), str):
                report["problem_module"] = outcome.get("module")
                report["reason"] = ("The kernel rejected a declaration recorded in " + str(outcome.get("module"))
                                    if outcome.get("module") else "The kernel rejected a replayed declaration")
                return report
        # The helper's own missing or malformed result says nothing about the modules.
        report.update(reason="Kernel replay did not complete", unreadable_output=True)
        return report

    def check_module_paths(self, sources: Sequence[str], libraries: Sequence[Path]) -> None:
        registered: set[str] = set()
        if self.blobs:
            for root in self.environment.library_roots:
                registered.update(registered_modules(self.blobs, root.manifest_sha256))
        prebuilt = [self.executable().parent.parent / "lib/lean", *libraries]
        for source in sources:
            module = source[:-5]
            if (module.split("/")[0].casefold() in {"lean", "init", "std"}
                    or module.casefold() in registered
                    or any((root / (module + ".olean")).is_file() for root in prebuilt)):
                raise RegistryError(f"Project source shadows a registered module: {source}")

    async def library_paths(self) -> list[Path]:
        """Check each registered library. After one full re-hash in this process, a library is
        reused while every one of its files keeps its size, modification time and file ID;
        any difference sends it through the full re-hash again (registry.check_library)."""
        if self.environment.library_roots and self.blobs is None:
            raise RegistryError("Registered libraries need the project blob store")
        try:
            if not self.isolate_library_checks:
                return [await asyncio.to_thread(validate_library, self.blobs, root.path, root.manifest_sha256,
                                                reuse_unchanged=True)
                        for root in self.environment.library_roots]
            return [await validate_library_isolated(self.blobs, root.path, root.manifest_sha256,
                                                    reuse_unchanged=True)
                    for root in self.environment.library_roots]
        except RegistryError as error:
            raise EnvironmentUnavailable(str(error)) from error

    async def doctor(self) -> dict:
        executable = await asyncio.to_thread(self.executable)
        await asyncio.to_thread(self.lake_executable)
        result = await self.runner.run([str(executable), "--version"], cwd=executable.parent,
                                       timeout=60.0,
                                       output_limit=self.environment.limits.output_bytes)
        version = result.stdout.strip()
        return {"ok": result.ok and version == self.environment.lean_version,
                "expected_version": self.environment.lean_version,
                "observed_version": version, "process": result.to_dict(),
                "validation_method": "lake-build",
                "proof_accepted": False}

    async def compile(self, source: Path | dict[str, bytes], modules: Sequence[str], *,
                      kernel_replay: bool = True, threads: int = 1) -> dict:
        """Build-only convenience. Use compilation() to inspect outputs under its lock."""
        async with self.compilation(source, modules, kernel_replay=kernel_replay, threads=threads) as report:
            return report

    def compilation(self, source: Path | dict[str, bytes], modules: Sequence[str], *,
                    kernel_replay: bool = True, threads: int = 1):
        """Build, then replay the built modules through the kernel unless this is only a
        development check (kernel_replay=False), which confers no acceptance."""
        from .lake_build import compilation
        return compilation(self, source, modules, kernel_replay=kernel_replay, threads=threads)

    async def inspect(self, directory: Path, module: str, declaration: str,
                      *, statement: str | None = None) -> dict:
        """Check the submitted declaration, its exact target and allowed assumptions."""
        if self.environment.local_execution != "trusted-code-only":
            raise RegistryError("Local Lean inspection is disabled for this environment")
        lean_name(module)
        lean_name(declaration)
        if statement:
            lean_name(statement)
        executable = self.executable()
        library_paths = await self.library_paths()
        helper = Path(str(files("formal_geyser").joinpath("lean_tools/Inspect.lean")))
        # Expr.repr spells out every shared subterm, so an instance-heavy statement's exact
        # expressions can run past any bound on a result; past a million nodes the helper writes
        # them with their sharing instead. The helper writes the small result and each
        # expression to its own private file; the expressions are hashed as they are read,
        # unchanged, and never retained. Result transport must never become a project source
        # or a cached output.
        with _result_directory(self.scratch, "inspect-") as temporary:
            output = temporary / "result.json"
            result = await self.runner.run(
                [str(executable), "--run", str(helper), str(executable.parent.parent), module, declaration,
                 *([statement] if statement else []), "--output", str(output)],
                cwd=directory, timeout=self.process_timeout(),
                output_limit=self.environment.limits.output_bytes,
                env={**process_environment([directory, *library_paths]), "LEAN_NUM_THREADS": "1"},
            )
            if not result.ok:
                return {"ok": False, "process": result.to_dict(), "proof_accepted": False}
            try:
                body = await asyncio.to_thread(_read, output, max_bytes=64 * 1024 * 1024)
                metadata = json.loads(body)
                expressions = await asyncio.to_thread(_expression_fingerprints, output)
            except (OSError, ValueError) as error:
                return {"ok": False, "error": str(error), "unreadable_output": True,
                        "process": result.to_dict(), "proof_accepted": False}
        return self._inspection_metadata(metadata, expressions, result, directory, module, declaration)

    def _inspection_metadata(self, metadata, expressions, result, directory, module, declaration) -> dict:
        try:
            axioms = metadata.get("axioms")
            origins = metadata.get("axiom_modules")
            if (not isinstance(axioms, list) or not all(isinstance(a, str) for a in axioms)
                    or not isinstance(origins, dict) or set(origins) != set(axioms)
                    or not all(isinstance(m, str) for m in origins.values())):
                raise ValueError("Inspector omitted target assumption evidence")
            if metadata.get("protocol") != INSPECT_PROTOCOL:
                raise ValueError("Inspector protocol mismatch")
            if metadata.get("declaration") != declaration:
                raise ValueError("Inspector target mismatch")
            if expressions["type"] is None:
                raise ValueError("Inspector omitted the declaration's type expression")
            if metadata.get("module") != module:
                raise ValueError("Inspector module mismatch")
            if not isinstance(metadata.get("statement_returns_prop"), bool):
                raise ValueError("Inspector omitted proposition-definition information")
            if not isinstance(metadata.get("target_matches"), bool):
                raise ValueError("Inspector omitted proposition comparison")
            if metadata["statement_returns_prop"] and expressions["statement_target"] is None:
                raise ValueError("Inspector omitted statement target expression")
            if metadata.get("type_encoding") not in EXPRESSION_ENCODINGS or (
                    metadata["statement_returns_prop"]
                    and metadata.get("statement_target_encoding") not in EXPRESSION_ENCODINGS):
                raise ValueError("Inspector omitted how an exact expression is written")
        except (ValueError, TypeError, AttributeError) as error:
            return {"ok": False, "error": str(error), "unreadable_output": True, "process": result.to_dict(),
                    "proof_accepted": False}
        disallowed = sorted(set(axioms) - set(self.environment.allowed_axioms))
        local = sorted(a for a, m in origins.items() if m and
                       (directory / (m.replace(".", "/") + ".olean")).is_file())
        metadata["type_sha256"], metadata["type_bytes"] = expressions["type"]
        if metadata["statement_returns_prop"]:
            metadata["statement_target_sha256"], metadata["statement_target_bytes"] = expressions["statement_target"]
        metadata["expression_transport"] = "private files; exact UTF-8 fingerprints retained"
        if disallowed or local or "sorryAx" in axioms:
            return {"ok": False, "reason": "Target depends on disallowed or project-declared axioms",
                    "metadata": metadata, "disallowed_axioms": disallowed, "local_axioms": local,
                    "process": result.to_dict(), "proof_accepted": False}
        return {"ok": True, "metadata": metadata, "process": result.to_dict(),
                "proof_accepted": False, "validation_method": "lake-build"}
