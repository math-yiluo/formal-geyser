"""Explicit, restartable initialization of a pinned Lean project instance.

The project keeps one dependency cache and one small readiness receipt. Workers
never provision dependencies. Imported source is an operator-supplied baseline,
not a collection of invented peer-review or proof-acceptance receipts.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .bootstrap_pins import LEAN_TOOLCHAIN, LEAN_VERSION, MATHLIB_COMMIT, pinned_versions
from .coordination.models import Actor, Command, Role
from .coordination.snapshot_limits import MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_FILES
from .coordination.storage import atomic_json, digest
from .lean_service import client as lean_service
from .mathematical_artifacts import snapshot_module_order, validate_snapshot_sources
from .registry import (
    EnvironmentUnavailable,
    ObjectRegistry,
    file_sha256,
    library_manifest,
    read_snapshot,
)
from .schemas import EnvironmentRecord, ExecutionLimits, LibraryRoot
from .tooling import LeanTooling, lean_version

Progress = Callable[[str], Any] | None
_EXCLUDED_DIRECTORIES = frozenset({
    ".git", ".lake", ".dependencies", ".task-based", ".private", ".venv",
    "__pycache__", "node_modules", ".taskhub-integration",
})
_BUILD_FILES = frozenset({"lakefile.lean", "lakefile.md"})
_LOCK_NAME = "lean-environment.lock.json"
# The license, notice and copying files of code ported from other projects, which their licenses
# require to travel with it (LICENSE, NOTICE, COPYING, LICENCE-MIT, NOTICE.txt, ...). Until
# 2026-11-08 an initial Library kept only .lean and .md: the pilot's move to the Mac left eight behind.
_LEGAL_FILE = re.compile(r"(?:licen[cs]e|notice|copying)(?:[-_.][a-z0-9._-]*)?", re.IGNORECASE)


def library_source(name: str) -> bool:
    """Whether a file of this name is part of an initial Library: Lean and Markdown sources, and
    the license and notice files that travel with ported code."""
    return name.lower().endswith((".lean", ".md")) or bool(_LEGAL_FILE.fullmatch(name))


def initialization_path(root: Path) -> Path:
    return Path(root) / ".task-based" / "initialization.json"


def read_initialization(root: Path) -> dict[str, Any] | None:
    path = initialization_path(root)
    if not path.is_file():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("format") != 1:
        raise ValueError("Unrecognized project initialization receipt; preserve it for inspection")
    if value.get("pins") != pinned_versions():
        raise ValueError("This instance was initialized for a different pinned Lean/Mathlib baseline")
    return value


def _progress(callback: Progress, message: str) -> None:
    if callback is not None:
        callback(message)


def _seed_versions(directory: Path) -> None:
    toolchain = directory / "lean-toolchain"
    if toolchain.exists():
        value = toolchain.read_text(encoding="utf-8-sig").strip()
        if value not in {LEAN_TOOLCHAIN, f"v{LEAN_VERSION}"}:
            raise ValueError(f"The supplied library uses {value!r}; migrate it to {LEAN_TOOLCHAIN} before importing")
    manifest = directory / "lake-manifest.json"
    if manifest.is_file():
        value = json.loads(manifest.read_text(encoding="utf-8-sig"))
        for package in value.get("packages", []):
            if str(package.get("name", "")).lower() == "mathlib" and package.get("rev") != MATHLIB_COMMIT:
                raise ValueError("The supplied library locks a different mathlib commit; migrate it before importing")


def capture_library(hub, directory: Path | None) -> str:
    """Import source pairs and Markdown, and the license and notice files that travel with ported
    code (library_source), never another project's state or caches.

    The existing module layout is retained. Lake configuration is replaced by
    the fixed environment metadata; compilation uses registered shared roots.
    This also supports initializing in an existing source directory in place.
    """
    files: list[dict[str, str]] = []
    total = 0
    if directory is not None:
        root = directory.expanduser().resolve()
        if not root.is_dir():
            raise ValueError("--from-library must name an existing source directory")
        _seed_versions(root)
        for base, directories, names in os.walk(root, followlinks=False):
            directories[:] = sorted(name for name in directories if name.casefold() not in _EXCLUDED_DIRECTORIES)
            for name in directories:
                child = Path(base) / name
                if child.is_symlink():
                    raise ValueError(f"Library import does not follow linked directories: {child}")
            for name in sorted(names):
                if not library_source(name):
                    continue
                path = Path(base) / name
                relative = path.relative_to(root).as_posix()
                if relative.casefold() in _BUILD_FILES:
                    continue
                if path.is_symlink():
                    raise ValueError(f"Library import does not follow linked files: {relative}")
                if len(files) >= MAX_SNAPSHOT_FILES - 2:
                    raise ValueError("Initial Library exceeds the source-file storage envelope")
                size = path.stat().st_size
                if total + size > MAX_SNAPSHOT_BYTES:
                    raise ValueError("Initial Library exceeds the source-byte storage envelope")
                with path.open("rb") as stream:
                    body = stream.read(size + 1)
                if len(body) != size:
                    raise ValueError(f"Library source changed while being captured: {relative}")
                total += size
                files.append({"path": relative, "sha256": hub.blobs.put(body)})
    from .coordination.wiki import ensure_guides

    existing = {item["path"].casefold() for item in files}
    for relative, source in ensure_guides(hub.root).items():
        if relative.casefold() not in existing:
            body = source.read_bytes()
            total += len(body)
            files.append({"path": relative, "sha256": hub.blobs.put(body)})
    if len(files) > MAX_SNAPSHOT_FILES - 2 or total > MAX_SNAPSHOT_BYTES:
        raise ValueError("Initial Library plus starter guidance exceeds the source storage envelope")
    files.extend([
        {"path": "lean-toolchain", "sha256": hub.blobs.put((LEAN_TOOLCHAIN + "\n").encode())},
        {"path": _LOCK_NAME, "sha256": hub.blobs.put(
            (json.dumps(pinned_versions(), indent=2) + "\n").encode())},
    ])
    snapshot = hub.blobs.put_json({"files": sorted(files, key=lambda item: item["path"])})
    validate_snapshot_sources(hub.blobs, snapshot)
    return snapshot


async def _environment(hub, cache: dict, previous: dict, progress: Progress) -> str:
    registry = ObjectRegistry(hub)
    executable = Path(cache["lean_executable"])
    # A retry may reuse the exact already-approved registration. It never
    # modifies an immutable record or adopts a worker-provided environment.
    old_id = previous.get("environment_artifact")
    if old_id:
        record, _ = await registry.get(old_id, "environment", approved=True)
        if (Path(record.lean_executable) != executable
                or record.lean_executable_sha256 != file_sha256(executable)
                or [root.path for root in record.library_roots] != cache["library_roots"]):
            raise ValueError("The prepared environment differs from the recorded instance environment")
        _progress(progress, "Verifying the previously registered compiler and dependencies.")
        await _validate_registered_environment(hub, record)
        return old_id
    _progress(progress, "Registering the exact compiler and shared prebuilt dependency manifests.")
    libraries = []
    for location in cache["library_roots"]:
        manifest = await asyncio.to_thread(library_manifest, Path(location))
        libraries.append(LibraryRoot(path=location, manifest_sha256=hub.blobs.put_json(manifest)))
    # Registration precedes every check, so the version is asked here; checks run in the Lean service.
    try:
        version = await asyncio.to_thread(lean_version, executable)
    except (EnvironmentUnavailable, OSError, subprocess.SubprocessError) as error:
        raise ValueError("The pinned Lean compiler could not report its version") from error
    record = EnvironmentRecord(
        name=f"Pinned Lean {LEAN_VERSION} and mathlib {MATHLIB_COMMIT[:12]}",
        lean_executable=str(executable), lean_executable_sha256=file_sha256(executable),
        lake_executable_sha256=file_sha256(executable.with_name("lake" + executable.suffix)),
        lean_version=version, library_roots=libraries,
        local_execution="trusted-code-only",
        limits=ExecutionLimits(snapshot_files=MAX_SNAPSHOT_FILES, snapshot_bytes=MAX_SNAPSHOT_BYTES),
    )
    actor = Actor(role=Role.OPERATOR)
    staged = await registry.stage(record, actor, key="initialization:environment:" + digest(record.model_dump(mode="json")))
    await hub.execute(Command(name="artifact.accept", key="initialization:approve:" + staged["id"], payload={
        "artifact_id": staged["id"], "policy": "lean.environment@1", "verdict": "accepted",
        "details": {"scope": "Operator-requested pinned execution environment", "pins": pinned_versions(),
                    "cache_receipt": cache["receipt_path"], "proof_accepted": False},
    }), actor)
    return staged["id"]


async def _validate_registered_environment(hub, record: EnvironmentRecord) -> None:
    """Explicit preparation rechecks content; ordinary admission remains cheap."""
    tooling = LeanTooling(record, hub.blobs)
    try:
        executable = await asyncio.to_thread(tooling.executable)
        version = await asyncio.to_thread(lean_version, executable)
    except (EnvironmentUnavailable, OSError, subprocess.SubprocessError):
        version = None
    if record.local_execution != "trusted-code-only" or version != record.lean_version:
        raise ValueError("The recorded environment cannot execute the pinned compiler")
    await tooling.library_paths()
    if not executable.with_name("lake" + executable.suffix).is_file():
        raise ValueError("The pinned Lean installation has no Lake executable")


async def _check_seed(hub, snapshot: str, environment_id: str) -> dict:
    entries = validate_snapshot_sources(hub.blobs, snapshot)
    modules = snapshot_module_order(hub.blobs, entries)
    if not modules:
        return {"ok": True, "checked_module_count": 0, "proof_accepted": False}
    environment, _ = await ObjectRegistry(hub).get(environment_id, "environment", approved=True)
    checked = await lean_service.check(hub.root, environment, hub.blobs, snapshot=snapshot, modules=modules,
                                       lane="official", owner="operator", label="Initial Library")
    result = checked["compilation"] if checked["doctor"]["ok"] else {"ok": False, "doctor": checked["doctor"]}
    # The full report, with every module, goes beside the receipt, which must stay small: each
    # admission reads at most 1 MiB of it (require_ready), and the module names of a large Library
    # pass that alone (21,144 modules, about 1.6 MB, on the Mac Studio, 2026-11-08).
    atomic_json(hub.state_dir / "initialization-check.json", result)
    if not result["ok"]:
        raise ValueError("Initial Library does not compile with the pinned environment; "
                         "see .task-based/initialization-check.json")
    return {"ok": True, "checked_module_count": len(modules), "checked_modules_sha256": digest(modules),
            "report": "initialization-check.json", "proof_accepted": False,
            "scope": "Compilation of operator-supplied baseline; no synthetic mathematical acceptance"}


async def prepare_project(hub, *, from_library: Path | None = None,
                          progress: Progress = None) -> dict[str, Any]:
    """Prepare one stopped instance; ownership is held by the caller's ProjectOwner."""
    from .bootstrap_cache import prepare_cache

    if hub.config.domain != "lean":
        raise ValueError("Pinned dependency preparation requires a Lean project")
    if not hub.config.repository.enabled:
        raise ValueError("Pinned Lean initialization requires the managed repository")

    def quiet(c):
        checks = (
            "SELECT 1 FROM runs WHERE state='running'",
            "SELECT 1 FROM attempts WHERE ended_at IS NULL",
            "SELECT 1 FROM turn_intents WHERE state IN ('prepared','sending','bound','uncertain')",
            "SELECT 1 FROM helper_intents WHERE state IN ('prepared','running','uncertain')",
        )
        if any(c.execute(query).fetchone() for query in checks):
            raise ValueError("Stop and reconcile all execution before preparing this instance")
    await hub.db.call(quiet)
    previous = read_initialization(hub.root) or {}
    source = str(from_library.expanduser().resolve()) if from_library is not None else previous.get("source_directory")
    if previous.get("initial_snapshot") and from_library is not None and source != previous.get("source_directory"):
        raise ValueError("An initial Library is already frozen; use a new instance for a different baseline")
    state = {**previous, "format": 1, "state": "preparing", "pins": pinned_versions(),
             "source_directory": source}
    state.pop("error", None)
    atomic_json(initialization_path(hub.root), state)
    try:
        status = await hub.repository_status()
        snapshot = previous.get("initial_snapshot")
        if snapshot is None:
            if status.get("initialized"):
                if from_library is not None:
                    raise ValueError("An existing managed repository cannot be replaced by --from-library")
                snapshot = await asyncio.to_thread(hub._repository().snapshot, status["head"], hub.blobs)
                if not isinstance(snapshot, str):
                    raise ValueError("Managed repository did not return an immutable source snapshot")
                validate_snapshot_sources(hub.blobs, snapshot)
            else:
                snapshot = await asyncio.to_thread(capture_library, hub, Path(source) if source else None)
            state["initial_snapshot"] = snapshot
            atomic_json(initialization_path(hub.root), state)
        else:
            # Verify retained bytes before provisioning or accepting readiness.
            read_snapshot(hub.blobs, snapshot)
        if not isinstance(snapshot, str):
            raise ValueError("The initial source snapshot is missing")
        _progress(progress, "Preparing the pinned dependency cache in this project instance.")
        cache = await prepare_cache(hub.root / ".dependencies", progress=progress)
        state["cache"] = cache
        environment_id = await _environment(hub, cache, previous, progress)
        state["environment_artifact"] = environment_id
        atomic_json(initialization_path(hub.root), state)
        if not previous.get("seed_check", {}).get("ok"):
            _progress(progress, "Checking the initial Library against the prepared environment.")
            state["seed_check"] = await _check_seed(hub, snapshot, environment_id)
            atomic_json(initialization_path(hub.root), state)
        if status.get("initialized"):
            head = previous.get("initial_commit", status["head"])
            if previous.get("initial_commit") is None:
                current = await asyncio.to_thread(hub._repository().snapshot, status["head"], hub.blobs)
                if current != snapshot:
                    raise ValueError("Managed repository changed during interrupted initialization")
                if status.get("reconciliation_required"):
                    recorded = await hub.db.call(lambda c: bool(c.execute(
                        "SELECT 1 FROM events WHERE kind='repository.initialized'").fetchone()) or bool(c.execute(
                        "SELECT 1 FROM repository_publications").fetchone()))
                    if recorded:
                        raise ValueError("Reconcile the existing public repository before preparing this instance")
                    # The initial Git commit may survive a rolled-back SQLite
                    # transaction. Only our exact frozen first tree may recover
                    # that initialization event; unrelated heads stay blocked.
                    result = await hub.execute(Command(name="repository.initialize",
                        key="initialization:repository:" + snapshot, payload={"snapshot": snapshot}),
                        Actor(role=Role.OPERATOR))
                    head = result["head"]
        else:
            result = await hub.execute(Command(name="repository.initialize", key="initialization:repository:" + snapshot,
                                               payload={"snapshot": snapshot}), Actor(role=Role.OPERATOR))
            head = result["head"]
        state.update(state="ready", initial_commit=head)
        atomic_json(initialization_path(hub.root), state)
        _progress(progress, "Initialization is ready. No workers were started.")
        return state
    except BaseException as error:
        state.update(state="failed", error=f"{type(error).__name__}: {error}")
        atomic_json(initialization_path(hub.root), state)
        raise
