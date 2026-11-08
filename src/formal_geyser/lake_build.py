"""Persistent Lake builds of frozen source, with instance-local shared caches.

Lake owns project dependency tracking. Registered external libraries are immutable
prebuilt inputs: their exact manifests partition both workspaces and artifact caches.
No dependency checkout or third-party build runs from a validation workspace.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import os
import time
import weakref
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import portalocker

from . import offload
from .coordination.storage import digest
from .lake_cache import collect, lock_file
from .lake_cache import remove_tree as _remove_tree
from .paths import module_name, module_path, relative_path, safe_child
from .process import process_environment
from .registry import RegistryError, file_sha256

METHOD = "lake-build"
RECIPE_VERSION = 2  # Bump only when build semantics or cache compatibility changes.
REPLAY_VERSION = 1  # Bump only when what a recorded kernel replay establishes changes.
OLEAN_PARTS = (".olean", ".olean.server", ".olean.private")
SLOT_POLL_SECONDS = 0.5  # While another process holds a slot under the limit.
# Lake prints a line for each module it builds and every message Lean reports, so a fresh build of
# a large Library prints far past the environment's output bound, which stops the process: the
# first build of the Linux pilot's 21,144-module Library on the Mac Studio was stopped at 1 MiB
# after about 2,000 modules, with no error (2026-11-08; about 520 bytes a module). A build's bound
# grows by this much for each Lean source it may compile, so only runaway output stops it.
BUILD_OUTPUT_PER_SOURCE = 4096


def build_output_limit(output_bytes: int, sources: dict[str, bytes]) -> int:
    """The output a build of `sources` may print before it is stopped (BUILD_OUTPUT_PER_SOURCE)."""
    return max(output_bytes, BUILD_OUTPUT_PER_SOURCE * sum(path.endswith(".lean") for path in sources))


def _replay_keys(build_directory: Path, sources: dict[str, bytes], modules: Sequence[str],
                 identity: str) -> dict[str, str]:
    """Replay keys for the requested modules and every project module they import.

    Replay re-checks a module's own declarations and trusts its imports as compiled,
    so a key names the module's compiled parts and, recursively, those of the project
    modules it imports. Registered libraries are integrity-checked apart.
    """
    from .mathematical_artifacts import module_imports

    project = {module_name(relative): body for relative, body in sources.items()}
    keys: dict[str, str] = {}

    def key(module: str, chain: tuple[str, ...] = ()) -> str:
        if module in keys:
            return keys[module]
        if module in chain:
            raise RegistryError("Project modules import each other cyclically: " + module)
        value = hashlib.sha256(json.dumps([REPLAY_VERSION, identity, module]).encode())
        olean = build_directory / (module_path(module)[:-5] + ".olean")
        for suffix in OLEAN_PARTS:
            part = olean.with_name(olean.name[:-len(".olean")] + suffix)
            value.update(suffix.encode() + (file_sha256(part).encode() if part.is_file() else b"absent"))
        for imported in sorted(module_imports(project[module].decode("utf-8-sig"))):
            if imported in project:
                value.update(key(imported, (*chain, module)).encode())
        keys[module] = value.hexdigest()
        return keys[module]

    for module in modules:
        key(module)
    return keys


def _pending_replays(store: Path, keys: dict[str, str]) -> list[str]:
    return [module for module, key in keys.items() if not (store / key).is_file()]


def _record_replays(store: Path, keys: Sequence[str]) -> None:
    """A marker records only that this exact compiled closure passed kernel replay."""
    store.mkdir(parents=True, exist_ok=True)
    for key in keys:
        (store / key).touch()


# A replay process loads the environment its modules import, then sends each module's own
# declarations to the kernel, trusting its compiled imports, the replayed ones included: each
# module is checked once whichever process checks it. So a long replay is split among up to
# `threads` processes, each with at least this many modules, since each loads its environment
# again. In the Linux pilot one edit to a widely imported module left about 5,200 modules to
# replay, about 4 s each, in one process.
REPLAY_GROUP_MODULES = 100


async def _replay_groups(tooling, build_directory: Path, pending: list[str], libraries, threads: int,
                         record) -> dict:
    """Replay `pending` in up to `threads` processes at once. `record(modules)` keeps the markers
    of each group that passes as it passes, so a replay stopped or failed later keeps that work.
    The report is the first failing group's, or the first group's, for all of `pending`."""
    groups = max(1, min(max(1, threads), len(pending) // REPLAY_GROUP_MODULES))
    parts = [pending[index::groups] for index in range(groups)]

    async def one(part: list[str]) -> dict:
        report = await tooling.kernel_replay(build_directory, part, libraries)
        if report["ok"]:
            await record(part)
        return report
    if groups == 1:
        return await one(parts[0])
    running = [asyncio.ensure_future(one(part)) for part in parts]
    try:
        reports = await asyncio.gather(*running)
    except BaseException:
        # The first error, or a cancellation, stops the other groups too; it is raised as it was.
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        raise
    failed = next((report for report in reports if not report["ok"]), None)
    merged = {**(failed or reports[0]), "replayed_modules": list(pending),
              "groups": [{"modules": len(part), "ok": report["ok"]} for part, report in zip(parts, reports)]}
    if failed is None:
        constants = [report.get("constants") for report in reports]
        merged["constants"] = sum(constants) if all(isinstance(item, int) for item in constants) else None
    return merged


@contextmanager
def timed(timings: dict[str, float], step: str) -> Iterator[None]:
    """Add the seconds the block took to `timings[step]`. A check's result keeps its timings: in
    the Linux pilot a development check ran 57 s (median), its Lean processes 27% of that."""
    started = time.monotonic()
    try:
        yield
    finally:
        timings[step] = round(timings.get(step, 0.0) + time.monotonic() - started, 3)


async def _disk(function, *args):
    """Keep a slot locked until a cancelled filesystem mutation has finished, however often the
    build is cancelled meanwhile. Run in the Lean service's bookkeeping processes when it has
    them (offload), else in a thread."""
    return await offload.call(function, *args)


@asynccontextmanager
async def _namespace_lock(root: Path, namespace: str):
    """Wait for idle maintenance without blocking the service's event loop."""
    path = root / "locks" / (namespace + ".lock")
    while True:
        with lock_file(path, shared=True) as held:
            if held:
                yield
                return
        await asyncio.sleep(0.1)


@asynccontextmanager
async def _maintenance(root: Path, namespace: str, slot_limit: int | None = None):
    """Size and collect older caches, and the old idle slots of the build's own namespace,
    alongside the build, under its namespace lock. Past `slot_limit`, idle slots are cleared
    at once: no build takes them again."""
    status = {"state": "scheduled"}
    async def run():
        try:
            return await _disk(functools.partial(collect, root, active_namespace=namespace,
                                                 slot_limit=slot_limit))
        except (OSError, ValueError) as error:
            return {"error": str(error)}  # Retention cannot invalidate mathematics.
    task = asyncio.create_task(run())
    try:
        yield status
    finally:
        # Never let a detached filesystem mutation outlive the protective lock.
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
        status.clear()
        status.update(result)


def _write(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_bytes() != body:
        path.write_bytes(body)


@contextmanager
def _workspace(root: Path, limit: int | None = None) -> Iterator[Path | None]:
    """Take a free persistent slot; unrelated validations never wait on this lock. With
    `limit`, only slots 0 to limit - 1 are taken, and None is given while each is held."""
    root.mkdir(parents=True, exist_ok=True)
    index = 0
    while True:
        if limit is not None and index >= limit:
            yield None
            return
        directory = root / str(index)
        directory.mkdir(exist_ok=True)
        if directory.is_symlink():
            raise RegistryError("Build workspaces must not be links")
        lock = (directory / "build.lock").open("a+b")
        try:
            portalocker.lock(lock, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException:
            lock.close()
            index += 1
            continue
        try:
            (directory / "last-used").touch()
            root.touch()
            yield directory
        finally:
            try:
                (directory / "last-used").touch()
                root.touch()
            finally:
                portalocker.unlock(lock)
                lock.close()
        return


def _part_order(item: tuple[str, str]) -> list[str]:
    return item[0].split("/")


def _sources(directory: Path) -> dict[str, bytes]:
    """The .lean files below `directory` by relative path: hidden entries are skipped, linked
    directories are not entered and a linked source file is refused. One walk instead of
    safe_child's path resolutions for every file, which took a third of the Lean service's
    own CPU in the Linux pilot: a build reads its slot's sources three times."""
    root, found = str(directory), []
    for base, directories, names in os.walk(root, followlinks=False):
        directories[:] = [name for name in directories if not name.startswith(".")]
        for name in names:
            if name.startswith(".") or not name.endswith(".lean"):
                continue
            path = os.path.join(base, name)
            relative = os.path.relpath(path, root).replace(os.sep, "/")
            if relative == "lakefile.lean":
                continue  # Only the coordinator supplies the build configuration.
            relative_path(relative)
            if os.path.islink(path):
                raise ValueError("Linked artifact paths are not allowed")
            found.append((relative, path))
    return {relative: Path(path).read_bytes() for relative, path in sorted(found, key=_part_order)}


def _same_sources(directory: Path, sources: dict[str, bytes]) -> bool:
    """Whether the .lean files below `directory` are exactly `sources`."""
    return _sources(directory) == sources


def _has_sources(directory: Path) -> bool:
    return bool(_sources(directory))


def _synchronize(root: Path, sources: dict[str, bytes], environment_key: str, libraries: Sequence[Path]) -> None:
    source_root = root / "source"
    source_root.mkdir(exist_ok=True)
    # One walk: a linked entry anywhere below refuses the projection, so no path written or
    # removed below passes through a link, which safe_child checked path by path.
    top, old = str(source_root), set[str]()
    for folder, directories, names in os.walk(top, followlinks=False):
        for name in [*directories, *names]:
            if os.path.islink(os.path.join(folder, name)):
                raise ValueError("Linked artifact paths are not allowed")
        old.update(os.path.relpath(os.path.join(folder, name), top).replace(os.sep, "/")
                   for name in names if name.endswith(".lean"))
    removed = old - sources.keys()
    for relative in removed:
        source_root.joinpath(*relative_path(relative).split("/")).unlink()
    # Remove all facets of deleted modules, not unrelated valid outputs. Lake
    # invalidates downstream import traces when it resolves the new source graph.
    for relative in removed:
        module = Path(relative[:-5])
        for base in (root / ".lake/build/lib/lean", root / ".lake/build/ir"):
            parent = base / module.parent
            if parent.exists():
                for output in parent.iterdir():
                    if output.is_file() and output.name.startswith(module.name + "."):
                        safe_child(base, output.relative_to(base).as_posix())
                        output.chmod(output.stat().st_mode | 0o200)
                        output.unlink()
    for relative, body in sources.items():
        _write(source_root.joinpath(*relative_path(relative).split("/")), body)
    roots = sorted(module_name(relative) for relative in sources)
    config = ('name = "TaskBasedValidation"\n'
              'srcDir = "source"\n'
              'moreLeanArgs = ["--json"]\n'
              'enableArtifactCache = true\n'
              'restoreAllArtifacts = true\n'
              '\n[[lean_lib]]\nname = "TaskBasedSources"\n'
              f'roots = {json.dumps(roots)}\n')
    packages = []
    for index, library in enumerate(libraries):
        name = f"TaskBasedPrebuilt{index}"
        stub = root / ".prebuilt" / str(index)
        # Lake builds use Workspace.leanPath, not the inherited LEAN_PATH.
        # Empty local packages expose prebuilt roots without loading third-party
        # lakefiles or declaring any buildable third-party source targets.
        library_path = library.resolve().as_posix()
        _write(stub / "lakefile.toml", (
            f'name = "{name}"\nbuildDir = "."\n'
            f'leanLibDir = {json.dumps(library_path)}\n').encode())
        config += f'\n[[require]]\nname = "{name}"\npath = ".prebuilt/{index}"\n'
        packages.append({"name": name, "type": "path", "dir": f".prebuilt/{index}",
                         "scope": "", "inherited": False, "configFile": "lakefile.toml"})
    _write(root / "lakefile.toml", config.encode())
    manifest = {"version": "1.1.0", "name": "TaskBasedValidation", "lakeDir": ".lake",
                "packagesDir": ".lake/packages", "packages": packages}
    _write(root / "lake-manifest.json", json.dumps(manifest).encode())
    _write(root / "environment.json", json.dumps({"key": environment_key}).encode())


def _output_files(root: Path) -> list[tuple[str, Path]]:
    """The slot's compiled .olean and .ilean files, by relative path. It walks thousands of
    files: run it in a thread (_disk), never on the event loop, which it held for seconds."""
    directory, found = str(root / ".lake" / "build" / "lib" / "lean"), []
    for base, _, names in os.walk(directory, followlinks=False):
        for name in names:
            path = os.path.join(base, name)
            if (".olean" in name or name.endswith(".ilean")) and os.path.isfile(path):
                found.append((os.path.relpath(path, directory).replace(os.sep, "/"), path))
    return [(relative, Path(path)) for relative, path in sorted(found, key=_part_order)]


def _output_times(root: Path) -> dict[str, int]:
    """Each compiled output's modification time."""
    return {relative: path.stat().st_mtime_ns for relative, path in _output_files(root)}


def _identity(path: Path) -> list[int]:
    st = path.stat()
    return [st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_dev, st.st_ino]


def _checkpoint(root: Path, known=None) -> None:
    known = known or {}
    entries = {}
    for name, path in _output_files(root):
        identity = _identity(path)
        previous = known.get(name, {})
        sha = previous["sha256"] if identity == previous.get("identity") else file_sha256(path)
        entries[name] = {"sha256": sha, "identity": identity}
    _write(root / "outputs.json", json.dumps({"version": 1, "files": entries}, sort_keys=True).encode())


def _recover(root: Path) -> dict:
    """Keep a slot's compiled outputs that match its receipt: each by its recorded identity,
    or else by its hash. Any mismatch discards them all and the build starts clean."""
    receipt = root / "outputs.json"
    valid = False
    try:
        expected = json.loads(receipt.read_text(encoding="utf-8"))["files"]
        actual = dict(_output_files(root))
        valid = actual.keys() == expected.keys() and all(
            _identity(path) == expected[name]["identity"]
            or file_sha256(path) == expected[name]["sha256"] for name, path in actual.items())
    except (OSError, ValueError, KeyError, TypeError):
        pass
    if not valid:
        _remove_tree(root, root / ".lake" / "build")
    # Only a fully quiescent completed command writes a new cache receipt.
    # A cache receipt describes bytes, never mathematical acceptance.
    receipt.unlink(missing_ok=True)
    return expected if valid else {}


def environment_key(tooling, lake: Path) -> str:
    """The exact compiler, Lake, libraries and recipe: they partition slots and caches."""
    return digest({
        "lean": tooling.environment.lean_executable_sha256,
        "version": tooling.environment.lean_version,
        "lake": file_sha256(lake),
        "libraries": [item.model_dump(mode="json") for item in tooling.environment.library_roots],
        "recipe": RECIPE_VERSION,
    })


@asynccontextmanager
async def compilation(tooling, source: Path | dict[str, bytes], modules: Sequence[str], *,
                      kernel_replay: bool = True, threads: int = 1):
    """Build `modules` of `source` (a directory, or the .lean files themselves) in a slot with
    `threads` Lean threads; the Lean service's scheduler chose that number."""
    from .tooling import SORRY_REASON, parse_diagnostics, uses_sorry

    if tooling.environment.local_execution != "trusted-code-only":
        raise RegistryError("Local Lean compilation is disabled for this environment")
    if not modules or len(set(modules)) != len(modules):
        raise RegistryError("Supply a nonempty, unique module list")
    timings: dict[str, float] = {}  # Seconds by step, in the report.
    executable = tooling.executable()
    lake = tooling.lake_executable()
    with timed(timings, "library_check"):
        libraries = await tooling.library_paths()
    with timed(timings, "sources"):
        sources = dict(source) if isinstance(source, dict) else await _disk(_sources, source)
    tooling.check_module_paths(list(sources), libraries)
    for module in modules:
        if module_path(module) not in sources:
            raise RegistryError(f"Missing source module: {module}")
    key = environment_key(tooling, lake)
    namespace = key[:20]  # Short build paths; receipts retain the full identity.
    if tooling.blobs is not None:
        parent = Path(tooling.blobs.root).parent
    elif isinstance(source, Path):
        parent = source.parent
    else:
        raise RegistryError("Builds of supplied files keep their caches beside the project's blob store")
    cache_root = parent / "lake"
    waited = time.monotonic()
    # The namespace lock also protects artifacts accessed through the shared cache.
    slot_limit = getattr(tooling, "build_slots", None) or None
    async with _namespace_lock(cache_root, namespace):
        async with (_maintenance(cache_root, namespace, slot_limit) as retention,
                    _compilation_slot(cache_root, namespace, slot_limit) as workspace):
            timings["slot"] = round(time.monotonic() - waited, 3)
            # Earlier compiled outputs are reused while each keeps the size, modification time
            # and file ID in the slot's receipt; any other difference re-hashes or discards them.
            with timed(timings, "recover"):
                known = await _disk(_recover, workspace)
            with timed(timings, "synchronize"):
                await _disk(_synchronize, workspace, sources, key, libraries)
            with timed(timings, "outputs"):
                prior = await _disk(_output_times, workspace)
            env = process_environment(libraries)
            env.update(LEAN=str(executable), LEAN_SYSROOT=str(executable.parent.parent),
                       LAKE_CACHE_DIR=str((cache_root / "cache" / key).resolve()),
                       LAKE_ARTIFACT_CACHE="true", LAKE_RESTORE_ARTIFACTS="true")
            argv = [str(lake), "build", *["+" + module + ":olean" for module in modules]]
            env["LEAN_NUM_THREADS"] = str(max(1, threads))
            with timed(timings, "build"):
                result = await tooling.runner.run(argv, cwd=workspace, env=env,
                    timeout=tooling.process_timeout(),
                    output_limit=build_output_limit(tooling.environment.limits.output_bytes, sources))
            diagnostics = parse_diagnostics(result)
            has_sorry = uses_sorry(diagnostics)
            report = {"ok": result.ok and not diagnostics["errors"] and not has_sorry, "proof_accepted": False,
                      "validation_method": METHOD, "process": result.to_dict(), "diagnostics": diagnostics,
                      "threads": max(1, threads),
                      "modules": [{"module": module,
                          "source_sha256": hashlib.sha256(sources[module_path(module)]).hexdigest()}
                          for module in modules], "environment_key": key,
                      "workspace": str(workspace), "cache_directory": env["LAKE_CACHE_DIR"],
                      "build_directory": str(workspace / ".lake/build/lib/lean"), "cache_retention": retention,
                      "timings": timings}
            if has_sorry:
                report["reason"] = SORRY_REASON
            # Lean may run the sources' own code while it compiles them: it must not have
            # changed the slot's copy, which inspection reads next.
            with timed(timings, "unchanged"):
                unchanged = await _disk(_same_sources, workspace / "source", sources)
            if not unchanged:
                report.update(ok=False, reason="Compilation changed a frozen source file")
            tooling.lake_executable()
            # Ordinary source errors leave unrelated modules usable. Crashes,
            # interruptions, truncated output and mutations do not leave a receipt.
            completed = (result.tree_quiet and not result.timed_out and not result.output_limited
                         and not result.error and (result.returncode == 0
                         or result.returncode == 1 and diagnostics["errors"] > 0))
            if completed and unchanged:
                with timed(timings, "checkpoint"):
                    await _disk(_checkpoint, workspace, known)
            with timed(timings, "outputs"):
                outputs = await _disk(_output_times, workspace)
            if report["ok"] and any(module_path(m)[:-5] + ".olean" not in outputs for m in modules):
                report.update(ok=False, reason="Lake omitted a requested compiled module", unreadable_output=True)
            report["reused_modules"] = [m for m in modules
                if (p := module_path(m)[:-5] + ".olean") in prior and p in outputs and outputs[p] == prior[p]]
            report["built_modules"] = [m for m in modules if m not in report["reused_modules"]]
            if report["ok"] and kernel_replay:
                # Requested modules and the project modules they import, reused or rebuilt:
                # a development check, which skips replay, may have built reused outputs.
                # Only a closure whose exact compiled bytes already passed is not replayed again.
                from .tooling import replay_helper

                with timed(timings, "kernel_replay"):
                    build_directory = Path(report["build_directory"])
                    helper = file_sha256(replay_helper())
                    keys = await _disk(_replay_keys, build_directory, sources, modules,
                                       digest({"environment": key, "replay_helper": helper}))
                    store = cache_root / "replays" / namespace
                    pending = await _disk(_pending_replays, store, keys)

                    async def record(passed: list[str]) -> None:
                        await _disk(_record_replays, store, [keys[module] for module in passed])
                    replay = (await _replay_groups(tooling, build_directory, pending, libraries, threads, record)
                              if pending else
                              {"ok": True, "replayed_modules": [], "validation_method": "kernel-replay",
                               "helper_sha256": helper})
                    replay["previously_replayed_modules"] = sorted(set(keys) - set(pending))
                    report["kernel_replay"] = replay
                    if not replay["ok"]:
                        report.update(ok=False, reason=replay["reason"])
            # Inspect uses this directory in place while the slot remains locked.
            yield report
            # Per-candidate frozen input may already have been disposed by a
            # completed batch member. Its validator checked it before disposal;
            # the shared source projection must remain unchanged until release.
            with timed(timings, "release_check"):
                changed = unchanged and (not await _disk(_same_sources, workspace / "source", sources)
                                         or await _disk(_has_sources, Path(report["build_directory"])))
            if changed:
                (workspace / "outputs.json").unlink(missing_ok=True)
                raise RegistryError("Inspection changed a frozen proof input")


_SLOT_GATES: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple[str, int], asyncio.Semaphore]] = (
    weakref.WeakKeyDictionary())


def _slot_gate(root: Path, limit: int) -> asyncio.Semaphore:
    gates = _SLOT_GATES.setdefault(asyncio.get_running_loop(), {})
    return gates.setdefault((str(root), limit), asyncio.Semaphore(limit))


@asynccontextmanager
async def _compilation_slot(cache_root, namespace, limit: int | None = None):
    """A free slot of the namespace. With `limit`, at most that many builds hold one: every
    slot holds a full copy of the Library's build (about 9.6 GB in the Linux pilot, where 13
    builds at once filled the disk), so a build past it waits, first come first served, for
    one to free. Each slot is still locked on its own, so a slot that another process holds
    is passed over, and a build waits for the next free one if all are held."""
    root = cache_root / "workspaces" / namespace
    if not limit:
        with _workspace(root) as workspace:
            assert workspace is not None
            yield workspace
        return
    async with _slot_gate(root, limit):
        while True:
            with _workspace(root, limit) as workspace:
                if workspace is not None:
                    yield workspace
                    return
            await asyncio.sleep(SLOT_POLL_SECONDS)
