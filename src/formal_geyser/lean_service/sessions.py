"""Resident incremental sessions for development checks.

A development check of one module first starts the Lean language server (`lean --server`)
for its worker's workspace and opens the module; later checks of the same module send only
its new text, and Lean elaborates again only what changed, with every import still loaded.
Lake builds the module's project imports when the module is opened (`lake setup-file`);
a check whose imports or project dependencies changed opens it again. A session runs in the
sandbox and in its own operating-system jobs, in a directory of its own beside the build
slots; an idle session is closed, and its directory removed. A check a session cannot serve
uses the ordinary build.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import shutil
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from .. import offload
from ..coordination.runtime.process import OwnedProcessHost, ProcessHostError
from ..coordination.storage import digest
from ..lake_build import _synchronize, environment_key
from ..mathematical_artifacts import module_imports
from ..paths import module_name, module_path
from ..process import process_environment
from ..tooling import SORRY_REASON, LeanTooling, uses_sorry
from .osjobs import JobGroup

logger = logging.getLogger(__name__)
FRAME_BYTES = 64 * 1024 * 1024
SEVERITIES = {1: "error", 2: "warning", 3: "information", 4: "information"}


class SessionFailed(RuntimeError):
    """The session could not answer; the check uses the ordinary build instead."""


class CheckTimedOut(TimeoutError):
    """The check did not end within its time limit. Its session is closed, since Lean would go on
    elaborating, and no build repeats it: the same input would not end there either. It carries
    what Lean had reported by then: its diagnostics, and the line ranges it was still working on."""

    diagnostics: dict | None = None
    processing: tuple[tuple[int, int], ...] = ()


class LspConnection:
    """JSON-RPC 2.0 over a language server's standard streams, framed by Content-Length."""

    def __init__(self, host: OwnedProcessHost):
        self.host = host
        self._ids = 0
        self._pending: dict[int, asyncio.Future] = {}
        self.diagnostics: dict[str, tuple[int | None, list]] = {}
        self.progress: dict[str, tuple[int | None, list]] = {}
        self.stderr: list[str] = []
        self.closed = asyncio.Event()
        self._reader = asyncio.create_task(self._read(), name="lean-session-reader")
        self._errors = asyncio.create_task(self._drain_stderr(), name="lean-session-stderr")

    async def _send(self, message: dict) -> None:
        body = json.dumps(message).encode("utf-8")
        self.host.stdin.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)
        await self.host.stdin.drain()

    async def request(self, method: str, params: Any, timeout: float | None = None) -> Any:
        if self.closed.is_set():
            raise SessionFailed("The Lean server has stopped")
        self._ids += 1
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[self._ids] = future
        await self._send({"jsonrpc": "2.0", "id": self._ids, "method": method, "params": params})
        return await asyncio.wait_for(future, timeout)

    async def notify(self, method: str, params: Any) -> None:
        if self.closed.is_set():
            raise SessionFailed("The Lean server has stopped")
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _read(self) -> None:
        stream = self.host.stdout
        try:
            while True:
                length = None
                while True:
                    line = await stream.readline()
                    if not line:
                        return
                    line = line.strip()
                    if not line:
                        break
                    name, _, value = line.decode("ascii", errors="replace").partition(":")
                    if name.strip().casefold() == "content-length":
                        length = int(value.strip())
                if length is None or length > FRAME_BYTES:
                    raise SessionFailed("The Lean server sent an unreadable message")
                await self._handle(json.loads(await stream.readexactly(length)))
        except (asyncio.IncompleteReadError, ValueError, SessionFailed, ConnectionError) as error:
            logger.warning("Lean session connection ended: %s", error)
        finally:
            self.closed.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(SessionFailed("The Lean server has stopped"))

    async def _handle(self, message: dict) -> None:
        if "id" in message and "method" not in message:  # A response.
            future = self._pending.pop(message["id"], None)
            if future is not None and not future.done():
                if "error" in message:
                    future.set_exception(SessionFailed(str(message["error"].get("message", message["error"]))))
                else:
                    future.set_result(message.get("result"))
        elif "id" in message:  # A request from the server, such as registering a capability.
            await self._send({"jsonrpc": "2.0", "id": message["id"], "result": None})
        elif message.get("method") == "textDocument/publishDiagnostics":
            params = message.get("params") or {}
            self.diagnostics[params.get("uri", "")] = (params.get("version"), params.get("diagnostics") or [])
        elif message.get("method") == "$/lean/fileProgress":  # The ranges Lean is still working on.
            params = message.get("params") or {}
            document = params.get("textDocument") or {}
            self.progress[document.get("uri", "")] = (document.get("version"), params.get("processing") or [])

    async def _drain_stderr(self) -> None:
        while line := await self.host.stderr.readline():
            self.stderr.append(line.decode("utf-8", errors="replace"))
            del self.stderr[:-200]

    async def close(self) -> None:
        for task in (self._reader, self._errors):
            task.cancel()
        await asyncio.gather(self._reader, self._errors, return_exceptions=True)


class Document:
    def __init__(self, version: int, header: str, dependencies: str):
        self.version, self.header, self.dependencies = version, header, dependencies


class Session:
    def __init__(self, key: str, root: Path, host: OwnedProcessHost, connection: LspConnection, group: JobGroup,
                 environment_key: str):
        self.key, self.root, self.host, self.connection, self.group = key, root, host, connection, group
        self.environment_key = environment_key
        self.documents: dict[str, Document] = {}
        self.lock = asyncio.Lock()
        self.last_used = time.monotonic()
        self.checks = 0

    @property
    def alive(self) -> bool:
        return not self.connection.closed.is_set()

    async def close(self) -> None:
        with suppress(Exception):
            if self.alive:
                await self.connection.request("shutdown", None, timeout=10)
                await self.connection.notify("exit", None)
        try:
            await self.host.stop(grace=5)
        finally:
            await self.connection.close()
            await asyncio.to_thread(self.group.close)


def _uri(path: Path) -> str:
    return path.resolve().as_uri()


def _prepared_session(root: Path, sources: dict[str, bytes], key: str, libraries: list[Path], module: str) -> str:
    """Project the sources into a session's slot and digest the project modules `module` imports.
    Both read the whole Library, so a check runs them in one bookkeeping call (offload)."""
    from .executor import project_closure

    _synchronize(root, sources, key, libraries)
    imported = project_closure(sources, [module]) - {module}
    return digest({path: hashlib.sha256(body).hexdigest() for path, body in sources.items()
                   if module_name(path) in imported})


def _header(source: bytes) -> str:
    """What decides a module's environment: its imports."""
    try:
        return digest(sorted(module_imports(source.decode("utf-8-sig"))))
    except (ValueError, UnicodeDecodeError):
        return hashlib.sha256(source).hexdigest()  # The server reports the header error.


def _current(reports: dict[str, tuple[int | None, list]], uri: str, version: int) -> list:
    """What Lean last published for this version of the document; nothing if what it last
    published was for an older version, which no longer applies."""
    published, items = reports.get(uri, (None, []))
    return [] if published is not None and published != version else items


def _processing(items: list) -> tuple[tuple[int, int], ...]:
    """The line ranges, numbered from 1, that Lean reported it was still working on."""
    ranges = []
    for item in items:
        if item.get("kind", 1) != 1:  # 2 marks a fatal error, not work in progress.
            continue
        start = ((item.get("range") or {}).get("start") or {})
        end = ((item.get("range") or {}).get("end") or {})
        first, last = int(start.get("line", 0)) + 1, int(end.get("line", 0)) + 1
        if last > first and not end.get("character"):
            last -= 1  # A range that ends where the next line starts.
        ranges.append((first, max(first, last)))
    return tuple(ranges)


def _diagnostics(path: str, items: list) -> dict:
    messages = []
    for item in items:
        start = (item.get("range") or {}).get("start") or {}
        end = (item.get("range") or {}).get("end") or {}
        messages.append({"severity": SEVERITIES.get(item.get("severity", 1), "error"), "fileName": path,
                         "pos": {"line": int(start.get("line", 0)) + 1, "column": int(start.get("character", 0))},
                         "endPos": {"line": int(end.get("line", 0)) + 1, "column": int(end.get("character", 0))},
                         "data": str(item.get("message", ""))})
    return {"messages": messages, "other_output": [],
            "errors": sum(item["severity"] == "error" for item in messages),
            "warnings": sum(item["severity"] == "warning" for item in messages)}


class SessionPool:
    """The service's resident sessions, keyed by worker, working directory and environment."""

    def __init__(self, *, sandbox, limit: int = 6, idle_seconds: float = 600.0,
                 command: Callable[[Path], list[str]] | None = None, group_limits: dict[str, int] | None = None):
        self.sandbox = sandbox
        self.limit = limit
        self.idle_seconds = idle_seconds
        self.command = command or (lambda lean: [str(lean), "--server"])
        self.group_limits = dict(group_limits or {})  # Each session's JobGroup memory bounds.
        self.sessions: dict[str, Session] = {}
        self._starting = asyncio.Lock()

    @staticmethod
    def eligible(request) -> bool:
        return (request.directory is not None and len(request.modules) == 1 and not request.inspections
                and not request.kernel_replay)

    async def check(self, tooling: LeanTooling, request, sources: dict[str, bytes], *, owner: str,
                    threads: int, borrow: Callable[[JobGroup], None], give_back: Callable[[JobGroup], None],
                    time_limit: float | None = None, paused: Callable[[], float] | None = None) -> dict:
        module = request.modules[0]
        relative = module_path(module)
        executable = await asyncio.to_thread(tooling.executable)
        lake = await asyncio.to_thread(tooling.lake_executable)
        libraries = await tooling.library_paths()
        tooling.check_module_paths(list(sources), libraries)
        key = environment_key(tooling, lake)
        try:
            # A draft checked over the workspace has a session of its own.
            directory = str(request.directory) + (" + " + request.overlay if request.overlay else "")
            session = await self._session(tooling, owner, directory, key, executable, lake, libraries, threads)
        except (OSError, TimeoutError, ProcessHostError) as error:
            raise SessionFailed(f"The Lean server did not start: {error}") from error
        async with session.lock:
            borrow(session.group)
            try:
                return await self._elaborate(session, sources, module, relative, libraries, time_limit, paused)
            except CheckTimedOut:
                await self.discard(session)
                raise
            except (SessionFailed, OSError, TimeoutError) as error:
                await self.discard(session)
                raise SessionFailed(str(error)) from error
            finally:
                session.last_used = time.monotonic()
                give_back(session.group)

    async def _session(self, tooling, owner, directory, key, executable, lake, libraries, threads) -> Session:
        name = digest({"owner": owner, "directory": directory, "environment": key})[:20]
        async with self._starting:
            session = self.sessions.get(name)
            if session is not None and session.alive:
                return session
            if session is not None:
                await self.discard(session)
            while len(self.sessions) >= self.limit:
                idle = [item for item in self.sessions.values() if not item.lock.locked()]
                if not idle:
                    raise SessionFailed("Every Lean session is busy")
                await self.discard(min(idle, key=lambda item: item.last_used))
            parent = Path(tooling.blobs.root).parent / "lake"
            root = parent / "sessions" / name
            root.mkdir(parents=True, exist_ok=True)
            cache = (parent / "cache" / key).resolve()
            cache.mkdir(parents=True, exist_ok=True)
            temporary = root / ".tmp"
            temporary.mkdir(exist_ok=True)
            env = process_environment(libraries)
            env.update(LEAN=str(executable), LAKE=str(lake), LEAN_SYSROOT=str(executable.parent.parent),
                       LAKE_CACHE_DIR=str(cache), LAKE_ARTIFACT_CACHE="true", LAKE_RESTORE_ARTIFACTS="true",
                       LEAN_NUM_THREADS=str(max(1, threads)), TMPDIR=str(temporary))
            command, env = self.sandbox.wrap(self.command(executable), cwd=root, env=env, writable=[root, cache])
            group = await asyncio.to_thread(functools.partial(JobGroup, **self.group_limits))
            try:
                host = await OwnedProcessHost.start(command, cwd=root, env=env, max_frame_bytes=FRAME_BYTES,
                                                    startup_timeout=60.0, group=group, register=False)
            except BaseException:
                await asyncio.to_thread(group.close)
                raise
            connection = LspConnection(host)
            session = Session(name, root, host, connection, group, key)
            self.sessions[name] = session
            try:
                await connection.request("initialize", {"processId": os.getpid(), "rootUri": _uri(root),
                                                        "capabilities": {}}, timeout=120)
                await connection.notify("initialized", {})
            except BaseException:
                await self.discard(session)
                raise
            return session

    @staticmethod
    async def _diagnostics(connection, uri: str, version: int, started: float, time_limit: float | None,
                           paused: Callable[[], float] | None) -> None:
        """Wait for Lean's diagnostics of this version, for at most `time_limit` seconds of the
        check's own time: the time its job spends suspended does not count."""
        waiting = asyncio.ensure_future(connection.request("textDocument/waitForDiagnostics",
                                                           {"uri": uri, "version": version}))
        if time_limit is None:
            await waiting
            return
        paused_before = paused() if paused else 0.0
        try:
            while True:
                left = time_limit - (time.monotonic() - started) + ((paused() if paused else 0.0) - paused_before)
                if left <= 0:
                    raise CheckTimedOut(f"Lean did not answer within {time_limit:g} seconds")
                done, _ = await asyncio.wait({waiting}, timeout=left)
                if done:
                    await waiting
                    return
        finally:
            if not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)

    async def _elaborate(self, session: Session, sources: dict[str, bytes], module: str, relative: str,
                         libraries: list[Path], time_limit: float | None = None,
                         paused: Callable[[], float] | None = None) -> dict:
        started = time.monotonic()
        dependencies = await offload.call(_prepared_session, session.root, sources, session.environment_key,
                                          libraries, module)
        text = sources[relative].decode("utf-8-sig", errors="replace")
        header = _header(sources[relative])
        uri = _uri(session.root / "source" / relative)
        document = session.documents.get(uri)
        reason = None
        connection = session.connection
        if document is None:
            reason = "opened"
        elif document.header != header:
            reason = "imports changed"
        elif document.dependencies != dependencies:
            reason = "a project dependency changed"
        if reason is None:
            assert document is not None
            document.version += 1
            await connection.notify("textDocument/didChange", {
                "textDocument": {"uri": uri, "version": document.version}, "contentChanges": [{"text": text}]})
        else:
            if document is not None:
                await connection.notify("textDocument/didClose", {"textDocument": {"uri": uri}})
            document = session.documents[uri] = Document(1, header, dependencies)
            await connection.notify("textDocument/didOpen", {"textDocument": {
                "uri": uri, "languageId": "lean4", "version": 1, "text": text}, "dependencyBuildMode": "always"})
        document.header, document.dependencies = header, dependencies
        try:
            await self._diagnostics(connection, uri, document.version, started, time_limit, paused)
        except CheckTimedOut as error:
            # Stopped with nothing said, a check that does not end gives no clue where. In the Linux
            # pilot Lean had reported a failed `rw` 15 seconds in, then checked that declaration in
            # the kernel until the check was stopped half an hour later.
            error.diagnostics = _diagnostics(relative, _current(connection.diagnostics, uri, document.version))
            error.processing = _processing(_current(connection.progress, uri, document.version))
            raise
        diagnostics = _diagnostics(relative, _current(connection.diagnostics, uri, document.version))
        has_sorry = uses_sorry(diagnostics)
        session.checks += 1
        report = {"ok": not diagnostics["errors"] and not has_sorry, "proof_accepted": False,
                  "validation_method": "lean-server", "diagnostics": diagnostics,
                  "modules": [{"module": module, "source_sha256": hashlib.sha256(sources[relative]).hexdigest()}],
                  "built_modules": [module], "reused_modules": [],
                  "session": {"incremental": reason is None, "reopened": reason, "checks": session.checks,
                              "seconds": round(time.monotonic() - started, 3)},
                  "process": {"returncode": 0 if not diagnostics["errors"] else 1, "tree_quiet": True,
                              "timed_out": False, "output_limited": False, "error": None},
                  "workspace": str(session.root)}
        if has_sorry:
            report["reason"] = SORRY_REASON
        return report

    async def discard(self, session: Session) -> None:
        """Close a session and remove its directory: what it built is in the shared cache, which
        the next session restores from. Kept, the Linux pilot's session directories grew by 14 GB
        a day until the disk was full. The directory moves aside first, so that a new session of
        the same key meanwhile starts in a fresh one."""
        self.sessions.pop(session.key, None)
        discarded = session.root.with_name(f".discarded-{session.root.name}-{uuid4().hex[:8]}")
        with suppress(OSError):
            session.root.rename(discarded)
        try:
            await session.close()
        except Exception:
            logger.exception("Closing a Lean session failed")
        await asyncio.to_thread(shutil.rmtree, discarded, True)

    async def evict(self, *, idle_seconds: float | None = None, one: bool = False) -> int:
        """Close sessions idle for longer than `idle_seconds` (the pool's own limit by default),
        or, with `one`, the least recently used idle session."""
        idle = sorted((session for session in self.sessions.values() if not session.lock.locked()),
                      key=lambda session: session.last_used)
        if one:
            idle = idle[:1]
        else:
            limit = self.idle_seconds if idle_seconds is None else idle_seconds
            idle = [session for session in idle if time.monotonic() - session.last_used >= limit]
        for session in idle:
            await self.discard(session)
        return len(idle)

    async def close(self) -> None:
        for session in list(self.sessions.values()):
            await self.discard(session)

    def status(self) -> list[dict]:
        now = time.monotonic()
        return [{"key": session.key, "checks": session.checks, "busy": session.lock.locked(),
                 "idle_seconds": round(now - session.last_used), "documents": len(session.documents),
                 "private_memory": session.group.usage().private_memory}
                for session in self.sessions.values()]
