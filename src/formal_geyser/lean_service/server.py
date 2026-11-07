"""The Lean service process: a loopback HTTP API over the service core, one per project.

Only local clients with the service's operator token may call it (local_operator.py), so a
sandboxed agent cannot. `supervise` keeps it running: it restarts the server when it exits, until
`stop` asks both to end. The server holds its own lock, so a second one never starts.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import portalocker
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .. import cgroups
from ..coordination.config import load_config
from ..coordination.local_operator import issue_token, operator_request
from ..coordination.runtime.process import raise_open_files_limit
from ..coordination.storage import atomic_json
from .client import state_directory
from .sandbox import CodexSandbox, Unavailable, Unsandboxed
from .service import LeanService
from .warm import WarmSet

logger = logging.getLogger(__name__)
WARM_SECONDS = 300.0


def create_app(service: LeanService, *, on_ready=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await service.start()
        warming = asyncio.create_task(_keep_warm(service), name="lean-service-warm")
        if on_ready is not None:
            on_ready()
        try:
            yield
        finally:
            warming.cancel()
            await asyncio.gather(warming, return_exceptions=True)
            await service.close()
            if service.warm is not None:
                service.warm.release()

    app = FastAPI(title="Lean service", lifespan=lifespan)
    token = issue_token(service.root, "lean-service")

    async def operator(request: Request):
        if not operator_request(request, token):
            raise HTTPException(403, "Only local clients with the operator token may use the Lean service")

    @app.exception_handler(ValidationError)
    async def invalid(_request: Request, error: ValidationError):
        return JSONResponse({"error": str(error)}, status_code=422)

    @app.exception_handler(KeyError)
    async def missing(_request: Request, error: KeyError):
        return JSONResponse({"error": "Unknown job"}, status_code=404)

    @app.get("/v1/health", dependencies=[Depends(operator)])
    async def health():
        return {"ok": True, "instance": service.instance, "pid": os.getpid()}

    @app.post("/v1/jobs", dependencies=[Depends(operator)])
    async def submit(body: dict = Body(...)):
        return await service.submit(body)

    @app.get("/v1/jobs/{job_id}", dependencies=[Depends(operator)])
    async def job(job_id: str):
        return await service.wait(job_id, 0)

    @app.post("/v1/jobs/{job_id}/wait", dependencies=[Depends(operator)])
    async def wait(job_id: str, body: dict = Body(default={})):
        return await service.wait(job_id, min(60.0, max(0.0, float(body.get("timeout", 25.0)))))

    @app.post("/v1/jobs/{job_id}/cancel", dependencies=[Depends(operator)])
    async def cancel(job_id: str):
        return await service.cancel(job_id)

    @app.get("/v1/status", dependencies=[Depends(operator)])
    async def status():
        return service.status()

    @app.get("/v1/telemetry", dependencies=[Depends(operator)])
    async def telemetry(hours: float = Query(default=24.0, gt=0, le=24 * 90)):
        return await service.telemetry(hours)

    @app.get("/v1/diagnostics/cpu", dependencies=[Depends(operator)])
    async def cpu_profile(seconds: float = Query(default=60.0, gt=0, le=300)):
        # CPU by thread and code, and where the event loop's thread is (diagnostics).
        from .diagnostics import diagnose

        return await asyncio.to_thread(diagnose, seconds)

    @app.post("/v1/shutdown", dependencies=[Depends(operator)])
    async def shutdown():
        server = getattr(app.state, "server", None)
        if server is not None:
            server.should_exit = True
        return {"ok": True}

    return app


async def _keep_warm(service: LeanService) -> None:
    """Refresh the warm pages every few minutes, and soon after checks import something new."""
    while True:
        try:
            await asyncio.wait_for(service.warm_changed.wait(), WARM_SECONDS)
            await asyncio.sleep(5)  # Let a burst of new checks arrive first.
        except TimeoutError:
            pass
        service.warm_changed.clear()
        if service.warm is not None:
            try:
                await asyncio.to_thread(service.warm.refresh)
            except Exception:
                logger.exception("Keeping library pages warm failed; it is retried")


def build_service(root: Path) -> LeanService:
    config = load_config(root)
    settings = config.lean_service
    state = state_directory(root)
    sandbox: CodexSandbox | Unavailable | Unsandboxed = Unsandboxed()
    if settings.sandbox == "codex":
        try:
            from ..coordination.claude_profiles import profile_homes
            from ..coordination.codex_profiles import sign_in_homes
            from ..coordination.native_permissions import protected_reads

            # Lean code that a check runs reads neither the operator tokens nor any sign-in,
            # including a profile's signed in while the service runs.
            sandbox = CodexSandbox(config.codex.executable, state / "codex-home",
                                   lambda: protected_reads(sign_in_homes(config.codex), profile_homes(config.claude)))
        except (FileNotFoundError, OSError) as error:
            sandbox = Unavailable(str(error))  # Checks fail as unavailable; the service still answers.
    service = LeanService(root, settings, sandbox=sandbox)
    service.warm = WarmSet(int(settings.warm_gib * 2**30))
    return service


def serve(root: Path) -> int:
    import uvicorn

    raise_open_files_limit()  # Many Lean processes at once take many pipes.
    root = root.resolve()
    state = state_directory(root)
    state.mkdir(parents=True, exist_ok=True)
    try:
        lock = portalocker.Lock(str(state / "serve.lock"), mode="a+b", timeout=0)
        lock.acquire()
    except portalocker.exceptions.LockException:
        print("A Lean service already runs for this project", file=sys.stderr)
        return 3
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        service = build_service(root)

        def ready():
            atomic_json(state / "service.json", {"pid": os.getpid(), "port": port, "instance": service.instance,
                                                 "started_at": time.time()})

        app = create_app(service, on_ready=ready)
        server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on", loop="asyncio"))
        app.state.server = server
        cgroups.root()  # Before the first check: a system without one fails at once.
        asyncio.run(server.serve(sockets=[listener]))
        return 0
    finally:
        (state / "service.json").unlink(missing_ok=True)
        lock.release()


def supervise(root: Path) -> int:
    """Run the server and start it again whenever it exits, until asked to stop."""
    root = root.resolve()
    state = state_directory(root)
    state.mkdir(parents=True, exist_ok=True)
    try:
        lock = portalocker.Lock(str(state / "supervisor.lock"), mode="a+b", timeout=0)
        lock.acquire()
    except portalocker.exceptions.LockException:
        return 0  # Another supervisor keeps this service.
    stop = state / "stop"
    if stop.exists():
        lock.release()
        return 0  # A stop came after the start that launched this supervisor: it wins.
    backoff = 1.0
    try:
        atomic_json(state / "supervisor.json", {"pid": os.getpid(), "started_at": time.time()})
        while not stop.exists():
            started = time.monotonic()
            # The server's own delegated scope, which ends with this supervisor's, so that a new
            # supervisor never finds two servers.
            unit = f"formal-geyser-lean-{uuid4().hex}.scope"
            command = cgroups.scope_command(
                unit, [sys.executable, "-m", "formal_geyser.lean_service", "serve", "--project", str(root)],
                delegate=True, bound_to=cgroups.own_unit())
            with (state / "service.log").open("ab") as log:
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                           cwd=str(root), env=cgroups.bus_environment())
            while process.poll() is None:
                time.sleep(0.5)
            cgroups.end_unit(unit)  # Ends anything the server left.
            _event(state, {"event": "server_exited", "code": process.returncode,
                           "seconds": round(time.monotonic() - started, 1)})
            if stop.exists() or process.returncode == 3:
                break
            backoff = 1.0 if time.monotonic() - started > 60 else min(30.0, backoff * 2)
            time.sleep(backoff)
    finally:
        (state / "supervisor.json").unlink(missing_ok=True)
        lock.release()
    return 0


def _event(state: Path, record: dict) -> None:
    try:
        with (state / "supervisor-events.jsonl").open("a", encoding="utf-8") as log:
            log.write(json.dumps({"time": time.time(), **record}) + "\n")
    except OSError:
        pass

