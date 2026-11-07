"""Local operator API and exclusive project ownership."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from html import escape
from pathlib import Path
from typing import Any, Literal

import portalocker
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from .errors import HubError
from .library_metrics import LibraryMetrics, seconds_to_next_hour
from .local_operator import COOKIE, issue_token, operator_request, sign_in
from .models import Actor, Command, Role
from .project_setup import (
    SetupStart,
    check_basis,
    create_setup,
    existing_receipt,
    prepare_local_project,
    setup_status,
    validate_selection,
)
from .run_driver import RunDriver, admits_work
from .session_settings import bound_settings
from .storage import canonical
from .transcripts import TranscriptStore, inline_message_bodies

logger = logging.getLogger(__name__)
DEFAULT_MAX_COMMAND_BYTES = 262144
OVERVIEW_SECONDS = 3.0  # The longest the dashboard's overview is shared before it is built again.


class ProjectOwner:
    """Hold one advisory owner lock for the complete Hub lifetime."""

    def __init__(self, root: Path):
        self.path = root.resolve() / ".task-based" / "hub.lock"
        self.lock: portalocker.Lock | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = portalocker.Lock(
            str(self.path),
            mode="a+b",
            timeout=0,
            flags=portalocker.LOCK_EX | portalocker.LOCK_NB,
        )
        try:
            self.lock.acquire()
        except portalocker.exceptions.LockException as error:
            self.lock = None
            raise HubError(
                "project_owned",
                "Another process owns this project; connect to its HTTP API with --url",
                409,
            ) from error
        return self

    def __exit__(self, *_):
        if self.lock is not None:
            self.lock.release()
            self.lock = None


class CommandBodyLimit:
    """Bound actual streamed input, including requests without Content-Length."""

    def __init__(self, app: ASGIApp, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers", []):
            if name.lower() == b"content-length":
                try:
                    size = int(value)
                    if size < 0:
                        raise ValueError
                except ValueError:
                    await JSONResponse(
                        {"error": {"code": "invalid_length", "message": "Invalid Content-Length"}},
                        status_code=400,
                    )(scope, receive, send)
                    return
                if size > self.max_bytes:
                    await self._too_large(scope, receive, send)
                    return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                continue
            body.extend(message.get("body", b""))
            if len(body) > self.max_bytes:
                await self._too_large(scope, receive, send)
                return
            if not message.get("more_body", False):
                break
        delivered = False

        async def bounded_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)

    async def _too_large(self, scope: Scope, receive: Receive, send: Send):
        await JSONResponse(
            {"error": {"code": "body_too_large", "message": "Command body exceeds the limit"}},
            status_code=413,
        )(scope, receive, send)


def _default_runner_factory(hub):
    from .runtime import WorkerRunner

    return WorkerRunner(hub)


def _page(message: str) -> str:
    return ("<!doctype html><meta charset=utf-8><title>TaskHub</title>"
            "<p style='font: 15px system-ui; margin: 2em'>" + escape(message) + "</p>")


def create_app(
    root: Path,
    *,
    hub=None,
    runner_factory: Callable | None = None,
    model_catalog_loader: Callable | None = None,
    max_command_bytes: int = DEFAULT_MAX_COMMAND_BYTES,
    resume_runs: bool = True,
    stall_watchdog: bool = False,
    environment_holds: bool = False,
) -> FastAPI:
    """Serve the operator API; runs driven before a restart resume unless resume_runs is false.
    `stall_watchdog` writes every thread's stack to the log when the service stalls;
    `environment_holds` holds new sessions while this machine cannot sandbox agents' commands."""
    if max_command_bytes < 1:
        raise ValueError("max_command_bytes must be positive")
    root = root.resolve()
    token = issue_token(root, "taskhub")
    supplied_hub = hub
    library_metrics = LibraryMetrics(root)
    make_runner = runner_factory or _default_runner_factory

    async def count_library_hourly() -> None:
        """Count the Library at start only when no count is saved, then every hour on the hour.
        A request never counts: it serves the last count."""
        if library_metrics.latest() is None:
            await asyncio.to_thread(library_metrics.refresh)
        while True:
            if library_metrics.error:
                logger.warning("Counting the published Library failed: %s", library_metrics.error)
            await asyncio.sleep(seconds_to_next_hour(time.time()))
            await asyncio.to_thread(library_metrics.refresh)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        with ProjectOwner(root):
            if supplied_hub is None:
                from .hub import Hub

                active_hub = Hub(root)
            else:
                active_hub = supplied_hub
            app.state.hub = active_hub
            if environment_holds:
                from .native_permissions import sandbox_blocker

                active_hub.environment_probe = sandbox_blocker  # Before any run resumes.
            driver = app.state.driver = RunDriver(active_hub, make_runner, resume_runs=resume_runs)
            # Setup has no live workers. Serializing it with operator mutations
            # prevents another client claiming work between configuration and
            # the atomic first-task/first-run commit.
            app.state.setup_lock = asyncio.Lock()
            app.state.overview_generation = 0  # Advanced by every operator command.
            await driver.recover()
            from .git_sync import GitSyncService

            app.state.git_sync = GitSyncService(active_hub)
            app.state.git_sync_task = (
                asyncio.create_task(app.state.git_sync.run(), name="taskhub-git-sync")
                if app.state.git_sync.config.enabled else None)
            watchdog = None
            if stall_watchdog:
                from .stall_watchdog import StallWatchdog

                watchdog = StallWatchdog(describe=lambda: [
                    f"Coordination database: {active_hub.db.diagnostics()['pending']} calls queued or running."])
                watchdog.start()
            driver.start_timer()
            library_count = asyncio.create_task(count_library_hourly(), name="taskhub-library-count")
            try:
                yield
            finally:
                if watchdog is not None:
                    watchdog.stop()
                tasks = driver.stop()
                tasks.append(library_count)
                app.state.git_sync.stop()
                if app.state.git_sync_task is not None:
                    tasks.append(app.state.git_sync_task)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if supplied_hub is None:
                    await active_hub.close()

    app = FastAPI(
        title="FormalGeyser Coordination",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(CommandBodyLimit, max_bytes=max_command_bytes)
    app.state.library_metrics = library_metrics

    signed_out = (f"Sign in again: run taskctl --project {root} dashboard, then open the link it prints.")

    async def operator(request: Request):
        if not operator_request(request, token):
            raise HTTPException(status_code=401, detail=signed_out)
        return Actor(role=Role.OPERATOR)

    @app.exception_handler(HubError)
    async def hub_error(_request: Request, error: HubError):
        return JSONResponse({"error": error.as_dict()}, status_code=error.status)

    # A pydantic ValidationError is a ValueError too.
    @app.exception_handler(ValueError)
    async def invalid_value(_request: Request, error: ValueError):
        return JSONResponse(
            {"error": {"code": "invalid_input", "message": str(error)}}, status_code=422
        )

    @app.get("/login", include_in_schema=False)
    async def login(request: Request, token_value: str = Query(default="", alias="token")):
        if not sign_in(request, token_value, token):
            return HTMLResponse(_page(signed_out), status_code=401, headers={"Cache-Control": "no-store"})
        response = RedirectResponse("/", status_code=303)  # The link leaves the address bar.
        response.set_cookie(COOKIE, token, httponly=True, samesite="strict", path="/")
        return response

    @app.get("/", response_class=HTMLResponse)
    async def project_page(request: Request):
        if not operator_request(request, token):
            return HTMLResponse(_page(signed_out), status_code=401, headers={"Cache-Control": "no-store"})
        nonce = secrets.token_urlsafe(24)
        html = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
        return HTMLResponse(html.replace("__CSP_NONCE__", nonce), headers={
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'self'; script-src 'nonce-" + nonce +
                "'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self'; "
                "object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        })

    # The dashboard's overview, built at most once every OVERVIEW_SECONDS for every tab that asks
    # (one build at a time), and again at once after an operator command or a change in the
    # driver's own state. In the Linux pilot each poll, every 2 s per tab, built 7 MB in 1.3-4.5 s.
    overview_cache: dict[str, Any] = {"key": None, "at": 0.0, "built": None, "building": None}

    @app.get("/v1/overview", dependencies=[Depends(operator)])
    async def overview(request: Request, compact: bool = Query(default=False)):
        if not compact:
            body, tag = await _overview(False)
        else:
            driver, cache = app.state.driver, overview_cache
            key = canonical(jsonable_encoder({
                "commands": app.state.overview_generation,
                "error": driver.delivery_recovery_error or driver.history_notice(), "drivers": driver.results,
                "armed": sorted(driver.armed), "driving": sorted(driver.driving_runs()),
                "startup": driver.startup_resume}))
            if cache["building"] is not None:
                await asyncio.shield(cache["building"])
            if cache["key"] != key or time.monotonic() - cache["at"] > OVERVIEW_SECONDS:
                cache["building"] = building = asyncio.ensure_future(_overview(True))
                try:
                    cache["built"] = await asyncio.shield(building)
                    cache["key"], cache["at"] = key, time.monotonic()
                finally:
                    if cache["building"] is building:
                        cache["building"] = None
            body, tag = cache["built"]
        headers = {"ETag": tag, "Cache-Control": "no-cache"}
        if request.headers.get("if-none-match") == tag:
            return Response(status_code=304, headers=headers)
        return Response(body, media_type="application/json", headers=headers)

    @app.get("/v1/retired-workers", dependencies=[Depends(operator)])
    async def retired_workers():
        # The dashboard reads it again only when the overview's retired_count changes.
        return await app.state.hub.retired_workers()

    async def _overview(compact: bool) -> tuple[bytes, str]:
        active, driver = app.state.hub, app.state.driver
        latest_messages = getattr(active, "latest_operator_messages", None)

        profile_summary = getattr(active, "claude_profile_summary", None)
        codex_summary = getattr(active, "codex_profile_summary", None)

        async def no_messages():
            return {}

        async def no_profile():
            return None

        async def no_count():
            return None

        # Independent SQL-only reads run together on the read connections; none of them
        # waits behind the coordination thread or reads mutable native files.
        (tasks, workers, runs, requests, queue, holds, messages, claude_profile, codex_profile,
         retired) = await asyncio.gather(
            active.task_overview() if compact else _all_tasks(active), active.list_workers(retired=not compact),
            active.list_runs(), active.list_human_requests(), active.integration_queue(),
            active.delivery_holds(), latest_messages() if latest_messages else no_messages(),
            profile_summary() if profile_summary else no_profile(),
            codex_summary() if codex_summary else no_profile(),
            active.retired_worker_count() if compact else no_count())
        if compact:
            # The dashboard never shows runtime profiles; they were a third of every poll.
            # It shows the model and effort the current conversation began with.
            workers = [{**{key: value for key, value in worker.items() if key != "profile_json"},
                        "session_settings": bound_settings(worker)} for worker in workers]
        payload = {"name": active.config.name, **tasks,
                   "workers": workers, "runs": runs,
                   # The compact overview leaves retired workers out; /v1/retired-workers names them.
                   **({"retired_count": retired} if compact else {}),
                   "requests": requests,
                   "integration_queue": queue,
                   "lean_resources": await domain_resources(active),
                   "delivery_holds": holds,
                   "delivery_recovery_error": driver.delivery_recovery_error or driver.history_notice(),
                   "drivers": driver.results,
                   # What this service is doing now, for the dashboard's status bar.
                   "service": {"armed_runs": sorted(driver.armed), "driving_runs": sorted(driver.driving_runs()),
                               "startup_resume": driver.startup_resume},
                   # The Claude and Codex profiles in use, for the header.
                   "claude_profile": claude_profile,
                   "codex_profile": codex_profile,
                   "messages_to_you": messages}

        def encode() -> tuple[bytes, str]:
            body = json.dumps(jsonable_encoder(payload), ensure_ascii=False, allow_nan=False,
                              separators=(",", ":")).encode("utf-8")
            # An unchanged overview costs the browser nothing: it keeps the page it has.
            # Memory figures and clocks move every second; only the jobs and their states count.
            lean = payload["lean_resources"] or {}
            basis = {**payload, "lean_resources": {
                "counts": lean.get("counts"),
                "jobs": [[job.get("id"), job.get("state"), job.get("threads")] for job in lean.get("jobs", [])]}}
            return body, '"' + hashlib.sha256(canonical(jsonable_encoder(basis)).encode("utf-8")).hexdigest()[:32] + '"'
        # Off the event loop: encoding a large overview held it for seconds.
        return await asyncio.to_thread(encode)

    async def _all_tasks(active):
        return {"tasks": await active.list_tasks()}

    async def domain_resources(active):
        """The domain's shared machine resources, such as the Lean service's queue: its status,
        {"running": False} while it is stopped, or None for a domain without one. Read at most
        every two seconds and never on the coordination thread."""
        from .plugins import load_plugin

        status = getattr(load_plugin(active.config.domain), "resource_status", None)
        if status is None:
            return None
        cached = getattr(app.state, "domain_resources", None)
        if cached is not None and time.monotonic() - cached[0] < 2.0:
            return cached[1]
        try:
            value = await asyncio.wait_for(status(active), 2.0)
        except TimeoutError:  # A slow service never delays the page.
            return cached[1] if cached is not None else {"running": False, "error": "The service did not answer in time."}
        value = {"running": False} if value is None else value
        app.state.domain_resources = (time.monotonic(), value)
        return value

    @app.get("/v1/task-outline", dependencies=[Depends(operator)])
    async def task_outline():
        return await app.state.hub.task_outline()

    @app.get("/v1/task-graph", dependencies=[Depends(operator)])
    async def task_graph(include_met: bool = False):
        return await app.state.hub.task_graph(include_met=include_met)

    async def catalog():
        loader = model_catalog_loader
        if loader is None:
            from .runtime.model_catalog import discover_models
            loader = discover_models
        return await loader(app.state.hub.config.codex, root)

    @app.get("/v1/setup", dependencies=[Depends(operator)])
    async def project_setup():
        result = await setup_status(app.state.hub)
        run = result["run"]
        if run is not None:
            run["armed"] = run["id"] in app.state.driver.armed
            run["driving"] = app.state.driver.driving(run["id"])
            result["can_drive"] = result["can_drive"] and not run["armed"] and not run["driving"]
        return result

    @app.get("/v1/setup/models", dependencies=[Depends(operator)])
    async def project_models():
        # Metadata only: no thread or model turn is created by this request.
        return await catalog()

    @app.post("/v1/setup/start", status_code=202, dependencies=[Depends(operator)])
    async def project_start(request: SetupStart):
        async with app.state.setup_lock:
            active = app.state.hub
            result = await existing_receipt(active, request)
            replayed = result is not None
            if result is None:
                await check_basis(active, request)
                available = await catalog()
                validate_selection(available, request)
                await prepare_local_project(active, request)
                result, replayed = await create_setup(active, request)
                if not replayed:
                    # No await after the committed result and before arming.
                    # A crash before this point leaves a visible prepared run;
                    # a replay never silently activates or resumes it.
                    app.state.driver.arm(result["run_id"])
                    app.state.git_sync.wake()
            return {**result, "replayed": replayed, "armed": result["run_id"] in app.state.driver.armed,
                    "driving": app.state.driver.driving(result["run_id"])}

    @app.get("/dashboard.js", include_in_schema=False, dependencies=[Depends(operator)])
    async def dashboard_script():
        return Response(Path(__file__).with_name("dashboard.js").read_text(encoding="utf-8"),
                        media_type="text/javascript", headers={
                            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})

    @app.get("/v1/workers/{worker_id}/messages", dependencies=[Depends(operator)])
    async def human_messages(worker_id: str, after: int = Query(default=0, ge=0),
                             limit: int = Query(default=100, ge=1, le=500)):
        worker = await app.state.hub.worker(worker_id)
        return await app.state.hub.human_history(worker["id"], after=after, limit=limit)

    @app.get("/v1/workers/{worker_id}/transcript", dependencies=[Depends(operator)])
    async def transcript(worker_id: str, cursor: int = Query(default=0, ge=0),
                         limit: int = Query(default=100, ge=1, le=500)):
        worker = await app.state.hub.worker(worker_id)
        return await asyncio.to_thread(TranscriptStore(root).page, worker["id"], cursor=cursor, limit=limit)

    @app.get("/v1/workers/{worker_id}/sessions", dependencies=[Depends(operator)])
    async def worker_sessions(worker_id: str):
        worker = await app.state.hub.worker(worker_id)
        return await asyncio.to_thread(TranscriptStore(root).sessions, worker["id"])

    @app.get("/v1/workers/{worker_id}/sessions/{session_key}", dependencies=[Depends(operator)])
    async def worker_session(worker_id: str, session_key: str,
                             before: int | None = Query(default=None, ge=1),
                             limit: int = Query(default=20, ge=1, le=100),
                             include_context: bool = Query(default=True)):
        worker = await app.state.hub.worker(worker_id)
        return await asyncio.to_thread(lambda: inline_message_bodies(TranscriptStore(root).session_page(
            worker["id"], session_key, before=before, limit=limit, include_context=include_context), root))

    @app.get("/v1/requests", dependencies=[Depends(operator)])
    async def human_requests():
        return await app.state.hub.list_human_requests()

    @app.get("/v1/literature", dependencies=[Depends(operator)])
    async def literature():
        return await app.state.hub.list_literature()

    @app.get("/v1/status", dependencies=[Depends(operator)])
    async def status():
        active = app.state.hub
        # SQL-only: the status must answer while the coordination thread is busy.
        counts = await active.db.read(lambda c: (
            c.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
            c.execute("SELECT COUNT(*) FROM workers").fetchone()[0]))
        return {
            "name": active.config.name,
            "domain": active.config.domain,
            "running_drivers": app.state.driver.driving_runs(),
            "armed_runs": sorted(app.state.driver.armed),
            "startup_resume": app.state.driver.startup_resume,
            "tasks": counts[0],
            "workers": counts[1],
            "lean_resources": await domain_resources(active),
            "coordination": active.db.diagnostics(),
        }

    @app.get("/v1/repository", dependencies=[Depends(operator)])
    async def repository():
        return await app.state.hub.repository_status()

    @app.get("/v1/library/stats", dependencies=[Depends(operator)])
    async def library_stats():
        result = library_metrics.latest()
        if result is None:
            raise HubError("LIBRARY_STATS_UNAVAILABLE", "The published Library has not been counted yet"
                           + (": " + library_metrics.error if library_metrics.error else ""), 503)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    @app.get("/v1/library", dependencies=[Depends(operator)])
    async def library():
        return await app.state.hub.library_status()

    @app.get("/v1/notes", dependencies=[Depends(operator)])
    async def notes(kind: str | None = None):
        return await app.state.hub.list_notes(kind=kind)

    @app.get("/v1/mathematical-artifacts", dependencies=[Depends(operator)])
    async def mathematical_artifacts(kind: str | None = None):
        records = await app.state.hub.list_math_artifacts()
        return [record for record in records if kind is None or record["kind"] == kind]

    @app.get("/v1/records/{record_id}")
    @app.get("/v1/notes/{record_id}")
    async def record(record_id: str, actor: Actor = Depends(operator)):
        return await app.state.hub.native_record(record_id, actor)

    @app.get("/v1/talk/conversations")
    async def talk_conversations(
        worker_id: str = Query(min_length=1), after_id: str = "",
        limit: int = Query(default=50, ge=1, le=100),
        max_bytes: int = Query(default=8192, ge=512, le=65536),
        actor: Actor = Depends(operator),
    ):
        return await app.state.hub.peer_conversations(
            actor, worker_id=worker_id, after_id=after_id, limit=limit, max_bytes=max_bytes)

    @app.get("/v1/talk/conversations/{conversation_id}")
    async def talk_history(
        conversation_id: str, worker_id: str | None = Query(default=None, min_length=1),
        after_sequence: int = Query(default=0, ge=0), offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        max_bytes: int = Query(default=8192, ge=512, le=65536),
        actor: Actor = Depends(operator),
    ):
        return await app.state.hub.peer_history(
            conversation_id, actor, worker_id=worker_id, after_sequence=after_sequence,
            offset=offset, limit=limit, max_bytes=max_bytes)

    @app.get("/v1/talk/inbox")
    async def talk_inbox(
        worker_id: str = Query(min_length=1), limit: int = Query(default=50, ge=1, le=100),
        max_bytes: int = Query(default=8192, ge=512, le=65536),
        actor: Actor = Depends(operator),
    ):
        return await app.state.hub.peer_unread(
            actor, worker_id=worker_id, limit=limit, max_bytes=max_bytes)

    @app.get("/v1/tasks", dependencies=[Depends(operator)])
    async def tasks():
        return await app.state.hub.list_tasks()

    @app.get("/v1/task-browser", dependencies=[Depends(operator)])
    async def task_browser(
        view: Literal["all", "running", "pool", "history"] = "all",
        query: str = Query(default="", max_length=1000),
        offset: int = Query(default=0, ge=0), limit: int = Query(default=25, ge=1, le=100),
        kind: Literal["work", "review", "both"] = "both",
        sort: Literal["newest", "oldest", "title"] = "newest",
        until: int | None = Query(default=None, ge=0),
    ):
        return await app.state.hub.task_page(view=view, query=query, offset=offset, limit=limit,
                                             kind=kind, sort=sort, until=until)

    @app.get("/v1/tasks/{task_id}/relations", dependencies=[Depends(operator)])
    async def task_relations(task_id: str):
        return await app.state.hub.task_relations(task_id)

    @app.get("/v1/tasks/{task_id}/revisions", dependencies=[Depends(operator)])
    async def task_revisions(
        task_id: str, offset: int = Query(default=0, ge=0), limit: int = Query(default=20, ge=1, le=100),
    ):
        return await app.state.hub.task_revision_page(task_id, offset=offset, limit=limit)

    @app.get("/v1/tasks/{task_id}", dependencies=[Depends(operator)])
    async def task(task_id: str, revision: int | None = Query(default=None, ge=1)):
        from .task_browser import task_detail
        return await task_detail(app.state.hub, task_id, revision=revision)

    @app.get("/v1/workers", dependencies=[Depends(operator)])
    async def workers():
        return await app.state.hub.list_workers()

    @app.get("/v1/workers/{worker_id}", dependencies=[Depends(operator)])
    async def worker(worker_id: str):
        return await app.state.hub.worker(worker_id)

    @app.get("/v1/workers/{worker_id}/subscriptions", dependencies=[Depends(operator)])
    async def subscriptions(worker_id: str):
        return await app.state.hub.subscriptions(worker_id)

    @app.get("/v1/reports/{sha256}", dependencies=[Depends(operator)])
    async def report(sha256: str):
        try:
            return await asyncio.to_thread(app.state.hub.reports.read, sha256)
        except FileNotFoundError as error:
            raise HubError("REPORT_NOT_FOUND", "The exact machine report is unavailable", 404) from error
        except (OSError, ValueError) as error:
            raise HubError("REPORT_UNAVAILABLE", "The exact machine report failed integrity checking", 422) from error

    @app.get("/v1/artifacts", dependencies=[Depends(operator)])
    @app.get("/v1/records", dependencies=[Depends(operator)])
    async def artifacts(kind: str | None = None):
        return await app.state.hub.list_artifacts(kind=kind)

    @app.get("/v1/artifacts/{artifact_id}", dependencies=[Depends(operator)])
    async def artifact(artifact_id: str):
        return await app.state.hub.artifact(artifact_id)

    @app.get("/v1/continuations", dependencies=[Depends(operator)])
    async def continuations():
        return await app.state.hub.list_continuations()

    @app.get("/v1/interruptions", dependencies=[Depends(operator)])
    async def interruptions(
        offset: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000),
    ):
        records = await app.state.hub.list_interruptions()
        return records[offset:offset + limit]

    @app.get("/v1/events", dependencies=[Depends(operator)])
    async def events(after: int = Query(default=0, ge=0)):
        return await app.state.hub.events(after=after)

    @app.get("/v1/sync", dependencies=[Depends(operator)])
    async def sync_status():
        result = await app.state.git_sync.status()
        watcher = app.state.git_sync_task
        result["watching"] = watcher is not None and not watcher.done()
        if watcher is not None and watcher.done() and not watcher.cancelled():
            error = watcher.exception()
            if error is not None:
                result["loop_error"] = {"type": type(error).__name__, "message": str(error)[:1000]}
        return result

    @app.post("/v1/sync", dependencies=[Depends(operator)])
    async def sync_once():
        return await app.state.git_sync.once()

    @app.get("/v1/runs", dependencies=[Depends(operator)])
    async def runs():
        return await app.state.hub.list_runs()

    @app.get("/v1/runtime/backend", dependencies=[Depends(operator)])
    async def runtime_backend():
        # Switch with the runtime.select_backend command; it applies to workers created next.
        return await app.state.hub.backend_status()

    @app.get("/v1/runtime/claude-profiles", dependencies=[Depends(operator)])
    async def runtime_claude_profiles():
        # The Claude tab: each profile's sign-in, limit, reported usage, and the recent limits and
        # selections; select one with runtime.select_claude_profile.
        return await app.state.hub.claude_profiles()

    @app.get("/v1/runtime/codex-profiles", dependencies=[Depends(operator)])
    async def runtime_codex_profiles():
        # The Codex tab, as the Claude tab; null without configured Codex profiles.
        return await app.state.hub.codex_profiles()

    @app.get("/v1/runtime/rotation", dependencies=[Depends(operator)])
    async def runtime_rotation():
        # Set the mode with runtime.set_rotation_mode, or per person with worker.set_rotation_mode.
        return await app.state.hub.rotation_summary()

    @app.get("/v1/diagnostics/cpu", dependencies=[Depends(operator)])
    async def cpu_profile(seconds: float = 60.0):
        # Where this process spends CPU (cpu_profile); a few percent of one core while it runs.
        from .cpu_profile import profile

        return await asyncio.to_thread(profile, seconds)

    @app.get("/v1/runtime/rotation/memory", dependencies=[Depends(operator)])
    async def runtime_rotation_memory():
        return await app.state.hub.rotation_memory_report()

    @app.get("/v1/runs/{run_id}", dependencies=[Depends(operator)])
    async def run(run_id: str):
        driver = app.state.driver
        result = dict(await app.state.hub.run(run_id))
        result["driving"] = driver.driving(run_id)
        result["armed"] = run_id in driver.armed
        result["wake_pending"] = run_id in driver.pending_wakes
        result["driver_result"] = driver.results.get(run_id)
        return result

    @app.post("/v1/commands")
    async def execute(command: Command, actor: Actor = Depends(operator)):
        if len(command.name) > 128:
            raise ValueError("Command names may not exceed 128 characters")
        # Pydantic's arbitrary JSON payload accepts NaN; persisted canonical JSON does not.
        canonical(command.model_dump(mode="json"))
        async with app.state.setup_lock:
            result = await app.state.hub.execute(command, actor)
            app.state.overview_generation += 1  # The operator sees the result on the next poll.
            app.state.git_sync.wake()
            app.state.driver.committed(command, result)
        return result

    @app.post("/v1/runs/{run_id}/drive", status_code=202, dependencies=[Depends(operator)])
    async def drive(run_id: str):
        async with app.state.setup_lock:
            record = await app.state.hub.run(run_id)
            if not admits_work(record):
                raise HubError("run_not_running", "Only a running run, or a paused one with a resumed person, can be driven")
            await app.state.hub.mark_run_resumable(run_id)
            already_driving = app.state.driver.arm(run_id)
        return {
            "run_id": run_id, "driving": True, "armed": True,
            "already_driving": already_driving,
        }

    return app
