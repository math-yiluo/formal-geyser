"""Local operator CLI; model execution always requires an explicit run command."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import httpx
import tomli_w
import typer
import uvicorn

from .. import cgroups
from .api import ProjectOwner, create_app
from .config import initialize, load_config
from .errors import HubError
from .local_operator import bearer, is_loopback, loopback_url, operator_token
from .models import (
    Actor,
    CodexConfig,
    Command,
    GitSyncConfig,
    ProjectConfig,
    RepositoryConfig,
    Role,
    TaskContract,
    ValidationReport,
)
from .plugins import load_plugin
from .runtime.process import raise_open_files_limit
from .storage import atomic_write

# The service's interpreter switch interval, in seconds (Python's default is 0.005); see serve.
# TASKHUB_SWITCH_INTERVAL overrides it.
SWITCH_INTERVAL = 0.0002

app = typer.Typer(no_args_is_help=True, help="Persistent, domain-independent task coordination.")
tasks_app = typer.Typer(no_args_is_help=True)
workers_app = typer.Typer(no_args_is_help=True)
continuations_app = typer.Typer(no_args_is_help=True)
artifacts_app = typer.Typer(no_args_is_help=True)
records_app = typer.Typer(no_args_is_help=True, help="Administrative access to all historical records.")
notes_app = typer.Typer(no_args_is_help=True, help="Inspect private and public research notes.")
library_app = typer.Typer(no_args_is_help=True, help="Inspect the accepted mathematical Library.")
wiki_app = typer.Typer(no_args_is_help=True, help="Inspect public notes/wiki at the Library commit.")
talk_app = typer.Typer(no_args_is_help=True, help="Inspect private conversations as the operator.")
run_app = typer.Typer(no_args_is_help=True)
backend_app = typer.Typer(no_args_is_help=True,
                          help="Choose Codex or Claude for the workers created next, and the decision model.")
repository_app = typer.Typer(no_args_is_help=True, help="Manage the review publication repository.")
sync_app = typer.Typer(no_args_is_help=True, help="Publish reviewed source and shared TaskHub state through optional Git sync.")
app.add_typer(tasks_app, name="tasks")
app.add_typer(workers_app, name="workers")
app.add_typer(continuations_app, name="continuations")
app.add_typer(artifacts_app, name="artifacts")
app.add_typer(records_app, name="records")
app.add_typer(notes_app, name="notes")
app.add_typer(library_app, name="library")
app.add_typer(wiki_app, name="wiki")
app.add_typer(talk_app, name="talk")
app.add_typer(run_app, name="run")
app.add_typer(backend_app, name="backend")
app.add_typer(repository_app, name="repository")
app.add_typer(sync_app, name="sync")


@dataclass
class Settings:
    root: Path
    url: str | None = None


@app.callback()
def main(
    ctx: typer.Context,
    project: Annotated[Path, typer.Option("--project", "-p", help="Project directory.")] = Path("."),
    url: Annotated[
        str | None, typer.Option(help="Connect to an already running loopback service.")
    ] = None,
):
    try:
        ctx.obj = Settings(project.resolve(), loopback_url(url) if url else None)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--url") from error


def _service_logging() -> None:
    """Timestamp framework diagnostics in UTC so they line up with events and receipts."""
    package = logging.getLogger("formal_geyser")
    if any(getattr(handler, "_taskhub_service", False) for handler in package.handlers):
        return
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
                                  "%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)
    handler._taskhub_service = True  # type: ignore[attr-defined]
    package.addHandler(handler)
    package.setLevel(logging.INFO)
    package.propagate = False


def _emit(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False, default=str))


def _perform(function):
    try:
        return function()
    except (HubError, OSError, ValueError, RuntimeError, httpx.HTTPError) as error:
        detail = error.as_dict() if isinstance(error, HubError) else {
            "code": "operator_error", "message": str(error)
        }
        typer.echo(json.dumps({"error": detail}, ensure_ascii=False), err=True)
        raise typer.Exit(1) from error


def _request(settings: Settings, method: str, path: str, **kwargs):
    assert settings.url is not None
    with httpx.Client(
        base_url=settings.url,
        timeout=30,
        trust_env=False,
        follow_redirects=False,
        headers=bearer(settings.root, "taskhub"),
    ) as client:
        response = client.request(method, path, **kwargs)
    if response.is_error:
        try:
            detail = response.json()
        except ValueError:
            detail = {"message": response.text[:2000]}
        raise HubError("api_error", json.dumps(detail, ensure_ascii=False), response.status_code)
    return response.json()


async def _local(settings: Settings, function):
    from .hub import Hub

    # Check the project before creating a lock directory in an unrelated location.
    config = load_config(settings.root)
    with ProjectOwner(settings.root):
        hub = Hub(settings.root, config=config)
        try:
            return await function(hub)
        finally:
            await hub.close()


def _query(ctx: typer.Context, method: str, api_path: str, *args, **kwargs):
    settings: Settings = ctx.obj
    if settings.url:
        return _request(settings, "GET", api_path, params={k: v for k, v in kwargs.items() if v is not None})

    async def read(hub):
        return await getattr(hub, method)(*args, **kwargs)

    return asyncio.run(_local(settings, read))


async def _drive(hub, run_id: str):
    from .git_sync import GitSyncService
    from .runtime import WorkerRunner

    service = GitSyncService(hub)
    sync_task = asyncio.create_task(service.run()) if hub.config.sync.enabled else None
    try:
        result = await WorkerRunner(hub).run_until_idle(run_id)
    finally:
        service.stop()
        if sync_task is not None:
            sync_task.cancel()
            await asyncio.gather(sync_task, return_exceptions=True)
    if hub.config.sync.enabled:
        result["git_sync"] = await service.once()
    return result


def _command(
    ctx: typer.Context,
    name: str,
    payload: dict[str, Any],
    *,
    key: str | None = None,
    drive: bool = False,
):
    settings: Settings = ctx.obj
    command = Command(name=name, key=key or str(uuid4()), payload=payload)
    if settings.url:
        result = _request(
            settings, "POST", "/v1/commands", json=command.model_dump(mode="json")
        )
        if drive:
            run_id = result.get("run_id") or result.get("id", payload.get("run_id"))
            driver = _request(settings, "POST", f"/v1/runs/{run_id}/drive")
            return {"run": result, "driver": driver}
        return result

    async def execute(hub):
        result = await hub.execute(command, Actor(role=Role.OPERATOR))
        if drive:
            run_id = result.get("run_id") or result.get("id", payload.get("run_id"))
            return {"run": result, "driver": await _drive(hub, run_id)}
        return result

    return asyncio.run(_local(settings, execute))


@app.command()
def init(
    ctx: typer.Context,
    directory: Annotated[Path | None, typer.Argument(help="Defaults to --project.")] = None,
    name: str = "FormalGeyser",
    domain: str = "general",
    repository: Annotated[
        bool, typer.Option(help="Enable managed Git review publication; then run repository initialize.")
    ] = False,
    github_sync: Annotated[str | None, typer.Option(
        "--github-sync", help="Enable optional Git sync to OWNER/REPO or an absolute local bare remote.")
    ] = None,
    codex_home: Annotated[Path | None, typer.Option(help="Isolated Codex home; defaults to the selected domain's home when supplied.")] = None,
    from_library: Annotated[Path | None, typer.Option(
        "--from-library", help="Import existing Lean/Markdown source as the initial Library of a Lean instance.")
    ] = None,
    codex_executable: Annotated[str | None, typer.Option(
        "--codex-executable", help="Existing supported native Codex binary for a new Lean instance; no installation.")
    ] = None,
):
    """Create an instance; Lean instances also prepare their pinned shared dependencies."""
    def create():
        root = (directory or ctx.obj.root).resolve()
        load_plugin(domain)
        if from_library is not None and domain != "lean":
            raise ValueError("--from-library requires --domain lean")
        codex_config = CodexConfig(home=str(codex_home) if codex_home is not None else None)
        if domain == "lean":
            from formal_geyser.bootstrap_codex import select_bootstrap_codex

            executable, version = select_bootstrap_codex(codex_executable)
            codex_config = codex_config.model_copy(update={"executable": executable, "version": version})
        elif codex_executable is not None:
            raise ValueError("--codex-executable on init requires --domain lean")
        target = initialize(root, ProjectConfig(name=name, domain=domain,
            repository=RepositoryConfig(enabled=domain == "lean" or repository or github_sync is not None),
            sync=GitSyncConfig(enabled=github_sync is not None, remote=github_sync),
            codex=codex_config))
        result: dict[str, Any] = {"project": str(target), "domain": domain}
        if domain == "lean":
            from formal_geyser.bootstrap import prepare_project

            async def prepare(hub):
                return await prepare_project(hub, from_library=from_library,
                    progress=lambda message: typer.echo(message, err=True))
            try:
                result["initialization"] = asyncio.run(_local(Settings(root), prepare))
            except (HubError, OSError, ValueError, RuntimeError) as error:
                raise ValueError(f"Initialization is not ready: {error}. "
                                 f'Retry with taskctl --project "{root}" prepare; no workers were started.') from error
        elif repository or github_sync is not None:
            result["repository"] = {
                "enabled": True, "initialized": False,
                "next_action": "Run taskctl --project <project> repository initialize.",
            }
        return result

    _emit(_perform(create))


@app.command("prepare")
def prepare_instance(
    ctx: typer.Context,
    from_library: Annotated[Path | None, typer.Option(
        "--from-library", help="Initial source directory, only before the baseline is frozen.")
    ] = None,
):
    """Finish or verify pinned Lean initialization while the local service is stopped."""
    def prepare():
        if ctx.obj.url:
            raise ValueError("Prepare locally while the service is stopped; --url is not supported")
        from formal_geyser.bootstrap import prepare_project

        async def run(hub):
            return await prepare_project(hub, from_library=from_library,
                progress=lambda message: typer.echo(message, err=True))
        return asyncio.run(_local(ctx.obj, run))
    _emit(_perform(prepare))


def _save_config(root: Path, config: ProjectConfig) -> None:
    atomic_write(root / "project.toml", tomli_w.dumps(config.model_dump(exclude_none=True)).encode("utf-8"),
                 prefix=".project-")


@sync_app.command("configure")
def sync_configure(
    ctx: typer.Context,
    remote: str,
    poll_seconds: float = 15.0,
    credential_helper: str | None = None,
):
    """Configure while the local service is stopped; contact no remote and start no run."""
    def configure():
        settings: Settings = ctx.obj
        if settings.url:
            raise ValueError("Stop the service and configure this local project without --url")
        config = load_config(settings.root)
        sync = GitSyncConfig(enabled=True, remote=remote,
                             poll_seconds=poll_seconds, credential_helper=credential_helper)
        values = config.model_dump()
        values["sync"] = sync.model_dump()
        values["repository"]["enabled"] = True
        updated = ProjectConfig.model_validate(values)
        with ProjectOwner(settings.root):
            _save_config(settings.root, updated)
        return {"configured": True, "sync": sync.model_dump(),
                "next_action": "Initialize the managed repository if necessary, then start taskctl serve or sync watch."}
    _emit(_perform(configure))


@sync_app.command("disable")
def sync_disable(ctx: typer.Context):
    """Disable automatic synchronization in a stopped project's configuration."""
    def disable():
        settings: Settings = ctx.obj
        if settings.url:
            raise ValueError("Stop the service and disable synchronization without --url")
        config = load_config(settings.root)
        values = config.model_dump()
        values["sync"]["enabled"] = False
        updated = ProjectConfig.model_validate(values)
        with ProjectOwner(settings.root):
            _save_config(settings.root, updated)
        return {"enabled": False, "history_preserved": True}
    _emit(_perform(disable))


@sync_app.command("status")
def sync_status(ctx: typer.Context):
    def inspect():
        settings: Settings = ctx.obj
        if settings.url:
            return _request(settings, "GET", "/v1/sync")
        from .git_sync import GitSyncService

        return asyncio.run(_local(settings, lambda hub: GitSyncService(hub).status()))
    _emit(_perform(inspect))


@sync_app.command("once")
def sync_once(ctx: typer.Context):
    """Publish reviewed source and public state without starting models or compilers."""
    def synchronize():
        settings: Settings = ctx.obj
        if settings.url:
            return _request(settings, "POST", "/v1/sync", timeout=600)
        from .git_sync import GitSyncService

        return asyncio.run(_local(settings, lambda hub: GitSyncService(hub).once()))
    result = _perform(synchronize)
    _emit(result)
    if result.get("state") == "error":
        raise typer.Exit(1)


@sync_app.command("watch")
def sync_watch(ctx: typer.Context):
    """Own the local project and continuously sync; serve already includes this loop."""
    def watch():
        settings: Settings = ctx.obj
        if settings.url:
            raise ValueError("The configured service already runs its synchronization loop")
        from .git_sync import GitSyncService

        async def follow(hub):
            if not hub.config.sync.enabled:
                raise ValueError("Enable synchronization before starting the watch loop")
            await GitSyncService(hub).run()
        asyncio.run(_local(settings, follow))
    _perform(watch)


@repository_app.command("initialize")
def repository_initialize(
    ctx: typer.Context,
    snapshot: Annotated[
        str | None, typer.Option(help="Existing immutable workspace snapshot SHA; omit for an empty tree.")
    ] = None,
    key: Annotated[str | None, typer.Option(help="Idempotency key for initialization.")] = None,
):
    """Initialize the configured managed repository without starting model work."""
    _emit(_perform(lambda: _command(
        ctx, "repository.initialize", {"snapshot": snapshot} if snapshot is not None else {}, key=key)))


@repository_app.command("status")
def repository_status(ctx: typer.Context):
    """Read managed repository state and publication history."""
    _emit(_perform(lambda: _query(ctx, "repository_status", "/v1/repository")))


@app.command()
def dashboard(ctx: typer.Context, port: Annotated[int, typer.Option(min=1, max=65535)] = 8765):
    """Print the link that signs a browser in to this project's dashboard. It holds the
    project's operator token: open it only in your own browser."""
    token = operator_token(ctx.obj.root, "taskhub")
    if token is None:
        typer.echo(json.dumps({"error": {"code": "operator_error", "message": (
            "The service has no operator token yet; start it with taskctl serve")}}), err=True)
        raise typer.Exit(1)
    typer.echo((ctx.obj.url or f"http://127.0.0.1:{port}") + "/login?token=" + token)


@app.command()
def doctor(ctx: typer.Context):
    """Check configuration, plugin loading, and Codex version without model inference."""
    def inspect():
        root = ctx.obj.root
        checks: dict[str, Any] = {"project": str(root)}
        try:
            config = load_config(root)
            checks["configuration"] = {"ok": True, "domain": config.domain,
                                       "codex_home": config.codex.home,
                                       "sqlite_home": config.codex.home}
        except (OSError, ValueError) as error:
            checks["configuration"] = {"ok": False, "message": str(error)}
            config = ProjectConfig(codex=CodexConfig())
        try:
            load_plugin(config.domain)
            checks["plugin"] = {"ok": True, "domain": config.domain}
        except (OSError, ValueError) as error:
            checks["plugin"] = {"ok": False, "message": str(error)}
        executable = config.codex.executable
        candidate = Path(executable).expanduser()
        if not candidate.is_absolute() and ("/" in executable or "\\" in executable):
            candidate = root / candidate
        from .runtime import resolve_codex_executable

        try:
            resolved = resolve_codex_executable(
                str(candidate) if candidate.is_file() else executable
            )
        except OSError as error:
            checks["codex"] = {"ok": False, "message": str(error)}
        else:
            try:
                result = subprocess.run(
                    [resolved, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    cwd=root if root.exists() else None,
                    check=False,
                )
                reported = (result.stdout + result.stderr).strip()
                match = re.search(r"(?<!\d)(\d+\.\d+\.\d+)(?!\d)", reported)
                checks["codex"] = {
                    "ok": result.returncode == 0 and match is not None
                    and match.group(1) == config.codex.version,
                    "executable": resolved,
                    "expected_version": config.codex.version,
                    "reported": reported[:2000],
                }
            except (OSError, subprocess.SubprocessError) as error:
                checks["codex"] = {"ok": False, "message": str(error)}
        from .native_permissions import INTEROP, wsl_interop

        try:
            checks["process_ownership"] = {"ok": True, "cgroup": str(cgroups.root())}
        except OSError as error:
            checks["process_ownership"] = {"ok": False, "message": str(error)}
        checks["sandbox"] = {"ok": False, "message": INTEROP} if wsl_interop() else {"ok": True}
        checks["ok"] = all(
            value.get("ok", True) for value in checks.values() if isinstance(value, dict)
        )
        return checks

    result = _perform(inspect)
    _emit(result)
    if not result["ok"]:
        raise typer.Exit(1)


@app.command()
def serve(
    ctx: typer.Context,
    host: str = "127.0.0.1",
    port: Annotated[int, typer.Option(min=1, max=65535)] = 8765,
    resume: Annotated[bool, typer.Option(
        "--resume/--no-resume", help="Resume runs that were driven before the service stopped.")] = True,
):
    """Serve the local API. Runs that were being driven resume; new runs start only when driven."""
    def start():
        _service_logging()
        bind_host = "127.0.0.1" if host == "localhost" else host
        if not is_loopback(bind_host):
            raise ValueError("The operator service must bind a loopback IP address")
        load_config(ctx.obj.root)
        cgroups.root()  # Workers' process trees need it: a system without one fails at once.
        raise_open_files_limit()  # Every worker's process holds pipes.
        # The coordination thread runs many short SQLite statements, each releasing the
        # interpreter lock; taking it back can wait a whole switch interval while another
        # thread runs Python (the agents' tool work). At the default 5 ms, a 0.3 s admission
        # pass took 5 to 20 s on a busy service. Measured on the pilot's VM, a 200-statement
        # callback beside three busy threads took 2.2 s at 0.5 ms and 0.84 s at 0.2 ms, while
        # those threads did about a fifth less work.
        sys.setswitchinterval(float(os.environ.get("TASKHUB_SWITCH_INTERVAL", SWITCH_INTERVAL)))
        config = uvicorn.Config(
            create_app(ctx.obj.root, resume_runs=resume, stall_watchdog=True, environment_holds=True),
            host=bind_host,
            port=port,
            workers=1,
            reload=False,
            loop="asyncio",
            access_log=False,
        )
        server = uvicorn.Server(config)
        asyncio.run(server.serve())

    _perform(start)


@tasks_app.command("list")
def tasks_list(ctx: typer.Context):
    _emit(_perform(lambda: _query(ctx, "list_tasks", "/v1/tasks")))


@tasks_app.command("show")
def tasks_show(
    ctx: typer.Context,
    task_id: str,
    revision: Annotated[int | None, typer.Option(min=1, help="An exact historical revision.")] = None,
):
    options = {"revision": revision} if revision is not None else {}
    _emit(_perform(lambda: _query(ctx, "task", f"/v1/tasks/{task_id}", task_id, **options)))


@tasks_app.command("publish")
def tasks_publish(
    ctx: typer.Context,
    contract_file: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
    key: str | None = None,
):
    """Publish a task contract from a JSON file."""
    def publish():
        contract = TaskContract.model_validate_json(contract_file.read_text(encoding="utf-8-sig"))
        domain = load_config(ctx.obj.root).domain
        if contract.domain != domain:
            raise ValueError(f"Task domain {contract.domain!r} does not match project domain {domain!r}")
        load_plugin(domain).validate_contract(contract)
        return _command(ctx, "task.publish", {
            "contract": contract.model_dump(mode="json", exclude_unset=True)}, key=key)

    _emit(_perform(publish))


@workers_app.command("list")
def workers_list(ctx: typer.Context):
    _emit(_perform(lambda: _query(ctx, "list_workers", "/v1/workers")))


@workers_app.command("show")
def workers_show(ctx: typer.Context, worker_id: str):
    _emit(_perform(lambda: _query(ctx, "worker", f"/v1/workers/{worker_id}", worker_id)))


@workers_app.command("subscriptions")
def workers_subscriptions(ctx: typer.Context, worker_id: str):
    """Inspect the tasks and bounded coordination state subscribed to by a worker."""
    _emit(_perform(lambda: _query(
        ctx, "subscriptions", f"/v1/workers/{worker_id}/subscriptions", worker_id)))


@workers_app.command("pause")
def workers_pause(ctx: typer.Context, worker_id: str, run_id: Annotated[str | None, typer.Option()] = None):
    """Pause one worker or reviewer; its running turn is interrupted, others continue."""
    _emit(_perform(lambda: _command(ctx, "worker.pause", {"worker_id": worker_id, "run_id": run_id})))


@workers_app.command("pause-after-turn")
def workers_pause_after_turn(ctx: typer.Context, worker_id: str,
                             run_id: Annotated[str | None, typer.Option()] = None):
    """Pause one worker or reviewer once its running turn ends on its own; others continue."""
    _emit(_perform(lambda: _command(ctx, "worker.pause_after_turn", {"worker_id": worker_id, "run_id": run_id})))


@workers_app.command("resume")
def workers_resume(ctx: typer.Context, worker_id: str, run_id: Annotated[str | None, typer.Option()] = None):
    """Resume one worker or reviewer; in a paused run it continues only its own current work."""
    _emit(_perform(lambda: _command(ctx, "worker.resume", {"worker_id": worker_id, "run_id": run_id}, drive=True)))


@workers_app.command("import-history")
def workers_import_history(
    ctx: typer.Context,
    worker_id: str,
    task: Annotated[list[str], typer.Option("--task", help="A task title, most recent first; repeat for each.")],
):
    """Record the tasks a worker carried over from another project executed there, so that
    assignment decisions see them as its previous tasks."""
    _emit(_perform(lambda: _command(ctx, "worker.import_history", {"worker_id": worker_id, "tasks": task})))


@workers_app.command("rotation")
def workers_rotation(
    ctx: typer.Context,
    worker_id: str,
    mode: Annotated[str, typer.Argument(help="conservative, aggressive, or project to follow the project mode")],
):
    """Choose when this worker's conversation is replaced by a new one that continues its task."""
    _emit(_perform(lambda: _command(ctx, "worker.set_rotation_mode", {
        "worker_id": worker_id, "mode": None if mode == "project" else mode})))


@workers_app.command("recover")
def workers_recover(
    ctx: typer.Context,
    worker_id: str,
    reason: Annotated[str, typer.Option(help="Evidence that the previous owned processes stopped.")],
    confirm_stopped: Annotated[
        bool,
        typer.Option(
            "--confirm-stopped",
            help="Confirm that you checked the old owned process tree and it is stopped.",
        ),
    ] = False,
):
    """Recover a crash-held worker only after its old owned processes have stopped."""
    if not confirm_stopped:
        raise typer.BadParameter(
            "Check that the old owned processes have stopped, then pass --confirm-stopped",
            param_hint="--confirm-stopped",
        )
    if not reason.strip():
        raise typer.BadParameter("A nonblank recovery reason is required", param_hint="--reason")
    _emit(_perform(lambda: _command(ctx, "worker.recover", {
        "worker_id": worker_id,
        "process_confirmed_stopped": True,
        "reason": reason,
    })))


@continuations_app.command("list")
def continuations_list(ctx: typer.Context):
    _emit(_perform(lambda: _query(ctx, "list_continuations", "/v1/continuations")))


@artifacts_app.command("list")
def artifacts_list(ctx: typer.Context, kind: str | None = None):
    """List mathematical artifacts, including accepted and withdrawn archives."""
    def records():
        records = _query(ctx, "list_math_artifacts", "/v1/mathematical-artifacts")
        return [record for record in records if kind is None or record["kind"] == kind]
    _emit(_perform(records))


@artifacts_app.command("show")
def artifacts_show(ctx: typer.Context, artifact_id: str):
    _emit(_perform(lambda: _query(ctx, "native_record", f"/v1/records/{artifact_id}",
                                  artifact_id, Actor(role=Role.OPERATOR))))


@records_app.command("list")
def records_list(ctx: typer.Context, kind: str | None = None):
    """List all stored records, including historical administrative/software records."""
    _emit(_perform(lambda: _query(ctx, "list_artifacts", "/v1/records", kind=kind)))


@records_app.command("show")
def records_show(ctx: typer.Context, record_id: str):
    _emit(_perform(lambda: _query(ctx, "native_record", f"/v1/records/{record_id}",
                                  record_id, Actor(role=Role.OPERATOR))))


@notes_app.command("list")
def notes_list(ctx: typer.Context, kind: str | None = None):
    _emit(_perform(lambda: _query(ctx, "list_notes", "/v1/notes", kind=kind)))


@notes_app.command("show")
def notes_show(ctx: typer.Context, record_id: str):
    _emit(_perform(lambda: _query(ctx, "native_record", f"/v1/notes/{record_id}",
                                  record_id, Actor(role=Role.OPERATOR))))


@library_app.command("show")
def library_show(ctx: typer.Context):
    _emit(_perform(lambda: _query(ctx, "library_status", "/v1/library")))


@wiki_app.command("show")
def wiki_show(ctx: typer.Context):
    """Show public knowledge files and their pinned public Library commit."""
    def view():
        status = _query(ctx, "library_status", "/v1/library")
        return {key: value for key, value in status.items() if key != "files"}
    _emit(_perform(view))


@talk_app.command("list")
def talk_list(
    ctx: typer.Context, worker_id: Annotated[str, typer.Option(help="Conversation participant.")],
    after_id: str = "", limit: Annotated[int, typer.Option(min=1, max=100)] = 50,
    max_bytes: Annotated[int, typer.Option(min=512, max=65536)] = 8192,
):
    _emit(_perform(lambda: _query(ctx, "peer_conversations", "/v1/talk/conversations",
        Actor(role=Role.OPERATOR), worker_id=worker_id, after_id=after_id,
        limit=limit, max_bytes=max_bytes)))


@talk_app.command("history")
def talk_history(
    ctx: typer.Context, conversation_id: str,
    worker_id: Annotated[str, typer.Option(help="A participant in this conversation.")],
    after_sequence: Annotated[int, typer.Option(min=0)] = 0,
    offset: Annotated[int, typer.Option(min=0)] = 0,
    limit: Annotated[int, typer.Option(min=1, max=100)] = 50,
    max_bytes: Annotated[int, typer.Option(min=512, max=65536)] = 8192,
):
    _emit(_perform(lambda: _query(ctx, "peer_history",
        f"/v1/talk/conversations/{conversation_id}", conversation_id, Actor(role=Role.OPERATOR),
        worker_id=worker_id, after_sequence=after_sequence, offset=offset,
        limit=limit, max_bytes=max_bytes)))


@talk_app.command("inbox")
def talk_inbox(
    ctx: typer.Context, worker_id: Annotated[str, typer.Option(help="Recipient to inspect.")],
    limit: Annotated[int, typer.Option(min=1, max=100)] = 50,
    max_bytes: Annotated[int, typer.Option(min=512, max=65536)] = 8192,
):
    """Inspect unread text without acknowledging it or starting the recipient."""
    _emit(_perform(lambda: _query(ctx, "peer_unread", "/v1/talk/inbox",
        Actor(role=Role.OPERATOR), worker_id=worker_id, limit=limit, max_bytes=max_bytes)))


@artifacts_app.command("accept")
def artifacts_accept(
    ctx: typer.Context,
    artifact_id: str,
    policy: Annotated[str, typer.Option(help="Exact policy identifier.")],
    reason: Annotated[str, typer.Option(help="Operator review evidence.")],
):
    report = ValidationReport(
        verdict="accepted", policy=policy, details={"operator_review": reason},
        qualifications=[],
    )
    _emit(_perform(lambda: _command(ctx, "artifact.accept", {
        "artifact_id": artifact_id, **report.model_dump(mode="json")
    })))


@artifacts_app.command("withdraw")
def artifacts_withdraw(
    ctx: typer.Context,
    artifact_id: str,
    reason: Annotated[str, typer.Option(help="Reason retained with the withdrawal history.")],
):
    """Withdraw an artifact with an explicit reason, preserving its history."""
    if not reason.strip():
        raise typer.BadParameter("A nonblank withdrawal reason is required", param_hint="--reason")
    _emit(_perform(lambda: _command(ctx, "artifact.withdraw", {
        "artifact_id": artifact_id, "reason": reason,
    })))


@app.command()
def events(ctx: typer.Context, after: Annotated[int, typer.Option(min=0)] = 0):
    _emit(_perform(lambda: _query(ctx, "events", "/v1/events", after=after)))


@run_app.command("start")
def run_start(
    ctx: typer.Context,
    key: str | None = None,
):
    """Create and drive eligible work with task-local admission and user controls."""
    _emit(_perform(lambda: _command(ctx, "run.start", {}, key=key, drive=True)))


@run_app.command("pause")
def run_pause(ctx: typer.Context, run_id: str):
    """Pause everyone now; running turns are interrupted."""
    _emit(_perform(lambda: _command(ctx, "run.pause", {"run_id": run_id})))


@run_app.command("pause-after-turn")
def run_pause_after_turn(ctx: typer.Context, run_id: str):
    """Pause everyone once their running turns end on their own; nothing new starts."""
    _emit(_perform(lambda: _command(ctx, "run.pause_after_turn", {"run_id": run_id})))


@run_app.command("resume")
def run_resume(ctx: typer.Context, run_id: str):
    _emit(_perform(lambda: _command(ctx, "run.resume", {"run_id": run_id}, drive=True)))


@run_app.command("pause-group-after-turn")
def run_pause_group_after_turn(ctx: typer.Context, group: str,
                               run_id: Annotated[str | None, typer.Option()] = None):
    """Pause a group once its members' running turns end on their own; nothing new starts for
    them. GROUP is codex, codex.workers, codex.reviewers or claude."""
    _emit(_perform(lambda: _command(ctx, "group.pause_after_turn", {"group": group, "run_id": run_id})))


@run_app.command("resume-group")
def run_resume_group(ctx: typer.Context, group: str, run_id: Annotated[str | None, typer.Option()] = None):
    """Resume a group, also while everyone else is paused. GROUP is codex, codex.workers,
    codex.reviewers or claude."""
    _emit(_perform(lambda: _command(ctx, "group.resume", {"group": group, "run_id": run_id}, drive=True)))


@run_app.command("stop")
def run_stop(ctx: typer.Context, run_id: str):
    _emit(_perform(lambda: _command(ctx, "run.stop", {"run_id": run_id})))


@run_app.command("reconcile-usage")
def run_reconcile_usage(
    ctx: typer.Context,
    run_id: str,
    tokens_used: Annotated[
        int, typer.Option(min=0, help="Reconciled cumulative tokens for this run.")
    ],
    reason: Annotated[str, typer.Option(help="External usage reconciliation evidence.")],
    backend: Annotated[
        str, typer.Option(help="The account that spent the added tokens: codex or claude.")
    ] = "codex",
):
    """Record operator-reviewed usage; execution still requires an explicit resume."""
    if not reason.strip():
        raise typer.BadParameter("A nonblank reconciliation reason is required", param_hint="--reason")
    _emit(_perform(lambda: _command(ctx, "run.reconcile_usage", {
        "run_id": run_id, "tokens_used": tokens_used, "reason": reason, "backend": backend,
    })))


@app.command("cpu-profile")
def cpu_profile(
    ctx: typer.Context,
    seconds: Annotated[float, typer.Option(min=1, max=300, help="How long to sample.")] = 60.0,
):
    """Where the running service spends CPU, by thread and code (needs --url)."""
    def read():
        if not ctx.obj.url:
            raise ValueError("Profile the running service: pass --url")
        return _request(ctx.obj, "GET", "/v1/diagnostics/cpu", params={"seconds": seconds}, timeout=seconds + 60)
    _emit(_perform(read))


@backend_app.command("show")
def backend_show(ctx: typer.Context):
    """The backend for workers created next and the assignment plan, each backend's workers, live
    sessions, usage, and account hold, and the decision model."""
    _emit(_perform(lambda: _query(ctx, "backend_status", "/v1/runtime/backend")))


@backend_app.command("set")
def backend_set(
    ctx: typer.Context,
    backend: Annotated[str, typer.Argument(help="codex or claude")],
):
    """Use this backend for workers created from now on, where the assignment plan names none;
    existing workers keep theirs."""
    _emit(_perform(lambda: _command(ctx, "runtime.select_backend", {"backend": backend})))


@backend_app.command("rotation-summary")
def backend_rotation_summary(ctx: typer.Context):
    """Rotations by reason, mode and handoff outcome, and per mode the first-request uncached tokens
    and largest context of turns and conversations, and the credit units per turn and per review."""
    _emit(_perform(lambda: _query(ctx, "rotation_summary", "/v1/runtime/rotation")))


@backend_app.command("rotation-memory")
def backend_rotation_memory(ctx: typer.Context):
    """Whether new conversations after rotations kept the old ones' working memory: per rotation
    and by mode, compared with Test 2's figures after compaction."""
    _emit(_perform(lambda: _query(ctx, "rotation_memory_report", "/v1/runtime/rotation/memory")))


@backend_app.command("rotation")
def backend_rotation(
    ctx: typer.Context,
    mode: Annotated[str, typer.Argument(help="conservative (rotate after idle time) or aggressive "
                                             "(also when the context grows large)")],
):
    """Choose when conversations are replaced by new ones that continue the same task, from now on."""
    _emit(_perform(lambda: _command(ctx, "runtime.set_rotation_mode", {"mode": mode})))


@backend_app.command("waker-model")
def backend_waker_model(
    ctx: typer.Context,
    model: Annotated[str, typer.Argument(help="Codex model for wake decisions")],
    effort: Annotated[str, typer.Option(help="low, medium, high, xhigh or max")],
):
    """Use this model for the wake decisions started from now on."""
    _emit(_perform(lambda: _command(ctx, "runtime.set_waker_model", {"model": model, "effort": effort})))


@backend_app.command("decision-model")
def backend_decision_model(
    ctx: typer.Context,
    model: Annotated[str, typer.Argument(help="Codex model for assignment and recruitment decisions")],
    effort: Annotated[str, typer.Option(help="low, medium, high, xhigh or max")],
):
    """Use this cheap model for the assignment and recruitment decisions started from now on."""
    _emit(_perform(lambda: _command(ctx, "runtime.set_decision_model", {"model": model, "effort": effort})))


@run_app.command("list")
def run_list(ctx: typer.Context):
    _emit(_perform(lambda: _query(ctx, "list_runs", "/v1/runs")))


@run_app.command("show")
def run_show(ctx: typer.Context, run_id: str):
    _emit(_perform(lambda: _query(ctx, "run", f"/v1/runs/{run_id}", run_id)))


def _copy_tree(source: Path, target: Path) -> None:
    if source.is_symlink():
        raise ValueError(f"Cannot include a linked path in a backup: {source}")
    if source.is_dir():
        target.mkdir(exist_ok=False)
        for item in source.iterdir():
            _copy_tree(item, target / item.name)
    else:
        shutil.copy2(source, target)


@app.command()
def backup(
    ctx: typer.Context,
    destination: Annotated[Path, typer.Argument(help="A new backup directory.")],
):
    """Back up authoritative native files, the ledger, and private local project state."""
    def create():
        if ctx.obj.url:
            raise ValueError("Backups require local ownership; stop the service and omit --url")
        target = destination.resolve()

        async def copy(hub):
            if target.exists():
                raise FileExistsError(f"Backup destination already exists: {target}")
            native_roots = [hub.root / name for name in (
                "tasks", "notes", "staging", "artifacts", "submission-history", "library",
                "wiki", "literature", "correspondence", "drafts", "graphs")]
            if any(target.is_relative_to(path.resolve()) for path in [hub.state_dir, *native_roots]):
                raise ValueError("The backup destination may not be inside project state or native content")
            await hub.reconcile_peer_talk()
            target.mkdir(parents=True)
            shutil.copy2(hub.root / "project.toml", target / "project.toml")
            state = target / ".task-based"
            state.mkdir()
            database = hub.db.path
            await hub.db.backup(state / database.name)
            excluded = {database.name, database.name + "-wal", database.name + "-shm", "hub.lock"}
            for item in hub.state_dir.iterdir():
                if item.name not in excluded:
                    _copy_tree(item, state / item.name)
            for directory in native_roots:
                if directory.exists():
                    _copy_tree(directory, target / directory.name)
            return {"backup": str(target)}

        return asyncio.run(_local(ctx.obj, copy))

    _emit(_perform(create))
