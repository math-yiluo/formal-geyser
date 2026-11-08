from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Role, TaskContract

from .application import LeanApplication
from .lean_service import client as lean_service
from .plugin import LeanPlugin
from .registry import (
    EnvironmentUnavailable,
    ObjectRegistry,
    file_sha256,
    library_manifest,
    read_snapshot,
    record_json,
    snapshot_sources,
    source_directory,
)
from .schemas import (
    OBJECT_TYPES,
    EnvironmentRecord,
    ExecutionLimits,
    LibraryRoot,
)
from .tooling import lean_version

app = typer.Typer(no_args_is_help=True, help="Self-contained Lean records and verification tools.")
registry_app = typer.Typer(no_args_is_help=True)
source_app = typer.Typer(no_args_is_help=True)
environment_app = typer.Typer(no_args_is_help=True)
service_app = typer.Typer(no_args_is_help=True, help="The project's Lean service, which runs every Lean process.")
app.add_typer(registry_app, name="registry")
app.add_typer(source_app, name="source")
app.add_typer(environment_app, name="environment")
app.add_typer(service_app, name="lean-service")


@app.callback()
def main(ctx: typer.Context,
         project: Annotated[Path, typer.Option("--project", "-p")] = Path(".")):
    ctx.obj = project.resolve()


def _emit(value):
    typer.echo(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False, default=str))


def _run(operation):
    try:
        return asyncio.run(operation)
    except (HubError, OSError, ValueError, RuntimeError) as error:
        _emit({"error": type(error).__name__, "message": str(error)})
        raise typer.Exit(1) from error


async def _project(root: Path, function):
    from formal_geyser.coordination.api import ProjectOwner
    from formal_geyser.coordination.config import load_config
    from formal_geyser.coordination.hub import Hub
    config = load_config(root)
    if config.domain != "lean":
        raise ValueError("This project must select domain='lean'")
    with ProjectOwner(root):
        hub = Hub(root, config=config)
        try:
            return await function(hub)
        finally:
            await hub.close()


def _operator():
    return Actor(role=Role.OPERATOR)


@app.command()
def schema(object_type: Annotated[str, typer.Argument(help="One of the current native object types.")]):
    if object_type not in OBJECT_TYPES:
        raise typer.BadParameter("Choose: " + ", ".join(OBJECT_TYPES))
    _emit(OBJECT_TYPES[object_type].model_json_schema())


@app.command()
def doctor(lean: Annotated[Path | None, typer.Option(help="Resolved Lean executable path.")] = None):
    """Probe executable/version only; no project, model call, or mathematical task is started."""
    async def probe():
        found = str(lean) if lean else shutil.which("lean")
        if not found:
            return {"ok": False, "reason": "Lean executable was not found", "proof_accepted": False}
        executable = Path(found).resolve()
        if not executable.is_file():
            return {"ok": False, "reason": "Lean executable was not found", "proof_accepted": False}
        try:
            version = await asyncio.to_thread(lean_version, executable)
        except (EnvironmentUnavailable, OSError, ValueError) as error:
            return {"ok": False, "executable": str(executable), "reason": str(error), "proof_accepted": False}
        return {"ok": True, "executable": str(executable), "executable_sha256": file_sha256(executable),
                "version": version, "proof_accepted": False}
    result = _run(probe())
    _emit(result)
    if not result["ok"]:
        raise typer.Exit(1)


@registry_app.command("put")
def registry_put(ctx: typer.Context, record: Path):
    """Register operator environment configuration without approving it."""
    async def perform(hub):
        value = record_json(record.read_text(encoding="utf-8-sig"))
        return await ObjectRegistry(hub).stage(value, _operator())
    _emit(_run(_project(ctx.obj, perform)))


@registry_app.command("show")
def registry_show(ctx: typer.Context, artifact_id: str):
    async def perform(hub):
        return await ObjectRegistry(hub).inspect(artifact_id)
    _emit(_run(_project(ctx.obj, perform)))


@registry_app.command("list")
def registry_list(ctx: typer.Context, object_type: str | None = None):
    if object_type is not None and object_type not in OBJECT_TYPES:
        raise typer.BadParameter("Unknown object type")
    _emit(_run(_project(ctx.obj, lambda hub: ObjectRegistry(hub).list(object_type))))


@source_app.command("import")
def source_import(ctx: typer.Context, path: Path, name: str | None = None,
                  version: int = 1, origin: str | None = None, source_id: str | None = None,
                  copy: bool = True):
    """Import/register a readable native literature file and exact citation pointer."""
    async def perform(hub):
        return await ObjectRegistry(hub).import_source(
            path, _operator(), name=name, version=version, origin=origin, source_id=source_id, copy=copy,
        )
    _emit(_run(_project(ctx.obj, perform)))


@environment_app.command("capture")
def environment_capture(
    ctx: typer.Context, lean: Annotated[Path, typer.Option()],
    name: str = "Lean environment",
    library: Annotated[list[Path] | None, typer.Option(help="A prebuilt library root to hash in full.")] = None,
    local_execution: Annotated[bool, typer.Option(help="Permit local development execution, including worker tools, outside the Codex sandbox. Trusted source only.")] = False,
    timeout_seconds: Annotated[float | None, typer.Option(help="Optional explicit check timeout in seconds; omitted means unlimited.")] = None,
):
    """Capture a pinned environment as a staged record; operator approval is separate."""
    async def perform(hub):
        executable = lean.resolve()
        # Before an environment is registered nothing compiles, so its version is asked here.
        version = await asyncio.to_thread(lean_version, executable)
        libraries = [LibraryRoot(path=str(path.resolve()),
                                 manifest_sha256=hub.blobs.put_json(library_manifest(path)))
                     for path in (library or [])]
        record = EnvironmentRecord(
            name=name, lean_executable=str(executable),
            lean_executable_sha256=file_sha256(executable),
            lake_executable_sha256=file_sha256(executable.with_name("lake" + executable.suffix)), lean_version=version,
            library_roots=libraries, local_execution="trusted-code-only" if local_execution else "disabled",
            limits=ExecutionLimits(timeout_seconds=timeout_seconds),
        )
        return await ObjectRegistry(hub).stage(record, _operator())
    _emit(_run(_project(ctx.obj, perform)))


@app.command()
def snapshot(ctx: typer.Context, directory: Path):
    """Capture supplied files for operator inspection or reproducible development checks."""
    async def perform(hub):
        if not directory.is_dir():
            raise ValueError("Snapshot source must be a directory")
        sha = snapshot_sources(hub, directory)
        entries = read_snapshot(hub.blobs, sha)
        return {"snapshot": sha, "files": entries}
    _emit(_run(_project(ctx.obj, perform)))


@app.command()
def publish(ctx: typer.Context, contract: Path,
            key: Annotated[str | None, typer.Option(help="Explicit core idempotency key.")] = None):
    """Publish one externally supplied contract. No task or phase graph is generated."""
    async def perform(hub):
        value = TaskContract.model_validate(json.loads(contract.read_text(encoding="utf-8-sig")))
        return await LeanApplication(hub).publish(value, _operator(), key=key or str(uuid4()))
    _emit(_run(_project(ctx.obj, perform)))


@app.command()
def check(
    ctx: typer.Context, directory: Path,
    environment: Annotated[str, typer.Option(help="Approved immutable environment artifact ID.")],
    module: Annotated[list[str], typer.Option(help="Module names in dependency-first compilation order.")],
    declaration: Annotated[str | None, typer.Option(help="Inspect this declaration in the final module.")] = None,
):
    """Compile the Lean files of a directory where they are. The result is always a development check."""
    async def perform(hub):
        record, _ = await ObjectRegistry(hub).get(environment, "environment", approved=True)
        checked = await lean_service.check(
            hub.root, record, hub.blobs, directory=source_directory(hub, directory), modules=module,
            kernel_replay=False, inspections=[{"module": module[-1], "declaration": declaration}] if declaration else [],
            lane="interactive", owner="operator", label="formalizectl check " + ", ".join(module[:3]))
        if not checked["doctor"]["ok"]:
            return {"ok": False, "doctor": checked["doctor"], "proof_accepted": False}
        result = {**checked["compilation"], "sources_sha256": checked["sources_sha256"]}
        if checked["inspections"]:
            result["inspection"] = checked["inspections"][0]
            result["ok"] = result["inspection"]["ok"]
        return result
    result = _run(_project(ctx.obj, perform))
    _emit(result)
    if not result["ok"]:
        raise typer.Exit(1)


@app.command()
def verify(ctx: typer.Context, task_id: str, artifact_id: str):
    """Return a validation report. Only a core validator/operator action can issue a receipt."""
    async def perform(hub):
        task = await hub.task(task_id)
        artifact = await hub.artifact(artifact_id)
        with lean_service.priority(blocking=False):  # No review or publication waits for it.
            report = await LeanPlugin().validate_candidate(hub, task, artifact)
        return report.model_dump(mode="json")
    result = _run(_project(ctx.obj, perform))
    _emit(result)
    if result["verdict"] != "accepted":
        raise typer.Exit(1)




def _service(ctx: typer.Context, command: str, hours: float = 24.0) -> None:
    from .lean_service.control import control

    _emit(_run(control(ctx.obj, command, hours=hours)))


@service_app.command("start")
def service_start(ctx: typer.Context):
    """Start the project's Lean service, if it is not running; TaskHub also starts it when needed."""
    _service(ctx, "start")


@service_app.command("stop")
def service_stop(ctx: typer.Context):
    """Stop it; running checks are queued again and continue when it next starts."""
    _service(ctx, "stop")


@service_app.command("restart")
def service_restart(ctx: typer.Context):
    """Start it again, for example to load a new version; its jobs continue."""
    _service(ctx, "restart")


@service_app.command("status")
def service_status(ctx: typer.Context):
    """Its jobs, by queue position, and the machine's threads and memory."""
    _service(ctx, "status")


@service_app.command("telemetry")
def service_telemetry(ctx: typer.Context, hours: Annotated[float, typer.Option(help="The period to cover.")] = 24.0):
    """Per lane: jobs, queue and running time, CPU time, peak memory; the machine's load."""
    _service(ctx, "telemetry", hours)


@app.command("revalidate")
def revalidate_candidate(ctx: typer.Context, artifact_id: str, record: bool = False):
    """Recheck original staged bytes; --record appends fresh machine evidence."""
    from .maintenance import run
    result = _run(run(ctx.obj, artifact_id, record=record))
    _emit(result)
    if result["report"]["verdict"] != "accepted" or result["report"]["qualifications"]:
        raise typer.Exit(1)


@app.command("restore-library-copies")
def restore_library_copies(ctx: typer.Context):
    """With the service stopped, write back every resting worker's Library copy: required before
    running a version older than Library copies at rest."""
    from .maintenance import restore_library_copies as restore
    _emit(_run(restore(ctx.obj)))


if __name__ == "__main__":
    app()
