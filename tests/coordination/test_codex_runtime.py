from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from formal_geyser.coordination.models import CodexConfig
from formal_geyser.coordination.runtime.codex import (
    CodexHarness,
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
)
from formal_geyser.coordination.runtime.process import OwnedProcessHost
from formal_geyser.coordination.runtime.rpc import (
    JsonRpcClient,
    RpcClosed,
    RpcProtocolError,
    RpcTimeout,
)
from formal_geyser.coordination.runtime.tools import dynamic_tools, operation_tools
from tests.coordination.gateway_helpers import file_arguments
from tests.coordination.lifecycle_helpers import finish_ordinary, select_review
from tests.coordination.publication_helpers import (
    file_payload,
    new_hub,
    publish_batch,
    source_payload,
)


async def start_rpc(tmp_path: Path, script: str, **kwargs):
    path = tmp_path / "peer.py"
    path.write_text(script, encoding="utf-8")
    host = await OwnedProcessHost.start(
        [sys.executable, "-u", str(path)], cwd=tmp_path,
        max_frame_bytes=kwargs.get("max_frame_bytes", 8 * 1024 * 1024),
    )
    return host, JsonRpcClient(host, **kwargs)


async def close_rpc(host, rpc):
    try:
        await host.stop(grace=0.2)
    finally:
        await rpc.close()


async def test_routes_out_of_order_ids_while_draining_large_stderr(tmp_path):
    host, rpc = await start_rpc(tmp_path, """
import json,sys
sys.stderr.write('x'*300000);sys.stderr.flush()
a=json.loads(sys.stdin.readline());b=json.loads(sys.stdin.readline())
print(json.dumps({'id':b['id'],'result':b['params']}),flush=True)
print(json.dumps({'id':a['id'],'result':a['params']}),flush=True)
sys.stdin.read()
""", stderr_bytes=2048)
    try:
        assert await asyncio.gather(
            rpc.request("first", {"value": 1}), rpc.request("second", {"value": 2})
        ) == [{"value": 1}, {"value": 2}]
        assert len(rpc.stderr_tail) <= 2048
    finally:
        await close_rpc(host, rpc)


async def test_server_request_can_make_nested_client_request(tmp_path):
    rpc = None

    async def handle(method, params):
        assert method == "ask"
        return {"nested": await rpc.request("nested")}

    host, rpc = await start_rpc(tmp_path, """
import json,sys
first=json.loads(sys.stdin.readline())
print(json.dumps({'id':'server-1','method':'ask','params':{}}),flush=True)
nested=json.loads(sys.stdin.readline())
print(json.dumps({'id':nested['id'],'result':'okay'}),flush=True)
reply=json.loads(sys.stdin.readline())
print(json.dumps({'id':first['id'],'result':reply['result']}),flush=True)
sys.stdin.read()
""", request_handler=handle)
    try:
        assert await rpc.request("first") == {"nested": "okay"}
    finally:
        await close_rpc(host, rpc)


async def test_timeout_does_not_retry_and_late_response_does_not_misroute(tmp_path):
    host, rpc = await start_rpc(tmp_path, """
import json,sys,time
first=json.loads(sys.stdin.readline());time.sleep(.15)
print(json.dumps({'id':first['id'],'result':'late'}),flush=True)
second=json.loads(sys.stdin.readline())
print(json.dumps({'id':second['id'],'result':second['method']}),flush=True)
sys.stdin.read()
""")
    try:
        with pytest.raises(RpcTimeout):
            await rpc.request("once", timeout=0.03)
        assert await rpc.request("second") == "second"
        assert rpc._next_id == 2
        assert not rpc._pending
    finally:
        await close_rpc(host, rpc)


async def test_cancelled_request_is_retired_without_breaking_transport(tmp_path):
    host, rpc = await start_rpc(tmp_path, """
import json,sys,time
for line in sys.stdin:
    request=json.loads(line);time.sleep(.1)
    print(json.dumps({'id':request['id'],'result':request['method']}),flush=True)
""")
    try:
        request = asyncio.create_task(rpc.request("cancel"))
        await asyncio.sleep(0.03)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert await rpc.request("next") == "next"
    finally:
        await close_rpc(host, rpc)


@pytest.mark.parametrize("output", ["'x'*2048+'\\n'", "'{invalid}\\n'", "'{}'"])
async def test_rejects_oversized_malformed_or_truncated_frames(tmp_path, output):
    host, rpc = await start_rpc(
        tmp_path, f"import sys\nsys.stdout.write({output});sys.stdout.flush()\n",
        max_frame_bytes=1024,
    )
    try:
        failure = await asyncio.wait_for(rpc.wait_failure(), 3)
        assert isinstance(failure, RpcProtocolError)
    finally:
        await close_rpc(host, rpc)


async def test_eof_fails_all_pending_requests(tmp_path):
    host, rpc = await start_rpc(tmp_path, "import sys\nsys.stdin.readline()\n")
    try:
        with pytest.raises(RpcClosed):
            await rpc.request("never-answered")
    finally:
        await close_rpc(host, rpc)


def test_tool_schemas_resolve_nested_model_definitions():
    tools = {tool["name"]: tool for tool in operation_tools()}
    assert all(tool["type"] == "function" for tool in tools.values())
    Draft202012Validator(tools["taskhub_pause"]["inputSchema"]).validate({
        "reason": "Wait for the exact source input.",
        "predicate": {"mode": "all", "subscriptions": [{"task_id": "t", "need": "the exact source input"}]},
        "checkpoint": {"summary": "Waiting for input", "next_action": "Use the result"},
    })


FAKE_CODEX = """
import json,sys
from pathlib import Path
log=Path(sys.argv[1])
replied=False
def emit(value):
    print(json.dumps(value),flush=True)
for line in sys.stdin:
    msg=json.loads(line)
    with log.open('a',encoding='utf-8') as f:
        f.write(json.dumps(msg)+'\\n')
    method=msg.get('method')
    if method=='initialize':
        emit({'id':msg['id'],'result':{'userAgent':'Codex Desktop/0.160.0'}})
    elif method in ('thread/start','thread/resume'):
        emit({'id':msg['id'],'result':{'thread':{'id':msg['params'].get('threadId','original-thread')},'model':'scripted-model'}})
    elif method=='turn/start':
        emit({'id':msg['id'],'result':{'turn':{'id':'turn-1','status':'inProgress'}}})
        emit({'method':'thread/tokenUsage/updated','params':{'threadId':'original-thread','turnId':'turn-1','tokenUsage':{'total':{'totalTokens':10,'inputTokens':8,'cachedInputTokens':2,'outputTokens':2,'reasoningOutputTokens':1}}}})
        emit({'id':'call-request','method':'item/tool/call','params':{'threadId':'original-thread','turnId':'turn-1','callId':'call-1','tool':'taskhub_wait','arguments':{}}})
    elif msg.get('id')=='call-request' and 'result' in msg:
        assert msg['result']['contentItems'][0]['type']=='inputText'
        assert msg['result']['success'] is True
        replied=True
    elif method=='turn/interrupt':
        assert replied, 'interrupted before tool reply'
        emit({'id':msg['id'],'result':{}})
        emit({'method':'turn/completed','params':{'threadId':'original-thread','turn':{'id':'turn-1','status':'interrupted'}}})
"""


@pytest.mark.parametrize(("tool_name", "directive"), [
    ("taskhub_wait", "drain_for_wait"),
    ("taskhub_complete", "drain_for_submission"),
    ("taskhub_release", "drain_for_release"),
    ("taskhub_review", "drain_for_review"),
])
async def test_dynamic_drain_replies_then_interrupts_and_confirms_process_quiet(
    tmp_path, tool_name, directive,
):
    script = tmp_path / "codex.py"
    script.write_text(FAKE_CODEX.replace("'tool':'taskhub_wait'", "'tool':" + repr(tool_name)),
                      encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.5),
        command=[sys.executable, "-u", str(script), str(log)],
    )
    session = await harness.open(
        workspace=tmp_path, thread_id=None, tools=dynamic_tools(), instructions="test")
    usages, starts = [], []

    async def tool(name, args, call_id):
        assert name == tool_name
        assert call_id == "call-1"
        return {"runtime_directive": directive, "continuation_id": "continuation-1"}

    async def usage(sample):
        usages.append(sample)

    async def started(turn_id):
        starts.append(turn_id)

    outcome = await harness.run_turn(
        "test", on_tool=tool, on_usage=usage, on_started=started)
    assert session.thread_id == "original-thread"
    assert outcome.status == "interrupted"
    assert outcome.terminal_observed and outcome.process_quiet
    assert outcome.directive == directive
    assert usages[0]["totalTokens"] == 10
    assert starts == ["turn-1"]
    messages = [json.loads(line) for line in log.read_text().splitlines()]
    reply_index = next(i for i, message in enumerate(messages)
                       if message.get("id") == "call-request" and "result" in message)
    stop_index = next(i for i, message in enumerate(messages)
                      if message.get("method") == "turn/interrupt")
    assert reply_index < stop_index
    initialize = messages[0]
    assert initialize["params"]["capabilities"]["experimentalApi"] is True
    assert messages[1]["method"] == "initialized"
    start = next(m for m in messages if m.get("method") == "thread/start")
    assert start["params"]["permissions"] == "taskhub-worker" and "sandbox" not in start["params"]
    assert start["params"]["approvalPolicy"] == "never"
    assert "multiAgentMode" not in start["params"]


async def test_resume_preserves_original_thread_and_omits_dynamic_tools(tmp_path):
    script = tmp_path / "codex.py"
    script.write_text(FAKE_CODEX, encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.2),
        command=[sys.executable, "-u", str(script), str(log)],
    )
    try:
        session = await harness.open(
            workspace=tmp_path, thread_id="original-thread",
            tools=dynamic_tools(), instructions="test")
        assert session.thread_id == "original-thread"
    finally:
        await harness.close()
    messages = [json.loads(line) for line in log.read_text().splitlines()]
    assert not any(m.get("method") == "thread/start" for m in messages)
    resume = next(m for m in messages if m.get("method") == "thread/resume")
    assert "dynamicTools" not in resume["params"]


async def test_ambiguous_turn_start_stops_tree_and_never_retries(tmp_path):
    script = tmp_path / "silent_codex.py"
    script.write_text("""
import json,sys,time
from pathlib import Path
for line in sys.stdin:
    m=json.loads(line)
    if m.get('method')=='initialize':
        print(json.dumps({'id':m['id'],'result':{'userAgent':'Codex Desktop/0.160.0'}}),flush=True)
    elif m.get('method')=='thread/start':
        print(json.dumps({'id':m['id'],'result':{'thread':{'id':'t'},'model':'fake'}}),flush=True)
    elif m.get('method')=='turn/start':
        Path('turn-started').write_text('once')
        time.sleep(120)
""", encoding="utf-8")
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.1),
        command=[sys.executable, "-u", str(script)],
    )
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="test")

    async def unused(*args):
        pass

    running = asyncio.create_task(harness.run_turn(
        "test", on_tool=unused, on_usage=unused, on_started=unused))
    for _ in range(100):
        if (tmp_path / "turn-started").exists():
            break
        await asyncio.sleep(0.02)
    assert not running.done()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert harness.process_quiet
    assert (tmp_path / "turn-started").read_text() == "once"


async def test_scripted_harness_is_model_free_and_reuses_session(tmp_path):
    harness = ScriptedHarness([
        ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'complete', "arguments": {"kind": "json", "data": {"ok": True}}})])
    ])
    original = await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="")

    async def tool(*args):
        return {"runtime_directive": "drain_for_submission"}

    async def unused(*args):
        pass

    result = await harness.run_turn(
        "", on_tool=tool, on_usage=unused, on_started=unused)
    assert result.process_quiet and result.terminal_observed
    resumed = await harness.open(
        workspace=tmp_path, thread_id=original.thread_id, tools=[], instructions="")
    assert resumed.thread_id == original.thread_id
    await harness.close()



async def test_cancellation_waits_for_owned_tree_shutdown(tmp_path):
    script = tmp_path / "cancellable_codex.py"
    script.write_text("""
import json,sys,time
from pathlib import Path
for line in sys.stdin:
    m=json.loads(line)
    if m.get('method')=='initialize':
        print(json.dumps({'id':m['id'],'result':{'userAgent':'Codex Desktop/0.160.0'}}),flush=True)
    elif m.get('method')=='thread/start':
        print(json.dumps({'id':m['id'],'result':{'thread':{'id':'t'},'model':'fake'}}),flush=True)
    elif m.get('method')=='turn/start':
        print(json.dumps({'id':m['id'],'result':{'turn':{'id':'turn'}}}),flush=True)
        Path('running').write_text('yes')
        time.sleep(120)
""", encoding="utf-8")
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.1),
        command=[sys.executable, "-u", str(script)],
    )
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="")

    async def unused(*args):
        pass

    task = asyncio.create_task(harness.run_turn(
        "test", on_tool=unused, on_usage=unused, on_started=unused))
    async with asyncio.timeout(3):
        while not (tmp_path / "running").exists():
            await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.process_quiet
    assert harness.host.tree_quiet
    assert harness.host.process.returncode is not None


@pytest.mark.parametrize("directive", ["drain_for_wait", "drain_for_review"])
async def test_context_ack_callback_runs_after_tool_response_is_written(tmp_path, directive):
    from formal_geyser.coordination.runtime.codex import ToolResult

    script = tmp_path / "codex.py"
    script.write_text(FAKE_CODEX, encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.5),
        command=[sys.executable, "-u", str(script), str(log)],
    )
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="test")
    acknowledgments = []

    async def acknowledge():
        # Writer drain has completed, so the response precedes the interrupt.
        acknowledgments.append("admitted")

    async def tool(*args):
        return ToolResult({"runtime_directive": directive}, after_send=acknowledge)

    async def unused(*args):
        pass

    outcome = await harness.run_turn(
        "test", on_tool=tool, on_usage=unused, on_started=unused)
    assert acknowledgments == ["admitted"]
    assert outcome.terminal_observed and outcome.process_quiet



@pytest.mark.parametrize("machine,package,cpu", [("x86_64", "x64", "x86_64"), ("aarch64", "arm64", "aarch64")])
@pytest.mark.parametrize("layout", ["sibling", "nested", "vendored"])
def test_resolves_the_npm_launcher_to_its_native_binary(tmp_path, monkeypatch, layout, machine, package, cpu):
    from formal_geyser.coordination.runtime import executable as executable_module
    from formal_geyser.coordination.runtime import resolve_codex_executable

    openai = tmp_path / "lib" / "node_modules" / "@openai"
    launcher = openai / "codex" / "bin" / "codex.js"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("#!/usr/bin/env node\n", encoding="utf-8")
    vendor = Path("vendor") / f"{cpu}-unknown-linux-musl" / "bin" / "codex"
    native = {"sibling": openai / f"codex-linux-{package}" / vendor,
              "nested": openai / "codex" / "node_modules" / "@openai" / f"codex-linux-{package}" / vendor,
              "vendored": openai / "codex" / vendor}[layout]
    native.parent.mkdir(parents=True)
    native.write_bytes(b"test fixture only")
    link = tmp_path / "bin" / "codex"
    link.parent.mkdir()
    link.symlink_to(launcher)
    monkeypatch.setattr(executable_module.shutil, "which", lambda value: str(link))
    monkeypatch.setattr(executable_module.platform, "machine", lambda: machine)
    assert resolve_codex_executable("codex") == str(native.resolve())
    command = CodexHarness.launch_command(CodexConfig())
    assert command[0] == str(native.resolve())
    assert command[-1] == "app-server"
    # The app-server refuses permission profiles without a default one.
    assert ["-c", 'default_permissions=":read-only"'] in [command[i:i + 2] for i in range(len(command) - 1)]
    for feature in CodexHarness.disabled_features:
        assert ["--disable", feature] in [command[i:i + 2] for i in range(len(command) - 1)]


def test_a_launcher_without_its_native_binary_is_rejected(tmp_path):
    from formal_geyser.coordination.runtime import resolve_codex_executable

    launcher = tmp_path / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
    launcher.parent.mkdir(parents=True)
    launcher.write_text("#!/usr/bin/env node\n", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="no native Codex binary"):
        resolve_codex_executable(str(launcher))



async def test_wait_helper_validation_and_original_thread_resume(tmp_path):
    from uuid import uuid4

    from formal_geyser.coordination.models import (
        Actor,
        Command,
        Limits,
        ProjectConfig,
        Role,
    )
    from formal_geyser.coordination.runtime import TurnOutcome, WorkerRunner

    hub = await new_hub(tmp_path, config=ProjectConfig(limits=Limits()))
    activations = []
    parent_thread = None
    parent_workspace = None
    helper_task = None

    async def command(name, payload):
        return await hub.execute(
            Command(name=name, payload=payload, key=uuid4().hex), Actor(role=Role.OPERATOR))

    class ProgramHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            nonlocal parent_thread, parent_workspace, helper_task
            packet = json.loads(prompt)
            task = packet["task"]
            title = task["title"]
            turn_id = "scripted-" + uuid4().hex
            await on_started(turn_id)
            if title == "Assemble record" and parent_thread is None:
                parent_thread = self.session.thread_id
                parent_workspace = self.workspaces[-1]
                (parent_workspace / "checkpoint.txt").write_text("saved progress", encoding="utf-8")
                helper_task = await on_tool("taskhub_call", {"operation": 'publish', "arguments": file_arguments(self.workspaces[-1], 'publish', {
                    "contract": {
                        "title": "Extract record", "objective": "Produce a JSON source record.",
                        "acceptance_policy": "general.json@1",
                        "payload": {"output_schema": {
                            "type": "object", "properties": {"value": {"type": "string"}},
                            "required": ["value"], "additionalProperties": False,
                        }},
                    },
                })}, uuid4().hex)
                result = await on_tool("taskhub_call", {"operation": 'pause', "arguments": file_arguments(self.workspaces[-1], 'pause', {"reason": "Preserve progress until actionable work is available.", **({
                    "predicate": {"subscriptions": [{
                        "task_id": helper_task["id"], "need": "The extracted JSON source record.",
                    }]},
                    "checkpoint": {"summary": "Prepared record assembly",
                                   "next_action": "Read the source artifact and finish assembly"},
                })})}, uuid4().hex)
                total = 10
            elif title == "Extract record":
                assert self.session.thread_id != parent_thread
                assert self.workspaces[-1] != parent_workspace
                assert self.opened_threads == [None]
                result = await on_tool("taskhub_call", {"operation": 'contribute', "arguments": file_arguments(self.workspaces[-1], 'contribute', source_payload(
                    self.workspaces[-1], kind="json", data={"value": "ready"}))}, uuid4().hex)
                total = 7
            else:
                assert title == "Assemble record"
                assert self.session.thread_id == parent_thread
                assert self.opened_threads == [parent_thread]
                assert self.workspaces[-1] == parent_workspace
                assert (parent_workspace / "checkpoint.txt").read_text() == "saved progress"
                result = await on_tool("taskhub_call", {"operation": 'contribute', "arguments": file_arguments(self.workspaces[-1], 'contribute', source_payload(
                    self.workspaces[-1], kind="json", data={"assembled": True}))}, uuid4().hex)
                total = 25
            await on_usage({
                "totalTokens": total, "inputTokens": total - 2, "cachedInputTokens": 0,
                "outputTokens": 2, "reasoningOutputTokens": 1,
                "lastTokenUsage": {"totalTokens": total},
            })
            await self.close()
            return TurnOutcome(turn_id, "interrupted", True, True, result["runtime_directive"])

    def factory():
        harness = ProgramHarness()
        activations.append(harness)
        return harness

    try:
        parent = await command("task.publish", {"contract": {
            "title": "Assemble record", "objective": "Assemble a record using a helper input.",
            "acceptance_policy": "general.json@1",
            "payload": {"output_schema": {
                "type": "object", "properties": {"assembled": {"const": True}},
                "required": ["assembled"], "additionalProperties": False,
            }},
        }})
        run = await command("run.start", {})
        runner = WorkerRunner(hub, harness_factory=factory)

        async def publish_and_finish(task, submitted):
            reviewer = await select_review(hub, submitted["review_task_id"], run["id"])
            basis = reviewer["task"]["collaboration"]["review"]
            candidate = await hub.artifact(submitted["artifact_id"])
            source = Path(candidate["content_file"]["source_files"][0]["path"])
            authored = json.loads(source.read_text(encoding="utf-8"))
            assert authored == candidate["data"]
            Draft202012Validator(task["contract"]["payload"]["output_schema"]).validate(authored)

            async def review_tool(name, args=None):
                return await runner._tool(reviewer, "synthetic-review-" + reviewer["worker_id"],
                                          "taskhub_call", {"operation": (name).removeprefix("taskhub_"), "arguments": file_arguments(runner.hub.workspace(reviewer["worker_id"]), (name).removeprefix("taskhub_"), args or {})}, uuid4().hex)

            await review_tool('review', {
                **{key: basis[key] for key in (
                    "candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")},
                "verdict": "accept", "rationale": "Compared the JSON source with the exact task schema.",
                "evidence": ["The selected JSON file satisfies the host task's exact output schema."]})
            await hub.ordinary_turn_completed(reviewer["worker_id"], reviewer["id"])
            await publish_batch(hub, run["id"])
            author = await command("task.claim", {"task_id": task["id"], "revision": 1,
                "worker_id": submitted["worker_id"], "run_id": run["id"]})
            await finish_ordinary(hub, task["id"], runner._worker_actor(author),
                                  artifact_ids=[submitted["artifact_id"]])
            await runner._command("runtime.quiescent", {
                "worker_id": author["worker_id"],
                "snapshot": hub.blobs.snapshot(hub.workspace(author["worker_id"]))})

        first = await runner.run_once(run["id"])
        assert first["directive"] == "waiting"
        assert (await hub.task(parent["id"]))["state"] == "waiting"
        assert all(worker["state"] not in {"preparing", "running", "draining"}
                   for worker in await hub.list_workers())
        # The waiting worker is not idle: the published task gets a new worker at once.
        assignment = await hub.next_assignment(run["id"])
        assert assignment["task_id"] == helper_task["id"] and assignment["worker_id"] != first["worker_id"]
        assert await hub.due_assignment_intents(run["id"]) == []
        second = await runner.run_once(run["id"], assignment=assignment)
        assert second["directive"] == "verify"
        assert (await hub.task(helper_task["id"]))["state"] != "done"
        await publish_and_finish(helper_task, second)
        assert (await hub.task(helper_task["id"]))["state"] == "done"
        # The helper is done and publishes nothing more: the subscription to it ends.
        assert any((c["state"], c["wake_reason"]) == ("reconsider", "subscribed_task_done")
                   for c in await hub.list_continuations())
        third = await runner.run_once(run["id"])
        assert third["directive"] == "verify"
        assert (await hub.task(parent["id"]))["state"] != "done"
        await publish_and_finish(parent, third)
        assert (await hub.task(parent["id"]))["state"] == "done"
        assert (await hub.run(run["id"]))["tokens_used"] == 32
        assert (await hub.run(run["id"]))["turns_used"] == 3
        assert len(activations) == 3
        workers = await hub.list_workers()
        assert len([worker for worker in workers if worker["kind"] == "review"]) == 2
        assert all(worker["state"] == "retired" for worker in workers if worker["kind"] == "review")
        assert all(harness.process_quiet for harness in activations)
    finally:
        await hub.close()



@pytest.mark.parametrize("fail_late", [False, True])
async def test_shutdown_drains_usage_and_surfaces_late_ledger_errors(tmp_path, fail_late):
    script = tmp_path / "late_usage_codex.py"
    late_usage = {
        "method": "thread/tokenUsage/updated",
        "params": {"threadId": "original-thread", "turnId": "turn-1",
                   "tokenUsage": {"total": {"totalTokens": 20}}},
    }
    script.write_text(FAKE_CODEX + "\nemit(" + repr(late_usage) + ")\n", encoding="utf-8")
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.5),
        command=[sys.executable, "-u", str(script), str(tmp_path / "protocol.jsonl")],
    )
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="test")
    samples = []

    async def tool(*args):
        return {"runtime_directive": "drain_for_wait"}

    async def usage(sample):
        await asyncio.sleep(0.03)
        if fail_late and sample["totalTokens"] == 20:
            raise RuntimeError("Late usage ledger failure")
        samples.append(sample["totalTokens"])

    async def unused(*args):
        pass

    if fail_late:
        with pytest.raises(RuntimeError, match="Late usage ledger failure"):
            await harness.run_turn(
                "test", on_tool=tool, on_usage=usage, on_started=unused)
        assert harness.process_quiet
        assert samples == [10]
    else:
        outcome = await harness.run_turn(
            "test", on_tool=tool, on_usage=usage, on_started=unused)
        assert outcome.terminal_observed and outcome.process_quiet
        assert samples == [10, 20]


async def test_protocol_version_mismatch_stops_before_creating_a_thread(tmp_path):
    script = tmp_path / "different_version.py"
    script.write_text(
        FAKE_CODEX.replace("Codex Desktop/0.160.0", "Codex Desktop/0.999.0"), encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.2),
        command=[sys.executable, "-u", str(script), str(log)],
    )
    with pytest.raises(RpcProtocolError, match="audited 0.160.0"):
        await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="test")
    assert harness.process_quiet
    methods = [json.loads(line).get("method") for line in log.read_text().splitlines()]
    assert methods == ["initialize"]



async def _runtime_project(
    tmp_path, *, packet_bytes=32768, environment=None, file_submission=False,
):
    from uuid import uuid4

    from formal_geyser.coordination.config import initialize
    from formal_geyser.coordination.hub import Hub
    from formal_geyser.coordination.models import (
        Actor,
        Command,
        Limits,
        ProjectConfig,
        Role,
    )

    config = ProjectConfig(
                           limits=Limits(packet_bytes=packet_bytes), environment=environment or {})
    if file_submission:
        hub = await new_hub(tmp_path, config=config)
    else:
        initialize(tmp_path, config)
        hub = Hub(tmp_path)

    async def command(name, payload):
        return await hub.execute(
            Command(name=name, payload=payload, key=uuid4().hex), Actor(role=Role.OPERATOR))

    run = await command("run.start", {"limits": {
    }})
    return hub, command, run


async def test_driver_runs_two_independent_tasks_concurrently(tmp_path):
    from uuid import uuid4

    from formal_geyser.coordination.runtime import TurnOutcome, WorkerRunner

    hub, command, run = await _runtime_project(tmp_path)
    both_running = asyncio.Event()
    active = peak = 0

    class ConcurrentHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            nonlocal active, peak
            turn_id = uuid4().hex
            await on_started(turn_id)
            active += 1
            peak = max(peak, active)
            if active == 2:
                both_running.set()
            try:
                await asyncio.wait_for(both_running.wait(), 2)
                await on_usage({"totalTokens": 5})
                result = await on_tool(
                    "taskhub_call", {"operation": 'pause', "arguments": file_arguments(self.workspaces[-1], 'pause', {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({"reason": "The concurrent activation check finished.",
                        "checkpoint": {"summary": "Both independent tasks ran concurrently.",
                                       "next_action": "Await further work."}})})}, uuid4().hex)
                await self.close()
                return TurnOutcome(turn_id, "interrupted", True, True, result["runtime_directive"])
            finally:
                active -= 1
                await self.close()

    try:
        assignments = []
        for title in ("First independent record", "Second independent record"):
            task = await command("task.publish", {"contract": {
                "title": title, "objective": "Produce an independent JSON record.",
                "acceptance_policy": "general.json@1",
                "payload": {"output_schema": {"type": "object"}},
            }})
            worker = await command("worker.register", {})
            assignments.append(await command("task.claim", {"task_id": task["id"], "revision": 1,
                "worker_id": worker["id"], "run_id": run["id"]}))

        class AdmittedRunner(WorkerRunner):
            async def _select(self, run_id):
                return assignments.pop(0) if assignments else None

        result = await AdmittedRunner(hub, ConcurrentHarness).run_until_idle(run["id"])
        assert result["steps"] == 2
        assert peak == 2
        assert all(task["state"] == "waiting" for task in await hub.list_tasks())
        assert (await hub.run(run["id"]))["turns_used"] == 2
        assert (await hub.run(run["id"]))["tokens_used"] == 10
    finally:
        await hub.close()


@pytest.mark.parametrize("control", ["pause", "stop", "cancel"])
async def test_run_controls_quiesce_active_worker_before_releasing_slot(tmp_path, control):
    from uuid import uuid4

    from formal_geyser.coordination.runtime import TurnOutcome, WorkerRunner

    hub, command, run = await _runtime_project(tmp_path)
    started = asyncio.Event()
    interrupted = asyncio.Event()
    harnesses = []
    task = ready = None

    class ControlledHarness(ScriptedHarness):
        async def interrupt(self):
            interrupted.set()

        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            turn_id = uuid4().hex
            try:
                await on_started(turn_id)
                await on_usage({"totalTokens": 20})
                started.set()
                await interrupted.wait()
                return TurnOutcome(turn_id, "interrupted", True, True)
            finally:
                await asyncio.sleep(0.02)
                await self.close()

    def factory():
        harness = ControlledHarness()
        harnesses.append(harness)
        return harness

    try:
        await command("task.publish", {"contract": {
            "title": "Controlled work", "objective": "Wait for an external run control.",
        }})
        task = asyncio.create_task(WorkerRunner(hub, factory).run_once(run["id"]))
        ready = asyncio.create_task(started.wait())
        # Native reference files and context are prepared before the run-control
        # race begins. Keep that startup budget separate from the drain deadline.
        finished, _ = await asyncio.wait({task, ready}, timeout=10,
                                         return_when=asyncio.FIRST_COMPLETED)
        if task in finished:
            task.result()  # Surface startup errors instead of masking them as readiness timeouts.
        assert ready in finished, "The controlled worker did not reach its ready boundary"
        if control == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await command("run." + control, {"run_id": run["id"]})
            result = await asyncio.wait_for(task, 2)
            assert result["turn_status"] == "interrupted"
            assert result["directive"] == "parked"
            assert result["stop_reason"] == {"pause": "operator_pause", "stop": "operator_stop"}[control]
            assert result["error"] is None
            assert interrupted.is_set()
        assert all(harness.process_quiet for harness in harnesses)
        assert all(worker["active_attempt"] is None for worker in await hub.list_workers())
        current = await hub.run(run["id"])
        assert current["state"] == {"stop": "stopped", "pause": "paused", "cancel": "running"}[control]
        assert bool(current["usage_uncertain"]) == (control == "cancel")
    finally:
        for pending in (ready, task):
            if pending is not None and not pending.done():
                pending.cancel()
        await asyncio.gather(*(pending for pending in (ready, task) if pending is not None),
                             return_exceptions=True)
        await hub.close()


async def test_compaction_refreshes_context_before_mutation_without_resetting_usage(tmp_path):
    from uuid import uuid4

    from formal_geyser.coordination.runtime import ToolResult, TurnOutcome, WorkerRunner
    from formal_geyser.coordination.runtime.runner import COMPACTION_REFRESH

    hub, command, run = await _runtime_project(tmp_path)
    refreshed_packets = []

    class CompactingHarness(ScriptedHarness):
        # Like Claude: compaction summarizes the inputs too, so the task view rides the next tool result.
        compaction_keeps_inputs = False

        async def open(self, **options):
            self.instructions = options["instructions"]
            return await super().open(**options)

        def set_compaction_handler(self, handler):
            self.compacted = handler

        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            turn_id = uuid4().hex
            await on_started(turn_id)
            await on_usage({"totalTokens": 10})
            await self.compacted()
            blocked = await on_tool(
                "taskhub_call", {"operation": 'pause', "arguments": file_arguments(self.workspaces[-1], 'pause', {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({"reason": "Retain refreshed progress.",
                    "checkpoint": {"summary": "Read refreshed context.",
                                   "next_action": "Await useful work."}})})}, uuid4().hex)
            assert isinstance(blocked, ToolResult)
            assert "error" not in blocked and blocked["runtime_directive"] == "drain_for_wait"
            refreshed_packets.append(blocked["context"])
            assert blocked["context"]["instruction"] == COMPACTION_REFRESH
            guidance = blocked["context"]["reading"]
            # The required guide is a standing instruction; reading lists only on-demand guides.
            assert "required" not in guidance
            assert ".private/instructions/shared-dags.md" in guidance["on_demand"]
            assert ".private/instructions/worker.md" not in guidance["on_demand"]
            guide = (self.workspaces[-1] / ".private/instructions/worker.md").read_text(encoding="utf-8")
            assert guide.strip() in self.instructions
            assert "context_epoch" not in guidance
            assert (await hub.list_workers())[0]["context_epoch"] == 1
            assert not await hub.list_artifacts()
            await blocked.after_send()
            await on_usage({"totalTokens": 15})
            result = blocked
            await self.close()
            return TurnOutcome(turn_id, "interrupted", True, True, result["runtime_directive"])

    try:
        await command("task.publish", {"contract": {
            "title": "Context refresh record", "objective": "Produce a JSON record.",
            "acceptance_policy": "general.json@1",
            "payload": {"output_schema": {"type": "object"}},
        }})
        await WorkerRunner(hub, CompactingHarness).run_once(run["id"])
        worker = (await hub.list_workers())[0]
        assert worker["context_epoch"] == 1  # Compaction no longer starts a new context epoch.
        assert worker["usage_epoch"] == 1
        assert "context_epoch" not in refreshed_packets[0]
        assert (await hub.run(run["id"]))["tokens_used"] == 15
    finally:
        await hub.close()


@pytest.mark.parametrize("legacy_first", [False, True])
async def test_compaction_item_and_legacy_event_are_counted_once(tmp_path, legacy_first):
    modern = {"method": "item/completed", "params": {
        "threadId": "original-thread", "turnId": "turn-1", "completedAtMs": 0,
        "item": {"type": "contextCompaction", "id": "compact-1"},
    }}
    legacy = {"method": "thread/compacted", "params": {
        "threadId": "original-thread", "turnId": "turn-1",
    }}
    notifications = [legacy, modern, modern] if legacy_first else [modern, modern, legacy]
    inserted = "".join("        emit(" + repr(message) + ")\n" for message in notifications)
    script = tmp_path / "compacting_codex.py"
    script.write_text(FAKE_CODEX.replace(
        "        emit({'id':'call-request'", inserted + "        emit({'id':'call-request'"),
        encoding="utf-8")
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.5),
        command=[sys.executable, "-u", str(script), str(tmp_path / "protocol.jsonl")],
    )
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="")
    compactions = []

    async def compacted():
        compactions.append("once")

    async def tool(*args):
        return {"runtime_directive": "drain_for_wait"}

    async def unused(*args):
        pass

    harness.set_compaction_handler(compacted)
    result = await harness.run_turn(
        "", on_tool=tool, on_usage=unused, on_started=unused)
    assert result.process_quiet and result.terminal_observed
    assert compactions == ["once"]



async def test_task_inspection_reads_exact_historical_revision(tmp_path):
    from formal_geyser.coordination.errors import HubError
    from formal_geyser.coordination.runtime import WorkerRunner

    hub, command, run = await _runtime_project(tmp_path)
    runner = WorkerRunner(hub, ScriptedHarness)
    worker = None
    try:
        original = await command("task.publish", {"contract": {
            "title": "Source record", "objective": "Read the original source record.",
        }})
        revised = await command("task.revise", {
            "task_id": original["id"], "revision": 1,
            "reason": "Keep both exact native source revisions available for inspection.",
            "contract": {"title": "Source record", "objective": "Read the updated source record."},
        })
        consumer = await command("task.publish", {"contract": {
            "title": "Inspect source history", "objective": "Compare the requested source revision.",
        }})
        worker = await command("worker.register", {})
        attempt = await command("task.claim", {
            "task_id": consumer["id"], "revision": consumer["revision"],
            "worker_id": worker["id"], "run_id": run["id"],
        })
        historical = await runner._tool(attempt, "scripted-thread", "taskhub_call", {"operation": 'inspect', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'inspect', {
            "task_id": original["id"], "revision": 1,
        })}, "historical-read")
        current = await runner._tool(attempt, "scripted-thread", "taskhub_call", {"operation": 'inspect', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'inspect', {
            "task_id": original["id"],
        })}, "current-read")
        assert historical["revision"] == 1
        assert historical["contract"] == original["contract"]
        assert historical["contract_hash"] == original["contract_hash"]
        assert current["revision"] == 2
        assert current["contract"] == revised["contract"]
        assert current["contract_hash"] != historical["contract_hash"]
        with pytest.raises(HubError) as missing:
            await runner._tool(attempt, "scripted-thread", "taskhub_call", {"operation": 'inspect', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'inspect', {
                "task_id": original["id"], "revision": 999,
            })}, "missing-read")
        assert missing.value.code == "NOT_FOUND"
        schema = next(tool["inputSchema"] for tool in operation_tools()
                      if tool["name"] == "taskhub_inspect")
        validator = Draft202012Validator(schema)
        assert validator.is_valid({"task_id": original["id"]})
        assert validator.is_valid({"task_id": original["id"], "revision": 1})
        assert not validator.is_valid({"task_id": original["id"], "revision": 0})
        assert not validator.is_valid({"task_id": original["id"], "revision": True})
    finally:
        if worker is not None:
            await runner._command("runtime.abandon", {
                "worker_id": worker["id"], "quiescent": True,
                "reason": "Historical inspection test completed without starting a process",
            })
        await hub.close()



async def test_oversized_tool_result_is_immutable_retrievable_and_keeps_routing(tmp_path):
    from formal_geyser.coordination.runtime import WorkerRunner
    from formal_geyser.coordination.runtime.model_view import tool_view
    from formal_geyser.coordination.storage import canonical

    packet_bytes = 512
    hub, command, run = await _runtime_project(tmp_path, packet_bytes=packet_bytes,
                                             file_submission=True)
    runner = WorkerRunner(hub, ScriptedHarness)
    worker = None
    try:
        provider = await command("task.publish", {"contract": {
            "title": "Source record", "objective": "Provide the source record.",
        }})
        consumer = await command("task.publish", {"contract": {
            "title": "Read material", "objective": "Read the complete result through bounded pages.",
        }})
        worker = await command("worker.register", {})
        attempt = await command("task.claim", {
            "task_id": consumer["id"], "revision": 1,
            "worker_id": worker["id"], "run_id": run["id"],
        })
        # Four-byte Unicode and JSON escaping must both count toward page limits.
        report = ("Record detail: " + chr(0x1F680) + chr(0) + '"\\') * 400
        artifact = await runner._command("artifact.stage", file_payload(
            hub, attempt, kind="record", data={"text": report}), actor=runner._worker_actor(attempt))
        expected = tool_view(await runner._present(await hub.artifact(artifact["id"])))
        assert expected["data"]["text"] == report
        response = await runner._tool(attempt, "thread", "taskhub_call", {"operation": 'fetch', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'fetch', {
            "artifact_id": artifact["id"],
        })}, "large-fetch")
        assert len(canonical(response).encode("utf-8")) <= packet_bytes
        assert response["id"] == artifact["id"]
        assert response["kind"] == "record"
        assert response["status"] == artifact["status"]
        assert response["requires_read"] is True
        assert response["material_bytes"] == len(canonical(expected).encode("utf-8"))
        sha = response["material_sha256"]
        body = (hub.workspace(worker["id"]) / response["material_path"]).read_bytes()
        assert json.loads(body) == expected
        assert hashlib.sha256(body).hexdigest() == sha
        with pytest.raises(FileNotFoundError):
            hub.blobs.get(sha)
        pieces, offset = [], 0
        while True:
            page = await runner._tool(attempt, "thread", "taskhub_call", {"operation": 'workspace_read', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'workspace_read', {
                "path": response["material_path"], "offset": offset, "limit": 4096,
            })}, "page-" + str(offset))
            assert len(canonical(page).encode("utf-8")) <= packet_bytes
            assert "material_sha256" not in page
            pieces.append(page["text"])
            assert page["next_offset"] == offset + len(page["text"])
            if not page["has_more"]:
                break
            assert page["next_offset"] > offset
            offset = page["next_offset"]
        assert json.loads("".join(pieces)) == expected
        waiting = await runner._tool(attempt, "thread", "taskhub_call", {"operation": 'pause', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'pause', {"reason": "Preserve progress until actionable work is available.", **({
            "predicate": {"subscriptions": [{"task_id": provider["id"], "need": "The source record."}]},
            "checkpoint": {"summary": "Material read completely", "next_action": "Await source"},
        })})}, "wait")
        large_wait_report = {**waiting, "report": report}
        bounded = await runner._bound_tool_result(large_wait_report, packet_bytes)
        assert len(canonical(bounded).encode("utf-8")) <= packet_bytes
        assert bounded["runtime_directive"] == waiting["runtime_directive"]
        assert "attempt_id" not in waiting
        assert "continuation_id" not in waiting
        assert hub.blobs.get_json(bounded["material_sha256"]) == large_wait_report
        failed = await runner._bound_tool_result({"error": report}, packet_bytes)
        assert failed["error"] is True
        assert hub.blobs.get_json(failed["material_sha256"]) == {"error": report}
    finally:
        if worker is not None:
            await runner._command("runtime.abandon", {
                "worker_id": worker["id"], "quiescent": True,
                "reason": "Bounded material test completed without starting a process",
            })
        await hub.close()


async def test_tool_result_acknowledgment_is_never_replaced_by_material_wrapper(tmp_path):
    from formal_geyser.coordination.runtime import ToolResult, WorkerRunner

    hub, _, _ = await _runtime_project(tmp_path)
    runner = WorkerRunner(hub, ScriptedHarness)
    acknowledged = []

    async def admitted():
        acknowledged.append("original-packet")

    packet = ToolResult({"packet_id": "original-packet", "context": "x" * 1000},
                        after_send=admitted)
    try:
        result = await runner._bound_tool_result(packet, 512)
        assert result is packet
        assert result.after_send is admitted
        assert not acknowledged
        await result.after_send()
        assert acknowledged == ["original-packet"]
    finally:
        await hub.close()



def test_scoped_files_are_confined_and_write_atomically_with_compare_and_swap(tmp_path):
    from formal_geyser.coordination.runtime.files import ScopedFiles

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("External content", encoding="utf-8")
    files = ScopedFiles(workspace)
    written = files.write("notes/progress.txt", "Saved progress.")
    read = files.read("notes/progress.txt", offset=0, limit=5)
    assert read["text"] == "Saved"
    assert read["has_more"] is True
    assert read["source_sha256"] == written["sha256"]
    with pytest.raises(ValueError, match="changed"):
        files.write("notes/progress.txt", "Incorrect replacement", "0" * 64)
    updated = files.write("notes/progress.txt", "Updated progress.", written["sha256"])
    assert updated["sha256"] != written["sha256"]
    assert files.read("notes/progress.txt")["text"] == "Updated progress."
    assert not list(workspace.rglob(".task-based-write-*"))
    for path in ("../outside.txt", str(outside), "notes/progress.txt:stream"):
        with pytest.raises(ValueError):
            files.read(path)
        with pytest.raises(ValueError):
            files.write(path, "Forbidden replacement")
    assert outside.read_text() == "External content"


def test_scoped_file_access_rejects_symbolic_link_escape(tmp_path):
    from formal_geyser.coordination.runtime.files import ScopedFiles

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("External content", encoding="utf-8")
    linked = workspace / "linked.txt"
    try:
        linked.symlink_to(outside)
    except OSError:
        pytest.skip("Symbolic link creation is unavailable on this host")
    files = ScopedFiles(workspace)
    assert files.list()["entries"][0]["kind"] == "blocked_link"
    with pytest.raises(ValueError, match="links"):
        files.read("linked.txt")
    with pytest.raises(ValueError, match="links"):
        files.write("linked.txt", "Forbidden replacement")
    assert outside.read_text() == "External content"


async def test_fenced_workspace_and_readonly_resource_tools_work_without_a_shell(tmp_path):
    from formal_geyser.coordination.runtime import WorkerRunner

    resources = tmp_path / "resources"
    resources.mkdir()
    (resources / "index.txt").write_text("Shared reference material.", encoding="utf-8")
    original = (resources / "index.txt").read_bytes()
    hub, command, run = await _runtime_project(
        tmp_path / "project", environment={"resource_roots": {"reference": str(resources)}})
    runner = WorkerRunner(hub, ScriptedHarness)
    worker = None
    try:
        task = await command("task.publish", {"contract": {
            "title": "Use shared material", "objective": "Read the source and save private notes.",
        }})
        worker = await command("worker.register", {})
        attempt = await command("task.claim", {
            "task_id": task["id"], "revision": 1, "worker_id": worker["id"], "run_id": run["id"],
        })

        async def tool(name, args):
            return await runner._tool(attempt, "thread", "taskhub_call", {"operation": (name).removeprefix("taskhub_"), "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), (name).removeprefix("taskhub_"), args)}, name)

        native = await hub.prepare_native_context(worker["id"])
        assert native["read_only_resources"]["reference"] == str(resources)
        assert (await tool('resource_roots', {}))["code"] == "UNKNOWN_OPERATION"
        source = await tool('resource_read', {"root": "reference", "path": "index.txt"})
        assert source["text"] == original.decode("utf-8")
        found = await tool('resource_search', {
            "root": "reference", "query": "shared", "mode": "text",
        })
        assert found["matches"][0]["path"] == "index.txt"
        saved = await tool('workspace_write', {
            "path": "notes.txt", "text": "Read shared material; prepare the next step.",
        })
        assert saved["written"] is True
        notes = await tool('workspace_read', {"path": "notes.txt"})
        assert notes["text"].startswith("Read shared material")
        with pytest.raises(ValueError):
            await tool('workspace_write', {"path": str(resources / "index.txt"),
                                                  "text": "Forbidden"})
        rejected = await tool('resource_write', {"root": "reference", "path": "index.txt", "text": "Forbidden"})
        assert rejected["code"] == "UNKNOWN_OPERATION"
        assert (resources / "index.txt").read_bytes() == original
        assert any(item["name"] == "taskhub_resource_pdf" for item in runner.actions.specs)
        assert {item["name"] for item in runner.tools} == {"taskhub_actions", "taskhub_call"}
    finally:
        if worker is not None:
            await runner._command("runtime.abandon", {
                "worker_id": worker["id"], "quiescent": True, "reason": "Scoped file test finished",
            })
        await hub.close()


def test_scoped_pdf_reader_reads_pages_without_modifying_source(tmp_path):
    pypdf = pytest.importorskip("pypdf")
    from formal_geyser.coordination.runtime.files import ScopedFiles

    target = tmp_path / "document.pdf"
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=72, height=72)
    with target.open("wb") as stream:
        writer.write(stream)
    original = target.read_bytes()
    result = ScopedFiles(tmp_path).pdf("document.pdf")
    assert result["total_pages"] == 1
    assert result["pages"] == [{"page": 1, "text": ""}]
    assert result["has_more"] is False
    assert target.read_bytes() == original


async def test_activity_log_excludes_tool_content_credentials_and_raw_errors(tmp_path):
    from formal_geyser.coordination.runtime.telemetry import ActivityLog

    log = ActivityLog(tmp_path)
    secret = "secret credential and source content"
    await log.emit(
        "tool_started", run_id="run-1", worker_id="worker-1", attempt_id="attempt-1",
        tool="taskhub_workspace_write", arguments={"token": secret}, text=secret,
        stdout=secret, error=secret, email=secret, api_key=secret,
    )
    text = log.path.read_text(encoding="utf-8")
    assert secret not in text
    record = json.loads(text)
    assert record["event"] == "tool_started"
    assert record["attempt_id"] == "attempt-1"
    assert record["timestamp"].endswith("+00:00")
    assert set(record) == {"timestamp", "event", "run_id", "worker_id", "attempt_id", "tool"}


async def test_thread_policy_writes_only_the_workspace(tmp_path):
    script = tmp_path / "codex.py"
    script.write_text(FAKE_CODEX, encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    harness = CodexHarness(
        CodexConfig(stop_grace_seconds=0.2),
        command=[sys.executable, "-u", str(script), str(log)],
    )
    try:
        await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="Test.")
    finally:
        await harness.close()
    messages = [json.loads(line) for line in log.read_text().splitlines()]
    start = next(item for item in messages if item.get("method") == "thread/start")
    # The worker profile, and a temporary directory in the workspace: /tmp is not writable.
    assert start["params"]["permissions"] == "taskhub-worker" and "sandbox" not in start["params"]
    assert start["params"]["runtimeWorkspaceRoots"] == [str(tmp_path.resolve())]
    temporary = tmp_path.resolve() / ".private" / "tmp"
    assert start["params"]["config"]["shell_environment_policy.set"] == {"TMPDIR": str(temporary)}
    assert temporary.is_dir()
    assert start["params"]["approvalPolicy"] == "never"



def _error_peer_script(notifications):
    return """
import json,sys
def emit(value):
    print(json.dumps(value),flush=True)
for line in sys.stdin:
    message=json.loads(line)
    method=message.get('method')
    if method=='initialize':
        emit({'id':message['id'],'result':{'userAgent':'Codex Desktop/0.160.0'}})
    elif method in ('thread/start','thread/resume'):
        emit({'id':message['id'],'result':{'thread':{'id':'error-thread'},'model':'scripted'}})
    elif method=='turn/start':
        emit({'id':message['id'],'result':{'turn':{'id':'error-turn','status':'inProgress'}}})
        for notification in NOTIFICATIONS:
            emit(notification)
""".replace("NOTIFICATIONS", repr(notifications))


@pytest.mark.parametrize("source", ["terminal", "notification", "missing"])
async def test_failed_turn_propagates_terminal_or_notification_error(tmp_path, source):
    message = "Configured model requires a newer client."
    notifications = []
    if source == "notification":
        notifications.append({"method": "error", "params": {
            "threadId": "error-thread", "turnId": "error-turn", "willRetry": False,
            "error": {"message": message, "codexErrorInfo": "other"},
        }})
    notifications.append({"method": "turn/completed", "params": {
        "threadId": "error-thread", "turn": {
            "id": "error-turn", "status": "failed",
            "error": {"message": message} if source == "terminal" else None,
        },
    }})
    script = tmp_path / "error_peer.py"
    script.write_text(_error_peer_script(notifications), encoding="utf-8")
    harness = CodexHarness(CodexConfig(stop_grace_seconds=0.2),
                           command=[sys.executable, "-u", str(script)])
    captured = []

    async def capture(record):
        captured.append(record)

    async def unused(*args):
        pass

    harness.set_diagnostic_handler(capture)
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="Test.")
    outcome = await harness.run_turn(
        "Test.", on_tool=unused, on_usage=unused, on_started=unused)
    assert outcome.status == "failed"
    assert outcome.terminal_observed and outcome.process_quiet
    assert outcome.error == (message if source != "missing" else
                             "Codex reported a failed turn without error details")
    assert captured and outcome.diagnostics
    assert captured[-1]["source"] == "terminal"


async def test_retry_notification_is_retained_without_turning_recovery_into_failure(tmp_path):
    notifications = [
        {"method": "error", "params": {
            "threadId": "error-thread", "turnId": "error-turn", "willRetry": True,
            "error": {"message": "Temporary connection interruption."},
        }},
        {"method": "turn/completed", "params": {
            "threadId": "error-thread",
            "turn": {"id": "error-turn", "status": "completed", "error": None},
        }},
    ]
    script = tmp_path / "retry_peer.py"
    script.write_text(_error_peer_script(notifications), encoding="utf-8")
    harness = CodexHarness(CodexConfig(stop_grace_seconds=0.2),
                           command=[sys.executable, "-u", str(script)])

    async def unused(*args):
        pass

    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="Test.")
    outcome = await harness.run_turn(
        "Test.", on_tool=unused, on_usage=unused, on_started=unused)
    assert outcome.status == "completed"
    assert outcome.error is None
    assert outcome.diagnostics[0]["will_retry"] is True


async def test_worker_failure_persists_safe_diagnostics_and_propagates_reason(tmp_path):
    from formal_geyser.coordination.runtime import WorkerRunner

    hub, command, run = await _runtime_project(tmp_path)
    secret = "test-secret-value"
    error = {"message": "Configured model requires a newer client.\nAuthorization: Bearer " + secret,
             "additionalDetails": '{"set-cookie":"session=' + secret + '"}'}
    notifications = [
        {"method": "error", "params": {
            "threadId": "error-thread", "turnId": "error-turn", "willRetry": False, "error": error,
        }},
        {"method": "turn/completed", "params": {
            "threadId": "error-thread", "turn": {"id": "error-turn", "status": "failed", "error": error},
        }},
    ]
    script = tmp_path / "failing_peer.py"
    script.write_text(_error_peer_script(notifications), encoding="utf-8")
    try:
        await command("task.publish", {"contract": {
            "title": "Report failure", "objective": "Preserve a safe runtime diagnostic.",
        }})
        runner = WorkerRunner(hub, lambda: CodexHarness(
            CodexConfig(stop_grace_seconds=0.2), command=[sys.executable, "-u", str(script)]))
        result = await runner.run_once(run["id"])
        assert result["turn_status"] == "failed"
        assert "requires a newer client" in result["error"]
        assert result["diagnostic_refs"]
        assert secret not in json.dumps(result)
        for reference in result["diagnostic_refs"]:
            record = hub.blobs.get_json(reference)
            assert record["run_id"] == run["id"]
            assert record["worker_id"] == result["worker_id"]
            assert record["turn_id"] == "error-turn"
            assert secret not in json.dumps(record)
        markers = [json.loads(line) for line in runner.activity.path.read_text().splitlines()]
        errors = [item for item in markers if item["event"] == "runtime_error"]
        assert [item["diagnostic_sha256"] for item in errors] == result["diagnostic_refs"]
        assert all("message" not in item for item in errors)
        assert secret not in runner.activity.path.read_text()
    finally:
        await hub.close()


def test_error_sanitizer_redacts_cookie_headers_tokens_and_bounds_messages():
    from formal_geyser.coordination.runtime.diagnostics import normalize_error

    raw = {"message": "Useful failure detail.\n"
           r'\"set-cookie\": \"session=fake-cookie-secret\"' + "\n"
           "api_key=fake-api-key\nOPENAI_API_KEY=fake-env-key\n"
           "client_secret=fake-client-secret\nsessionToken=fake-session-token\n"
           "Bearer fake-bearer-token\n" + "x" * 10000,
           "authorization": "Ignored structured credential"}
    result = normalize_error(raw, source="terminal")
    serialized = json.dumps(result)
    assert "Useful failure detail." in result["message"]
    for secret in ("fake-cookie-secret", "fake-api-key", "fake-bearer-token", "Ignored structured",
                   "fake-env-key", "fake-client-secret", "fake-session-token"):
        assert secret not in serialized
    assert len(result["message"].encode("utf-8")) < 4200



@pytest.mark.parametrize("resuming", [False, True])
async def test_audited_direct_tool_namespace_is_applied_to_start_and_resume(tmp_path, resuming):
    script = tmp_path / "tool_surface_peer.py"
    script.write_text(FAKE_CODEX, encoding="utf-8")
    log = tmp_path / "tool_surface.jsonl"
    harness = CodexHarness(CodexConfig(stop_grace_seconds=0.3),
                           command=[sys.executable, "-u", str(script), str(log)])
    try:
        await harness.open(workspace=tmp_path, thread_id="original-thread" if resuming else None,
                           tools=dynamic_tools(), instructions="Use direct tools.")
    finally:
        await harness.close()
    messages = [json.loads(line) for line in log.read_text().splitlines()]
    request = next(item for item in messages if item.get("method") == ("thread/resume" if resuming else "thread/start"))
    config = request["params"]["config"]
    assert config["features.code_mode.direct_only_tool_namespaces"] == ["functions"]
    assert config["features.code_mode.default_exec_yield_time_ms"] == 60000
    assert request["params"]["approvalPolicy"] == "never"
    assert request["params"]["permissions"] == "taskhub-worker"
    if resuming:
        assert request["params"]["threadId"] == "original-thread"
        assert "dynamicTools" not in request["params"]


LONG_TOOL_PEER = r"""
import json,sys
from pathlib import Path
log=Path(sys.argv[1])
exit_after_interrupt=sys.argv[2]=='yes'
def emit(value):
    print(json.dumps(value),flush=True)
for line in sys.stdin:
    message=json.loads(line)
    with log.open('a',encoding='utf-8') as f:
        f.write(json.dumps(message)+'\n')
    method=message.get('method')
    if method=='initialize':
        emit({'id':message['id'],'result':{'userAgent':'Codex Desktop/0.160.0'}})
    elif method=='thread/start':
        emit({'id':message['id'],'result':{'thread':{'id':'long-thread'},'model':'scripted'}})
    elif method=='turn/start':
        emit({'id':message['id'],'result':{'turn':{'id':'long-turn','status':'inProgress'}}})
        emit({'id':'long-call','method':'item/tool/call','params':{'threadId':'long-thread','turnId':'long-turn','callId':'long-call-id','tool':'taskhub_sync','arguments':{}}})
    elif method=='turn/interrupt':
        emit({'id':message['id'],'result':{}})
        emit({'method':'turn/completed','params':{'threadId':'long-thread','turn':{'id':'long-turn','status':'interrupted'}}})
        emit({'method':'thread/tokenUsage/updated','params':{'threadId':'long-thread','turnId':'long-turn','tokenUsage':{'total':{'totalTokens':25}}}})
        if exit_after_interrupt:
            sys.exit(0)
"""


@pytest.mark.parametrize("peer_exits", [False, True])
@pytest.mark.parametrize("queued_request", [False, True])
async def test_interrupt_cancels_long_tool_awaits_cleanup_and_preserves_terminal_and_usage(tmp_path, peer_exits, queued_request):
    script = tmp_path / "long_tool_peer.py"
    source = LONG_TOOL_PEER
    if queued_request:
        request = {"id": "queued-call", "method": "item/tool/call", "params": {
            "threadId": "long-thread", "turnId": "long-turn", "callId": "queued-call-id",
            "tool": "taskhub_sync", "arguments": {}}}
        source = source.replace("    elif method=='turn/interrupt':", "        emit(" + repr(request) + ")\n    elif method=='turn/interrupt':")
    script.write_text(source, encoding="utf-8")
    log = tmp_path / "long_tool.jsonl"
    harness = CodexHarness(CodexConfig(version="0.160.0", stop_grace_seconds=0.4),
                           command=[sys.executable, "-u", str(script), str(log), "yes" if peer_exits else "no"])
    entered, cleaned = asyncio.Event(), asyncio.Event()
    usages = []

    async def long_tool(name, arguments, call_id):
        assert call_id == "long-call-id", "Queued work must not execute after interruption"
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.05)
            cleaned.set()

    async def usage(sample):
        await asyncio.sleep(0.01)
        usages.append(sample)

    async def started(turn_id):
        pass

    await harness.open(workspace=tmp_path, thread_id=None, tools=dynamic_tools(), instructions="Use direct tools.")
    turn = asyncio.create_task(harness.run_turn("Test.", on_tool=long_tool, on_usage=usage, on_started=started))
    await asyncio.wait_for(entered.wait(), 2)
    await harness.interrupt()
    assert not harness.process_quiet
    outcome = await asyncio.wait_for(turn, 3)
    assert cleaned.is_set()
    assert outcome.status == "interrupted" and outcome.terminal_observed
    assert outcome.error is None and not outcome.diagnostics
    assert outcome.process_quiet and harness.process_quiet
    assert usages == [{"totalTokens": 25}]
    messages = [json.loads(line) for line in log.read_text().splitlines()]
    assert sum(message.get("method") == "turn/start" for message in messages) == 1
    assert not any(message.get("method") in {"wait", "exec"} for message in messages)


@pytest.mark.parametrize("action", ["submit", "wait", "pause"])
async def test_pause_after_tool_commit_preserves_durable_coordination(tmp_path, action):
    from formal_geyser.coordination.runtime import WorkerRunner

    hub, command, run = await _runtime_project(tmp_path, file_submission=action == "submit")
    hub.config.codex.version = "0.160.0"
    script = tmp_path / "committed_tool_peer.py"
    script.write_text(LONG_TOOL_PEER, encoding="utf-8")
    log = tmp_path / "committed_tool.jsonl"
    entered, committed = asyncio.Event(), asyncio.Event()
    activation = None
    runner = WorkerRunner(hub, lambda: CodexHarness(
        CodexConfig(version="0.160.0", stop_grace_seconds=0.4),
        command=[sys.executable, "-u", str(script), str(log), "no"]))

    provider = await command("task.publish", {"contract": {
        "title": "Future software input", "objective": "Provide a later dependency.", "priority": 3}})
    checkpoint = {"summary": "Saved progress.", "next_action": "Continue from the saved result.", "input_refs": []}
    payloads = {
        "submit": {"kind": "software.result", "data": {"saved": True}},
        "wait": {"checkpoint": checkpoint, "predicate": {"subscriptions": [{
            "task_id": provider["id"], "need": "The later software input."}]}},
        "pause": {"checkpoint": checkpoint, "reason": "Preserve committed progress until useful work resumes.",
                  "predicate": {"external_response_id": "unanswered-request"}},
    }

    async def delayed_tool(attempt, thread_id, name, args, call_id):
        entered.set()
        payload = (file_payload(hub, attempt, data={"saved": True})
                   if action == "submit" else payloads[action])
        await runner._command("artifact.submit" if action == "submit" else "task." + action,
                              payload, actor=runner._worker_actor(attempt))
        committed.set()
        await asyncio.Event().wait()

    runner._tool = delayed_tool
    try:
        task = await command("task.publish", {"contract": {
            "title": "Durable tool result", "objective": "Preserve committed coordination during pause.", "priority": 0}})
        activation = asyncio.create_task(runner.run_once(run["id"]))
        # Native file verification and the subprocess handshake happen before
        # this test's commit/pause race. Bound startup separately, retaining the
        # original commit and process-drain budgets once the tool is executing.
        await asyncio.wait_for(entered.wait(), 10)
        await asyncio.wait_for(committed.wait(), 2)
        await command("run.pause", {"run_id": run["id"]})
        result = await asyncio.wait_for(activation, 3)
        assert result["directive"] == {"submit": "verify", "wait": "waiting", "pause": "waiting"}[action]
        assert result["turn_status"] == "interrupted"
        if action == "submit":
            assert (await hub.artifact(result["artifact_id"]))["data"] == {"saved": True}
        elif action == "wait":
            assert (await hub.list_continuations())[0]["checkpoint"] == checkpoint
        assert (await hub.task(task["id"]))["state"] not in {"done", "retired"}
        ledger = await hub.run(run["id"])
        assert ledger["state"] == "paused" and not ledger["usage_uncertain"]
        assert ledger["turns_used"] == 1 and ledger["tokens_used"] == 25
        assert result["error"] is None and result["diagnostic_refs"] == []
        assert (await hub.worker(result["worker_id"]))["active_attempt"] is None
        messages = [json.loads(line) for line in log.read_text().splitlines()]
        assert sum(message.get("method") == "turn/start" for message in messages) == 1
        terminals = [event["payload"] for event in await hub.events() if event["kind"] == "turn.finished"]
        assert len(terminals) == 1
        assert terminals[0]["terminal_observed"] and terminals[0]["process_quiet"]
        assert terminals[0]["status"] == "interrupted"
        events = [json.loads(line) for line in runner.activity.path.read_text().splitlines()]
        cancelled = [event for event in events if event["event"] == "tool_finished"]
        assert cancelled[-1]["status"] == "cancelled"
        assert "error_type" not in cancelled[-1]
    finally:
        if activation is not None and not activation.done():
            activation.cancel()
            await asyncio.gather(activation, return_exceptions=True)
        await hub.close()


async def test_repeated_turn_cancellation_does_not_interrupt_tool_cleanup(tmp_path):
    script = tmp_path / "repeated_cancel_peer.py"
    script.write_text(LONG_TOOL_PEER, encoding="utf-8")
    harness = CodexHarness(CodexConfig(version="0.160.0", stop_grace_seconds=0.5),
                           command=[sys.executable, "-u", str(script), str(tmp_path / "repeated.jsonl"), "no"])
    entered, cleaning, finish_cleanup, cleaned = (asyncio.Event() for _ in range(4))

    async def long_tool(name, arguments, call_id):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await finish_cleanup.wait()
            cleaned.set()

    async def unused(value):
        pass

    await harness.open(workspace=tmp_path, thread_id=None, tools=dynamic_tools(), instructions="Test cleanup.")
    turn = asyncio.create_task(harness.run_turn("Test.", on_tool=long_tool, on_usage=unused, on_started=unused))
    await asyncio.wait_for(entered.wait(), 2)
    turn.cancel()
    await asyncio.wait_for(cleaning.wait(), 2)
    turn.cancel()
    await asyncio.sleep(0)
    assert not turn.done() and not harness.process_quiet
    finish_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await turn
    assert cleaned.is_set() and harness.process_quiet


async def test_0160_resume_omits_large_response_history_without_losing_context(tmp_path):
    script = tmp_path / "large_history.py"
    script.write_text("""
import json, sys
from pathlib import Path
log = Path(sys.argv[1])
for line in sys.stdin:
    message = json.loads(line)
    with log.open('a', encoding='utf-8') as file:
        file.write(json.dumps(message) + '\\n')
    method = message.get('method')
    if method == 'initialize':
        result = {'userAgent': 'Codex Desktop/0.160.0'}
    elif method == 'thread/resume':
        params = message['params']
        thread = {'id': params['threadId']}
        if not params.get('excludeTurns'):
            thread['turns'] = [{'id': 'old-turn', 'output': 'x' * 65536}]
        result = {'thread': thread, 'model': 'scripted-model'}
    else:
        continue
    print(json.dumps({'id': message['id'], 'result': result}), flush=True)
""", encoding="utf-8")
    log = tmp_path / "requests.jsonl"
    harness = CodexHarness(CodexConfig(version="0.160.0", max_frame_bytes=4096, stop_grace_seconds=0.3),
                           command=[sys.executable, "-u", str(script), str(log)])
    try:
        session = await harness.open(workspace=tmp_path, thread_id="existing-thread",
                                     tools=[], instructions="Continue the existing task.")
        assert session.thread_id == "existing-thread"
    finally:
        await harness.close()
    requests = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    resumed = next(item for item in requests if item.get("method") == "thread/resume")
    assert resumed["params"]["excludeTurns"] is True
    assert "history" not in resumed["params"]
    # The worker's own thread is resumed: nothing starts a new one or forks it.
    assert not any(item.get("method") in {"thread/start", "thread/fork", "turn/start"} for item in requests)
    assert harness.process_quiet
