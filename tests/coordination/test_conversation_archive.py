"""Native files of conversations TaskHub has let go are compressed in place once unchanged for an hour; bound
conversations are not touched; Codex's debug log keeps a day of rows and gives its pages back."""
from __future__ import annotations

import gzip
import os
import sqlite3
import time
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.conversation_archive import (
    ARCHIVE_AFTER_SECONDS,
    archive,
    bound_conversations,
    trim_codex_log,
)
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role


def rollout(home, conversation, age, body=b'{"type":"message"}\n' * 2000):
    path = home / "sessions" / "2026" / "11" / "07" / f"rollout-2026-11-07T00-00-00-{conversation}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    stamp = time.time() - age
    os.utime(path, (stamp, stamp))
    return path


def test_released_conversations_are_compressed_and_bound_ones_are_not(tmp_path):
    codex, claude = tmp_path / "codex", tmp_path / "claude"
    released, bound, recent = str(uuid4()), str(uuid4()), str(uuid4())
    old = rollout(codex, released, 2 * ARCHIVE_AFTER_SECONDS)
    kept = rollout(codex, bound, 2 * ARCHIVE_AFTER_SECONDS)
    fresh = rollout(codex, recent, 60)
    session = str(uuid4())
    transcript = claude / "projects" / "-home-taskhub-work" / f"{session}.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_bytes(b'{"role":"assistant"}\n' * 1000)
    stamp = time.time() - 2 * ARCHIVE_AFTER_SECONDS
    os.utime(transcript, (stamp, stamp))
    body, mtime = old.read_bytes(), old.stat().st_mtime_ns
    report = archive([codex], [claude], {bound})
    assert report["files"] == 2 and report["compressed_bytes"] < report["bytes"] / 5
    packed = old.with_name(old.name + ".gz")
    assert not old.exists() and gzip.decompress(packed.read_bytes()) == body
    assert packed.stat().st_mtime_ns == mtime
    assert kept.exists() and fresh.exists() and transcript.with_name(transcript.name + ".gz").exists()
    assert archive([codex], [claude], {bound})["files"] == 0  # Compressed files are not looked at again.


def test_a_pass_stops_at_its_budget(tmp_path):
    codex = tmp_path / "codex"
    for _ in range(3):
        rollout(codex, str(uuid4()), 2 * ARCHIVE_AFTER_SECONDS)
    assert archive([codex], [], set(), budget=1)["files"] == 1


@pytest.fixture
async def hub(tmp_path):
    initialize(tmp_path, ProjectConfig())
    result = Hub(tmp_path)
    yield result
    await result.close()


async def test_a_conversation_bound_to_a_worker_is_bound(hub):
    task = await hub.execute(Command(name="task.publish", key=uuid4().hex, payload={
        "contract": {"title": "Bound", "objective": "Hold a conversation."}}), Actor(role=Role.OPERATOR))
    run = await hub.execute(Command(name="run.start", key=uuid4().hex, payload={}), Actor(role=Role.OPERATOR))
    worker = await hub.execute(Command(name="worker.register", key=uuid4().hex, payload={}), Actor(role=Role.OPERATOR))
    await hub.execute(Command(name="task.claim", key=uuid4().hex, payload={
        "task_id": task["id"], "revision": task["revision"], "run_id": run["id"], "worker_id": worker["id"]}),
        Actor(role=Role.OPERATOR))
    thread = str(uuid4())
    await hub.execute(Command(name="worker.bind_session", key=uuid4().hex, payload={
        "worker_id": worker["id"], "thread_id": thread, "model": "m", "codex_home": None, "codex_profile": {},
        "settings": {"model": "m", "effort": "high"}}), Actor(role=Role.RUNNER))
    assert thread in await hub.db.read(bound_conversations)
    await hub.release_conversation(worker["id"], "rotation")
    assert thread not in await hub.db.read(bound_conversations)


def test_codex_log_keeps_a_day_and_gives_pages_back(tmp_path):
    path = tmp_path / "logs_2.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA auto_vacuum=INCREMENTAL")
    db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, ts_nanos INTEGER NOT NULL, "
               "level TEXT NOT NULL, target TEXT NOT NULL, feedback_log_body TEXT)")
    now = int(time.time())
    db.executemany("INSERT INTO logs(ts,ts_nanos,level,target,feedback_log_body) VALUES(?,?,?,?,?)",
                   [(now - 3 * 86400 + index, 0, "INFO", "codex", os.urandom(500).hex()) for index in range(6000)]
                   + [(now - 60, 0, "INFO", "codex", "recent")])
    db.close()
    report = trim_codex_log(path, now=now)
    assert report["rows"] == 6000 and report["pages_freed"] > 0
    check = sqlite3.connect(path)
    assert check.execute("SELECT count(*), max(feedback_log_body) FROM logs").fetchone() == (1, "recent")
    assert check.execute("PRAGMA freelist_count").fetchone()[0] == 0
    check.close()
    assert trim_codex_log(tmp_path / "missing.sqlite") == {"rows": 0, "pages_freed": 0}
