"""The Lean service's own records: jobs and their results, estimates and machine samples.

Jobs survive the service: one queued, running or suspended when the service stops is queued
again when it starts. Results of official checks are kept for reuse by content; telemetry
(queue time, running time, CPU time, peak memory, machine samples) is kept for tuning and
long-term evaluation.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id TEXT PRIMARY KEY,
  content_key TEXT,
  estimate_key TEXT NOT NULL,
  lane TEXT NOT NULL,
  kind TEXT NOT NULL,
  owner TEXT NOT NULL,
  label TEXT NOT NULL,
  blocking INTEGER NOT NULL DEFAULT 0,
  spec_json TEXT NOT NULL,
  state TEXT NOT NULL,
  outcome TEXT,
  result_json TEXT,
  definitive INTEGER NOT NULL DEFAULT 0,
  submitted_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  last_polled_at REAL,
  threads INTEGER NOT NULL DEFAULT 0,
  requeues INTEGER NOT NULL DEFAULT 0,
  suspensions INTEGER NOT NULL DEFAULT 0,
  suspended_seconds REAL NOT NULL DEFAULT 0,
  queue_seconds REAL,
  run_seconds REAL,
  cpu_seconds REAL,
  peak_private_memory INTEGER,
  peak_committed_memory INTEGER
);
CREATE INDEX IF NOT EXISTS jobs_live ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_content ON jobs(content_key, finished_at);
CREATE INDEX IF NOT EXISTS jobs_finished ON jobs(finished_at);
CREATE TABLE IF NOT EXISTS estimates(
  key TEXT PRIMARY KEY,
  memory INTEGER NOT NULL,
  committed INTEGER NOT NULL,
  processes INTEGER NOT NULL,
  seconds REAL NOT NULL,
  samples INTEGER NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS machine_samples(
  at REAL PRIMARY KEY,
  cpu_busy REAL,
  memory_available INTEGER,
  commit_available INTEGER,
  threads_in_use INTEGER NOT NULL,
  running INTEGER NOT NULL,
  queued INTEGER NOT NULL,
  suspended INTEGER NOT NULL
);
"""
RESULT_DAYS = 7  # Results of finished jobs, for reuse and for late readers.
TELEMETRY_DAYS = 90


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _rows(self, sql: str, args=()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._db.execute(sql, args).fetchall()]

    def insert(self, row: dict[str, Any]) -> None:
        names = ",".join(row)
        with self._lock:
            self._db.execute(f"INSERT INTO jobs({names}) VALUES({','.join('?' * len(row))})", tuple(row.values()))

    def update(self, job_id: str, **fields: Any) -> None:
        with self._lock:
            self._update(job_id, fields)

    def write_row(self, job_id: str, row: dict[str, Any], names: Iterable[str]) -> None:
        """Write these fields of a job as `row` holds them when the write is made, under the lock:
        of writes in flight at once, whatever their order, the last one made carries the latest
        values, and a write made again after it failed sets nothing back."""
        with self._lock:
            self._update(job_id, {name: row[name] for name in names})

    def _update(self, job_id: str, fields: dict[str, Any]) -> None:
        if "result" in fields:
            result = fields.pop("result")
            fields["result_json"] = None if result is None else json.dumps(result)
        assignments = ",".join(f"{name}=?" for name in fields)
        self._db.execute(f"UPDATE jobs SET {assignments} WHERE id=?", (*fields.values(), job_id))

    def get(self, job_id: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM jobs WHERE id=?", (job_id,))
        return rows[0] if rows else None

    def live(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM jobs WHERE state IN ('queued','running','suspended') ORDER BY submitted_at,id")

    def reusable(self, content_key: str, since: float) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM jobs WHERE content_key=? AND state='done' AND outcome='completed' "
                          "AND definitive=1 AND finished_at>=? AND result_json IS NOT NULL "
                          "ORDER BY finished_at DESC LIMIT 1", (content_key, since))
        return rows[0] if rows else None

    def estimate(self, key: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM estimates WHERE key=?", (key,))
        return rows[0] if rows else None

    def record_estimate(self, key: str, *, memory: int, committed: int, processes: int, seconds: float) -> None:
        """The observed peak of jobs like this one: a higher one at once, a lower one a tenth at
        a time, since a check Lake answered from its cache measures far less than a rebuild."""
        with self._lock:
            self._db.execute(
                "INSERT INTO estimates(key,memory,committed,processes,seconds,samples,updated_at) "
                "VALUES(?,?,?,?,?,1,?) ON CONFLICT(key) DO UPDATE SET "
                "memory=MAX(excluded.memory,CAST(memory*0.9 AS INTEGER)),"
                "committed=MAX(excluded.committed,CAST(committed*0.9 AS INTEGER)),"
                "processes=excluded.processes,seconds=excluded.seconds,"
                "samples=samples+1,updated_at=excluded.updated_at",
                (key, memory, committed, processes, seconds, time.time()))

    def sample(self, **fields: Any) -> None:
        names = ",".join(fields)
        with self._lock:
            self._db.execute(f"INSERT OR REPLACE INTO machine_samples({names}) VALUES({','.join('?' * len(fields))})",
                             tuple(fields.values()))

    def prune(self, now: float) -> None:
        with self._lock:
            self._db.execute("UPDATE jobs SET result_json=NULL WHERE finished_at<? AND result_json IS NOT NULL",
                             (now - RESULT_DAYS * 86400,))
            self._db.execute("DELETE FROM jobs WHERE finished_at<?", (now - TELEMETRY_DAYS * 86400,))
            self._db.execute("DELETE FROM machine_samples WHERE at<?", (now - TELEMETRY_DAYS * 86400,))

    def telemetry(self, since: float) -> dict[str, Any]:
        """Per lane: jobs, queue and running time, CPU time and peak memory; the machine's load."""
        lanes: dict[str, dict[str, Any]] = {}
        for row in self._rows("SELECT lane,outcome,queue_seconds,run_seconds,cpu_seconds,peak_private_memory,"
                              "peak_committed_memory,threads,requeues,suspensions FROM jobs "
                              "WHERE finished_at>=? AND started_at IS NOT NULL", (since,)):
            lane = lanes.setdefault(row["lane"], {"jobs": 0, "outcomes": {}, "queue_seconds": [], "run_seconds": [],
                                                   "cpu_seconds": 0.0, "peak_private_memory": 0,
                                                   "peak_committed_memory": 0, "requeues": 0, "suspensions": 0})
            lane["jobs"] += 1
            lane["outcomes"][row["outcome"]] = lane["outcomes"].get(row["outcome"], 0) + 1
            for name in ("queue_seconds", "run_seconds"):
                if row[name] is not None:
                    lane[name].append(row[name])
            lane["cpu_seconds"] += row["cpu_seconds"] or 0.0
            for name in ("peak_private_memory", "peak_committed_memory"):
                lane[name] = max(lane[name], row[name] or 0)
            lane["requeues"] += row["requeues"]
            lane["suspensions"] += row["suspensions"]
        for lane in lanes.values():
            for name in ("queue_seconds", "run_seconds"):
                values = sorted(lane.pop(name))
                lane[name] = ({"mean": sum(values) / len(values), "p50": values[len(values) // 2],
                               "p95": values[min(len(values) - 1, int(len(values) * 0.95))], "max": values[-1]}
                              if values else None)
        machine = self._rows("SELECT count(*) AS samples,avg(cpu_busy) AS cpu_busy,min(memory_available) AS "
                             "memory_available_min,avg(threads_in_use) AS threads_in_use,max(queued) AS queued_max "
                             "FROM machine_samples WHERE at>=?", (since,))[0]
        return {"since": since, "lanes": lanes, "machine": machine}
