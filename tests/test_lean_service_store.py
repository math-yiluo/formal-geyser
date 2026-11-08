"""The Lean service's database stays bounded: results kept result_days and compressed, an expired job keeps only
its telemetry, freed pages are given back, reuses are counted, and `compact` rewrites an existing database."""
from __future__ import annotations

import json
import os
import sqlite3
import time

import pytest

from formal_geyser.coordination.models import LeanServiceConfig
from formal_geyser.lean_service import store as store_module
from formal_geyser.lean_service.store import PACKED, Store, compact, unpack

DAY = 86400.0


def job(job_id, *, finished_at, key=None, state="done", outcome="completed", definitive=1, spec=None, result=None):
    return {"id": job_id, "content_key": key, "estimate_key": "lane:official", "lane": "official", "kind": "check",
            "owner": "w", "label": job_id, "spec_json": json.dumps(spec or {"kind": "check", "pad": os.urandom(2000).hex()}),
            "state": state, "outcome": outcome, "definitive": definitive,
            "submitted_at": (finished_at or time.time()) - 60, "finished_at": finished_at,
            "started_at": (finished_at or time.time()) - 30,
            **({"result_json": json.dumps(result)} if result is not None else {})}


def raw(path, sql, args=()):
    db = sqlite3.connect(path)
    try:
        return db.execute(sql, args).fetchall()
    finally:
        db.close()


def test_results_are_kept_result_days_by_default(tmp_path):
    assert LeanServiceConfig().result_days == 2
    with pytest.raises(ValueError):
        LeanServiceConfig(result_days=0)
    store = Store(tmp_path / "service.sqlite3")
    assert store.result_days == 2


def test_specifications_and_results_are_stored_compressed_and_read_back(tmp_path):
    path = tmp_path / "service.sqlite3"
    store = Store(path)
    store.insert(job("a", finished_at=time.time(), key="k"))
    store.update("a", result={"ok": True, "report": "y" * 10000})
    stored_spec, stored_result = raw(path, "SELECT spec_json,result_json FROM jobs WHERE id='a'")[0]
    assert stored_spec.startswith(PACKED) and stored_result.startswith(PACKED)
    assert len(stored_result) < 1000  # 10,000 repeated characters compress to little.
    row = store.get("a")
    assert json.loads(row["spec_json"])["kind"] == "check"
    assert json.loads(row["result_json"]) == {"ok": True, "report": "y" * 10000}
    # A row written before compression is read as it was.
    store.insert(job("old", finished_at=time.time(), result={"plain": 1}))
    raw_db = sqlite3.connect(path)
    raw_db.execute("UPDATE jobs SET spec_json=? WHERE id='old'", ('{"kind": "plain"}',))
    raw_db.commit()
    raw_db.close()
    assert json.loads(store.get("old")["spec_json"]) == {"kind": "plain"}
    assert json.loads(store.get("old")["result_json"]) == {"plain": 1}
    assert unpack(None) is None


def test_an_expired_job_keeps_only_its_telemetry_and_its_pages_are_given_back(tmp_path):
    path = tmp_path / "service.sqlite3"
    store = Store(path)
    assert raw(path, "PRAGMA auto_vacuum")[0][0] == 2  # A new database frees pages as it goes.
    now = time.time()
    for index in range(1200):  # More than one bounded step of PRUNE_ROWS.
        store.insert(job(f"old{index}", finished_at=now - 3 * DAY))
        store.update(f"old{index}", result={"report": os.urandom(1500).hex()})
    store.insert(job("recent", finished_at=now - 0.5 * DAY, key="r"))
    store.update("recent", result={"ok": 1})
    store.insert(job("ancient", finished_at=now - 100 * DAY))
    store.insert(job("live", finished_at=None, state="running"))
    counts = store.prune(now)
    # The 1,200 three-day-old jobs and the ancient one, expired before it is deleted.
    assert counts["expired"] == 1201 and counts["deleted"] == 1 and counts["pages_freed"] > 0
    expired = store.get("old7")
    assert expired["result_json"] is None and expired["spec_json"] == "" and expired["run_seconds"] is None
    assert expired["lane"] == "official" and expired["started_at"] is not None  # Its telemetry stays.
    assert store.get("recent")["result_json"] is not None and store.get("ancient") is None
    assert json.loads(store.get("live")["spec_json"])["kind"] == "check"  # A live job keeps what it runs.
    assert raw(path, "PRAGMA freelist_count")[0][0] == 0
    assert store.prune(now) == {"expired": 0, "deleted": 0, "pages_freed": 0}


def test_reuses_are_counted_with_the_results_age(tmp_path):
    store = Store(tmp_path / "service.sqlite3", result_days=1)
    now = time.time()
    store.insert(job("fresh", finished_at=now - 3600, key="k"))
    store.update("fresh", result={"ok": 1})
    store.insert(job("stale", finished_at=now - 2 * DAY, key="s"))
    store.update("stale", result={"ok": 1})
    assert store.reusable("k")["id"] == "fresh"
    assert store.reusable("s") is None  # Older than result_days.
    reuses = store.telemetry(now - DAY)["reuses"]
    assert reuses["count"] == 1 and reuses["result_days"] == 1
    assert 0.9 < reuses["age_hours_p50"] < 1.1


def test_compact_rewrites_an_existing_database_small_and_freeing_as_it_goes(tmp_path):
    path = tmp_path / "service.sqlite3"
    db = sqlite3.connect(path)  # As every database before 2026-11-08: no auto-vacuum.
    db.executescript(store_module.SCHEMA)
    db.commit()
    db.close()
    store = Store(path)
    now = time.time()
    for index in range(300):
        store.insert(job(f"old{index}", finished_at=now - 5 * DAY))
        store.update(f"old{index}", result={"report": "z" * 50000})
    store.insert(job("recent", finished_at=now - 3600, key="k"))
    store.update("recent", result={"ok": 1})
    store.close()
    assert raw(path, "PRAGMA auto_vacuum")[0][0] == 0
    result = compact(path, result_days=2, now=now)
    assert result["expired"] == 300 and result["bytes_after"] < result["bytes_before"]
    assert raw(path, "PRAGMA auto_vacuum")[0][0] == 2
    assert raw(path, "PRAGMA integrity_check")[0][0] == "ok"
    reopened = Store(path)
    assert json.loads(reopened.get("recent")["result_json"]) == {"ok": 1}
    assert reopened.get("old0")["result_json"] is None and reopened.get("old0")["lane"] == "official"
    assert not path.with_name(path.name + ".compact").exists()
