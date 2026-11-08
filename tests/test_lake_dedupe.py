"""Identical compiled outputs of idle build slots share their extents on disk; busy slots, other files and
filesystems that cannot share are left alone, and a file shared once is not compared again."""
from __future__ import annotations

import errno
import os
from pathlib import Path

from formal_geyser.lake_cache import lock_file
from formal_geyser.lake_dedupe import SlotDeduper, dedupe_range


def slot(root: Path, number: int, files: dict[str, bytes]) -> Path:
    path = root / "workspaces" / "namespace0000000000" / str(number)
    for relative, body in files.items():
        target = path / ".lake" / "build" / "lib" / "lean" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    (path / "build.lock").touch()
    return path


class Fake:
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def __call__(self, source: int, target: int, length: int) -> int:
        if self.error:
            raise OSError(self.error, os.strerror(self.error))
        self.calls.append((os.readlink(f"/proc/self/fd/{source}"), os.readlink(f"/proc/self/fd/{target}"), length))
        return length


def test_identical_outputs_of_idle_slots_are_shared_once(tmp_path):
    same = {"Langlands/A.olean": b"a" * 5000, "Langlands/A.olean.private": b"p" * 300, "Langlands/A.ilean": b"i"}
    first = slot(tmp_path, 0, same)
    second = slot(tmp_path, 1, {**same, "Langlands/B.olean": b"b" * 10})
    third = slot(tmp_path, 2, {"Langlands/A.olean": b"x" * 4999})  # Another size: never compared.
    fake = Fake()
    deduper = SlotDeduper(tmp_path, dedupe=fake)
    report = deduper.run(10**9)
    assert report == {"compared": 5300, "shared": 5300, "files": 2, "supported": True}
    pairs = {(Path(source).relative_to(tmp_path).parts[2], Path(target).relative_to(tmp_path).parts[2], length)
             for source, target, length in fake.calls}
    assert pairs == {("0", "1", 5000), ("0", "1", 300)}  # No .ilean, nothing of slot 2, nothing of slot 0 itself.
    assert first.exists() and third.exists() and second.exists()
    assert deduper.run(10**9)["files"] == 0  # Unchanged since: not compared again.
    (second / ".lake/build/lib/lean/Langlands/A.olean").write_bytes(b"a" * 5000)  # Rebuilt: a new identity.
    assert deduper.run(10**9)["files"] == 1


def test_a_slot_a_build_holds_is_skipped_and_the_budget_bounds_a_pass(tmp_path):
    slot(tmp_path, 0, {"M.olean": b"m" * 100, "N.olean": b"n" * 100})
    busy = slot(tmp_path, 1, {"M.olean": b"m" * 100, "N.olean": b"n" * 100})
    slot(tmp_path, 2, {"M.olean": b"m" * 100, "N.olean": b"n" * 100})
    fake = Fake()
    with lock_file(busy / "build.lock") as held:
        assert held
        report = SlotDeduper(tmp_path, dedupe=fake).run(100)
    assert report["files"] == 1 and all("/1/" not in target for _, target, _ in fake.calls)


def test_a_filesystem_that_cannot_share_is_tried_once(tmp_path):
    slot(tmp_path, 0, {"M.olean": b"m" * 100})
    slot(tmp_path, 1, {"M.olean": b"m" * 100})
    deduper = SlotDeduper(tmp_path, dedupe=Fake(errno.EOPNOTSUPP))
    assert deduper.run(10**9)["supported"] is False
    assert deduper.run(10**9) == {"compared": 0, "shared": 0, "files": 0, "supported": False}


def test_the_real_request_shares_or_reports_that_it_cannot(tmp_path):
    source, target = tmp_path / "a.olean", tmp_path / "b.olean"
    source.write_bytes(os.urandom(65536))
    target.write_bytes(source.read_bytes())
    with open(source, "rb") as read, open(target, "rb+") as write:
        try:
            shared = dedupe_range(read.fileno(), write.fileno(), 65536)
        except OSError as error:  # ext4 and tmpfs cannot share extents.
            assert error.errno in {errno.EOPNOTSUPP, errno.ENOTTY, errno.EINVAL, errno.EXDEV, errno.ENOSYS, errno.EPERM}
            return
    assert shared == 65536 and target.read_bytes() == source.read_bytes()
