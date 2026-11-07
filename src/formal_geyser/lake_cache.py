"""Bounded retention of disposable Lake caches, never published source/evidence."""
from __future__ import annotations

import json
import re
import shutil
import stat
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

import portalocker

from .registry import RegistryError

MAX_IDLE_SLOTS = 2
MAX_CACHE_BYTES = 4 * 1024**3
IDLE_SECONDS = 24 * 3600
GC_INTERVAL_SECONDS = 60
_KEY = re.compile(r"[a-f0-9]{20}(?:[a-f0-9]{44})?\Z")


@contextmanager
def lock_file(path: Path, *, shared=False, blocking=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RegistryError("Cache locks must not be links")
    handle = path.open("a+b")
    flags = portalocker.LOCK_SH if shared else portalocker.LOCK_EX
    if not blocking:
        flags |= portalocker.LOCK_NB
    acquired = False
    try:
        try:
            portalocker.lock(handle, flags)
            acquired = True
        except portalocker.exceptions.LockException:
            pass
        yield acquired
    finally:
        if acquired:
            portalocker.unlock(handle)
        handle.close()


def remove_tree(root: Path, child: Path) -> None:
    if child.is_symlink():
        raise RegistryError("Cache directories must not be links")
    if not child.resolve().is_relative_to(root.resolve()) or child.resolve() == root.resolve():
        raise RegistryError("Refusing to remove a path outside the owned cache")
    if child.exists():
        # Check descendants before any recursive mutation, so that no link is followed.
        for path in child.rglob("*"):
            if path.is_symlink():
                raise RegistryError("Cache descendants must not be links")

        def writable_remove(function, path, error):
            target = Path(path)
            if not target.resolve().is_relative_to(root.resolve()) or not target.is_file():
                raise error
            target.chmod(target.stat().st_mode | stat.S_IWRITE)
            function(path)

        shutil.rmtree(child, onexc=writable_remove)


def _size(path: Path) -> int:
    if path.is_symlink() or not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        try:
            if not item.is_symlink():
                info = item.stat(follow_symlinks=False)
                if stat.S_ISREG(info.st_mode):
                    total += info.st_size
        except FileNotFoundError:
            continue  # A cache entry can disappear between enumeration and stat.
    return total


def _slots(namespace: Path) -> list[Path]:
    """The namespace's build slots, most recently used first; cleared slots come last."""
    return sorted((p for p in namespace.iterdir() if p.name.isdecimal() and p.is_dir()
                   and not p.is_symlink()),
                  key=lambda p: (p / "last-used").stat().st_mtime
                  if (p / "last-used").exists() else 0, reverse=True)


def _used(slot: Path) -> float:
    """When a build last held the slot; a cleared slot counts from its clearing."""
    marker = slot / "last-used"
    return marker.stat().st_mtime if marker.exists() else slot.stat().st_mtime


@contextmanager
def _idle_slots(root: Path, namespace: Path):
    """Hold only one idle namespace at a time, including older slot-only locks."""
    with ExitStack() as locks:
        if not locks.enter_context(lock_file(root / "locks" / (namespace.name + ".lock"))):
            yield None
            return
        slots = _slots(namespace)
        for slot in slots:
            if not locks.enter_context(lock_file(slot / "build.lock")):
                yield None
                return
        yield slots


def _clear_slot(root: Path, slot: Path) -> None:
    if slot.is_symlink() or not slot.resolve().is_relative_to(root.resolve()):
        raise RegistryError("Refusing to clear a linked or unowned cache slot")
    # Keep the lock's inode/path stable, including while its handle is open.
    for path in slot.iterdir():
        if path.name == "build.lock":
            continue
        if path.is_dir():
            remove_tree(root, path)
        elif path.is_symlink():
            raise RegistryError("Cache entries must not be links")
        else:
            path.chmod(path.stat().st_mode | stat.S_IWRITE)
            path.unlink()


def _trim_active(root: Path, namespace: Path, *, now: float, max_idle: int,
                 idle_seconds: float, errors: list[str],
                 slot_limit: int | None = None) -> tuple[int, int]:
    """Clear old idle slots of a namespace that builds are using: (slots, bytes) removed.

    Its builds hold the namespace lock shared while they run, and with one pinned environment
    some build always does, so the namespace lock is not taken here and the artifact cache
    stays. Each slot is locked exclusively, without blocking, only while it is looked at or
    cleared: a slot a build holds is skipped, and a build that finds a slot held here takes
    the next free one rather than wait. The `max_idle` most recently used idle slots stay
    warm; another idle slot is cleared once it has gone unused for `idle_seconds`. A slot past
    `slot_limit` is cleared as soon as it is idle: builds no longer take it."""
    cleared = removed = kept = 0
    for slot in _slots(namespace):
        try:
            with lock_file(slot / "build.lock") as idle:
                if not idle or all(p.name == "build.lock" for p in slot.iterdir()):
                    continue  # A build holds it, or nothing is left to clear.
                if slot_limit is not None and int(slot.name) >= slot_limit:
                    size = _size(slot)
                    _clear_slot(root, slot)
                    cleared, removed = cleared + 1, removed + size
                elif kept < max_idle:
                    kept += 1
                elif now - _used(slot) > idle_seconds:
                    size = _size(slot)
                    _clear_slot(root, slot)
                    cleared, removed = cleared + 1, removed + size
        except (OSError, ValueError) as error:
            errors.append(str(error))
    return cleared, removed


def collect(root: Path, *, now=None, max_bytes=MAX_CACHE_BYTES,
            max_idle=MAX_IDLE_SLOTS, idle_seconds=IDLE_SECONDS, force=False,
            active_namespace: str | None = None, slot_limit: int | None = None) -> dict:
    """Best-effort GC. Active namespaces are never evicted, even above the target; only their
    old idle slots are cleared, one slot lock at a time (_trim_active)."""
    now = time.time() if now is None else now
    root.mkdir(parents=True, exist_ok=True)
    status = root / "retention.json"
    with lock_file(root / "retention.lock") as acquired:
        if not acquired:
            return {"state": "maintenance_busy"}
        try:
            previous = json.loads(status.read_text(encoding="utf-8"))
            if not force and now - previous["checked_at"] < GC_INTERVAL_SECONDS:
                return previous
        except (OSError, ValueError, KeyError):
            pass
        workspaces, caches = root / "workspaces", root / "cache"
        active = {active_namespace} if active_namespace else set()
        errors = []
        namespaces = sorted((p for p in workspaces.glob("*") if _KEY.fullmatch(p.name)
                             and p.is_dir() and not p.is_symlink()),
                            key=lambda p: p.stat().st_mtime)
        newest = namespaces[-1].name if namespaces else None
        cache_paths = [p for p in caches.glob("*") if _KEY.fullmatch(p.name)]
        measured = {}
        # Never traverse a namespace a build currently owns, or its artifact
        # cache. The resulting size is explicitly a partial, idle-only sample.
        for namespace in namespaces:
            if namespace.name in active:
                continue
            try:
                with _idle_slots(root, namespace) as slots:
                    if slots is None:
                        active.add(namespace.name)
                        continue
                    related = [p for p in cache_paths if p.name.startswith(namespace.name)]
                    measured[namespace] = (_size(namespace) + sum(_size(p) for p in related), related)
            except (OSError, ValueError) as error:
                errors.append(str(error))
        before = total = sum(size for size, _ in measured.values())
        for namespace, (_, related) in measured.items():
            if namespace.name in active:
                continue
            # The target is a soft budget: never make the most recent working
            # set cold merely because that one project has grown past it.
            over_budget = total > max_bytes and namespace.name != newest
            # A build may have started since the size sample. Reacquire locks
            # before clearing anything; do not hold other namespaces while sizing.
            try:
                with _idle_slots(root, namespace) as slots:
                    if slots is None:
                        active.add(namespace.name)
                        continue
                    for i, slot in enumerate(slots):
                        if i >= max_idle or now - _used(slot) > idle_seconds or over_budget:
                            size = _size(slot)
                            _clear_slot(root, slot)
                            total -= size
                    if over_budget or now - namespace.stat().st_mtime > idle_seconds:
                        for cache in related:
                            size = _size(cache)
                            remove_tree(root, cache)
                            total -= size
            except (OSError, ValueError) as error:
                errors.append(str(error))
        # An active namespace is never evicted or sized, but its idle slots still expire.
        slots_cleared = slot_bytes = 0
        for namespace in namespaces:
            if namespace.name not in active:
                continue
            try:
                cleared, size = _trim_active(root, namespace, now=now, max_idle=max_idle,
                                             idle_seconds=idle_seconds, errors=errors,
                                             slot_limit=slot_limit)
                slots_cleared, slot_bytes = slots_cleared + cleared, slot_bytes + size
            except (OSError, ValueError) as error:
                errors.append(str(error))
        result = {"checked_at": now, "bytes": max(0, total),
                  "removed_bytes": max(0, before - total) + slot_bytes, "target_bytes": max_bytes,
                  "size_scope": "observed_idle_namespaces",
                  "active_namespaces_skipped": len(active),
                  "active_slots_cleared": slots_cleared, "errors": errors}
        status.write_text(json.dumps(result), encoding="utf-8")
        return result
