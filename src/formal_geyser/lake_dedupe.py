"""Share the identical compiled outputs of Lake build slots on disk (copy-on-write extents).

Each build slot holds a full copy of the Library's build: in the Linux pilot 8 slots of 12.4 GB,
86 GB, nearly all of it the same bytes, since Lake restores a slot's outputs from its artifact
cache by copying. On a filesystem with shared extents (XFS with reflink, btrfs) the kernel's
FIDEDUPERANGE makes two identical files share their blocks after comparing them itself: each keeps
its own inode, size and timestamps, so a slot's receipt still matches, and a later write to either
copies first. Hard links would not do: a link changes the shared inode's change time, which every
other slot's receipt records. Where extents cannot be shared (ext4, tmpfs) the first attempt says
so and nothing more is tried.

Only `.olean` files and their parts are shared. Lean writes them to a temporary file and renames
it, so a rebuild replaces a shared file rather than writing into it. A slot is shared only while
idle, under its build lock, a bounded number of bytes per pass; what a pass shared is remembered by
the file's identity, so later passes compare only what changed.
"""
from __future__ import annotations

import errno
import os
import struct
from pathlib import Path
from typing import Callable

from .lake_cache import lock_file

FIDEDUPERANGE = 0xC0189436  # _IOWR(0x94, 54, struct file_dedupe_range)
CHUNK = 16 * 1024**2  # Bytes compared in one request; kernels cap a request near this.
SAME, DIFFERS = 0, 1
UNSUPPORTED = {errno.EOPNOTSUPP, errno.ENOTTY, errno.EINVAL, errno.EXDEV, errno.ENOSYS, errno.EPERM}
SUFFIXES = (".olean", ".olean.private", ".olean.server")


def dedupe_range(source: int, target: int, length: int) -> int:
    """Share `length` bytes of `target` with `source`, both open; the bytes shared. Raises OSError
    when the filesystem cannot share extents; stops at the first chunk that differs."""
    import fcntl

    shared = 0
    while shared < length:
        size = min(CHUNK, length - shared)
        request = bytearray(struct.pack("=QQHHI", shared, size, 1, 0, 0) + struct.pack("=qQQiI", target, shared, 0, 0, 0))
        fcntl.ioctl(source, FIDEDUPERANGE, request)
        done, status = struct.unpack_from("=Qi", request, 40)
        if status < 0:
            raise OSError(-status, os.strerror(-status))
        if status != SAME or done == 0:
            break
        shared += done
    return shared


def _outputs(slot: Path) -> dict[str, os.stat_result]:
    root = slot / ".lake" / "build" / "lib" / "lean"
    found = {}
    for directory, _, names in os.walk(root):
        for name in names:
            if name.endswith(SUFFIXES):
                path = Path(directory) / name
                found[path.relative_to(root).as_posix()] = path.lstat()
    return found


def _identity(stat: os.stat_result) -> tuple[int, int, int, int]:
    return stat.st_size, stat.st_mtime_ns, stat.st_ino, stat.st_dev


class SlotDeduper:
    """Shares identical outputs among a Lake root's build slots, across passes of one process."""

    def __init__(self, lake_root: Path, *, dedupe: Callable[[int, int, int], int] = dedupe_range):
        self.root = Path(lake_root)
        self.dedupe = dedupe
        self.supported: bool | None = None
        self.done: dict[str, tuple[int, int, int, int]] = {}  # Path shared -> its identity then.

    def run(self, budget_bytes: int) -> dict[str, int | bool | None]:
        """One bounded pass over every namespace; what it compared and shared."""
        compared = shared_bytes = files = 0
        workspaces = self.root / "workspaces"

        def report() -> dict[str, int | bool | None]:
            return {"compared": compared, "shared": shared_bytes, "files": files, "supported": self.supported}
        if self.supported is False or not workspaces.is_dir():
            return report()
        for namespace in sorted(path for path in workspaces.iterdir() if path.is_dir() and not path.is_symlink()):
            slots = sorted((path for path in namespace.iterdir() if path.is_dir() and not path.is_symlink()),
                           key=lambda path: (len(path.name), path.name))
            references: dict[str, tuple[Path, int]] = {}  # Relative path -> (a copy, its size).
            for slot in slots:
                with lock_file(slot / "build.lock") as held:
                    if not held:
                        continue  # A build holds it: its outputs are changing.
                    for relative, stat in _outputs(slot).items():
                        path = slot / ".lake" / "build" / "lib" / "lean" / relative
                        reference = references.setdefault(relative, (path, stat.st_size))
                        if reference[0] == path or reference[1] != stat.st_size:
                            continue
                        if self.done.get(str(path)) == _identity(stat) or compared >= budget_bytes:
                            continue
                        shared = self._share(reference[0], path, stat.st_size)
                        if shared is None:
                            return report()
                        compared += stat.st_size
                        shared_bytes += shared
                        files += 1
                        try:
                            self.done[str(path)] = _identity(path.lstat())
                        except FileNotFoundError:
                            pass
        return report()

    def _share(self, source: Path, target: Path, size: int) -> int | None:
        try:
            with open(source, "rb") as read, open(target, "rb+") as write:
                shared = self.dedupe(read.fileno(), write.fileno(), size)
        except FileNotFoundError:
            return 0  # Replaced meanwhile; the next pass looks again.
        except OSError as error:
            if error.errno in UNSUPPORTED and not self.supported:
                self.supported = False
                return None
            return 0
        self.supported = True
        return shared
