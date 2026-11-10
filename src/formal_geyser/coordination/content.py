"""Native content files and exact file references for coordination.

Git commit/path pairs own public bodies. SQLite owns operational state and file
references; it is not the body store. Blob snapshots remain validation evidence.
All installation methods are explicit so callers can journal their references.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO, Collection, Iterator, Mapping, Sequence, cast
from uuid import uuid4

import portalocker

from .draft_paths import DRAFT_DIRECTORY, is_draft_path
from .graph_paths import GRAPH_DIRECTORY, is_graph_path
from .snapshot_limits import MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_FILES
from .storage import BlobStore, canonical, digest, trust_directories, trusted_directory

if TYPE_CHECKING:
    from .repository import RepositoryStore

PRIVATE_DIRECTORY = ".private"
PUBLIC_WIKI_DIRECTORY = "wiki"
TASK_DIRECTORY = "tasks"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
VIEWS_KEPT = 3  # Published Library views always kept, the current one among them (prune_public_views).
VIEW_GRACE_SECONDS = 24 * 3600  # How long a superseded view stays for turns still reading it.
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,199}\Z")
# Windows device names, with any extension: no portable name may be one.
DEVICE_NAMES = re.compile(
    r"(?:con|prn|aux|nul|conin\$|conout\$|com[1-9¹²³]|lpt[1-9¹²³])(?:\..*)?\Z", re.I,
)
_RESERVED = {".git", ".gitconfig", ".gitmodules", ".gitattributes",
             ".taskhub-integration", ".task-based", PRIVATE_DIRECTORY}
_MAX_FILES = MAX_SNAPSHOT_FILES
_MAX_BYTES = MAX_SNAPSHOT_BYTES
# Directory listings for the case-alias check, valid while the directory's
# modification time is unchanged (creating, removing or renaming an entry changes it).
# Each holds that time, when its listing began (monotonic) and the names by folded name;
# the least recently used goes first. Clearing them all at once made every thread list
# the pilot's workers folder (184,000 entries, 0.46 s each) again together: about 420 times
# in five minutes, a third of the hub's CPU.
_LISTINGS: OrderedDict[str, tuple[int, int, dict[str, list[str]]]] = OrderedDict()
_LISTINGS_LOCK = threading.Lock()
_MAX_LISTINGS = 20000
# One thread lists a folder at a time; the others wait for its listing instead of taking their own.
_LISTING_SCANS = tuple(threading.Lock() for _ in range(64))
# Timestamps can be coarser than successive edits, so anything changed this recently
# is always read again rather than trusted by its metadata.
SETTLED_NANOSECONDS = 2_000_000_000
# A just-written Library view of this many files waits for its metadata to settle before its
# pointer moves, so it is read outside the content lock (NativeContentStore._settled_identity).
SETTLE_WAIT_FILES = 512


def settled(modified_ns: int) -> bool:
    return time.time_ns() - modified_ns >= SETTLED_NANOSECONDS


def unsafe_characters(value: str) -> bool:
    """Whether a path holds a control character or one Windows forbids in names."""
    return any(ord(char) < 32 or char in "\\:*?\"<>|" for char in value)


def unsafe_component(part: str, reserved: Collection[str]) -> bool:
    """Whether a relative path's component is empty, a traversal, ambiguous on Windows (a trailing
    dot or space), a device name, a `reserved` name (case-folded) or a Git short name."""
    folded = part.upper().casefold()
    return bool(not part or part in {".", ".."} or part.endswith((".", " "))
                or folded in reserved or folded.startswith(".git~")
                or re.fullmatch(r"git~[0-9]+", folded) or DEVICE_NAMES.fullmatch(part))


class ContentError(ValueError):
    """Unsafe native content or a mismatched immutable reference."""


class ContentConflict(ContentError):
    """An existing native body cannot be replaced without its current hash."""


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _identity(value: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value) or DEVICE_NAMES.fullmatch(value):
        raise ContentError("Invalid content owner ID")
    return value


def _hash(value: str) -> str:
    if not isinstance(value, str) or not _HASH.fullmatch(value):
        raise ContentError("Invalid content hash")
    return value


def _relative(value: str, *, private: bool = False) -> str:
    if (not isinstance(value, str) or not value or len(value.encode("utf-8")) > 1024
            or unsafe_characters(value)):
        raise ContentError("Invalid native content path")
    reserved = _RESERVED - {PRIVATE_DIRECTORY} if private else _RESERVED
    if any(unsafe_component(part, reserved) for part in value.split("/")):
        raise ContentError(f"Unsafe native content path: {value!r}")
    return value


def _paths(values: Sequence[str], *, max_files: int = _MAX_FILES) -> list[str]:
    if isinstance(values, (str, bytes)) or len(values) > max_files:
        raise ContentError("Content paths must be a bounded sequence")
    names: dict[str, str] = {}
    files: set[str] = set()
    for value in values:
        value = _relative(value)
        if value in files:
            raise ContentError("Duplicate native content path")
        files.add(value)
        pieces = value.split("/")
        for count in range(1, len(pieces) + 1):
            prefix = "/".join(pieces[:count])
            key = prefix.upper().casefold()
            if key in names and names[key] != prefix:
                raise ContentError("Portable names forbid case collisions")
            names[key] = prefix
    for value in files:
        if any("/".join(value.split("/")[:count]) in files
               for count in range(1, len(value.split("/")))):
            raise ContentError("A content path cannot be both a file and a directory")
    return sorted(files)


def _normal_path(path: Path) -> Path:
    """The absolute, normalized spelling of a path, as references record it."""
    return Path(os.path.abspath(path))


def _linked_info(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode)


def _component_info(item: Path) -> os.stat_result | None:
    try:
        info = _normal_path(item).lstat()
    except FileNotFoundError:
        return None
    if _linked_info(info):
        raise ContentError(f"Linked native content paths are forbidden: {item}")
    return info


def _plain(path: Path) -> Path:
    """Reject links before resolving or opening any owned path."""
    path = _normal_path(path)
    for item in (path, *path.parents):
        if trusted_directory(item):
            break
        _component_info(item)
    return path


def trust_root(path: Path) -> Path:
    """Verify a project root and its ancestors once, then skip them on later checks."""
    path = _plain(path)
    trust_directories(path)
    return path


def _cached_listing(key: str, stamp: int, asked: int) -> dict[str, list[str]] | None:
    """The cached listing of a folder last modified at `stamp`, or one begun after `asked`."""
    with _LISTINGS_LOCK:
        cached = _LISTINGS.get(key)
        if cached is None or not (cached[1] >= asked or (cached[0] == stamp and settled(stamp))):
            return None
        _LISTINGS.move_to_end(key)
        return cached[2]


def _same_folded_names(directory: Path, folded: str) -> list[str]:
    target = _normal_path(directory)
    asked = time.monotonic_ns()
    try:
        stamp = os.stat(target).st_mtime_ns
    except (FileNotFoundError, NotADirectoryError):
        return []
    key = str(target)
    names = _cached_listing(key, stamp, asked)
    if names is None:
        with _LISTING_SCANS[hash(key) % len(_LISTING_SCANS)]:
            # A listing begun while this one waited is as new as this check needs.
            names = _cached_listing(key, stamp, asked)
            if names is None:
                names = {}
                begun = time.monotonic_ns()
                try:
                    stamp = os.stat(target).st_mtime_ns
                    with os.scandir(target) as entries:
                        for entry in entries:
                            names.setdefault(entry.name.upper().casefold(), []).append(entry.name)
                except (FileNotFoundError, NotADirectoryError):
                    return []
                with _LISTINGS_LOCK:
                    _LISTINGS[key] = (stamp, begun, names)
                    _LISTINGS.move_to_end(key)
                    while len(_LISTINGS) > _MAX_LISTINGS:
                        _LISTINGS.popitem(last=False)
    return list(names.get(folded, []))


def _file_identity(paths: Sequence[Path]) -> tuple | None:
    """Size, modification time and file ID of each path; None when one is missing or was
    changed too recently for its metadata to identify its content."""
    result = []
    try:
        for path in paths:
            info = os.stat(_normal_path(path), follow_symlinks=False)
            if not settled(info.st_mtime_ns):
                return None
            result.append((info.st_size, info.st_mtime_ns, info.st_ino))
    except OSError:
        return None
    return tuple(result)


def _path(root: Path, relative: str, *, private: bool = False) -> Path:
    root = _plain(root)
    current = root
    for part in _relative(relative, private=private).split("/"):
        candidate = current / part
        # Each component is checked once; its ancestors were checked above. Names that
        # differ only in case would collide in a case-insensitive checkout of the content.
        _component_info(candidate)
        matches = _same_folded_names(current, part.upper().casefold())
        if matches and matches != [part]:
            raise ContentError("Portable names forbid case aliases")
        current = candidate
    if not current.is_relative_to(root):
        raise ContentError("Native content path escapes its root")
    return current


def _directory(path: Path) -> Path:
    path = _plain(path)
    _normal_path(path).mkdir(parents=True, exist_ok=True)
    _plain(path)
    if not _normal_path(path).is_dir():
        raise ContentError("A native content directory is required")
    return path


def _handle(stream: BinaryIO, expected: Path) -> None:
    info = os.fstat(stream.fileno())
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ContentError("Native content must be a regular file without hard links")
    actual = Path(os.readlink(f"/proc/self/fd/{stream.fileno()}"))
    expected = _normal_path(expected)
    if actual != expected:
        # The kernel's path of the open file resolves every link, so equality proves the
        # opened file was reached without one.
        raise ContentError("Opened native content path changed")
    if not os.path.samestat(info, expected.stat()):
        raise ContentError("Opened native content path changed")


def _read(path: Path, *, max_bytes: int = _MAX_BYTES) -> bytes:
    if type(max_bytes) is not int or not 0 <= max_bytes <= _MAX_BYTES:
        raise ContentError("Invalid native read byte limit")
    path = _plain(path)
    with _normal_path(path).open("rb") as stream:
        _handle(stream, path)
        before = os.fstat(stream.fileno())
        if before.st_size > max_bytes:
            raise ContentError("Native content exceeds the supported byte limit")
        body = stream.read(min(before.st_size + 1, max_bytes + 1))
        after = os.fstat(stream.fileno())
        _handle(stream, path)
    if len(body) > max_bytes:
        raise ContentError("Native content exceeds the supported byte limit")
    if (len(body) != before.st_size
            or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)):
        raise ContentConflict("Native content changed during the read")
    return body


def _write_new(path: Path, body: bytes, *, sync: bool = True) -> None:
    """Create one file. `sync=False` leaves its durability to one `_flush` of the whole tree
    before that tree becomes visible."""
    _directory(path.parent)
    path = _plain(path)
    with _normal_path(path).open("xb") as stream:
        _handle(stream, path)
        stream.write(body)
        stream.flush()
        if sync:
            os.fsync(stream.fileno())


def _flush(paths: Sequence[Path]) -> None:
    """Make files written with `sync=False` durable, in one flush of the file system.

    An fsync per file costs a journal commit each: on the pilot's busy ext4 disk 85 ms, so a
    Library view (1,174 files) took 100 s and an archive move (1,029 files) over a minute.
    """
    sync = getattr(os, "sync", None)
    if sync is not None:
        sync()
        return
    for path in paths:
        with _normal_path(path).open("rb") as stream:
            os.fsync(stream.fileno())


@dataclass(frozen=True)
class FileReference:
    path: str
    sha256: str
    size: int
    commit: str | None = None

    def __post_init__(self) -> None:
        _relative(self.path)
        _hash(self.sha256)
        if type(self.size) is not int or not 0 <= self.size <= _MAX_BYTES:
            raise ContentError("Invalid native file size")
        if self.commit is not None and (
                not isinstance(self.commit, str) or not _COMMIT.fullmatch(self.commit)):
            raise ContentError("An exact Git commit is required")

    def as_dict(self) -> dict[str, Any]:
        return {"path": self.path, "sha256": self.sha256, "bytes": self.size,
                **({"commit": self.commit} if self.commit is not None else {})}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FileReference:
        if (not {"path", "sha256", "bytes"} <= set(value)
                or set(value) - {"path", "sha256", "bytes", "commit"}):
            raise ContentError("Invalid native file reference fields")
        return cls(value["path"], value["sha256"], value["bytes"], value.get("commit"))


def verify_references(root: Path, references: Sequence[FileReference]) -> None:
    _paths([item.path for item in references])
    if sum(item.size for item in references) > _MAX_BYTES:
        raise ContentError("Native content tree exceeds the supported byte limit")
    for item in references:
        body = _read(_path(root, item.path))
        if len(body) != item.size or _sha(body) != item.sha256:
            raise ContentError(f"Native file integrity failure: {item.path}")


@dataclass(frozen=True)
class NativeFileView:
    root: Path
    files: tuple[FileReference, ...]
    manifest_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {"root": str(self.root), "manifest_sha256": self.manifest_sha256,
                "files": [item.as_dict() for item in self.files]}


@dataclass(frozen=True)
class _StagedTree:
    """A validated tree for `directory`: written and verified in `temporary`, or None when
    `directory` already existed."""
    directory: Path
    refs: tuple[FileReference, ...]
    manifest_body: bytes
    temporary: Path | None


@dataclass(frozen=True)
class PinnedPublicView(NativeFileView):
    commit: str
    snapshot: str

    @property
    def wiki(self) -> tuple[FileReference, ...]:
        return tuple(item for item in self.files
                     if item.path.startswith(PUBLIC_WIKI_DIRECTORY + "/")
                     and item.path.endswith(".md"))

    @property
    def knowledge(self) -> tuple[FileReference, ...]:
        return tuple(item for item in self.files
                     if item.path.startswith(("wiki/", "notes/")) and item.path.endswith(".md"))

    def as_dict(self) -> dict[str, Any]:
        return {**super().as_dict(), "commit": self.commit, "snapshot": self.snapshot,
                "wiki": [item.as_dict() for item in self.wiki],
                "knowledge": [item.as_dict() for item in self.knowledge]}


@dataclass(frozen=True)
class TaskFileView(NativeFileView):
    task_id: str
    revision: int
    contract_sha256: str

    @property
    def path(self) -> Path:
        return self.root / "task.json"

    def as_dict(self) -> dict[str, Any]:
        return {**super().as_dict(), "task_id": self.task_id, "revision": self.revision,
                "contract_sha256": self.contract_sha256, "path": str(self.path)}


def fresh_tree(parent: Path) -> Path:
    """A new directory for a public frozen tree: mkdir is exclusive, and UUID names are never
    reused or opened after a collision."""
    temporary = _plain(parent / (".native-content-" + uuid4().hex))
    os.mkdir(_normal_path(temporary))
    return temporary


class TreeView:
    """Many paths under one root checked as _path checks one (no linked component, no existing
    name that differs from one of its components only in case), with one listing per directory
    instead of an lstat and a listing per component of each path; and what each path is now.
    A worker's Library checkout checked its ~1,500 paths one by one: ~1.2 s of CPU under the
    interpreter lock at each admission. The listings are kept: write nothing while it is used."""

    def __init__(self, root: Path):
        self.root = _plain(root)
        self._listings: dict[str, dict[str, list[os.DirEntry]] | None] = {}

    def _listing(self, directory: str) -> dict[str, list[os.DirEntry]] | None:
        if directory not in self._listings:
            names: dict[str, list[os.DirEntry]] | None = {}
            try:
                with os.scandir(_normal_path(self.root / directory)) as entries:
                    for entry in entries:
                        names.setdefault(entry.name.upper().casefold(), []).append(entry)  # type: ignore[union-attr]
            except (FileNotFoundError, NotADirectoryError):
                names = None
            self._listings[directory] = names
        return self._listings[directory]

    def state(self, relative: str, *, private: bool = False) -> tuple[str, os.DirEntry | None]:
        """What is at `relative`: "absent", "file", "directory", "other" (not a regular file or
        directory) or "under_file" (a parent is a file), with its entry."""
        parts = _relative(relative, private=private).split("/")
        directory = ""
        for index, part in enumerate(parts):
            listing = self._listing(directory)
            matches = listing.get(part.upper().casefold(), []) if listing is not None else []
            if matches and [entry.name for entry in matches] != [part]:
                raise ContentError("Portable names forbid case aliases")
            if not matches:
                return "absent", None
            entry = matches[0]
            if entry.is_symlink():
                raise ContentError(f"Linked native content paths are forbidden: {self.root / directory / part}")
            if index < len(parts) - 1:
                if not entry.is_dir(follow_symlinks=False):
                    return "under_file", entry
                directory = f"{directory}/{part}" if directory else part
            elif entry.is_file(follow_symlinks=False):
                return "file", entry
            else:
                return ("directory" if entry.is_dir(follow_symlinks=False) else "other"), entry
        raise AssertionError("A relative path has at least one component")


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_NEW_FILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


class FreshTree:
    """Read-only files written into `root`, a folder this process just made (fresh_tree) and
    nothing else writes to, under names already validated together (_paths); then read back
    after one _flush. Every directory is made here and every file created exclusively, both
    through directory handles opened without following a link, so no component can be a link
    and each file is a new regular one: the per-component checks of _path, _plain and _handle
    are not repeated for each file. On the pilot they took 3.5 s of the preparation of one
    1,500-file candidate, holding the interpreter lock the coordination thread waits for."""

    def __init__(self, root: Path):
        self.root = _plain(root)
        self._handles = {"": os.open(_normal_path(self.root), _DIRECTORY_FLAGS)}
        self.written: list[Path] = []

    def __enter__(self) -> FreshTree:
        return self

    def __exit__(self, *_) -> None:
        for handle in self._handles.values():
            os.close(handle)
        self._handles = {}

    def _directory(self, path: str, *, create: bool) -> int:
        handle = self._handles.get(path)
        if handle is None:
            parent, _, name = path.rpartition("/")
            parent_handle = self._directory(parent, create=create)
            if create:
                os.mkdir(name, dir_fd=parent_handle)  # Exclusive: nothing else is in this folder.
            handle = self._handles[path] = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_handle)
        return handle

    def directory(self, name: str) -> None:
        """Make a directory that may stay empty."""
        self._directory(name, create=True)

    def write(self, name: str, body: bytes, *, sync: bool = False) -> None:
        """`sync=False` leaves durability to one _flush of `written` before the tree is visible."""
        parent, _, base = name.rpartition("/")
        descriptor = os.open(base, _NEW_FILE_FLAGS, stat.S_IREAD, dir_fd=self._directory(parent, create=True))
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            if sync:
                stream.flush()
                os.fsync(stream.fileno())
        self.written.append(self.root / name)

    def verify(self, references: Sequence[FileReference]) -> None:
        """Each file's flushed bytes are those referenced."""
        for item in references:
            parent, _, base = item.path.rpartition("/")
            descriptor = os.open(base, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 dir_fd=self._directory(parent, create=False))
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                body = stream.read(item.size + 1)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or len(body) != item.size
                    or _sha(body) != item.sha256):
                raise ContentError(f"Native file integrity failure: {item.path}")


class NativeContentStore:
    def __init__(self, state_dir: Path):
        self.state_dir = _plain(state_dir)
        self.project_root = trust_root(self.state_dir.parent)
        self.root = self.state_dir / "content"
        self.library_root = self.project_root / "library"
        self.tasks_root = self.project_root / TASK_DIRECTORY
        self._mutex = threading.RLock()
        self._view_lock = threading.Lock()  # One Library view is written at a time.
        self._pointer_lock = threading.Lock()  # One Library pointer move at a time.
        # Verified task revisions and Library views, with the file identities seen when
        # they were verified.
        self._task_cache: dict[tuple[str, int, str], tuple[list[Path], tuple, dict[str, Any]]] = {}
        self._views: dict[str, tuple[list[Path], tuple, PinnedPublicView]] = {}

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with self._mutex:
            _directory(self.root)
            lock = _plain(self.root / "lock")
            if _normal_path(lock).exists():
                info = _normal_path(lock).stat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ContentError("Invalid native content lock file")
            with portalocker.Lock(str(_normal_path(lock)), mode="a+b", timeout=30) as stream:
                _handle(cast(BinaryIO, stream), lock)
                if os.fstat(stream.fileno()).st_size:
                    raise ContentError("Native content lock contains unexpected data")
                yield

    def _tree(self, directory: Path, files: Mapping[str, bytes], metadata: Mapping[str, Any],
              *, commit: str | None = None, bulk: bool = False) -> NativeFileView:
        return self._install_tree(self._stage_tree(directory, files, metadata, commit=commit, bulk=bulk))

    def _stage_tree(self, directory: Path, files: Mapping[str, bytes], metadata: Mapping[str, Any],
                    *, commit: str | None = None, bulk: bool = False) -> _StagedTree:
        """Validate a tree and, unless `directory` exists, write and verify it in a fresh
        folder. Needs no lock: nothing else opens a fresh folder. `bulk` flushes once."""
        names = _paths(list(files))
        if any(not isinstance(files[name], bytes) for name in names):
            raise ContentError("Native tree bodies must be exact bytes")
        if sum(len(files[name]) for name in names) > _MAX_BYTES:
            raise ContentError("Native content tree exceeds the supported byte limit")
        refs = tuple(FileReference(name, _sha(files[name]), len(files[name]), commit) for name in names)
        manifest = {"schema_version": 1, **metadata, "files": [item.as_dict() for item in refs]}
        manifest_body = canonical(manifest).encode("utf-8")
        directory = _plain(directory)
        if _normal_path(directory).exists():
            return _StagedTree(directory, refs, manifest_body, None)
        _directory(directory.parent)
        temporary = fresh_tree(directory.parent)
        try:
            with FreshTree(temporary) as tree:
                tree.directory("files")  # An empty tree still has it.
                for name in names:
                    tree.write("files/" + name, files[name], sync=not bulk)
                tree.write("manifest.json", manifest_body, sync=not bulk)
                if bulk:
                    _flush(tree.written)
                tree.verify([FileReference("files/" + item.path, item.sha256, item.size) for item in refs])
        except BaseException:
            self._discard_temporary(temporary)
            raise
        return _StagedTree(directory, refs, manifest_body, temporary)

    def _install_tree(self, staged: _StagedTree) -> NativeFileView:
        """Rename a staged tree into place, or verify the identical one already there (another
        writer was first, and the staged copy is discarded)."""
        directory = staged.directory
        try:
            installed = False
            if staged.temporary is not None and not _normal_path(directory).exists():
                try:
                    os.rename(_plain(staged.temporary), _normal_path(directory))
                    installed = True
                except OSError:
                    if not _normal_path(directory).exists():
                        raise
            if not installed:
                if _read(directory / "manifest.json") != staged.manifest_body:
                    raise ContentConflict("An immutable native content manifest already differs")
                self._verify_tree(directory / "files", staged.refs)
        finally:
            if staged.temporary is not None and _normal_path(staged.temporary).exists():
                self._discard_temporary(staged.temporary)
        return NativeFileView(directory / "files", staged.refs, _sha(staged.manifest_body))

    @staticmethod
    def _replace_file(root: Path, path: str, body: bytes, expected_sha256: str | None) -> None:
        target = _path(root, path)
        if _normal_path(target).exists():
            if expected_sha256 is None or _sha(_read(target)) != expected_sha256:
                raise ContentConflict("Read the current file hash before replacing it")
        elif expected_sha256 is not None:
            raise ContentConflict("Expected native file is missing")
        _directory(target.parent)
        descriptor, name = tempfile.mkstemp(prefix=".native-replace-", dir=_normal_path(target.parent))
        temporary = _normal_path(Path(name))
        try:
            with os.fdopen(descriptor, "wb") as stream:
                _handle(stream, temporary)
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            target = _path(root, path)
            if expected_sha256 is None:
                if _normal_path(target).exists():
                    raise ContentConflict("Native file appeared during write")
                # Installation is atomic and cannot overwrite another file.
                os.link(_normal_path(temporary), _normal_path(target))
                _normal_path(temporary).unlink()
            else:
                if not _normal_path(target).exists() or _sha(_read(target)) != expected_sha256:
                    raise ContentConflict("Native file changed during write")
                os.replace(_plain(temporary), _normal_path(target))
        finally:
            if _normal_path(temporary).exists():
                _plain(temporary).unlink()

    def _discard_temporary(self, directory: Path) -> None:
        directory = _plain(directory)
        if (not any(directory.is_relative_to(root)
                    for root in (self.root, self.library_root, self.tasks_root,
                                 self.project_root / "staging", self.project_root / "artifacts",
                                 self.project_root / DRAFT_DIRECTORY,
                                 self.project_root / GRAPH_DIRECTORY,
                                 self.project_root / "submission-history"))
                or not directory.name.startswith(".native-content-")):
            raise ContentError("Invalid native content temporary directory")
        for current, dirs, files in os.walk(_normal_path(directory), topdown=False, followlinks=False):
            for name in files:
                path = _plain(Path(current) / name)
                _normal_path(path).chmod(stat.S_IREAD | stat.S_IWRITE)
                _normal_path(path).unlink()
            for name in dirs:
                _plain(Path(current) / name).rmdir()
        _normal_path(directory).rmdir()

    @staticmethod
    def _verify_tree(root: Path, refs: Sequence[FileReference]) -> None:
        actual = []
        root = _plain(root)
        if not _normal_path(root).is_dir():
            raise ContentError("Native content tree is missing")
        for current, dirs, files in os.walk(_normal_path(root), followlinks=False):
            for name in dirs:
                _plain(Path(current) / name)
            for name in files:
                path = _plain(Path(current) / name)
                actual.append(path.relative_to(root).as_posix())
        if _paths(actual) != [item.path for item in refs]:
            raise ContentError("Native content tree differs from its immutable manifest")
        verify_references(root, refs)

    @staticmethod
    def _verify_view(view: PinnedPublicView) -> None:
        """Read every file of a Library view and its manifest against their hashes."""
        NativeContentStore._verify_tree(view.root, view.files)
        if _sha(_read(view.root.parent / "manifest.json")) != view.manifest_sha256:
            raise ContentError("Pinned public manifest integrity failure")

    @staticmethod
    def _view_watch(view: NativeFileView) -> list[Path]:
        """The paths whose identities show a Library view unchanged: manifest, folders, files."""
        folders = {view.root, *((view.root / item.path).parent for item in view.files)}
        folders.update(parent for folder in list(folders) for parent in folder.parents
                       if parent.is_relative_to(view.root))
        return [view.root.parent / "manifest.json", *sorted(folders),
                *(view.root / item.path for item in view.files)]

    @staticmethod
    def _settled_identity(paths: Sequence[Path], files: int) -> tuple | None:
        """The identity of `paths`. For a view of SETTLE_WAIT_FILES files or more it waits once
        for just-written metadata to settle (off the coordination thread): reading that view
        again under the content lock would cost far more. A smaller one is not waited for."""
        identity = _file_identity(paths)
        if identity is None and files >= SETTLE_WAIT_FILES:
            time.sleep(SETTLED_NANOSECONDS / 1e9)
            identity = _file_identity(paths)
        return identity

    def _remember_view(self, watched: list[Path], identity: tuple | None,
                       view: PinnedPublicView) -> None:
        if identity is None:
            return
        with self._mutex:
            if len(self._views) >= 64:
                self._views.clear()
            self._views[view.commit] = (watched, identity, view)

    def public_view(self, repository: RepositoryStore, blobs: BlobStore, commit: str
                    ) -> PinnedPublicView:
        """Materialize a caller-authorized accepted commit without moving Lean modules.

        A view is immutable. Once verified, it is reused while every file and folder in it
        keeps its size, modification time and ID: no blob or view file is read again and
        no lock is taken. Any change sends the read through full materialization again.
        A new view is written and flushed outside the content lock, which it takes only to
        rename the tree into place: in the Linux pilot writing one under the lock (100 s)
        after each publication held up archive moves, task files and admission. A view that
        is already in place is verified without the lock: on the pilot on 2026-11-10 reading
        a 42,985-file view under it held up admission. Its identity is remembered once its
        files' metadata has settled, so the pointer move that follows reads nothing again.
        """
        if not isinstance(commit, str) or not _COMMIT.fullmatch(commit):
            raise ContentError("An exact public Git commit is required")
        cached = self._views.get(commit)
        if cached is not None and _file_identity(cached[0]) == cached[1]:
            return cached[2]
        with self._view_lock:
            cached = self._views.get(commit)  # Written meanwhile by the reader this one waited for.
            if cached is not None and _file_identity(cached[0]) == cached[1]:
                return cached[2]
            snapshot = repository.snapshot(commit, blobs)
            entries = blobs.get_json(snapshot)["files"]
            names = _paths([item["path"] for item in entries])
            if any(is_draft_path(name) for name in names):
                raise ContentError("Public drafts cannot enter a published Library view")
            if any(is_graph_path(name) for name in names):
                raise ContentError("Shared graphs cannot enter a published Library view")
            hashes = {item["path"]: _hash(item["sha256"]) for item in entries}
            files = {name: blobs.get(hashes[name]) for name in names}
            staged = self._stage_tree(_path(self.project_root, f"library/{commit}"), files,
                                      {"kind": "public", "commit": commit, "snapshot": snapshot},
                                      commit=commit, bulk=True)
            if staged.temporary is None:
                # Already in place: an immutable view is read without the lock. The view at
                # the repository head is the newest one, which pruning never removes.
                pending = NativeFileView(staged.directory / "files", staged.refs, _sha(staged.manifest_body))
                watched = self._view_watch(pending)
                identity = _file_identity(watched)  # Taken first: a later change cannot be missed.
                view = self._install_tree(staged)
            else:
                with self._locked():
                    view = self._install_tree(staged)
                watched = self._view_watch(view)
                identity = self._settled_identity(watched, len(view.files))
            result = PinnedPublicView(view.root, view.files, view.manifest_sha256, commit, snapshot)
            self._remember_view(watched, identity, result)
            return result

    def prune_public_views(self, *, now: float | None = None) -> list[str]:
        """Remove published Library views superseded more than VIEW_GRACE_SECONDS ago, keeping the
        current one and the VIEWS_KEPT newest; the commits removed. A view is only ever requested at
        the repository head, and the repository and blob store can make any view again, so an old
        one is a cache: in the Linux pilot 145 full views, 42.5 GB, none read after it was written,
        grew about 12 GB a day. A turn still reading a path it was given in an older view finds it
        gone after the grace period; its next turn is given the current one."""
        now = time.time() if now is None else now
        pointer = _path(self.library_root, "current.json")
        if not _normal_path(pointer).exists():
            return []
        current = json.loads(_read(pointer)).get("commit")
        views = sorted((path for path in _normal_path(self.library_root).iterdir()
                        if _COMMIT.fullmatch(path.name) and path.is_dir() and not path.is_symlink()),
                       key=lambda path: path.stat().st_mtime)
        removed = []
        for index, view in enumerate(views[:-VIEWS_KEPT]):
            # The time it was superseded: when the next newer view was installed.
            if view.name == current or now - views[index + 1].stat().st_mtime < VIEW_GRACE_SECONDS:
                continue
            temporary = _plain(self.library_root / f".native-content-{uuid4().hex}")
            with self._locked():
                if json.loads(_read(pointer)).get("commit") == view.name:
                    continue
                os.replace(view, _normal_path(temporary))
                self._views.pop(view.name, None)
            self._discard_temporary(temporary)
            removed.append(view.name)
        return removed

    def publish_current_view(self, view: PinnedPublicView,
                             *, expected_commit: str | None = None) -> Path:
        """Update navigation only after accepted publication; this never updates Git.

        The view's files are read before the content lock is taken, and not at all when
        `public_view` verified it and every file keeps its identity; under the lock only
        that identity is checked. Pointer moves run one at a time, and a caller that waited
        for another one moving the pointer to the same view returns without reading. On
        the pilot on 2026-11-10 three callers each read the 42,985-file view (272 MB) in
        turn under the content lock, and admission waited 279 s behind them.
        """
        if expected_commit is not None and (
                not isinstance(expected_commit, str) or not _COMMIT.fullmatch(expected_commit)):
            raise ContentError("An exact previous public commit is required")
        if view.root != _path(self.project_root, f"library/{view.commit}/files"):
            raise ContentError("Current Library must reference an owned pinned view")
        body = canonical({"schema_version": 1, "commit": view.commit,
                          "root": f"{view.commit}/files",
                          "manifest_sha256": view.manifest_sha256}).encode("utf-8")
        target = _path(self.library_root, "current.json")
        with self._pointer_lock:
            if self._names_view(target, body):
                return target
            watched, identity = self._verified_view(view)
            with self._locked():
                if identity is None or _file_identity(watched) != identity:
                    self._verify_view(view)  # Changed or unsettled since it was read: read it again.
                return self._move_pointer(target, body, expected_commit)

    @staticmethod
    def _names_view(target: Path, body: bytes) -> bool:
        """Whether the Library pointer already names this view (it is replaced atomically)."""
        try:
            return _normal_path(target).exists() and _read(target) == body
        except (OSError, ContentError):
            return False  # The locked move below reads it again and reports a fault.

    def _verified_view(self, view: PinnedPublicView) -> tuple[list[Path], tuple | None]:
        """The paths and identity of a view whose files are known to match it, reading them
        unless `public_view` verified it and none changed since."""
        cached = self._views.get(view.commit)
        if cached is not None and (cached[2] is view or cached[2] == view):
            identity = _file_identity(cached[0])
            if identity is not None and identity == cached[1]:
                return cached[0], identity
        watched = self._view_watch(view)
        # Taken before the read: a change after it cannot be missed.
        identity = self._settled_identity(watched, len(view.files))
        if identity is None:
            return watched, None  # Just written and small: read once, under the lock.
        self._verify_view(view)
        self._remember_view(watched, identity, view)
        return watched, identity

    def _move_pointer(self, target: Path, body: bytes, expected_commit: str | None) -> Path:
        """Replace the Library pointer under the content lock."""
        previous_sha = None
        if _normal_path(target).exists():
            previous_body = _read(target)
            if previous_body == body:
                return target
            if expected_commit is None:
                raise ContentConflict("Read the current Library commit before replacing it")
            try:
                previous = json.loads(previous_body)
                if (set(previous) != {"schema_version", "commit", "root", "manifest_sha256"}
                        or previous["schema_version"] != 1
                        or previous["commit"] != expected_commit
                        or previous["root"] != f"{expected_commit}/files"):
                    raise ContentConflict("Current Library changed or has unknown content")
                _hash(previous["manifest_sha256"])
            except (TypeError, KeyError, json.JSONDecodeError) as exc:
                raise ContentConflict("Invalid current Library navigation file") from exc
            previous_sha = _sha(previous_body)
        elif expected_commit is not None:
            raise ContentConflict("Expected current Library navigation file is missing")
        self._replace_file(self.library_root, "current.json", body, previous_sha)
        return target

    def task_files(self, task_id: str, revision: int, contract: Mapping[str, Any]) -> TaskFileView:
        """Install immutable authoritative task files before recording their DB references."""
        _identity(task_id)
        if type(revision) is not int or revision < 1:
            raise ContentError("Invalid native task revision")
        contract = dict(contract)
        if not isinstance(contract.get("objective"), str) or not isinstance(contract.get("context", ""), str):
            raise ContentError("Task objective and context must be Markdown text")
        contract.setdefault("context", "")
        sha = digest(contract)
        objective = contract.pop("objective")
        context = contract.pop("context")
        task = {"schema_version": 1, "id": task_id, "revision": revision,
                "contract_sha256": sha, "contract": contract,
                "objective": "objective.md", "context": "context.md"}
        files = {"task.json": canonical(task).encode("utf-8"),
                 "objective.md": objective.encode("utf-8"),
                 "context.md": context.encode("utf-8")}
        with self._locked():
            directory = _path(self.project_root,
                              f"{TASK_DIRECTORY}/{task_id}/revisions/{revision}-{sha}")
            view = self._tree(directory, files,
                              {"kind": "task", "task_id": task_id, "revision": revision,
                               "contract_sha256": sha})
        return TaskFileView(view.root, view.files, view.manifest_sha256, task_id, revision, sha)

    def read_task(self, task_id: str, revision: int, contract_sha256: str) -> dict[str, Any]:
        _identity(task_id)
        _hash(contract_sha256)
        if type(revision) is not int or revision < 1:
            raise ContentError("Invalid native task revision")
        # Immutable trees appear by atomic rename; readers do not need to create
        # or acquire the installation lock, including during read-only monitoring.
        # A revision is immutable: once verified, it is read again only when the size,
        # modification time or identity of one of its files or folders changes. A reuse
        # takes no lock, so a long archive move cannot delay it.
        key = (task_id, revision, contract_sha256)
        cached = self._task_cache.get(key)
        if cached is not None and _file_identity(cached[0]) == cached[1]:
            return deepcopy(cached[2])
        # Verifying an immutable revision needs no lock either: holders of the content lock
        # (archive moves, Library views) must not delay a task read on the coordination thread.
        directory = _path(self.project_root,
                          f"{TASK_DIRECTORY}/{task_id}/revisions/{revision}-{contract_sha256}")
        watched = [directory, directory / "manifest.json", directory / "files",
                   *(directory / "files" / name for name in ("context.md", "objective.md", "task.json"))]
        identity = _file_identity(watched)  # Taken first: a later change cannot be missed.
        try:
            manifest_body = _read(directory / "manifest.json")
            manifest = json.loads(manifest_body)
            refs = tuple(FileReference.from_dict(item) for item in manifest["files"])
            if (manifest.get("schema_version") != 1 or manifest.get("kind") != "task"
                    or manifest.get("task_id") != task_id
                    or manifest.get("revision") != revision
                    or manifest.get("contract_sha256") != contract_sha256
                    or any(item.commit is not None for item in refs)
                    or [item.path for item in refs] != ["context.md", "objective.md", "task.json"]):
                raise ContentError("Native task manifest does not match its reference")
            root = directory / "files"
            self._verify_tree(root, refs)
            task_body = _read(_path(root, "task.json"))
            task = json.loads(task_body)
            if (task.get("schema_version") != 1 or task.get("id") != task_id
                    or task.get("revision") != revision
                    or task.get("contract_sha256") != contract_sha256
                    or task.get("objective") != "objective.md" or task.get("context") != "context.md"):
                raise ContentError("Native task definition does not match its reference")
            contract = dict(task["contract"])
            for field in ("objective", "context"):
                contract[field] = _read(_path(root, task[field])).decode("utf-8")
            if digest(contract) != contract_sha256:
                raise ContentError("Native task integrity failure")
            structured = {key: value for key, value in contract.items()
                          if key not in {"objective", "context"}}
            expected_task = {"schema_version": 1, "id": task_id, "revision": revision,
                             "contract_sha256": contract_sha256, "contract": structured,
                             "objective": "objective.md", "context": "context.md"}
            expected_manifest = {"schema_version": 1, "kind": "task", "task_id": task_id,
                                 "revision": revision, "contract_sha256": contract_sha256,
                                 "files": [item.as_dict() for item in refs]}
            if (task_body != canonical(expected_task).encode("utf-8")
                    or manifest_body != canonical(expected_manifest).encode("utf-8")):
                raise ContentError("Native task metadata integrity failure")
            if identity is not None:
                if len(self._task_cache) >= 4096:
                    self._task_cache.clear()
                self._task_cache[key] = (watched, identity, deepcopy(contract))
            return contract
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ContentError("Invalid native task content") from exc

    def worker_notes(self, worker_id: str, workspace: Path | None = None) -> Path:
        _identity(worker_id)
        owned = _path(self.state_dir, f"workers/{worker_id}/work")
        if workspace is not None and _plain(workspace) != owned:
            raise ContentError("Private notes must stay inside the owned worker workspace")
        # Creating an existing folder is idempotent; this runs on the coordination thread for
        # private record reads, so it must not wait for the content lock.
        return _directory(_path(owned, f"{PRIVATE_DIRECTORY}/notes", private=True))
