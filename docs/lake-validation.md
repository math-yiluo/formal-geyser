# Lake validation

Machine validation uses the pinned Lean and Lake executables, an incremental
build, a kernel replay, and inspection of the submitted declaration's kind, exact
proposition target and transitive assumptions. After every official build,
`lean_tools/KernelReplay.lean` (the algorithm of Lean's `leanchecker`) sends every
declaration of the built project modules to the kernel again, loading their other
imports once and trusting them as compiled; development checks skip it. A kernel
rejection is a finding naming the module; a replay that cannot load its environment
is unavailable. A closure whose exact compiled bytes already passed is not replayed
again. A long replay runs in several processes, each a run of consecutive modules in
import order; a group that reaches one name twice, as two modules that never import
each other may both declare it, is replayed again in halves, down to single modules. Definitions, proposition statements and proofs retain independent semantic
review against their Markdown and cited literature. Module-wide axiom audits are
not run.

## Persistent builds

Each instance owns `.task-based/objects/lake/workspaces/<environment>/<slot>/` and
`.task-based/objects/lake/cache/<environment>/`. The key binds Lean, Lake, registered
prebuilt manifests and an explicit build recipe version. Comments and unrelated
implementation edits do not invalidate caches. Lake traces own dependency changes.
Project source cannot replace a registered prebuilt module (case-insensitive path
comparison); new modules under an existing namespace remain allowed.

Lake restores requested cached artifacts into the local slot. Only the coordinator
supplies the package configuration; no dependency checkout or third-party build
runs. Frozen inputs remain separate from outputs. Inspection reads outputs in
place while the slot and namespace locks remain held. Never reuse a reported
`build_directory` after its compilation context has closed.

Ordinary compilation errors retain completed cache outputs. Cache receipts record
bytes, not proof acceptance. Interrupted commands, source mutations or corrupt
outputs invalidate the affected workspace. Removing source deletes that module's
facets; Lake handles downstream rebuilds without clearing unrelated outputs.
A slot reuses its earlier outputs while each keeps the size, modification and change
times and file ID in its receipt, for development and formal checks alike. An output
whose identity differs is hashed and must match the receipt; otherwise the slot's
outputs are discarded and the build starts clean. Integration builds the complete
final tree once.

Diagnostics are read from Lake's output, so a build stopped at its output bound has not
completed and leaves no receipt. The bound grows with the build: the environment's
`output_bytes`, or 4 KiB for each Lean source the build may compile if that is more
(`BUILD_OUTPUT_PER_SOURCE`). A fresh build of the Linux pilot's 21,144-module Library
printed about 520 bytes a module and was stopped at 1 MiB before this.

## Retention

Maintenance runs lazily at most once per minute, after acquiring the caller's
shared namespace lock. Sizing runs in a joined bookkeeping process or thread alongside the build; no
separate service is started and cleanup cannot outlive the namespace lock. It retains at most two
idle slots per namespace, expires idle data after 24 hours, and targets 4 GiB for
disposable cache/workspace data across the instance. Oldest idle namespaces are
evicted first. These are retention limits, not execution limits: active namespaces
are never evicted and may exceed the target. The most recently used namespace
also survives size pressure alone: 4 GiB is a soft target, not a maximum Library
size. Its idle slot-count and age policies still apply on idle maintenance passes.

A namespace that builds are using, which with one pinned environment is every
pass's own, keeps its artifact cache and is not sized, but its idle slots still
expire: its two most recently used idle slots stay warm, and every other idle slot
is cleared once unused for 24 hours. Maintenance never takes that namespace's lock.
It locks one slot at a time, exclusively and without blocking, only while it looks
at or clears that slot. It skips a slot a build holds, and a starting build that
finds a slot held takes the next free one rather than wait. A cleared slot keeps
its lock file; the next build there starts clean and restores or rebuilds its modules.

At most `[lean_service] build_slots` builds (default 8) hold a slot at once, and they take
only slots 0 to `build_slots` - 1: each slot holds a full copy of the Library's build, about
9.6 GB in the Linux pilot, where 13 builds at once filled the disk on 2026-11-06. A build past
the limit waits, first come first served, for a slot to free; a slot under the limit that
another process holds is passed over, and the build waits for the next free one. Maintenance
clears an idle slot numbered `build_slots` or above as soon as it is idle, since no build takes
it again; 0 lifts the limit.

The slots' outputs are mostly the same bytes, since Lake restores a slot from its artifact cache
by copying. Where the filesystem shares extents (XFS with reflink, btrfs), the Lean service's
hourly maintenance makes identical `.olean` files of idle slots share their blocks with the
kernel's FIDEDUPERANGE, at most `[lean_service] share_outputs_gib` compared a pass
(`lake_dedupe.py`). The kernel compares the bytes itself; each file keeps its inode, size and
timestamps, so a slot's receipt still matches, and a write to either copies first. Lean writes an
`.olean` to a temporary file and renames it, so a rebuild replaces a shared file. Hard links are
not used: a link changes the shared inode's change time, which every other slot's receipt
records. On ext4 or tmpfs the first attempt reports that extents cannot be shared, and the pass
does nothing more. A new machine's build volume should be XFS with reflink or btrfs to benefit.

Stable lock files are retained. Size walks skip active workspace trees, except an
idle slot being cleared under its lock, and their artifact caches, including older
builds holding only slot locks. Missing files during enumeration are ignored. The
collector rechecks locks before eviction. Builds waiting for maintenance's namespace
lock yield asynchronously and can be cancelled; a cache size walk cannot block the
coordinator's event loop. `retention.json` and build reports record observed idle
size (`size_scope`), reclaimed bytes, skipped active namespaces, the slots cleared
in them (`active_slots_cleared`) and cleanup errors. The size is a partial sample,
not the total size of all live caches. Source Library, artifact archives, receipts
and `.dependencies` are outside this collector. Maintenance errors do not stop work.

## Targets and outcomes

Statements are proposition definitions. Proofs must prove their exact approved
statement, including parameters. The existing inspector returns the target's
axiom dependencies using Lean's compiled metadata. `sorryAx`, assumptions outside
the environment allowlist and axioms defined in the project output tree reject.
This checks the submitted declaration, not every unrelated declaration in a file.
Source pairing and the explicit-hole prohibition remain mandatory. A warning's
absence after cache restoration is never evidence that a target has no holes.

Deterministic heartbeat/recursion errors during candidate compilation are source
findings: the author must revise the code or its explicit options. They do not
prove the mathematical statement false. Inspector exhaustion, process interruption,
out-of-memory and unreadable imports remain unavailable. Neither outcome pauses
the entire project.

Unavailable original checks retry locally after 5 minutes, doubling up to a
3-hour backoff. Only generation zero can run in author preflight; later retries
use the background driver and cannot hold up the author's next turn.
Each attempt appends evidence under a new idempotency key; no
model inference or submitted bytes are replayed or changed. Rejected and
operator-needed results do not auto-retry. Pauses and changed input bases remain
effective. The integration queue displays the missing validation and retry time.
An operator can queue a retry through `validation.retry` with `artifact_id` and
a nonempty `reason`, without stopping the service.

## Where builds run

Every build, kernel replay and inspection runs in the project's Lean service, which
schedules them across the machine's cores and memory and runs each Lean process in the
Codex sandbox; see [lean-service.md](lean-service.md). The status API and dashboard show its
running, queued and suspended jobs. Lean elaboration can perform IO: the sandbox lets it
write only its build slot, the Lake cache and its job's scratch directory. This accepted
execution boundary does not weaken target checks or independent semantic review.

## Environment pins and existing instances

New captures record `lake_executable_sha256` as well as the compiler pin and
library manifests. Older immutable checker registrations may supply Lake's hash
from their already registered full-toolchain manifest. Nothing implicitly adopts
a changed executable. Environments with neither form of pin require an explicit
new environment capture/approval; they report unavailable in the meantime.
Historical records and receipts are not rewritten. No checker process is used.

Restart while paused to load these changes; resuming is a separate operator action.
`formalizectl revalidate` can create new evidence for an original candidate. It
does not supply semantic acceptance or publish the Library.
