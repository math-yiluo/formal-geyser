# Pinned Lean instance initialization

Run these commands from the application checkout. Initialization is explicit;
installing/importing the Python package never downloads dependencies or starts a
model. Native Git, Elan, and a supported native Codex executable must be installed.

```bash
uv run taskctl init ~/my-instance --domain lean --from-library ~/existing-library
```

Omit `--from-library` for an initially empty mathematical Library. The source
directory may also be the new instance directory. The importer copies Lean and
Markdown files, and the license, notice and copying files of ported code (`LICENSE`,
`NOTICE`, `COPYING`, `LICENCE-MIT`, `NOTICE.txt` and the like), while preserving
module paths and the original files. Other files are not imported. It excludes
Git/runtime/private directories and dependency/build caches. Lake project
configuration is not imported: this runtime compiles directly with its registered
environment. An existing `lean-toolchain` or Mathlib entry in `lake-manifest.json`
must agree with the fixed versions. Every imported Lean source requires its
same-stem, nonempty Markdown. Missing translations, proof holes, unsupported
imports, and compilation failures leave the instance unready, with diagnostics.
No Markdown translations or mathematical acceptance receipts are fabricated.

## Fixed versions

The application has one supported baseline, fixed in `bootstrap_pins.py`:

| Component | Release | Exact commit |
| --- | --- | --- |
| Lean | 4.34.0 | `293d5d0c0c3f3dded4688b3ccd6a33939ac5102b` |
| Mathlib | v4.34.0 | `5ed2965256430c3649e86755f9576b54eca72435` |

The [Mathlib release's toolchain file](https://github.com/leanprover-community/mathlib4/blob/5ed2965256430c3649e86755f9576b54eca72435/lean-toolchain)
selects this Lean release. Transitive package commits come from that exact
Mathlib `lake-manifest.json`; initialization never follows a moving branch or
runs a floating dependency update. It verifies the real compiler's Git identity
and the detached checkouts. No version-override option is exposed.

The new instance records an existing supported Codex executable and its observed
version. Automatic discovery prefers this application's optional local 0.160.0
installation, then a supported executable on PATH. `--codex-executable PATH`
selects an explicit installed executable. No Codex installation, application
version change, global default change, or older-instance migration is performed.

## One cache per instance

```text
my-instance/
  project.toml
  .dependencies/
    lm-<baseline-key>/      # Mathlib sources, exact packages, compiled artifacts
  .task-based/
    initialization.json   # Preparing, failed, or ready; exact environment and seed
    repository.git/       # Managed public Library and wiki
    workers/              # Each worker's own source workspace, no Mathlib copies
```

All workers in this instance share the same prebuilt roots. Different instances
have their own cache. An already installed matching Lean toolchain is reused;
if absent, Elan installs the fixed toolchain under this instance's cache without
changing the user's global toolchain or default.

A process lock serializes cache preparation. The official
[Mathlib cache command](https://github.com/leanprover-community/mathlib4/blob/5ed2965256430c3649e86755f9576b54eca72435/Cache/README.md)
downloads and unpacks the fixed build cache once. A full `import Mathlib` check
must succeed. If the cache is incomplete, initialization builds missing outputs
in the same shared checkout. Healthy retries reuse compiled outputs without
repeating cache downloads/builds. Verbose subprocess logs are bounded; builds
have no elapsed-time deadline. Disk exhaustion or interruption cannot mark a
partial cache ready. Partial owned downloads remain available for retry.

The initializer registers exact library manifests and the pinned compiler, then
records operator authorization of that execution environment. Project validation
uses the bundled Lake build adapter. It compiles the initial project source and initializes
the managed repository. Imported source is an operator-supplied baseline; it does
not acquire invented artifact IDs, peer-review verdicts, or proof receipts.
New publications still require the normal independent review and verification.

## Readiness and recovery

```bash
uv run taskctl --project ~/my-instance prepare
```

Use `prepare` to finish an interrupted initialization or verify an existing
instance, with the service stopped and all previous execution reconciled.
Its original captured source is retained across retries; a retry cannot silently
replace it with edits made since capture. Use a new instance for a different
initial baseline. Existing managed repositories are preserved.

Only a complete `.task-based/initialization.json` and matching cache/environment
receipts allow Lean run admission. The receipt stays small, since every admission reads
it: the seed check records how many modules it compiled and their digest, and writes its
full report, every module named, to `.task-based/initialization-check.json`, whether the
check passed or not. The backend gates run start/resume, worker
claims, and native worker/helper launches. Removing the cache, changing its pins,
or withdrawing its environment blocks new execution. Routine admission performs
small identity/presence checks; compilation and publication retain the existing
full content-integrity verification.

Workers receive the prepared environment ID and Mathlib's source locations (also
`LEAN_SRC_PATH`). They compile and check with `lean_check`, which the project's Lean
service runs (see [the Lean service](lean-service.md)); their shells have no Lean or Lake
command, and they must not clone/update/rebuild Mathlib in individual workspaces. Initialization
never starts workers, resumes a stopped project, or authenticates a Codex profile.
Start a run explicitly after initialization succeeds.
