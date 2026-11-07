# FormalGeyser

Task-driven formalization of mathematical literature in Lean.

A self-contained application in which Codex workers collaborate on tasks and
formalize mathematics in Lean. The bundled coordinator (TaskHub) handles task
admission, waits, independent review, publication into a Git-managed Library,
and recovery. Workers do their actual work in ordinary files with native tools.
There is no embedded mathematical workload.

- **[DESIGN.md](DESIGN.md)** is the authoritative description of behavior.
- `docs/` holds topic guides, including [Lake validation](docs/lake-validation.md),
  [the Lean service](docs/lean-service.md) and [public knowledge](docs/public-knowledge.md).

## Where it runs

On a dedicated Linux virtual machine: a WSL 2 distribution on Windows, a Lima VM on
macOS. It needs systemd (each process tree runs in a cgroup of its own), and for Claude
workers bubblewrap and socat. See [the Linux host](docs/linux-host.md) for setting one up,
including the WSL settings that keep sandboxed commands away from Windows.

## Install

Use Python 3.13 and uv. From this directory:

```bash
uv sync --locked --group dev
uv run taskctl --help
uv run formalizectl --help
```

`taskctl` operates the coordinator; `formalizectl` provides Lean-domain commands.
Installing starts nothing and downloads no toolchain. Install the audited Codex release
into `.tools`, where initialization looks for it first; the script checks each download's
sha512 against the release's own:

```bash
uv run python scripts/install_codex.py
```

## Create a Lean instance

```bash
uv run taskctl init ~/project --name 'Lean workspace' --domain lean --from-library ~/existing-library
```

- Omit `--from-library` for a Library with no mathematics. Starter knowledge
  guidance is included under `wiki/`, with `wiki/README.md` as its entry point.
- Initialization pins Lean and Mathlib (`bootstrap_pins.py`). It prepares and
  verifies compiled dependencies in the instance's `.dependencies/`, checks the
  imported Lean/Markdown source through Lake, and initializes the managed repository.
  No worker can start until it succeeds.
- If setup is interrupted, retry with `taskctl --project PATH prepare` while the
  service is stopped. See [initialization](docs/initialization.md).
- For non-Lean collaboration use `--domain general`.
- Add `--github-sync OWNER/REPO` for optional Git sharing.

### Codex home

Lean workers use the isolated Codex home `~/.local/share/formal-geyser/codex`.
Override it with `[codex].home` in `project.toml` or `taskctl init --codex-home PATH`.
Sign in with the ChatGPT subscription before running workers (on arm64, the package is
`codex-linux-arm64` and the directory `aarch64-unknown-linux-musl`):

```bash
CODEX_HOME=~/.local/share/formal-geyser/codex \
  .tools/codex-0.160.0/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/bin/codex login
```

## Run and supervise

```bash
uv run taskctl --project ~/project serve --port 8765
```

1. Sign the browser in: `uv run taskctl --project ~/project dashboard` prints a link to
   `http://127.0.0.1:8765/` that holds the project's operator token; open it once. Every
   operator request needs that token, which sandboxed commands cannot read; a browser keeps
   it in an HTTP-only cookie. From Windows or macOS, the VM's forwarded `localhost` works.
2. For the first run, use **Project setup**: enter the Task 0 goal, choose the agents'
   models and sessions under **Agents and sessions**, and click **Start**. See
   [project setup](docs/project-setup.md).

What the dashboard shows:

- A status bar says what the project is doing now (running, idle, paused, usage
  limit, not driven by this service) and offers its one action.
- Workers and reviewers appear in separate groups, with specific states and
  elapsed time for live turns. Anyone who asked you something or messaged you is
  highlighted. Each turn shows a one-line action summary and the visible reply
  first; the exact model input stays one click away.
- **Tasks** opens on an overview with progress, Task 0 first and reviews under
  their task; the paged task browser views remain.
- **Requests** lists sources and questions waiting for you.
- On a phone, **People** opens the worker list as a drawer.
- Viewing pages never starts a model or acknowledges messages.

Run control is yours:

- Pause, resume and stop are explicit operator actions. Pause interrupts turns in
  progress; Pause after turn lets them finish on their own and starts nothing after
  them. Both exist for everyone and for one person. **Controls** also pauses after
  turn, or resumes, all of Codex, Codex's workers or reviewers alone, or all of
  Claude; a group resumed while everyone else is paused runs in full.
- Completing Task 0 stops autonomous work.
- After a service restart, runs that were being driven resume automatically; a
  paused run stays paused until you resume it. `serve --no-resume` starts the
  service for inspection only.
- If Codex reports that the account's usage limit is reached (or that sign-in is
  required), no new turns start; turns already running finish on their own. The
  dashboard shows the reason. Switch or sign in to the account, then click
  **Resume**. Restarts and quota resets do not clear this wait.

## Lean commands

```bash
uv run formalizectl doctor --lean ~/.elan/toolchains/TOOLCHAIN/bin/lean
uv run formalizectl schema statement            # definition | statement | proof | build-candidate | environment
uv run formalizectl --project ~/project check ~/sources --environment ENVIRONMENT_ID --module Workspace.Entry
```

`check` compiles a directory's Lean files where they are under an approved environment.
It never accepts a proof (`proof_accepted: false`). Local execution must be enabled for
that environment, because Lean elaboration can run arbitrary code.

Every Lean and Lake process runs in the project's Lean service, one long-running process
that schedules the machine's cores and memory and runs Lean in the Codex sandbox. Clients
start it when needed; see [the Lean service](docs/lean-service.md):

```bash
uv run formalizectl --project ~/project lean-service status
```

### Acceptance policies

| Policy | What it establishes |
| --- | --- |
| `lean.definition@1`, `lean.statement@1` | Independent semantic review: literature, Lean and Markdown correspondence, plus exact dependencies. |
| `lean.proof@1` | Lake incremental build, kernel replay and exact proposition-target checks plus peer review and publication. |
| `lean.build@1` | Successful Lake compilation and kernel replay of frozen paired files; build evidence only. |
| `lean.peer@1` | Peer review of an ordinary work product, such as a report. |
| `lean.development@1` | Development comparison; always ends in `needs_operator`. |

Authored source must not contain `sorry`, `admit`, `axiom`, or `sorryAx`, nor the
commands and options that bypass the kernel or run code while the source compiles
(listed in [mathematical artifacts](docs/mathematical-artifacts.md)). Official machine
validation builds the candidate with Lake, replays every declaration of the built project
modules through the kernel, and checks the submitted declaration's allowed assumptions; it
does not audit every module's axioms. See [Lake validation](docs/lake-validation.md), and
[DESIGN.md](DESIGN.md) for batch integration.

## Development

The project root is a Git repository. Commit changes in reviewable steps; text files
use LF line endings.

```bash
uv run python scripts/test_parallel.py
uv run ruff check src tests scripts
uv run mypy src/formal_geyser --follow-imports=silent
```

`scripts/test_parallel.py` runs the same suite as `uv run pytest -q` in parallel
processes. The suite uses synthetic fixtures and never calls a model, but it runs real
processes in real cgroups, so it needs the Linux host's systemd user manager.

The real Lake/target tests are opt-in and require only the local Lean toolchain:

```bash
FORMAL_GEYSER_TEST_LEAN=~/.elan/toolchains/TOOLCHAIN/bin/lean uv run pytest tests/test_lake_build.py tests/test_lean_service_real.py -q
```

They use tiny language fixtures, never start model turns, and check incremental
reuse, affected imports, deleted modules, and exact proposition targets. Set
`FORMAL_GEYSER_TEST_INITIALIZATION_RECEIPT` to a prepared instance initialization JSON
to also test read-only use of its shared Mathlib cache.

## Guides

| Topic | Guide |
| --- | --- |
| Setup | [the Linux host](docs/linux-host.md), [initialization](docs/initialization.md), [project setup](docs/project-setup.md) |
| Lean | [the Lean service](docs/lean-service.md), [Lake validation](docs/lake-validation.md), [mathematical artifacts](docs/mathematical-artifacts.md) |
| Knowledge and tasks | [public knowledge](docs/public-knowledge.md), [shared DAGs](docs/shared-dags.md) |
| Storage | [coordination database and reports](docs/database-storage.md) |
| Backends | [Claude backend](docs/claude-backend.md) |
| Development | [test runs](docs/testing.md) |

## License

MIT; see [LICENSE](LICENSE).
