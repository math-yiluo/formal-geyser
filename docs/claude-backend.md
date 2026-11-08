# Claude workers

TaskHub can run a worker on Claude Code instead of Codex. The choice is made per
worker: each worker keeps the backend it was created with, because a native session
cannot move between them. The operator chooses the backend for workers created next.

Claude workers run on the Linux host (see [the Linux host](linux-host.md)), where Claude
Code's command sandbox uses bubblewrap and socat: both must be installed, WSL 1 is refused,
and so is WSL interop, which would let a sandboxed command start Windows programs. Their
sandbox cannot read the operator tokens, the systemd and D-Bus sockets, either backend's
stored sign-in or Windows drives. They are billed only to the operator's Claude
subscription; API-key billing is refused.

## Setup

1. Install the extra: `uv sync --extra claude`. It pins `claude-agent-sdk==0.2.163`,
   which bundles Claude Code 2.1.286. Both versions are checked at every activation.
2. Sign in once, through Anthropic's own flow, in the configuration directory the
   project uses (`claude.home`, by default
   `~/.local/share/formal-geyser/claude`):

   ```bash
   CLAUDE_CONFIG_DIR=~/.local/share/formal-geyser/claude claude
   ```

   Run `/login`, choose the Claude subscription account, then quit. The credential
   stays in that directory's `.credentials.json`, which no sandbox can read; TaskHub
   never reads, stores, or forwards it.
3. On claude.ai, keep **Settings > Usage > Usage credits** off. Fast mode and usage
   beyond the plan draw on those credits. Claude Code's fast mode is disabled for
   workers, and the runtime stops a turn as soon as it reports overage.
4. `project.toml` settings under `[claude]`: `model` and `effort` (`low` to `max`) of
   workers on Claude, with no default: where Claude workers can start, a run starts only
   once both are chosen here or in Project setup. Optional: `allowed_domains` (hosts sandboxed commands may
   reach; `["*"]` by default, which never includes local addresses), and `executable`
   (a Claude Code binary of the audited version instead of the bundled one).

## Usage limits

When the subscription reaches its 5-hour or weekly limit, new Claude sessions wait until the
reset and then start again by themselves, without Resume; a turn stopped by the limit is
recovered as usual. Other account errors, such as a missing sign-in, hold Claude dispatch until
Resume.

## Choosing the backend

```bash
taskctl backend show
taskctl backend set claude
taskctl backend set codex
```

`set` applies to workers created from then on, including new collaborators, over the
assignment plan's `worker_backends`; reviewers and the Integrator follow the plan, else the
project's `new_worker_backend`. Decision helpers always run on the Codex account. `show` reports the current choice and, for each backend,
its workers, its token usage, its live sessions and limit, and any account hold. The same status is at
`GET /v1/runtime/backend`; the switch is the operator command
`runtime.select_backend` with `{"backend": "codex" | "claude"}`. Selecting Claude
on a machine that cannot run it (not Linux, WSL 1 or WSL interop, bubblewrap or socat
missing, the extra not installed, no `claude.home`) is refused with the reason.

### Switching an existing worker

`worker.select_backend` with `{"worker_id", "backend"}` (the dashboard's *Switch to …* on a
worker's page) moves one worker to the other backend. A worker between conversations moves at
once, and so does one whose next turn is a message about no task in hand. A worker in a task
moves at its next activation, without a further turn in its old conversation:

- admission gives it a session on the new account, not the old one. It waits until that account
  can start one (not held and, for a worker, not full); the old account's limit does not apply,
  and its session counts toward the new account from admission;
- that activation continues its task in a new conversation there, started from its handoff file
  and a state file like a rotation (reason `backend`).

It takes no further session on the old account, whatever that account's limit. Records bound
to the old conversation make it wait until they settle: a parked interruption, which resumes in
its own conversation, an unresolved message delivery, or an unfinished validation of a call made
in it. A new task goes to it only when its new account has a free session, and starts it there.
Only if the new account is held between admission and the activation does that one turn
continue the old conversation, rather than opening a new one on the old account. Until it moves,
the worker's page says the request waits. Selecting its current backend cancels a waiting
request, for instance for a worker whose old conversation keeps it waiting. Reviewers, the
Integrator and the Dispatcher run on Codex and do not switch.

### Models and effort by role

`runtime.set_role_model` with `{"role": "worker" | "review" | "integrator", "backend", "model",
"effort", "apply_to_existing"}` chooses the model and effort for conversations that begin later:
workers on Codex and on Claude separately, reviewers and the Integrator on Codex, each its own.
`null` for both returns a role to what project.toml chooses for it (`[codex]` or `[claude]`
`model` and `effort` for workers, `[reviewers]` and `[integrator]` for the others, whose effort
may also be the older `review_effort` and `integrator_effort`). No role takes another's, and
none has a default: a run starts only once each has both.
Without `"apply_to_existing": true` (the dashboard's *Also existing* box, unticked by default)
a change reaches only workers created afterwards, and workers that move to that backend later:
each worker of that role on that backend keeps, for its new conversations, the model and effort
they started with until then (one that already kept earlier ones keeps those). With it, every
worker's next new conversation starts with the choice. `role_models` reports, per role and
backend, how many existing workers keep earlier choices (`kept_by_existing`). A conversation
records the model and effort it began with and keeps them while it continues, after an
interruption too; one bound before this was recorded keeps the project's. The Dispatcher's
model is `runtime.set_decision_model`, the Waker's `runtime.set_waker_model`, and the answer
deadline `runtime.set_answer_deadline`. `GET /v1/runtime/backend` reports all of them
(`role_models`, `decision_model`, `waker_model`, `answer_deadline`) and what is still to choose
(`missing_choices`), and the dashboard's Project settings edit them, before the first run too
([Project setup](project-setup.md)).

### The assignment plan

`project.toml` can fix the backend, and a worker's rotation mode, by role. For reviewers and
the Integrator the plan's backend applies; for new workers the operator's choice, once made,
overrides it:

```toml
[assignment]
worker_backends = ["codex", "claude"]      # by worker number, repeating: even Codex, odd Claude
worker_rotation = ["conservative", "conservative", "aggressive", "aggressive"]
reviewer_backend = "claude"
reviewer_rotation = "conservative"     # reviewers' own mode; unset follows the project
integrator_backend = "codex"
```

A list applies by worker number: Worker 0 takes its first entry, Worker 1 the next. The
rotation mode is stored as the agent's own mode, which the operator can still change. A
review is one conversation and usually one turn, and a later question about it starts from
its report, so reviewers gain nothing from a context rotation's handoff: `reviewer_rotation
= "conservative"` keeps them on idle rotation while workers rotate aggressively.
`taskctl backend show` reports the plan and the next worker's number and backend. When that
number's account is held or full while another account of the plan has a free session, the
new worker takes the first later number on a free account: each account keeps its numbers,
and the skipped ones stay unused. When no account of the plan is free, it waits.

### Concurrent sessions

```toml
[concurrency]
codex = 20            # sessions at once on each account; -1 (the default) sets no limit,
                      # 0 starts no worker session
claude = 26
launch_seconds = 30   # while ramping up, an account's worker sessions start this far apart
launch_ramp_minutes = 10  # ramping up ends this long after a start or resume (0: when full)
```

`runtime.set_concurrency` with `{"backend", "sessions"}` (the dashboard's Project settings)
changes an account's limit while TaskHub runs: -1 sets no limit, 0 starts no worker session
(reviewers and the Integrator still start), and `null` returns to `project.toml`. It applies
from the next admission, within two seconds; a lower limit stops no running session.
`GET /v1/runtime/backend` reports each account's live sessions, limit (`null` for no limit)
and its source. Before 2026-10-22, 0 set no limit: a limit saved that way reads as -1, and a
`project.toml` that sets 0 for no limit must now say -1.

A session runs from its admission until it pauses, submits, completes, waits or rotates. An
ordinary worker waits while its account's sessions are all in use; reviewers and the
Integrator start anyway, since authors and accepted work wait on them, and count toward the
total. Decision helpers do not count. While dispatch ramps up, an account's worker sessions
start `launch_seconds` after its own previous one, so a large project does not start all at
once; another account's starts do not count. Ramping up ends when the account first fills,
or `launch_ramp_minutes` after the service starts or the run starts or resumes, whichever
comes first; after that a freed session is taken at once. A run start or resume ramps up
again. A freed session goes to the first waiting work in
the usual admission order: a due Integrator batch, reviews, resumptions and satisfied
waits, messages, then the tasks the operator pinned, then other tasks by priority and
age.

### Idle rotation by backend

A conversation idle for `rotation.idle_minutes` (40) continues in a new one at its next turn,
because a resumed conversation whose prompt cache expired re-reads its whole context
uncached. Claude's cache expires an hour after its last use: in the pilot every Claude
conversation resumed within an hour read 96 to 98% of its first request from it, and every
one resumed later 7 to 9%. Codex's has no fixed expiry but is not kept either: of the
pilot's Codex conversations (2026-10-20 to 22), about nine in ten resumed within an hour
read most of their first request from the cache, seven in ten after one to two hours, three
in ten after two to four, and two of six after more than four (at nearly ten hours, 97%).
Misses within the hour gathered around repeated Codex sign-ins. Against a resumed miss
(about 90k uncached tokens), a rotated conversation's first turn read about 30k more
uncached tokens than a resumed hit, and it starts without the old conversation's memory.
`runtime.set_rotation_idle` with `{"backend", "minutes"}` (the dashboard's Project settings)
sets each backend's idle time from the next activation on; 0 turns idle rotation off and
`null` returns to `project.toml`. Rotation for a large context, in aggressive mode, is
unaffected: cached tokens still cost (Codex's measured weight is 0.4 of an uncached token).

A conversation idle past the limit still continues as it is while a context rotation of it
waits for its handoff, while a candidate it staged has an unsettled validation, and for a
message about a task its worker has left.

## Separate accounts

- **Holds.** A Claude account problem holds only Claude workers, and a Codex one only
  Codex workers. Claude holds are `claudeUsageLimit` (the plan's limit was reached, or
  usage credits would be used), `claudeSignInRequired`, and
  `claudeSubscriptionRequired` (an API key or other paid credential was selected).
  Each backend keeps its own hold record and marker file
  (`.task-based/claude-dispatch-hold.json` for Claude). Resume releases every hold.
- **Usage.** Runs keep `codex_tokens_used` and `claude_tokens_used` beside the total
  `tokens_used`. Decision helpers always run on the Codex account and are charged
  to it, and
  `taskctl run reconcile-usage --backend` names the account for reconciled tokens.

## Subscription-only billing

Claude Code prefers, in order: cloud-provider flags, `ANTHROPIC_AUTH_TOKEN`,
`ANTHROPIC_API_KEY` (always used in non-interactive mode when present),
`apiKeyHelper`, `CLAUDE_CODE_OAUTH_TOKEN`, Anthropic profiles, and finally the
`/login` subscription sign-in. Workers therefore start Claude Code with:

- every `ANTHROPIC_*` and `CLAUDE_CODE_USE_*` variable removed, and no settings
  files loaded (so no `apiKeyHelper`);
- a check, before any prompt is sent, that Claude Code reports a subscription account
  (`subscriptionType`) and no API key (`apiKeySource` of `none`); otherwise the
  activation stops and Claude dispatch is held;
- the same check again on each turn's `init` message, which interrupts the turn.

A `CLAUDE_CODE_OAUTH_TOKEN` from `claude setup-token` is also a subscription
credential and is passed through if the operator sets it.

## How a Claude activation works

Each turn starts `runtime/claude_bridge.py` in an owned process group. The bridge
drives the Agent SDK and speaks the same app-server subset as Codex, so turns, tool
calls, usage samples, compaction, interruption, and shutdown use the shared harness.

- **Tools.** Workers get Bash, Read, Write, Edit, Glob, Grep and TodoWrite, plus the
  TaskHub gateway as the MCP tools `mcp__taskhub__taskhub_actions` and
  `mcp__taskhub__taskhub_call`. Subagents, questions to a user, skills, web tools
  and background commands are unavailable. Decision helpers never run on Claude:
  they run on Codex, without tools. Claude Code refuses tool input that fails the
  schema it is shown, and Claude often writes an operation's fields beside
  `operation` or `arguments` as JSON text, so the bridge shows `taskhub_call` a lenient
  copy of its schema (only `operation` required, other fields allowed, `arguments` an
  object or text) and passes the call on in the exact shape: fields beside `operation`
  moved into `arguments`, text arguments parsed (anything but a JSON object is refused).
  The session record keeps the exact gateway definitions, which the catalog check
  compares, so this changes no stored catalog.
- **Containment.** Bash runs in Claude Code's sandbox (`failIfUnavailable`, no
  unsandboxed retry): writes only in the workspace and a per-session temporary
  directory, no connection to local addresses. File edits are allowed only inside
  the workspace, enforced by a permission rule and by a PreToolUse hook that resolves
  symbolic links. No mode approves anything else (`dontAsk`).
- **Isolation between workers.** One configuration directory serves all workers,
  so Claude Code's auto memory, `CLAUDE.md` loading, slash commands and `@`
  expansion in prompts are all off. Transcripts are kept under
  `projects/task-based/`, and TaskHub's per-session record (tool catalog, turn list,
  cumulative usage) under `task-based/sessions/`.
- **Sessions.** A new worker, and each new task, starts a session with an identity
  TaskHub chooses; later activations of the same task resume it until a rotation
  starts a new one. Sessions are never forked.
- **Time.** Commands and TaskHub calls have no practical time limit, as with Codex.
- **Differences from Codex.** Claude Code cannot add input to a running turn, so
  context refreshes after compaction reach the worker with its next tool result,
  and directed messages at its next turn or when it calls `sync` (its standing note
  says so).

## First run on a Mac

Before a real run, confirm on the target machine:

1. `taskctl backend set claude` succeeds and `taskctl backend show` lists Claude.
2. A first Claude activation reports a subscription account; with an
   `ANTHROPIC_API_KEY` exported in the service's shell it is still refused.
3. A worker's `lean_check` succeeds through the project's Lean service, and the
   worker cannot write the Library or another workspace, or reach the dashboard port
   on `localhost`.
4. Stopping the service leaves no `claude` process behind. Lean processes belong to
   the Lean service, which `formalizectl lean-service stop` ends.
