# Design

This is the authoritative description of current behavior. Changes to behavior
update this file.

## 1. Purpose and boundaries

FormalGeyser is one Python 3.13 distribution: a coordination runtime
(`formal_geyser.coordination`, "TaskHub") plus a general domain and a Lean
domain. It contains no mathematical workload, proof corpus, or fixed reasoning phases.
LLM workers are Codex or Claude Code conversations that work in ordinary files. Each
worker keeps the backend it was created with; the operator chooses the backend for
workers created next, unless the project's assignment plan fixes it by role and worker
number, and each account runs at most its configured number of sessions at once
(`docs/claude-backend.md`). TaskHub coordinates
admission, waits, review, publication, and recovery with deterministic rules. It is
not a model supervising the workers' reasoning.

The project was called Task-Based Auto-Formalize until 2026-11-02. Names that existing
instances store or compare keep that form: the instance state directory `.task-based/`,
the `task-based/` directories in the Claude home, the Lean tools' `task-based-lean-*`
protocols, the Lake validation package names and the dependency cache's
`.taskbased-checkout.json`.

The project root is the only active implementation. Do not create alternate release
trees; use Git branches. Package and dependency versions stay as locked.

## 2. Vocabulary

| Term | Meaning |
| --- | --- |
| Task | A unit of work with objective, constraints, inputs, requirements, and acceptance conditions. Versioned by revisions. Separate from any conversation. |
| Task 0 | The root task that preserves the user's original request. Its completion stops autonomous work. |
| Worker | A persistent execution identity (`Worker 0`, `Worker 1`, …) with one workspace, at most one current task, and one current native conversation (a new one for each new task and after each rotation). |
| Reviewer | A disposable execution identity (`Reviewer 1`, …) that executes exactly one review task. Never reused. |
| Helper | A disposable decision: whether a task recruits, which idle worker takes an open task, or which subscribed waits a publication ends. Runs on the decision model (wake decisions on the Waker's) in a new conversation with no tools. Not a worker; it only decides. |
| Dispatcher | The system identity that owns assignment decisions. It never executes a task. |
| Waker | The system identity that owns wake decisions: after each publication of Lean, whether what a subscribed task published meets a waiting worker's need. It never executes a task. |
| Membership | A worker's standing in a task team. Distinct from active execution and from waiting. |
| Artifact | A mathematical contribution: a definition, proposition statement, or proof, submitted as a same-stem `.lean` + `.md` pair. Once published it is history; the Library keeps no per-artifact identity. |
| Note | Any other contribution (public wiki pages, reports). |
| Staging / archive / Library | Pending candidates; accepted-and-published artifacts; the current integrated source tree. Separate concepts. |
| Literature | The shared collection of reference documents (distinct from the Library). |
| Acceptance / publication / completion | Independent review verdict; successful integration into the Library; a task's evidence-backed completion decision. Three separate gates. |

## 3. Architecture

- **Native files are authoritative.** Task prose, wiki, literature, correspondence,
  staged and archived artifacts, private notes, and Library source are ordinary
  files. Workers read, search, and edit them with native tools. A missing or changed
  authoritative file fails closed; a database cache never overrides it.
- **SQLite is coordination only:** claims, execution bindings, waits and wakes,
  idempotency receipts, delivery positions, review and publication state, and
  indexes. The native backend (Codex or Claude Code) owns conversation history; Git
  owns published source history (published Library and wiki content is authoritative
  at its exact commit).
- **Coordination storage.** Audited SQL-only status and message-signal reads use four
  read-only WAL connections; mutations, native-file reconciliation and delivery
  creation stay serialized on one writer with FULL durability. Command replies retain
  the identities and state needed for exact replay, and native core replies reference
  canonical receipts. Large exact machine reports are compressed once in
  `.task-based/reports/`; the ledger keeps summaries and hashes, including negative
  process-quiescence evidence. Workspace snapshots and claim-time Library checkout run
  outside the writer; prepared input commits recheck execution identity and the
  recorded base, and a durable checkout marker gates model admission and makes claim
  retries harmless. Ordinary usage keeps one cumulative row per worker and epoch, and
  per-turn start/end totals; helper usage is kept as deltas. A project an older version
  made, on Windows, is not opened; it stays with that version. Idle bounded incremental
  vacuum and
  diagnostic-log rotation never expire command keys, evidence, transcripts or
  snapshots.
  See [storage details](docs/database-storage.md).
- **Narrow commit boundaries.** Exploration is free-form file work. TaskHub claims and
  joins tasks for workers; revising a task, staging a candidate, recording a verdict,
  and publishing use narrow gateway operations with snapshots and receipts.
- **Project-readable versus published.** Every project worker may read all project
  research files, including personal notes and talk histories. Decision helpers read
  nothing beyond their prompt.
  Personal files (`.private/`) and talk never
  enter published source or Git exports implicitly. Reading conveys no write,
  execution, review, or publication authority.

| Area | Responsibility |
| --- | --- |
| `coordination/hub.py`, `lifecycle.py`, `collaboration.py` | Tasks, membership, recruitment, assignment, waits, completion, shutdown |
| `coordination/runtime/runner.py`, `codex.py`, `helpers.py` | Activation, native Codex transport, steering, decisions |
| `coordination/runtime/claude.py`, `claude_bridge.py` | Claude Code workers through the Agent SDK, subscription-only, behind the same transport |
| `coordination/communications.py`, `talk.py` | Human messages, literature requests, private peer talk |
| `coordination/native.py`, `content.py`, `repository.py`, `integration.py` | Native files, staging and archives, Git integration and publication |
| `coordination/faults.py` | Local fault isolation and automatic recovery |
| `coordination/api.py`, `dashboard.js`, `monitoring.py` | Operator API, dashboard, read-only monitoring |
| `registry.py`, `schemas.py`, `publication.py`, `verification.py`, `lake_build.py` | Lean records, Lake builds, exact target validation |
| `lean_service/` | The Lean service: every Lean and Lake process, scheduled on the machine's cores and memory |
| `bootstrap*.py` | Pinned Lean/Mathlib initialization |

## 4. Tasks

- **Publishing.** Workers may publish tasks freely. The publisher identity is
  immutable provenance, not an office.
- **Maintenance rights.** Original publishing parties and current executor members
  may revise a task from the start, as equals. Every revision records author, reason,
  and prior/new version and requires the expected current version (no lost updates).
  Material requirement changes notify dependents; dependencies and evidence stay bound
  to the versions they reference. A revision that changes only the title, context or
  priority asks for the same: the task's candidates, reviews and evidence stay valid,
  and they meet a requirement on either revision. Maintenance never lowers the completion standard
  (a special case does not complete a general task) and never grants review
  eligibility.
- **Task 0.** The initial worker maintains its execution instructions and decomposes
  the work; the human's original request and constraints are preserved.
- **Completion** is a recorded declaration, separate from publishing:
  1. The declaring maintainer maps every acceptance condition to accepted, published
     evidence with exact references.
  2. One feedback opportunity goes to the other executor if it intends to keep
     working (not to a sleeper handling a message). It may name a concrete unmet
     condition after checkpointing. A wait on the declaration holds for its declarer
     once the feedback arrives; for anyone else, the feedback giver included, only
     once the declaration is decided.
  3. The maintainer answers concrete objections with evidence and decides. A bare
     assertion cannot answer an objection; an optional extension may become a new task.
  4. Finalization ends execution relationships without discarding conversations,
     notes, or unpublished work; workers become idle.
- **Artifact gate.** A task cannot close while a required contribution lacks
  independent acceptance plus successful publication. Rejected work awaiting
  correction keeps it open. A genuinely unnecessary contribution may be explicitly
  withdrawn with a reason, retained history, dependent notifications, and a fresh
  check of the acceptance conditions. The gate is rechecked at finalization, and a
  material requirement change re-evaluates the declaration.
- **Unavailable feedback** is resolved through recovery, never treated as consent.
  An unrecoverable pending declaration may be cancelled by the operator
  (`task.complete.cancel`) and declared again through all gates.
- **Lost evidence.** If a completed task's evidence becomes invalid, its historical
  completion stays recorded, its current evidence is marked invalid, and one linked
  repair task is created (not duplicated on repeated observation). Repair never
  restarts a stopped project or rehabilitates an old receipt.
- **Retirement.** An ordinary task is retired when all executors permanently leave
  or its saved execution cannot be recovered; publishers and dependents are notified.
- **Invited back.** While a task has an executor, that executor or the operator can bring
  back a worker that left it (`task.invite_back`, with a reason). Only a former executor
  returns, so a task still has at most two executors over its lifetime. The worker becomes a
  member again, like a paused member whose wait has just ended: a `reconsider` continuation
  (`invited_back`) from its last attempt's checkpoint and files, which admission resumes;
  a `task.invited_back` event tells it who invited it and why. A worker that is in a turn,
  recovering, holding an interrupted turn, or working on another unfinished task cannot
  be invited (`WORKER_BUSY`): the inviter messages it instead. A done or retired task, and
  a membership ended by completion, take no one back.
- **Planning DAGs.** Two project-wide planning graphs (definitions and proofs) belong
  to no task. Every active worker and reviewer may maintain them through `dag_update`
  (the Integrator only reads them); they are never acceptance evidence. An update
  based on an older committed revision merges disjoint node and edge edits with the
  current pair; overlapping changes, cycles and dangling references fail explicitly.
  Installation runs outside the transaction, and the commit checks the exact current
  revision the merge was prepared against. See [shared DAGs](docs/shared-dags.md).
- **Task 0 completion** stops automatic assignment, claiming, recruitment, and worker
  creation, brings active work to a safe stop, and keeps unfinished tasks, candidates,
  and reviews for explicit future resumption. Messages and dependency changes do not
  restart the project; only the operator does.

## 5. Workers, teams, and admission

- **One current task per worker**, with possible membership in several unfinished
  tasks. Pausing preserves membership, checkpoint, subscriptions, and maintenance
  rights; the task keeps its conversation unless a rotation replaces it.
- **Team size.** An ordinary task admits its initial executor and at most one
  recruited collaborator. A review admits exactly one executor and never recruits.
- **One-time recruitment.** At the end of the first executor's first turn, one
  decision answers yes or no: does the task need a collaborator? No: the task never
  recruits. Yes: one collaborator opportunity is advertised with a standard invitation;
  the executor may edit its invitation; withdrawing it, or explicitly pausing the task,
  cancels it permanently. The worker given the collaborator place joins with the
  invitation as its contribution plan, consuming the opportunity. Used and cancelled
  opportunities are distinct outcomes; neither can be reopened. A task ending in its
  first turn needs no decision.
- **Execution loop.**

  ```text
  Normal turn ends on U        -> continue U in the same conversation
  First executor's first turn  -> one-time recruitment decision
  Worker pauses U              -> U preserved; the worker is waiting, not idle
  U completes or is left       -> the worker is idle
  An ordinary task V opens     -> an idle worker chosen by one decision, or a new
                                  worker; either way V starts in a new conversation
  A review R is validated      -> a new reviewer in its own conversation, at once
  U's dependency changes       -> resume or reconsider U in its conversation (a new
                                  one if a rotation is due)
  Task 0 completes             -> stop autonomous admission; checkpoint the project
  ```

  A normal turn ending is neither completion nor a voluntary pause.
- **Idle.** A worker is idle when it has finished or left its previous task and has
  nothing else to resume: no saved continuation or parked interruption, no message to
  handle, no operator pause, and no account hold. A worker that paused its task to wait
  (`pause` with a predicate) is waiting, not idle, and takes no new task.
- **Assignment.** Every open ordinary opportunity (a task's first executor, or its
  collaborator place) gets a worker at once:
  - With no idle worker, a new worker takes it immediately. Workers are not capped.
  - Otherwise one decision asks: which of these idle workers suits the task best, or
    none? It sees only the task (title, kind, objective, the opening) and, for each idle
    worker, the titles of the tasks it executed; a worker carried over from another
    project also shows those the operator imported for it (`taskctl workers
    import-history`), after its own. "None", a name not offered, or a worker no longer
    idle gives the task a new worker.
  - One decision runs at a time, so each sees the workers still idle; tasks that need a
    decision wait for theirs. A decision whose outcome is unknown holds only its own
    task until it is reconciled. A task whose decisions failed twice (an error, or a
    claim that failed) gets a new worker, and so does one that would need a decision
    while the Codex account is held.
  - TaskHub rechecks eligibility atomically at claim; reviews are never offered
    (section 6).
  - A task set aside because an input is unavailable (not yet published, or withdrawn)
    can be claimed by no one. It returns by itself once all its inputs are available;
    a withdrawn input needs a revision of the task.
- **New task, new conversation.** A worker always starts a new task in a new
  conversation, including an idle worker taking another task and a worker an operator
  assigns with `task.claim` or `task.join` (workers themselves cannot claim or join):
  its identity, workspace, private notes, authorship, and correspondence stay; its old
  conversation does not.
  Notes for later work belong in the workspace. Only turns of the same task continue
  its conversation. In Test 2, Worker 0 ran one conversation for five days through 41
  compactions.
- **Idle retirement.** An ordinary worker with nothing of its own left (no task, no
  membership of an open task, no continuation, saved turn, message or completion feedback
  owed, and no open task it published) retires after 48 hours without an attempt. A
  retired worker's workspace, reviewers' included, is reclaimed only when it is exactly its
  last stored snapshot, read again in full; `.private` keeps its notes, records and
  material. In the Linux pilot ordinary workers were never retired: 723 of them held 44 GB
  after three days.
- **Rotation.** A conversation is replaced, at a safe point, by a new one that
  continues the same task. The prompt cache lasts about an hour: in Test 2, 501 turns
  resumed after more than an hour sent 74M uncached tokens in their first requests.
  Two modes, set for the project (`runtime.set_rotation_mode`, `taskctl backend
  rotation`) and per worker (`worker.set_rotation_mode`, `taskctl workers rotation`):
  - conservative (default): an activation whose conversation has been idle at least
    40 minutes (`[rotation] idle_minutes`) starts a new one;
  - aggressive: in addition, once a request's context exceeds 100k tokens
    (`context_tokens`) and has grown 50k since the conversation began (so one that
    starts large does not rotate again at once), the next tool result asks the agent to
    let its command finish, update its handoff file and journal and end the turn; the
    next activation starts a new conversation. Codex compaction remains the fallback
    inside a long turn.

  A new conversation on a task whose handoff file is already filled in (an earlier
  conversation's, or one carried over from another project) starts from it the same way,
  with a state file written then (reason `handoff`).

  The assignment plan can give reviewers their own mode (`[assignment] reviewer_rotation`);
  a review is one conversation, usually one turn, so a conservative reviewer is not asked
  for a handoff it would not use.
  Workers and reviewers rotate; the Integrator does not: every batch has its own new
  conversation, kept for the whole batch. A message for a worker that left the task
  does not rotate, since there is no task to continue. Codex keeps a conversation's
  developer instructions as they were when it began (or was last compacted), so each
  conversation records the digest of its standing instructions, which are the same for
  every mode of one conversation (an ordinary worker's message turns use its work text;
  the input says what the turn is). One whose instructions changed continues in a new
  conversation (reason `instructions`). A new conversation is not sent the updates its worker already
  had: its delivery cursor carries over, and the current state comes from its first
  input and the task's state file.

  A parked interruption resumes in its own thread (and rotates at its next
  activation if it must), and nothing rotates while a message delivery or a native-call
  validation bound to the thread is unsettled.
  Every turn records the mode in force, its first request's input and cached tokens,
  its largest context, and its own input, cached input and output tokens and requests
  (what the conversation's cumulative counts grew by; a repeated sample adds nothing);
  `taskctl backend rotation-summary` (`/v1/runtime/rotation`) compares the modes with the
  rotations by reason and handoff outcome. It prices Codex workers' turns, and their
  finished reviews, in credit units: uncached input tokens, with a cached input token at
  0.1 and an output token at 5 by OpenAI's published rates, or at 0.4 and 34 by Test 2's
  measured weekly quota, the two measures of the agent performance review.
  `taskctl backend rotation-memory` (`/v1/runtime/rotation/memory`) measures whether each
  new conversation kept the old one's working memory: actions and orienting actions before
  its first work, the share of files it touched first that the old one had just touched,
  operation error codes and Lean errors that recur, and check failures and returned reviews
  before and after, by mode against Test 2's compactions measured the same way (2 actions
  before the first work for workers and reviewers; 26.5% and 33.4% of files).
- **Handoff.** The agent keeps two files current while it works: the task's
  `.private/tasks/<task>/handoff.md` (Goal, Current state, Next step, Remaining plan,
  Key decisions, Dead ends, Key facts, Open questions; at most about 3,000 tokens) and
  its journal `.private/notes/journal.md`. After a reminded turn, before the worker is free
  for its next activation, the runtime checks the handoff file: changed since the reminder, Goal, Current state and Next step filled,
  every named file present, within the limit. An unusable file gets one more turn to
  be fixed; otherwise the new conversation starts from the runtime's state file alone.
  The new conversation's first input names the handoff file and the state file
  `.private/tasks/<task>/state.md`: TaskHub's records of the task, its team and the
  worker's candidates; the files the old conversation touched; its last replies; its
  recent errors. `history_search` searches the worker's earlier conversations.
- **Reviews and batches.** A reviewer's verdict is kept as a report file
  (`.private/reviews/<review>/report.md`) recorded with the review; a later question
  about the review starts a new conversation from it. In Test 2, 91 operator notices to
  finished reviewers resumed their whole old conversations, about 65M tokens. Each
  integration batch starts a new conversation.
- **Choices before a run.** No role has a default model or effort, and no role takes
  another's: workers on each backend Claude workers can start on, reviewers, the
  Integrator, the Dispatcher and the Waker each have their own, as does the answer
  deadline. Each is chosen in project.toml (`[codex]`/`[claude]` for workers,
  `[reviewers]`, `[integrator]`, `[decisions]`, `[waker]`, `[waits]`) or by the operator in
  Project setup's Agents and sessions panel. TaskHub starts without them, but a run
  starts or resumes (`run.start`, `run.resume`, `worker.resume`, `group.resume`) only once
  every one is chosen (`CHOICES_REQUIRED` lists what is missing; `GET /v1/runtime/backend`
  reports it as `missing_choices`).
- **Decisions** (recruitment and assignment) run on the Dispatcher's model, which the
  operator can change at runtime (`runtime.set_decision_model`, `taskctl backend
  decision-model`). Each runs in a new
  Codex conversation with no tools and a minimal self-contained prompt, and answers only
  its schema: `{"worker": one of the offered names or null}` or `{"decision": true or
  false}`. An answer outside the schema counts as none or no and is recorded as invalid.
  The conversation runs read-only over an empty directory, with Codex's native tools
  turned off except the command runner, which Codex 0.160 cannot turn off. A recruitment
  decision also reads the state and plan of the executor's handoff file. Assignment
  decisions belong to the Dispatcher and to the project: an unfinished one continues in
  the next run. A decision whose task changed before it ran ends without a model call;
  one a pause stopped is asked again and is not a failure. Two failed recruitment
  decisions mean no collaborator, as two failed assignment decisions mean a new worker.
  A new worker chosen while its account is held starts once the account is free,
  without asking again. In Test 2, 1,177 selection helpers each forked the whole worker
  conversation (a median of 166k tokens on the first request, almost all uncached),
  about 400M tokens in all, and they kept running while their workers paused.
- **No forks.** No conversation is forked: every worker, reviewer and decision starts
  its own. Histories never merge; results cross back through files, artifacts, and
  updates.
- **No global caps.** No active-worker, cumulative-turn, token, or turn-duration
  limit. Per-task admission, exclusive review execution, and the single integration
  turn (section 6) remain. Lean checks default to no timeout; an explicit check
  timeout is optional. Runs count usage per backend account (`codex_tokens_used`,
  `claude_tokens_used`) beside the total.
- **Turns.** Every turn runs in a fresh owned Codex process that is torn down at the
  turn boundary; conversation and files persist, shells and background processes do
  not. Before voluntarily ending a turn or a draining operation, workers must collect
  the results of everything they started. Native resume restores the original
  conversation context; the audited 0.160.0 transport requests metadata only,
  avoiding a full-history response that grows past the RPC frame limit.
- **Sleep and wake.** Workers express intention changes through `pause` (reason,
  checkpoint, and a predicate naming what ends it) and `resume_work`. Waiting runs no
  inference. A pause waits for one of three things; one with none is refused
  (`WAIT_CONDITION_REQUIRED`), and a member with nothing left to do leaves the task
  instead; another executor can invite it back. Since 2026-11-07 a pause takes no `requirements` (`PAUSE_REQUIREMENTS_REMOVED`);
  stored waits keep theirs and wake as before.
  - *Another task's results:* `subscriptions`, each a fixed task id and its `need`, the
    results awaited in words. The task must be another open one. After each integration
    batch that publishes Lean (anything but public notes), the Waker gets one decision
    covering every wait subscribed to a task that published in it: for each (wait, task)
    pair, the need and that task's artifacts published to the Library since the wait
    began (id, kind, declaration, statement declaration, entry module, task title,
    publication time), read when the decision runs. It answers one boolean per pair. A
    wake ends the wait (`reconsider`, `subscribed_task_changed`) and tells the worker
    only that its subscribed task changed; the worker checks the Library itself. An
    answer outside the schema, or none in time, asks again, five times in all; then the
    decision waits for the operator as an uncertain helper. A pause that stops it asks
    again later. A subscribed task that becomes done or retired ends the wait too
    (`subscribed_task_done`, `subscribed_task_retired`). The Waker's model and effort
    are chosen like every role's (see Choices before a run).
  - *Its own submission's result:* `validations`, `completion_declaration_id`.
  - *A person's answer:* `answers`, ids of questions the worker asked in peer talk, or
    `external_response_id`, the operator's answer to its request. Each such wait gets a
    deadline from the answer deadline (`waits.answer_deadline_minutes`, or the operator's
    `runtime.set_answer_deadline`); the driver ends overdue ones
    (`reconsider`, `answer_deadline`), reading the deadlines at most once a minute.

  `mode` combines all but subscriptions, each of which ends the wait by itself.
- **No wait cycles.** Task A waits on task B when a waiting worker of A subscribes to B,
  when a wait recorded before 2026-11-07 names B in a requirement, or when A's contract
  requires B while A is open. The graph is derived from these records whenever a
  subscription is made (`wait_graph.py`), so it never drifts. A subscription to the
  waiting task itself, or one that would close a cycle, is refused
  (`SUBSCRIPTION_CYCLE`), naming the cycle's tasks by id and title.
- **Messages and waits.** A directed human or eligible peer message steers into an active Codex
  turn (a Claude worker receives it at its next turn or when it calls `sync`), or
  wakes a sleeping worker for one message-handling turn in its current conversation
  (a new one if a rotation is due), after which it returns to waiting unless it
  resumes work. Simultaneous messages are
  combined; transport receipts never wake anyone. A satisfied recorded predicate
  resumes work; a message claiming success is not proof. A wait on a candidate that can
  no longer be accepted (rejected, withdrawn or superseded) is reconsidered, whoever
  waits. A requirement, in a predicate or a task contract, names an artifact or a task
  revision and no policy: it holds once the artifact's newest version is accepted, or the
  task is completed with an accepted output, whatever kind (definition, statement, proof)
  it is. A former `policy` in a stored or supplied requirement is ignored, also in
  comparisons of task substance.

## 6. Review, integration, and publication

- **Authorship.** Candidates identify all actual contributing workers, including
  authors of reused drafts. Coauthors must have existing identities but need not
  belong to the submission's task. Attribution grants no execution or maintenance
  permissions. Membership does not establish authorship.
- **Reviewer eligibility.** Authors and coauthors (including through new identities)
  are never assigned their artifact's review. They see its status and receive revision feedback.
- **Dispatch.** A review starts only after machine validation has accepted its exact
  candidate: the newest validation for the candidate's policy is an unqualified
  acceptance bound to the reviewed bytes and basis, the same evidence publication
  requires. An unavailable check is no receipt, and a machine rejection never gets a
  reviewer: it rejects the candidate, closes its review before it starts, and resumes
  the author, whose corrected candidate replaces the rejected one. In Test 2 reviews opened without it and reviewers waited 13 to 22 hours for
  it, or re-ran the build themselves. Once validated, every open review gets its own new
  reviewer as soon as admission allows, in a new conversation with no inherited
  history: no worker selects it, and no teammate's conversation is copied into it. The
  reviewer has normal filesystem visibility and no redaction. A fork would inherit the
  source's whole context at full price, since a fork shares no prompt cache with its
  source: in Test 2, forked reviewers started at a median of 175k tokens and used about
  three times the tokens of reviewers started in a new conversation, and a third of
  them were forks of a teammate on the reviewed candidate's own task.
- **Entry.** The author supplies a short introduction; the reviewer receives direct
  paths to the artifact, Library, wiki, literature, and acceptance conditions.
- **Checks.** Review must compare cited original literature with the Lean file and its
  paired Markdown, with concrete source locations and findings. Definitions and
  statements need three-way correspondence (literature, Lean, Markdown); proofs need
  Lean/Markdown correspondence and the right target statement, not the literature's
  proof route.
- **Verdicts.** Reject mathematical errors, mistranslation, or reproducible build or
  check failures, with concrete defects and revision suggestions; never repair the
  mathematics into a different result and approve it. A missing source, unavailable
  environment, or transient tool failure is an unresolved dependency, not a defect.
- **Review handoff.** Reviews remain parallel and independently bind each exact
  candidate. Acceptance ends the review execution. The original reviewer is parked
  for scoped Integrator questions, each answered in a new conversation from its
  recorded review report; it is never resumed to integrate. It retires after
  rejection or successful publication. An accepted reviewer recovered after a lost
  turn is parked again for those questions.
- **Integration notes.** Reviewers also check whether a candidate duplicates or
  overlaps existing Library content and where its files belong. An acceptance may
  carry optional `integration_notes` (overlaps, placement, remarks), shown with the
  contribution in the Integrator's `batch_status`. The Integrator organizes the
  Library by them: it moves files and same-topic modules together and replaces a
  duplicate with an identical existing definition; unifying different definitions
  needs a reviewed contribution. Notes are not acceptance evidence.
- **One persistent Integrator.** A separate non-numbered identity owns one fixed
  integration batch at a time, each batch in a new conversation. It cannot choose
  ordinary tasks, recruit peers, or create mathematical acceptance. Ordinary workers and
  reviewers remain uncapped. The internal integration task is not in the task pool.
- **Batch admission.** Public notes may start a separate batch immediately when
  idle (see Public knowledge below). Other batches start if at least five eligible accepted,
  unpublished contributions are queued, or the queue is nonempty and thirteen
  minutes have elapsed since the previous batch of mathematics finished (a batch of
  public notes does not restart this wait); the operator can change
  both numbers while the service runs (`runtime.set_integrator_batch`). Before the first batch,
  use the first eligible acceptance time. Freeze membership on admission; later
  arrivals wait for the next batch. In-progress and explicitly deferred items do
  not count. Model turn boundaries within a batch impose no batching delay.
  Operator pauses, account holds, recovery, and Task 0 shutdown still apply.
  A due batch is admitted before any other waiting work, resumptions and messages
  included; reviews come next, also before resumptions and messages. Before a batch is due, admission uses only a count/oldest-time SQL hint. Exact
  candidate eligibility is still checked immediately before freezing membership.
  The Integrator is enabled at the first admission of a repository project; it is
  the only integration path.
- **Integration.** Prepare one combined private workspace, explicitly stage
  conflict resolutions, and freeze its complete tree. Contributions keep no fixed
  place in the Library: the Integrator may split, merge, move and rename files,
  modules and declarations, keeping every reviewed meaning and every Lean file
  paired with matching same-stem Markdown.
  Changed mathematical meaning requires a revised artifact and independent review,
  not Integrator self-approval. Original acceptance and machine evidence remain
  necessary per candidate.
  When the cheap queue gate opens but exact admission must still wait, a negative
  hint lasts at most 30 seconds and is invalidated by queue/evidence/material-event
  changes. Filesystem-only repairs are found on the bounded refresh. Cached hints
  never authorize a batch: actual admission always rechecks current eligibility.
- **Final validation.** Pure public Markdown-only changes use the document
  checks described below. Other batches use one source scan (no `sorry`, `admit`, `axiom` or
  `sorryAx`, and no kernel-bypassing construct; section 7) and one Lake build of the
  complete final tree with its kernel replay; unchanged modules reuse persistent build
  outputs and earlier replays. The single report is recorded for every
  contribution under its own policy. There are no per-item path, declaration or
  target checks at integration. Missing, unavailable, rejected, stale, or
  qualified reports cannot authorize publication.
- **Publication.** Each independent acceptance is the semantic acceptance of its
  exact candidate against a fixed Library version; there is no separate per-item
  semantic re-confirmation. The Integrator is responsible for a semantically
  consistent integrated Library and submits the exact validated batch round.
  After process quiescence, one expected-head Git update publishes the batch, and
  each contribution's submitted record and receipts move to the accepted archive.
  A durable intent permits exact recovery after Git advances before the database
  commits. Changed inputs or Library head require new final validation. Sharing one
  final-tree build never reuses or waives any contribution's own independent review.
  Later Library changes never invalidate earlier acceptances; later work simply builds
  against the current tree. A worker that finds an error in the published Library
  publishes a correction task; the fix is an ordinary reviewed candidate that edits
  those files.
- **Isolation and questions.** Defer a blocked contribution with a reason, preserve
  it and earlier workspaces, rebuild the remaining batch, and validate again.
  Deferral is not rejection, withdrawal, or acceptance. Retry only after new
  evidence resolves the blocker. The operator may also retry from the queue, with
  a reason naming the new evidence. A Library head change alone is not a retry.
  Requeueing never changes the membership of a batch already in progress.
  The Integrator may contact an artifact's original reviewer and authors through
  ordinary peer talk, naming the artifact in the text. Active Codex recipients
  receive steering, active Claude recipients the next turn or `sync`; idle
  recipients get one reply activation. A reviewer answers in a new conversation
  started from its recorded review report (which names the candidate's files) and
  ends without new work. The Integrator never pauses: it waits for an answer by
  deferring the item and retries it once the answer resolves it, so a batch never
  stalls on a reply.
- **Integration workspace retention.** Keep current-batch workspaces and the most
  recently finished batch. At the next preparation, reclaim older finalized
  checkouts only when their bytes still match the retained immutable snapshot.
  Keep Git commits, snapshot objects and session receipts. Modified, unfrozen,
  linked, or extra private files are retained; this is conservative reclamation,
  not a hard disk quota. Maintenance runs off the coordination thread and records
  reclaimed bytes and paths. It never makes a failed deletion block integration.
- **Waiting and completion.** Independent review acceptance and successful batch
  validation do not satisfy a wait for a usable artifact. Actual accepted Library
  publication does. A requirement on a task also requires its evidence-backed
  completion. A worker can continue independent work instead of voluntarily
  waiting; messages can trigger a reply without satisfying its dependency.
- **Validation report transport.** Exact target inspection uses bounded private
  UTF-8 result files, independent of diagnostic pipe limits. Missing or incomplete
  results are unavailable. Machine evidence identifies `validation_method: lake-build`
  and records its kernel replay (`kernel_replay`); it never claims a module-wide axiom audit.
- **Reviewer recovery.** Prefer recovering the original reviewer. If recovery is
  impossible and its process has definitely ended, give the same review a new reviewer.
  Process failure is neither acceptance
  nor rejection; there is never more than one valid reviewer execution.
- **Artifact storage.** `staging/<id>/<version>/` holds pending candidates (durable);
  `artifacts/<id>/<version>/` holds accepted-and-published artifacts as submitted;
  `library/` is the current Library. A submission's `files/` holds the files that
  differ from the Library base its worker started from, with the Lean/Markdown pair;
  `base.json` names that base, whose published view has every other file (archives
  from before 2026-10-23 hold the whole snapshot). Library changes never erase archive history. Rejected and withdrawn submissions keep their files and
  reasons, separate from the archive. Corrections use explicit `replaces`, preserving
  authorship, coauthors, and outstanding obligations.
- **Public knowledge.** Public notes and wiki are one space, entered through
  `wiki/README.md`. It contains short Essential guidance and a Reference notes
  list (title, relative link, one-sentence purpose). Authors maintain entries with
  Markdown submissions under `wiki/` or `notes/` using `wiki_submit`; reviewers
  check accuracy, citations and the entry; the existing Integrator merges prose
  and maintains links. No additional maintainer worker or content registry.
- **Document integration.** When the Integrator is idle, eligible `note.wiki`
  contributions form a separate batch without the five-item/13-minute delay.
  They never preempt an active batch. A frozen base-to-final diff containing only
  unpaired `wiki/*.md` / `notes/*.md` changes uses nonempty UTF-8/README-presence checks without
  Lean; nested paths are allowed. Lean, paired Markdown, build files and all other
  changes retain normal domain validation. Reports bind the exact final snapshot;
  independent review, current-head checks and process quiescence remain mandatory.
  README classification, accurate summaries and valid links are review/integration
  responsibilities, not a new machine-enforced Markdown schema.
  Content-only note submission does not exist.

## 7. Lean domain

- **Artifacts.** Definition, proposition statement, and proof records, each
  pairing a `.lean` file with a same-stem `.md`; every Library Lean file keeps
  its own same-stem Markdown. Records carry no references to other mathematical artifacts; work
  imports the current Library by Lean name. Statements are `def`/`abbrev`
  returning `Prop`. Source scans reject explicit `sorry`, `admit`, `axiom`, and `sorryAx`,
  and constructs that record declarations the kernel never checked or run code while the
  source compiles: `debug.skipKernelTC`, `addDeclWithoutChecking`, `extern`,
  `implemented_by`, `#eval`, `run_cmd`, `run_elab`, `run_meta`, `initialize`. This second
  defense is deliberately incomplete; the kernel replay below is the real one.
  The inspector additionally checks the submitted declaration's transitive assumptions against the pinned allowlist and rejects project-declared axioms. It does not audit every module. Other Lean record types: build candidates and execution environments.
- **Kernel replay.** Compilation trusts what elaboration records: under
  `debug.skipKernelTC`, or through an unchecked addition, a module can record a theorem
  the kernel never checked. It compiles, `#print axioms` reports nothing, and target
  inspection accepts it. Every official build (proof, definition and statement,
  build-policy and final-tree validation, initialization) therefore sends each
  declaration the built project modules recorded to the kernel again, with
  `lean_tools/KernelReplay.lean`: Lean's own `leanchecker` algorithm, loading their
  other imports once. A rejection is a finding naming the module; a replay that cannot
  load its environment is unavailable. Development checks (`lean_check`,
  `formalizectl check`) skip it. A replay covers the requested modules and every
  project module they import, since replay trusts imports as compiled; a closure whose
  exact compiled bytes already passed, under the same environment and helper, is not
  replayed again (`.task-based/objects/lake/replays/`). On 10-19 the published Test 2
  Library (269 modules, 6025 declarations) replayed cleanly in 259 seconds.
- **Snapshot size.** A candidate snapshot holds its author's whole Library checkout, and the
  integration workspace holds the Library and its batch, so both grow with the Library. Each
  is bounded by the platform maximum of 100,000 files and 1 GiB and by the pinned
  environment's own limits. An environment recorded at the former maximum of 20,000 files
  allows the current maximum: in the Linux pilot the Library reached 19,999 files on
  2026-11-02, and every task pins that environment, so a replacement record would mean
  revising them all. A lower recorded limit still binds.
- **Author scope notes.** A record's `qualifications` is retained review material,
  not a machine-validation failure. The independent reviewer assesses it against
  the pinned task and records the disposition in the rationale. Essential scope
  and assumptions belong in the paired Markdown; record metadata is not injected
  into Library files. Reviewer and validation qualifications remain unresolved
  findings that block acceptance/publication, and machine checks remain mandatory.
- **Citations** are native literature references: `literature_id`, `content_sha256`,
  and a precise locator. Literature registration is not mathematical acceptance.
- **Proofs** name the Library statement they prove in `statement_declaration`.
  Proof tasks describe their target in prose and pin only the environment.
- **Proof acceptance** requires a successful Lake build of the candidate over its
  Library base, a theorem whose type is the named statement, and unchanged bytes
  for the statement's file, its import closure and their Markdown relative to that base,
  and a kernel replay of the built modules. Independent semantic review and final
  publication remain required. No module-wide axiom audit or registered checker runs.
  The existing inspector checks each submitted declaration's allowed assumptions.
- **Incremental builds.** Each instance retains `.task-based/objects/lake/workspaces/`
  and `.task-based/objects/lake/cache/`. A free persistent slot owns one build at a time;
  concurrent checks use separate slots, at most `[lean_service] build_slots` (8) of them:
  each holds a full copy of the Library's build, so a build past the limit waits, first come
  first served, for a slot to free, and an idle slot past the limit is cleared as soon as it
  is idle. The Lean service (below) runs every build and
  chooses its `LEAN_NUM_THREADS`; unexpected resource failures remain unavailable. Lake owns
  module traces and artifact reuse. Registered prebuilt libraries are supplied
  through local prebuilt-only Lake package descriptors, never downloaded or rebuilt by validation. Exact compiler,
  Lake, dependency-manifest and recipe identities partition both workspaces and
  shared caches. Markdown changes do not invalidate Lean compilation. Removed
  source or incomplete/corrupted outputs discard stale local build outputs. A slot
  reuses its earlier compiled outputs while each keeps the size, timestamps and file
  ID recorded in the slot's receipt; an output whose identity differs must match its
  recorded hash, or the slot's outputs are discarded. Retention
  ([lake-validation.md](docs/lake-validation.md#retention)) clears the slots and caches of
  idle namespaces by slot count, age and a soft size target. In a namespace builds are
  using, the only one with a single pinned environment, it keeps the two most recently
  used idle slots warm and clears every other idle slot after 24 hours unused, one slot
  lock at a time; it never clears a slot a build holds or that namespace's artifact cache.
- **Failure verdicts.** Reported source/type/target findings reject. Environment
  loading failures, process crashes, memory exhaustion, interrupted checks and
  incomplete reports are unavailable. An inconclusive recheck never permanently
  withdraws a previously validated candidate; its receipt remains visible and a
  fresh check is allowed. A later genuine rejection still invalidates dependent
  results. No machine outcome grants semantic approval.
- **Unavailable original checks.** Completed unavailable results schedule a new
  local generation after 5 minutes, doubling to a maximum 3-hour delay. Every
  generation has a distinct idempotency key and appends evidence; old reports and
  native tool replies remain unchanged. Worker preflight finishes only generation
  zero; subsequent retries run in the background driver, independently of the
  author's next turn. No model inference is replayed. Rejected
  and operator-needed results never auto-retry. Changed candidates, task revisions,
  dependency bases, pauses, and shutdown still gate execution. The online operator
  `validation.retry` command can queue an unavailable check sooner, with a reason.
  Accepted reviews without successful original evidence remain visible in the
  integration queue, with validation state and retry time, but cannot publish.
- **Local execution trust.** Worker-authored Lean is trusted local code; elaboration can
  perform IO. Every Lean and Lake process runs in the Lean service under the Codex sandbox,
  which lets it write only its build slot, the Lake cache and its job's scratch directory,
  and read what the user can read except the protected paths (the operator tokens, the
  systemd and D-Bus sockets, the stored sign-ins, Windows drives); it is not cut off from
  the network. This is the existing `trusted-code-only` environment decision, not a
  mathematical acceptance guarantee. Submitted sources may not use the compile-time
  execution commands listed above, and the kernel replays every official build.
- **Existing instances.** Task, artifact, environment and session identities stay
  unchanged. New reports identify the Lake method.
  `formalizectl revalidate --record` rebuilds original staged bytes and appends its
  real result, without rewriting old failures, granting peer approval or publishing.
  Previously established mathematical defects still require correction/review.
  Preparing or revalidating never resumes workers. Changing Lean/Mathlib pins is
  a separate environment migration.
- **Initialization** pins Lean `293d5d0c…` and Mathlib `5ed29652…` (see
  `bootstrap_pins.py`), keeps dependency sources and compiled outputs in the
  instance's `.dependencies/` (shared by all its workers, never per worker), verifies
  a full Mathlib import, and compiles imported source before admitting any worker.
  Imported seed source is a baseline, not synthetic acceptance.
- **Inspection transport.** Large expression JSON uses a bounded private file;
  declaration and proposition fingerprints still hash the exact original UTF-8
  representations. Diagnostic pipe limits and exact target checks remain
  enforced. Retained reports keep fingerprints instead of bulky expressions.
- **Library validation.** The first check of a registered library in a service
  process hashes every byte and checks the exact file set and links. Later checks
  (acceptance, build evidence, publication, worker `lean_check`) walk the files and
  reuse that result while every file keeps the size, mtime, and file ID it had;
  any difference repeats the full check. Registering an environment always
  re-hashes. The service runs these checks in a child process, and concurrent
  requests for one library share one check: hashing a multi-gigabyte library on its
  own threads starved the coordination database thread and event loop, and
  repeating it for every validation saturated the disk.
- **Metadata resolution.** One resolution may reuse already read registry records
  and literature fingerprints; each distinct input is fetched again through the
  same serialized API before it returns. No approval cache persists across operations.
- **Native read consistency.** Archive relocation and Library publication update
  both authoritative files and the ledger. Their reads remain on the serialized
  coordination boundary; a separate SQLite WAL reader alone could see old
  references with new files. Slow source discovery may be optimized without
  removing that consistency boundary.
- **Literature.** `literature/_central-library-pointers.md` indexes works,
  editions, and locations (retained files or read-only `C:/Central-Library`
  references); `wiki/literature.md` holds the usage procedure. Changed editions are
  new versions. Missing sources become one human literature request; a supplied
  link resolves it only once the content is accessible and registered.

### The Lean service

One long-running process per project starts every Lean and Lake process, so that the
machine's cores and memory are used fully and safely (`lean_service/`, `docs/lean-service.md`).

- **Process.** TaskHub, the command line and setup are its clients over loopback HTTP; only
  local clients with its operator token may call it, so a sandboxed agent cannot. A client
  starts it on demand, in a systemd user scope of its own; a supervisor restarts it when it
  exits. TaskHub restarts and deployments leave it, its jobs and its warm pages in
  place; `formalizectl lean-service start|stop|restart|status|telemetry` manage it. Its jobs,
  results, estimates and machine samples are in `.task-based/lean-service/service.sqlite3`.
  Each check's reading, syncing and hashing of the Library's sources and build outputs runs in
  a pool of bookkeeping processes (`bookkeeping_processes`), not in the server's own
  interpreter, which that work alone otherwise saturates.
- **Two lanes, differing in what a result is for, not in speed.** A development check
  (`lean_check`, `formalizectl check`) reads an agent's working files in place, builds them
  in a service slot (no outputs in the source tree), answers only its caller, has no
  authority and ends with its caller. An official check (candidate, publication and
  Integrator validation, setup, revalidation) builds a frozen snapshot from the blob store,
  replays it through the kernel and inspects the requested declarations in the same slot;
  it is one durable job per content (environment, snapshot, modules, inspections, helper
  versions), its definitive result is reused, and pauses or restarts of TaskHub or the
  service do not void it: asking again attaches to it.
- **Scheduling**, in the manner of HPC batch schedulers: strict classes — the Integrator's
  batch validation, development checks, official checks (those a review or a
  publication waits for first; others age into them after 20 minutes; otherwise first
  come, first served); within a class workers take turns, at most two running each.
  Running work is never stopped for a higher class: a queued Integrator job waits at the
  head for room while only work that cannot delay it starts, takes at most six threads, and
  shares the machine while it runs. A check whose sources turn Lean's heartbeat limit off
  (`maxHeartbeats 0`) is refused unrun, a development check is stopped after 30 minutes
  and a validation after 60 (unavailable, tried again later). Jobs share the logical cores
  and the memory: what is
  available now, less a reserve, less the room running jobs are still expected to grow by, each job sized from
  the last measured peak of jobs on the same modules (else of its lane). A head job that
  does not fit gets an EASY-backfill reservation. Below the low memory watermark nothing
  starts; below the critical one the newest job of the lowest class is suspended. A build
  gets threads by its size, a validation at most four (Lake restores nearly all of it from its
  cache), a single-file check one or two; a validation's build that has run two minutes keeps
  only the threads its CPU shows it uses, plus one, and after its build a job keeps one.
- **Resident sessions.** A development check of one module, without inspections, goes to its
  worker's resident session: a Lean language server (`lean --server`) for that workspace in a
  directory of its own, sandboxed like every Lean process. It opens the module once, and Lake
  builds the module's project imports then (`lake setup-file`); later checks send only the new
  text, so Lean elaborates again only what changed, with its imports still loaded. Changed
  imports or a changed project dependency open the module again. A job using a session
  suspends it with itself. Idle sessions close after `session_idle_minutes`, the least recently
  used one closes when memory runs low, and a check a session cannot serve, or one whose session
  fails, uses the ordinary build.
- **Processes.** A service job's processes run in one cgroup, each process tree in a cgroup
  below it, and are suspended (frozen, all at once, a process that starts later included),
  stopped and measured together: CPU time and peak memory from the job's cgroup, which
  keeps what ended processes used, and private memory sampled every second. Suspended time does not count
  against a check's timeout. Telemetry (queue and running time, CPU, peak memory,
  suspensions, restarts, machine samples) is kept 90 days, results 7.
- **Faults clear themselves.** A job becomes running only once its cgroup, its scratch
  directory and its run all exist; a start that fails before then (a full disk, say) leaves it
  queued, holding nothing, and it is tried again from one second, doubling to a minute, while
  other jobs start. Stopping or ending a job is carried out to its end even if it is cancelled
  again meanwhile, and a job whose run ended without either is stopped at the next decision as
  a cancelled run is (normally queued again). A job record the store cannot write is written
  again every second, from the job's state as it is then, until it can be; the service's own
  state does not wait for it. A job's next process waits only while the job is suspended: a
  freeze that fails, even part-way, is undone and the job runs on, or, if what froze cannot be
  thawed, its run is stopped and it is queued again.
- **Warm library pages.** The service maps the `.olean` import closure of the library modules
  recent checks used, most used first within `warm_gib`, and touches every page every few
  minutes, so imports find them in memory (a cold typical check took 182 s, warm 13.6 s).
- **TaskHub.** A submission waits up to `validation_wait_seconds` for its validation; then
  the validation phase is `queued`, the reply says so, and the driver attaches to the job
  again whatever its author does, recording the receipt when it finishes. The predicate
  `validations` wakes a paused author on the result; a resumption after long idleness
  rotates to a new conversation as usual. A pause or TaskHub restart during a validation
  leaves it queued, not uncertain. So does a finished check whose report cannot be kept (a
  full disk): asking again attaches to the service's result. A validation stays `running`
  only while a wait in this TaskHub holds it; one that no wait holds any more (its wait could
  not even record why it ended) goes back on the driver's next pass as a restart would put
  it, queued here, and the driver attaches again to a validation whose attempt failed once it
  is queued or pending again. The workers' shell has no Lean or Lake command, only the
  readable Mathlib sources (`LEAN_SRC_PATH`).
- **Exceptions.** Installing the toolchain and Mathlib (initialization) and asking a compiler
  its version while registering an environment run before any check and outside the service.
- **Configuration.** `[lean_service]`: `threads` (default the logical cores), `reserve_gib`,
  `commit_reserve_gib`, `low_watermark_gib`, `critical_watermark_gib`, `per_owner`,
  `aging_minutes`, `sandbox` (`codex` or `none`), `warm_gib`, `validation_wait_seconds`,
  `sessions` (0 turns resident sessions off), `session_idle_minutes`.

## 8. Communication

- **Human channel.** The dashboard sends directed messages and answers literature
  requests. Human text is preserved verbatim; only `sender_role: "operator"` is human
  authority. Message bodies are native files; SQLite keeps routing and delivery state.
- **Peer talk.** Each pair of project workers shares exactly one conversation;
  tasks never own, scope, open, or close talk. Any active project identity may
  contact any other worker or reviewer and names relevant task or artifact IDs in
  the text; a reviewer receives replies before its verdict like any worker. There is
  no send-rate limit; a replayed message ID is not a new send, and a send without one
  is its own message.
  All project talk histories are readable; new sends still require the sender's
  current execution fence. Replies steer active Codex recipients, reach active Claude recipients at
  their next turn or `sync`, or wake one answer turn. A peer message is a notice (the
  default), a question, or an answer naming its question (`peer_message_kinds`; a
  message without a row is a notice). Only the worker asked answers, and the answer
  goes to the asker. A notice does not wake an ordinary worker that paused, and a
  worker that pauses stops being woken by notices it has not read: it reads them when
  it next works. Questions, answers and operator messages wake as before, and the
  Integrator and reviewers are woken by every message. An agreed division of work is
  recorded with publication, the collaborator invitation or a submission's coauthors,
  not in messages alone. Peer text is
  collaboration between equals, never a command or review approval, and never
  enters public exports. A message cannot resume a user-paused run.
- **Delivery.** Messages are delivered as complete file references with SHA-256;
  workers read the whole message before acting. Delivery is not comprehension and
  adds no acknowledgment loop. Ambiguous transport is held for reconciliation, never
  blindly resent.
- **Requests.** Question keys are task-and-revision scoped; literature requests may
  share a source/version key when identity and content agree. Invalid replies publish
  nothing; completed responses are immutable.

## 9. Worker inputs and the gateway

- **Model-facing input** separates `task`, `paths`, and `reading`, plus
  messages and changes. Lease, cursor, receipt, session, and recovery bookkeeping stay
  internal.
- **Shared first, then only what is new.** Content every agent of a role shares comes
  first, so it can be a shared cached prefix; what is personal comes later.
  - The standing instructions are the same for every worker of a role: the shared
    rules, the required guides' text, and a one-line-per-operation cheat sheet
    generated from the operation schemas, with the arguments' bounds such as
    `[limit≤20]` (`taskhub_actions` still gives an exact schema). No workspace path, identity or task appears in them. Codex never compacts
    them, so the required guides are never reread. In Test 2, agents looked up schemas
    4,272 times, and a fork's per-worker permission text cut the shared prefix.
  - On-demand guides are copied to `.private/instructions` and read when needed.
  - The first input of a conversation carries the full task view; within the same
    conversation every later input, including a resume after a wait, is a delta. The
    first input after a directed-message turn, an infrastructure failure or an
    interrupted turn is complete again. In Test 2, 533 continuations were full packets
    and 89 deltas.
  - Task text stays in the task file: inputs carry the task's identity and a short
    objective excerpt, because Codex keeps every input verbatim across compaction.
  - Notifications are one-line summaries with the material files that hold anything
    longer; hashes and bookkeeping stay out.
  - A result too large to return is a material file, and its large fields, two levels
    deep, also get their own files, listed as `parts` with a one-line outline each, so a
    reader opens the part it needs rather than a whole multi-megabyte record.
  - `sync` returns only notifications and messages not yet delivered, with the task's
    revision and state. What it shows of the task becomes the continuation baseline, so
    the next input stays a delta and still reports a later change.
- **Knowledge reading.** Read `paths.knowledge_readme` and the short Essential
  guidance before exploration; reuse retained guidance. Long notes are on demand.
  No full-wiki requirement, per-page reminders, delivery/read tracking, or frozen
  copies of reading pages. Ordinary continuations carry no wiki update payload.
- **Compaction** sends nothing again to Codex, which keeps the conversation's inputs
  across compaction: the delivery cursor continues and nothing delivered is replayed.
  In Test 2, inputs kept across compaction were 38.5% of all input, and 31 agents
  received byte-identical sync pages again after compaction. Claude, whose compaction
  summarizes the inputs too, gets its current task view with the next tool result.
  Compaction never rejects a call; fencing, revision checks and idempotency still apply
  to mutations.
- **Reads.** Project and resource read operations accept declared aliases and
  absolute paths. Inspection is read-only; task and review eligibility is enforced when
  taking action.
- **Gateway.** Exactly two tools: `taskhub_actions(category?, operation?)` for short
  summaries or one schema, and `taskhub_call(operation, arguments)`. Roles are enforced
  server-side; decision helpers get no tools at all. `stage`
  freezes a candidate and continues; `contribute` drains,
  then freezes; `complete` starts the completion declaration.

## 10. Reliability and recovery

- **No leases.** Execution authority belongs to the live runner in the coordinator
  process. Fencing tokens reject stale actors; elapsed time never revokes authority.
- **Local fault isolation.** An infrastructure error quarantines only the affected
  worker, helper, validation, or publication. Run state, admission of unrelated work,
  and messaging continue. Engineering errors never pause the run or become a worker's
  voluntary pause.
- **Admission isolation.** A task, ticket, or worker that cannot be evaluated (for
  example a corrupted task file) is skipped and reported once, both for work
  admission and for scheduling decisions (`admission.item_skipped`). Run pause, project stop, and Lean readiness still stop
  admission globally.
- **Unsaved threads.** Codex saves a thread only after its first turn, and each
  activation runs in its own process. A bound thread that never received a turn is
  released (`worker.empty_thread_released`) and the next activation starts a new
  thread. Turns are counted per thread, so a worker's next task in a new conversation
  is released the same way even after turns on its earlier thread.
- **Stopped turns.** Background commands end with their activation. The next input
  after an infrastructure failure or an interrupted turn says so, so the worker
  reruns what it still needs instead of polling a session that no longer exists.
- **No blind replay.** Unknown native or paid outcomes stay recorded as unknown.
  Once owned-process closure is confirmed (job-object probes, also after a coordinator
  restart), the original conversation continues with a new input; the old turn is
  never replayed and publication checks are never waived.
- **Failure backoff.** Consecutive identical stopped-worker failures back off for 1, 2,
  4 minutes, doubling up to 60 minutes; at five repeats one operator notice is kept for
  that failure signature. An infrastructure failure never becomes a voluntary pause,
  and an uncertain paid turn is never replayed. Metadata RPCs avoid full histories,
  and the bounded frame default is 64 MiB; the host frame-size setting is excluded
  from the saved execution-profile identity, while account home, model, runtime
  version and all other settings remain checked.
- **Native sandbox.** Codex workers run under the permission profile `taskhub-worker`:
  network access, writes only to the current workspace (with its own `TMPDIR`; `/tmp` is
  read-only), and no reads of the protected paths: the user's runtime directory (operator
  tokens, systemd and D-Bus sockets), either backend's stored sign-in, and Windows drives
  that WSL mounts. Claude's sandbox denies the same reads. Before a native runner admits
  model work, a real sandboxed command in that profile checks Library and staging reads,
  refused reads of the protected paths, refused writes to the Library, project root,
  coordinator state, correspondence and `/tmp`, and its own workspace write; it also
  tries HTTPS, and an unreachable network is only a warning (`warnings` in
  `latest.json`). It fails when WSL interop is on, which would let a sandboxed command
  start Windows programs. The probe runs at every start without starting a model turn.
  Failed access checks keep their diagnostics and retry through recovery; they never
  silently grant more write authority or resume a paused project. Apart from the
  protected paths, sandboxed commands read what the user can read; this is not a
  secret-isolation boundary for other files.
- **Claims that never launched.** Each activation records its launch, with the cgroup
  it will use, before any process exists. After a restart, a claim with no ownership
  record is released at once if it never reached its launch, or once its planned cgroup
  is empty; it no longer holds the worker in `recovery_required`.
- **Accounting** uncertainty is a visible warning, not an execution gate.
- **Run control** belongs to the operator: explicit pause/stop and Task 0 shutdown
  override automatic activation. Driving a run is remembered: a service restart
  re-arms the runs it was driving once delivery recovery succeeds (paused runs
  stay paused; `serve --no-resume` starts for inspection only). A failed startup
  recovery is retried with backoff (5 s doubling to 5 min); retries apply only
  committed delivery outcomes, so no message is sent twice. Live execution starts
  only with explicit user authorization.
- **One person's control.** The operator may pause one worker or reviewer while the
  run continues (`worker.pause`), or resume one while the run stays paused
  (`worker.resume`). Pausing interrupts its turn and parks it exactly as Pause all
  does; nothing of it is admitted (turns, continuations, message wakes, helpers,
  validations, automatic recovery) until it is resumed. A person resumed in a paused
  run continues only its own current work: its parked turn, continuation, current
  task, messages, recruitment decision, and validation, but no new task and no new
  worker. Its task's one-time recruitment decision may still run its one decision-model
  turn; it does not launch a collaborator.
  The service drives a paused run while someone is resumed, and the dashboard keeps
  Pause all available, including during an account hold.
  A control is kept only while it differs from the run, and Pause all / Resume all
  (`run.pause` / `run.resume`), stop, and completion end every individual control.
- **Pause after turn.** `run.pause_after_turn` (Pause all after turn) and
  `worker.pause_after_turn` pause like Pause all and `worker.pause`, except that a
  turn already under way is not interrupted. That turn finishes on its own: its tool
  calls, submission and turn bookkeeping proceed as usual. Of new input only a person's
  directed messages are steered into it, so the operator can ask a draining worker to
  wrap up; a peer's message waits for a later turn, so peers do not keep each other's
  turns going, and a turn not yet dispatched does not start. Nothing starts after it: no next turn, continuation,
  task, helper or validation. A decision helper that finishes its turn keeps its
  decision and commits it, without another model turn, once its worker may run again.
  A later Pause (`run.pause` or `worker.pause`), stop or shutdown interrupts a turn
  that is still finishing. Resume and Resume all end the pause as usual.
- **A group's control.** `group.pause_after_turn` and `group.resume` (dashboard:
  Controls; `taskctl run pause-group-after-turn` / `resume-group`) apply to a group:
  all Codex workers and reviewers (`codex`), Codex workers (`codex.workers`) or
  reviewers (`codex.reviewers`) alone, or all Claude workers and reviewers (`claude`).
  The Integrator and the Dispatcher are in no group. A paused group's members pause
  after turn as with `worker.pause_after_turn`, and no new worker or reviewer starts
  on its account: a new worker takes the next number of an account whose workers run,
  and a review waits for its reviewer. A group resumed while the run is paused runs in
  full: its members' own work, new tasks for its idle workers (with assignment
  decisions), new workers and reviewers on its account; the others stay paused, and
  the service drives the run while a group is resumed. Assignment decisions run and
  recover while any group of workers is resumed, on the Codex account even with Codex's
  own groups paused; a group of reviewers alone takes no new work and runs none. A
  person's own control comes before its role's group, which comes before its backend's,
  which comes before the run. A group's control ends its narrower groups' controls and
  its members' own resumes, and a member already paused keeps its pause. It is kept
  only while it differs from what applies around it (pausing after turn always
  differs), and Pause all / Resume all, stop and completion end every group's control.
- **Account holds.** A final `usageLimitExceeded` or `unauthorized` error from Codex
  (not network errors or `rateLimitExceeded`) sets a persisted dispatch hold on Codex
  workers. An `unauthorized` error of a Codex process that started before the sign-in
  now on disk (`auth.json` changed since) holds nothing: each sign-in revokes the tokens
  that running processes keep, so only that turn ends, and its next activation starts
  with the current sign-in. `claudeUsageLimit`, `claudeSignInRequired` and `claudeSubscriptionRequired`
  hold Claude workers, which bill a separate account (Claude's file is
  `.task-based/claude-dispatch-hold.json`). A hold stops new model input to its own
  backend's workers: worker and reviewer turns, message turns, continuations,
  new workers, and claimed turns not yet sent. Decisions always use the Codex account,
  so only a Codex hold stops them.
  It does not pause the run: in-flight turns, their tools, checks, and merges finish
  normally, messages and files keep working, and work intentions are unchanged.
  Restarts and quota resets keep the hold; only `run.resume` clears it, without
  checking the account, and a still-exhausted account simply holds again. The hold
  applies in memory first, then is written to `.task-based/dispatch-hold.json` and the
  database; a lost database write is restored from that file at startup (an
  unreadable or malformed file still holds). Each report gets a new hold ID. Resume
  releases holds only inside its database transaction, by recording the released IDs;
  memory and the file are dropped after the commit, and the file only if it still names
  a released ID. A failed Resume changes nothing, a late write or leftover file of a
  released hold never revives it, and a report during Resume's cleanup is a new hold.
- **Observation never gates execution.** The activity log, visible-history updates,
  and transcript capture are best-effort: a failure is logged and retried (history
  records every 30 seconds, with a notice that clears once they apply), and never
  blocks a turn, recovery, or startup re-arming. A recovery record that cannot be
  read holds only its own execution, and a failed recovery step never stops admission.
- **Transcript writes.** Visible activity (replies, actions, turn states) is written by
  a background thread, in order for each worker, with one sync per file for what has
  accumulated. A streaming reply's text is collected and written as one record every
  2 seconds. Any other record of that worker, and any read of its history, first writes
  what was collected. Inputs and their delivery states are written and synced before
  the caller continues.

## 11. Operator surfaces

- **Operator access.** Every operator request carries the project's operator token:
  TaskHub and the Lean service each keep one in the user's runtime directory, readable
  only by the user and denied to every sandbox. Local clients send it as a bearer
  credential; a browser signs in once with the link `taskctl dashboard` prints and keeps
  it in a strict, HTTP-only cookie. Requests must name a loopback host (no DNS
  rebinding) and, from a browser, come from the service's own origin.
- **Library size.** The dashboard checks published Lean line and file counts every
  minute. Physical lines include comments and blanks; only the latest public
  snapshot counts, never staging, history, or dependency caches. A separate read-only
  endpoint counts off the event loop, caches immutable source hashes, and does not
  access the coordination database or start model work. Failed refreshes retain the
  last value visibly marked stale.
- **Dashboard** (`taskctl serve`, loopback, signed in with the operator token): workers and
  reviewers in separate collapsible groups, and the Integrator with the Dispatcher; each
  worker's sessions; the task browser; human requests; project setup (Task 0 and, in the
  Agents and sessions panel it keeps after the first run, the agents' models and sessions).
  A person's page shows one turn at a time, chosen from a turn list (by default the
  latest), as a single chronological stream: what the worker said and sent on the
  left, and what it received on the right (a condensed system prompt, and human and
  peer messages with their senders). Every message bubble shows its input's delivery
  status; prepared, undelivered, uncertain, or unrecorded delivery never looks confirmed.
  Delivery confirms model input, not reading a referenced file. Tool calls and exact
  inputs are folded, including each message-bearing steer. Its open
  requests are pinned above the stream and highlighted where they were asked. Message
  bodies are read from correspondence and peer-talk files only. Viewing never starts
  a model or acknowledges messages.
- **Task center** (the dashboard's Tasks dialog): the list of tasks on the left and the
  selected task on the right, each scrolling on its own; in a narrow window the task takes
  the list's place, with a way back. Everything in it only reads.
  - *Overview* is the publication tree of work tasks: each under the task in whose attempt
    it was published (`task_origins`, recorded by `task.publish`; tasks published before it
    existed were filled once at startup from their `task.publish` call receipts), a repair
    task under the task it repairs, Task 0 first. Reviews are counted on their task, never
    listed. Finished subtrees can be hidden; open ones expanded; a search shows matches with
    the tasks above them. It reads every work task compactly, at most every 30 s.
  - *Dependencies* draws, after Geyser's task graph, the open tasks joined by unmet links:
    a requirement of a task's current contract, and a waiting worker's subscription, stored
    requirement or request to the operator. Met links are shown on request. Unmet links
    that form a cycle are reported as a deadlock, and tasks that reach the operator through
    unmet links as waiting on the operator. Selecting a task dims all but its neighbours.
    Waits on a worker's own submission or on a peer's answer involve no other task and are
    listed apart.
  - *All, Running, Task pool, History* page work tasks, reviews or both (work tasks by
    default), newest, oldest or by title, and hold still while tasks arrive: a page lists
    only tasks published before the view was first read, and offers the newer ones.
  - *The selected task* shows its state and executors, then its relations (where it came
    from, what it needs and what needs it, waits within it, the tasks published from it, its
    reviews), its objective rendered from Markdown as text, its evidence, admission, and its
    revisions and records.
- **CLI:** `taskctl` (coordination) and `formalizectl` (Lean domain).
- **Git sharing** (optional): reviewed source, public wiki, and generated state under
  `taskhub/` on one branch. Private notes, messages, transcripts, credentials, and live
  databases are never exported.

## 12. Engineering conventions

- English and Python 3.13; locked dependency versions. The system runs on a Linux host,
  a dedicated VM ([docs/linux-host.md](docs/linux-host.md)); Lean workers use the Codex
  home `~/.local/share/formal-geyser/codex`.
- The project root is a Git repository; commit reviewable changes. Text files use LF.
- Validate with `uv run python scripts/test_parallel.py` (or `uv run pytest -q`),
  `uv run ruff check src tests scripts`, and
  `uv run mypy src/formal_geyser --follow-imports=silent`. Real Lake/target
  tests are opt-in (see the README).
- Auto-lean-test-1 is permanently terminated and must never be resumed; its files are
  retained as history only.

Inspector computation has no heartbeat quota. Exhausted heartbeats or recursion
depth do not establish a mathematical defect and return unavailable. Inductive
result types may be recognized without opening local instance parameters;
proposition target fingerprints and independent kernel checks are unchanged.

A batch commit owns all of its per-artifact publication rows. Within one
serialized operation, repeated reads may reuse a fully verified immutable artifact
body; every later operation verifies the native bytes again, and relocation or
manifest changes invalidate the view.


### Drained contribution validation recovery

A native `contribute` reply drains the turn. At quiescence, staging and the
candidate's original-validation phase commit together. A restart resumes that
phase from its frozen candidate, without inference or a new candidate. Saved
verdicts remain immutable, including negative and unavailable outcomes. The
receipt's explicit `preserve_reply` policy protects the reply already returned to
the worker. An impossible ambiguous call binding retains the staged candidate,
records a validation hold, and lets quiescence complete; it never guesses a call.
Held validation is reported as pending, not as successful completion.

A drain that never finishes (its callbacks time out, an interruption times out, or
TaskHub restarts) still commits what TaskHub accepted. Recovery confirms the processes
are quiet and then commits a retained review verdict or contribution as the drain
would have; one that can no longer be committed is abandoned with its reason
(`attempt.submission_not_committed`). Before this, Test 2 lost five rejections, for two
reviews.

The operator-only `validation.recover` reconstruction path repairs **pre-fix**
contribution records missing that phase. It requires confirmed process quiescence,
the exact quiet-transition receipt, and one matching committed execution identity.
It does not infer identity from potentially normalized content or create a second
verdict. For ambiguous records, the operator may supply `native_call` with exact
`worker_id`, `thread_id`, and `call_id`. All quiet-attempt, snapshot, author and
committed-call checks still apply; choosing an unrelated call is rejected. This repair path is not
part of normal worker admission or automatic retries and can retire once affected
instances have been repaired.


### Pre-inference helper failures

An explicit operator preflight retry may restore the same helper intent to
`prepared` only with confirmed stopped processes, no bound thread, no remote
turn/result/usage, and no inference uncertainty. It preserves the recruitment
decision. All normal run/account controls still apply.
Maintenance may leave such a prepared helper queued; it still requires a paused
run, no active or ambiguous execution and quiet owned processes.

Parked interruption tickets retain their suspended session state through Resume all; historical preflight failures do not supersede a later checkpoint. A safely stopped message-only preflight failure reopens message handling without converting voluntary waiting into task exploration. A claim held before its session started only because the run, its person or its account stopped dispatching is no failure: its worker waits suspended like a paused one, and Resume admits it again, without recovery or retry backoff. A resumed continuation or interruption held this way still goes through recovery.
