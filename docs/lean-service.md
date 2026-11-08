# The Lean service

Every Lean and Lake process of a project runs in one long-running process, the project's
Lean service. It shares the machine's cores and memory between the Integrator's batch
validations, the agents' development checks and official validations, runs each Lean
process in the Codex sandbox, and measures what each job uses. The design is in
`DESIGN.md` (section 7, "The Lean service").

## Running it

A client starts the service when it needs it, as a detached process outside its own
process tree, so TaskHub restarts and deployments do not touch it. A supervisor restarts the
server whenever it exits.

```bash
formalizectl --project PATH lean-service status
formalizectl --project PATH lean-service telemetry --hours 24
formalizectl --project PATH lean-service restart
formalizectl --project PATH lean-service stop
```

`restart` loads a new version of the service; `stop` ends it and its supervisor. Running
jobs are queued again and continue when it next starts. `python -m
formal_geyser.lean_service COMMAND --project PATH` does the same.

`python -m formal_geyser.lean_service compact --project PATH`, with the service stopped,
rewrites `service.sqlite3` into a copy that keeps only what `result_days` keeps and gives
freed pages back from then on, then puts it in place. A database made before 2026-11-08 never
gave space back: in the Linux pilot it reached 79 GB. A plain `VACUUM` would need about that much
free space; `compact` needs only the size of the copy. It holds the server's lock, so no service
starts meanwhile; `start` afterwards.

Its files are in `.task-based/lean-service/`: `service.sqlite3` (jobs, results, estimates,
machine samples), `service.json` (address of the running server), `service.log`,
`supervisor-events.jsonl`, `codex-home/` (the sandbox's Codex home) and `scratch/` (one
directory per running job). Build slots and Lake caches stay in `.task-based/objects/lake/`.

A check reads its sources, digests and scans them, syncs them into a build slot, and records
and compares the slot's outputs: each pass covers the whole Library. The server runs these
steps in `bookkeeping_processes` worker processes (8 by default; 0 runs them in its own
threads). In the Linux pilot on 2026-11-03 that work, in the server's threads, held its one
interpreter at 120-130% CPU while the machine was 80% idle: a check's Lake build took 3.5 s
and the whole check 71 s, and development checks waited a median 15 minutes to start.

Only local clients with its operator token can call it; the token is in the user's
runtime directory, which sandboxed agents cannot read. The supervisor runs in a systemd user
scope of its own, each server in a delegated scope bound to the supervisor's, and each
job's processes in the job's cgroup ([the Linux host](linux-host.md)): suspending a job
freezes that cgroup, and its accounting keeps what ended processes used.

## Lanes

| Lane | Who | Input | Result |
|---|---|---|---|
| integrator | the Integrator's final-tree validation | a frozen snapshot | durable, shared, runs alone |
| interactive | `lean_check`, `formalizectl check` | working files, read in place | private, no authority, ends with its caller |
| official | candidate and publication validation, setup, revalidation | a frozen snapshot | durable, shared, reused |

A development check reads a worker's workspace in place. With `directory` set to a draft,
`.private/drafts/<name>`, it reads the draft's Lean files over the workspace's other Lean
files (a draft file takes the place of a workspace file of the same module), so an
unfinished draft can import the Library; staging ignores `.private`.

A development check of one module without inspections uses its worker's resident session: a
Lean language server that keeps the module open with its imports loaded, so later checks
elaborate again only what changed (`compilation.session.incremental`). Changed imports or a
changed project dependency open the module again (`compilation.session.reopened`), and Lake
builds the module's project imports when it is opened. Other development checks, and any whose
session fails (`session_fallback`), use the ordinary build. Idle sessions close after
`session_idle_minutes`; `status` lists them.

An official check of the same content (environment, snapshot, modules, inspections and
helper versions) is one job: a second request attaches to it, and a definitive result is
reused for `result_days` (two by default). An unavailable result (a crash, a timeout, a missing
tool) is not reused. Each reuse is counted with the result's age; `telemetry` reports them under
`reuses`, the evidence for `result_days`. In the Linux pilot 17,350 of 125,000 checks in 14 days
could be reused, and the 88 that ran again did so within a day of the first.

A finished job keeps its result and its specification `result_days`, both compressed; then it
keeps only its telemetry for 90 days. The database frees pages as it goes (incremental
auto-vacuum): pruning, every hour, gives them back. Evidence is TaskHub's, not the service's:
TaskHub records every result it accepts.

Each result's `timings` gives the seconds of its steps: the environment probe, reading and
hashing the sources, the heartbeat scan, the session check or the compilation, the inspections,
releasing the slot, the closing library check, and `event_loop_lag`, the time the service's event
loop ran late meanwhile (blocking work there delays every running job). `compilation.timings`
splits the compilation: the library check, the wait for a slot, recovering and synchronizing it,
listing its outputs, the Lake build, the checks that the sources stayed unchanged, the receipt and
the kernel replay. In the Linux pilot a development check took 57 s (median), of which its Lean
processes took 27%.

## Scheduling

- The Integrator's job goes first, but running work is never stopped for it: at the head of
  the queue it waits for room, and meanwhile only work that cannot delay it starts. It takes at
  most six threads (`integrator_threads`), and other work shares the machine while it runs. In
  the Linux pilot it mostly restored Lake's cached modules: a median 51 s, 0.06 cores and
  0.1 GiB (at most 1.35 cores and 2.2 GiB).
- Lean's heartbeat limit bounds each declaration's elaboration. A check whose modules, or the
  project modules they import, set `maxHeartbeats 0` or `synthInstance.maxHeartbeats 0` is refused
  unrun, as a source finding (comments do not count; a finite raise is allowed): in the Linux
  pilot one such development check ran for over 20 minutes and grew to 14 GB.
- A development check that has not ended after `check_minutes` (30), and a validation after
  `validation_minutes` (60), is stopped and ends unavailable, its suspended time not counted:
  each Lean or Lake process gets what is left of that time as its timeout (the build, the
  kernel replay and the inspection, which Lean does not bound by heartbeats), and a resident
  session that has not answered is closed, since Lean would go on elaborating, without a build
  repeating it. A stopped session check answers with the diagnostics Lean had published by then
  and the lines it last reported still working on (its `$/lean/fileProgress`), quoting the
  first. Heartbeats do not bound time: in the Linux pilot one declaration whose elaboration
  failed after 15 s was then checked by the kernel for the rest of the 30 minutes at the
  default 200,000, and stopped with a kernel deterministic timeout after 30 s at 20,000; another
  check ran its 30 minutes with a limit of 100,000. An unavailable validation is tried again
  later, after the usual backoff. The Integrator's job has no such limit: a legitimate rebuild of much of the Library may take long,
  and a timeout would hold up its batch. In the Linux pilot the longest of 1,198 development
  checks took 11 minutes, of 210 validations 13, of 20 Integrator jobs 3.
- A check of any lane one of whose Lean processes holds more than `process_memory_gib` (12) of
  private memory, resident or swapped out, is stopped, measured every second, and ends
  unavailable as one past its time; a resident session holding such a process is closed with
  it. Running out the machine's memory instead stops every check and queues it again, the large
  one first: in the Linux pilot two inspections grew past 8 GB and no check ended for 12
  minutes. Of the 11,500 checks there that ended, the largest held 13.9 GB in all.
- The inspection writes the declaration's type and a statement's proposition as `Expr.repr`
  writes them, each shared subterm at every use, unless that has more than a million nodes;
  then with each distinct subterm once (see `lean-tools/Inspect.md`). Written out, statements
  of the pilot ran to 7 GB.
- A job suspended under memory pressure resumes before anything new of its class or a lower
  one starts; a higher class goes first. One that does not fit resumes alone once nothing else
  runs, so a suspended job that never fits cannot stop all Lean work.
- Development checks go before official checks. Official checks that a review or a
  publication waits for go first; the others count as such after `aging_minutes`.
  Otherwise jobs run first come, first served. Within a class workers take turns, with at most
  `per_owner` running each. Up to `validation_slots` such waited-for official checks run
  ahead of development checks anyway: with many agents the development checks never run out,
  and the reviews and publications waiting for validations would otherwise starve. A job that
  resumes in the same decision counts for both.
- Threads: the logical cores (or `threads`). A build gets threads by its size, a single-file
  check one or two, and after its build a job keeps one. A validation gets at most
  `validation_threads` (4): Lake restores nearly all of its modules from the cache and compiles
  the few a candidate changes. Until 2026-11-06 it got a thread for each module, up to all 32
  of the Linux pilot, while of 5,236 validations in three days the median used 0.43 cores and
  the most 7.3; one or two validations held the machine and over a hundred jobs waited. Once a
  validation's build has run two minutes, it keeps only the cores its CPU used over the last
  two minutes, rounded up, plus one: its Lake build cannot start more processes than it was
  given, so this overcommits the machine by at most those few threads. Builds: at most
  `build_slots` (8) at once, of every lane. Each holds a build slot with a full copy of the
  Library's build (9.6 GB in the Linux pilot, where 13 builds at once filled the disk), so a
  job whose build finds every slot held waits, first come first served, for one to free while
  it keeps its place among the running jobs; a waiting validation keeps one thread after two
  minutes, as it uses none. Memory: what is available now, less
  `reserve_gib`, less the room running jobs are still expected to grow by. A job's need is the
  measured peak of jobs on the same modules, with a margin and at least 1 GiB per Lean
  process (1.25 GiB committed): a higher peak raises it at once, a lower one lowers it a tenth
  at a time, since a check Lake answers from its cache measures far less than a rebuild. A
  job nothing like has run yet takes the defaults (1.35 GiB, 1.65 GiB committed: the measured
  checks' 90th percentiles); the lane's
  last job sized it until 2026-10-22, when a cached check left 16 MB for every new one.
- A job at the head that does not fit gets a reservation: later jobs may start only if they end
  before it could start, or use only what will be spare then (EASY backfilling).
- Below `low_watermark_gib` of available memory nothing starts; below `critical_watermark_gib`
  the newest job of the lowest class is suspended, and it resumes after memory recovers. A
  suspended job keeps its memory: once nothing runs and memory is still below
  `low_watermark_gib`, the newest suspended job of the lowest class is stopped and queued
  again at the head of its class, one per decision, after the suspension's cooldown.
- Available memory that cannot be read is not unlimited: the last reading serves for 30
  seconds, and past that nothing starts or resumes until it can be read again.
- A job's cgroup that cannot be removed when the job ends or is stopped (something in it could
  not be ended) is kept and its removal retried, from 5 seconds doubling to 5 minutes, without
  a time limit. Until it is removed its threads go to no other job, its scratch directory stays,
  and the job, if it was queued again, does not start a second run beside the first. A result
  the job gave stands: its processes were confirmed ended. `status` lists such cgroups under
  `cleanup`.
- A job becomes running, with its threads, only together with the task of its run, once its
  cgroup and scratch directory exist. A start that fails before then, as when the disk is full,
  removes the cgroup it made and leaves the job queued, holding nothing; it is tried again after
  one second, doubling to a minute, and other jobs start meanwhile. A job without its cgroup still
  ends unavailable.
- A job record the store cannot write (`database or disk is full`) is kept and written again every
  second, each time with the job's fields as they are then, so a late write never sets a record
  back. The job does not wait for it: it runs and ends, and clients waiting for it learn its end.
  `status` gives the number of records still unwritten under `unwritten`; at a stop the service
  tries them once more, and a job whose end was still unwritten is queued again at the next start.
- Stopping a run (`_stopped`) and recording a job's end are carried out to their end even when the
  job is cancelled again meanwhile, and a job left running or suspended whose run has ended
  without ending it is stopped as a cancelled run is, at the next decision: queued again, or
  ended if its client cancelled it or it was stopped for its memory. On 2026-11-05 seven jobs of
  the Linux pilot stayed running with no run while its disk was full, and once space returned
  no job started until the service was restarted.
- A job's next process waits only while the job is suspended. A freeze that fails, even after
  part of the job froze (its cgroup but not a session it borrows, say), is undone: everything
  of the job is thawed and it runs on, and a later decision may suspend it again. If what froze
  cannot be thawed either, the job's run is stopped and it is queued again. Before, a failed
  freeze left the job running with its next process waiting for a resume that never came.

## TaskHub

A submission waits up to `validation_wait_seconds` for its machine validation. A longer one
continues in the background: the reply's `validation.state` is `queued`, the receipt is
recorded when the job finishes, and the author is notified. A paused author with the
predicate `{"validations": ["<candidate id>"]}` wakes on the result. Pausing the run or
restarting TaskHub leaves the validation queued; it is not marked uncertain. Nor does a
request to a busy service that times out: the check stays queued, and asking again attaches
to the job, or submits it if the service never received it. A restart queues again any
validation that older code had left uncertain after such a timeout.

The dashboard shows the service's running, queued and suspended jobs.

## Sandbox

With `sandbox = "codex"` (the default) each Lean process runs as

```text
codex -c permissions.lean.extends=":workspace"
      -c permissions.lean.filesystem={":slash_tmp" = "read", "<protected>" = "deny",
                                      "<slot>" = "write", "<cache>" = "write", "<scratch>" = "write"}
      sandbox -P lean -C <slot> -- <lean or lake command>
```

It may write only its slot (its working directory), the Lake cache and its job's scratch
directory, where `TMPDIR` points; `/tmp`, which is memory on most Linux hosts, is read-only
(Codex names it `:slash_tmp`: a `"/tmp"` entry would not override the workspace profile).
It cannot read the paths workers cannot read either (see [the Linux host](linux-host.md)).
No model is called and nothing is billed. A process that
needs no working directory, such as a version probe, runs in its scratch directory, so the
sandbox never gains write access to the toolchain. The service creates the directories a
sandboxed process writes before it starts, so that the sandbox can grant them. Tests of
the sandbox use directories inside the checkout, since nothing below `/tmp` is writable.

## Settings

```toml
[lean_service]
threads = 16                 # default: the logical cores
reserve_gib = 4              # kept free for the system, agents and shared pages
commit_reserve_gib = 8
low_watermark_gib = 3        # below it nothing new starts
critical_watermark_gib = 1.5 # below it a job is suspended
per_owner = 2                # running jobs per worker within a class
aging_minutes = 20
validation_slots = 4         # waited-for validations that run ahead of development checks
validation_threads = 4       # the most threads a validation starts with
build_slots = 8              # the most Lake builds at once, each in a slot with a full Library build; 0: no limit
bookkeeping_processes = 8    # worker processes for each check's file bookkeeping; 0: the server's threads
sandbox = "codex"            # or "none"
warm_gib = 6                 # library .olean pages kept resident
validation_wait_seconds = 90 # a submission's wait before its validation continues in the background
sessions = 6                 # resident development-check sessions; 0 turns them off
session_idle_minutes = 10
check_minutes = 30           # a development check is stopped after this long; 0: no limit
validation_minutes = 60      # a validation is stopped after this long; 0: no limit
process_memory_gib = 12      # a check one of whose Lean processes holds more is stopped; 0: no limit
result_days = 2              # a finished job's result and specification are kept this long
share_outputs_gib = 32       # idle build slots' identical outputs compared and shared each hour; 0: off
```

## Not in the service

Installing the toolchain and Mathlib (initialization), and asking a compiler its version
while an environment is registered (`formalizectl doctor`, `environment capture`, setup),
run before any check and outside the service. Agents' shells have no Lean or Lake command;
they read Mathlib's sources through `LEAN_SRC_PATH`.

