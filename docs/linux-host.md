# The Linux host

TaskHub, the Lean service and every worker run on one Linux machine, in practice a
virtual machine dedicated to them: a WSL 2 distribution on Windows, an Ubuntu VM in Lima
on macOS. Both use the same setup and the same project configuration; only the VM's CPU
and memory and a few Lean service settings depend on the machine.

## What the system needs

- systemd as init, with the user's manager kept running by lingering
  (`loginctl enable-linger`), so services stay up without a login session.
- cgroup v2, with the memory and pids controllers delegated to the user's manager
  (systemd's default).
- `systemctl`, `systemd-run` and `busctl` (part of systemd), `/bin/sh`, Git and curl.
- For Claude workers: bubblewrap and socat. Claude workers also refuse WSL 1 and WSL
  interop.
- Python 3.13 through uv. `taskctl init --domain lean` prepares Lean and Mathlib, and
  `scripts/install_codex.py` installs the audited Codex release.

`taskctl doctor` reports whether this process can own process trees and whether WSL
interop is on.

## Process ownership

Every process tree runs in a cgroup of its own (`cgroups.py`): a Codex app-server or
Claude bridge with all it starts, a Lean or Lake process with its children. The owner
(TaskHub, the Lean service, a command-line tool) works in a delegated cgroup: its systemd
user unit's when that unit has `Delegate=yes`, or else a new delegated scope it moves
itself into. A tree's first process enters its cgroup before it runs the command; the tree
ends as a whole (`cgroup.kill`), and it has ended once its cgroup is empty.

- TaskHub records each worker's or helper's cgroup in an ownership receipt before any
  model input is sent. After a restart, recovery treats an empty or missing cgroup as
  ended, and ends a tree whose recorded owner has died, as the owner's unit would have.
- A Lean job is one cgroup with a cgroup per process below it: freezing it suspends the
  job (a process that starts later is frozen too), and its CPU and memory accounting keeps
  what ended processes used.
- The Lean service's supervisor runs in its own user scope, so stopping TaskHub does not
  stop it. Each server runs in a delegated scope bound to the supervisor's, and the
  supervisor kills whatever a server leaves.

The cgroups hold processes only while sandboxed commands cannot reach the user's systemd
manager, which could start processes elsewhere: the sandboxes deny the runtime directory
that holds its sockets (below).

## Running TaskHub as a user service

Run TaskHub as a systemd user service with delegation, for example
`~/.config/systemd/user/taskhub.service`:

```ini
[Unit]
Description=TaskHub for ~/project

[Service]
ExecStart=%h/formal-geyser/.venv/bin/taskctl --project %h/project serve --port 8765
Delegate=yes
Restart=on-failure

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now taskhub
```

When TaskHub's main process ends, systemd ends the whole unit, workers included. Started
from a terminal instead, TaskHub moves itself into a scope of its own, and recovery ends
the workers a dead TaskHub left.

TaskHub and the Lean service each raise their soft open-file limit to the hard one when they
start, since every worker or Lean process they own holds pipes; the unit needs no
`LimitNOFILE` unless the hard limit itself is too low.

## When TaskHub stalls

TaskHub watches itself, since py-spy cannot attach to it without root while
`kernel.yama.ptrace_scope` is 1, as on Ubuntu. When its event loop has not run for 10 seconds,
or one admission has run for two minutes, it writes a report to its log, starting
`TaskHub stall:`. The report gives the reasons and what the admission is doing (selecting, or
preparing or capturing a worker's workspace), the default thread pool's busy threads and
queued calls, and the coordination queue. Then it gives every thread's stack with its CPU use
over one second, identical stacks once. While the event loop still runs, it ends with the
admission's await chain and all tasks counted by where they wait. The report is repeated every
five minutes while the stall lasts, and `TaskHub stall ended after N s` closes it.

`taskctl --url http://127.0.0.1:8765 cpu-profile --seconds 60` samples the running service for a
minute and reports the CPU seconds of each thread family and of the project code it ran, innermost
function and outermost operation. It costs a few percent of one core while it runs.

Every SQLite statement on the coordination thread releases the interpreter lock and must take
it back from threads running Python, which can take a whole switch interval each time. The
service runs at 0.2 ms instead of Python's 5 ms; the environment variable
`TASKHUB_SWITCH_INTERVAL` (seconds) overrides it when the service starts.

Reads on the four read connections give up the lock too, for every statement and every row.
What turns and validations under way check every half second (the run's control, the worker's
binding and stop request, waiting messages) is therefore read once for all of them: four
statements and a row per watched turn, where each turn's own reads took about forty statements.
Every five minutes the log has a `Read database summary:` line beside the coordination thread's
`Coordination database summary:`, with the reads by function and how long they queued and ran.

## Operator sign-in

TaskHub and the Lean service each keep a random token in
`$XDG_RUNTIME_DIR/formal-geyser/<project>/`, readable only by the user and kept until reboot.
Local clients send it as a bearer credential. `taskctl --project ~/project dashboard`
prints a sign-in link: opening it sets a strict, HTTP-only cookie and leaves the address
bar. Every request must name a loopback host, and a browser's must come from the service's
own origin.

## What sandboxed commands cannot reach

Codex workers run under the permission profile `taskhub-worker`
(`native_permissions.py`): Codex's workspace profile with network access, `/tmp` only for
reading (each session gets a `TMPDIR` in its workspace), and no reading of

- the user's runtime directory: the operator tokens and the systemd and D-Bus sockets;
- either backend's stored sign-in (`auth.json` in the Codex home, `.credentials.json` in
  the Claude home);
- Windows drives that WSL mounts.

Before the first Codex worker starts, a real sandboxed probe checks these refusals, the
refused writes to shared trees and `/tmp`, and the workspace write, and it fails closed
when WSL interop is on; see `.task-based/native-access-check/latest.json`. While the service
runs it reads the interop state every five seconds: while interop is on, no new session
starts on either backend (a hold the dashboard shows, which ends by itself once interop is
off), and turns already running finish. A read counts
as refused unless it returns the file's real content. A protected directory is probed
through a file the preflight places in it or, where it cannot write, one already there;
one with neither is reported under `warnings`. Claude's sandbox denies the same paths.

## WSL 2 on Windows

1. Create a distribution only for this system (Ubuntu), not the one you use every day.
2. In it, `/etc/wsl.conf`:

   ```ini
   [boot]
   systemd=true

   [user]
   default=YOUR_USER

   [interop]
   enabled=false
   appendWindowsPath=false

   [automount]
   enabled=false
   ```

   Interop would let a sandboxed command start Windows programs outside the sandbox;
   automount would show it the Windows drives, sign-ins included. Restart the
   distribution after changing it (`wsl --terminate NAME`).

   `wsl.conf` applies when the distribution starts, and WSL can register its Windows
   interpreter again later: in the pilot interop came back on when `binfmt_misc` was
   mounted anew. A root timer turns it off again within 15 seconds:

   ```bash
   sudo install -m 755 scripts/linux/wsl-interop-off /usr/local/sbin/
   sudo install -m 644 scripts/linux/wsl-interop-off.service scripts/linux/wsl-interop-off.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now wsl-interop-off.timer
   ```

   `journalctl -u wsl-interop-off` shows when it did.
3. In `%UserProfile%\.wslconfig` on Windows, the machine-dependent part, for example on a
   16-core, 32 GB machine:

   ```ini
   [wsl2]
   memory=24GB
   processors=16
   swap=8GB
   vmIdleTimeout=-1
   ```

4. Install the packages (`bubblewrap socat git curl`), uv, and clone this repository and
   the project into the Linux file system (`~`), not under `/mnt/c`.
5. Enable lingering, install Codex, sign in, initialize and run as above. The Windows
   browser reaches `http://localhost:8765/` through WSL's localhost forwarding.

## Lima on macOS

1. Create an Ubuntu VM with the `vz` type, without mounting the Mac's home directory
   (Lima mounts `~` read-only by default; a worker could then read `~/.ssh`), and forward
   the dashboard's port to the Mac's `localhost`:

   ```yaml
   vmType: vz
   cpus: 8
   memory: 24GiB
   mounts: []
   portForwards:
     - guestPort: 8765
       hostIP: 127.0.0.1
   ```

2. Inside the VM, follow the same steps as for WSL 2 (packages, lingering, Codex,
   sign-in). Codex, Lean and the Claude Agent SDK all have linux-arm64 builds.

3. To move a running project here from another host, carry its core with `taskctl export-core`
   and `taskctl import-core` ([DESIGN.md](../DESIGN.md)).

## Settings that depend on the machine

- The VM's CPUs and memory (`.wslconfig`, or the Lima configuration).
- `[lean_service]` in `project.toml`: `threads` (default: the logical cores),
  `reserve_gib` and `commit_reserve_gib` (memory kept free), `warm_gib` (library pages
  kept resident) and `sessions` (resident development-check sessions).
