# Project setup

Open the initialized project's local service in your browser.
Choose **Project setup** in the page header:

1. Enter the Task 0 goal, including what would count as completion.
2. Under **Agents and sessions**, choose the models and sessions: the same panel the
   dialog shows once the project runs (backend of the next new worker, the model and
   effort of workers on Codex and on Claude, of reviewers, of the Integrator, of the
   Dispatcher and of the Waker, the answer deadline, session limits, idle rotation and the
   Integrator's batch rule). None of the models, efforts or the deadline has a default:
   the panel lists what is still to choose, and Start starts nothing until every one is
   chosen (Workers on Claude only where Claude workers can start).
3. Click **Start**.

Before the first run the **Worker · Codex** row is the project's own model and effort:
select its model, then one of its supported efforts. Start saves them in this project's
`project.toml` (`[codex]` `model` and `effort`), so the project, not a separate operator
choice, holds them. No other role takes them: reviewers and the Integrator have rows of their
own, which project.toml may also fill (`[reviewers]`, `[integrator]`). Start first saves every
other choice that changed in the panel, exactly as its **Save** button does, and starts
nothing if that save fails; a choice saved there stays saved if a later step of Start
fails.

The service then preserves the exact goal in Task 0's native contract and prepares the
local managed Git repository used for independent review and publication. It creates
the first run and explicitly activates it. Other configuration, the selected Codex
profile, and dependency/application versions are preserved. No GitHub remote is added.
The normal worker list, task browser, sessions, messages, and Pause/Resume controls
then show the run. After the first run the panel's rows are ordinary choices for
conversations that begin later; what project.toml chooses stays there for every role the
operator has not chosen for. A run resumes, like it starts, only while every choice is made.

## Existing projects

An existing Task 0 with no execution history can be started from this form. Its
goal is displayed read-only; its exact contract and revision remain unchanged.
The agents' models and sessions can still be chosen before the first run.

Once a project has any run, worker, attempt, or helper history, setup settings are
locked. Use the existing run controls to resume a paused project. Project setup
does not rewrite previous sessions, replace Task 0, or create a second run after
a previous run has completed or stopped.

Opening the page, opening setup, refreshing status, or reading the model list
does not start a worker. The authenticated model-list request starts only a
short metadata connection to the configured Codex executable/profile; it creates
no Codex thread or model turn. Sign in to that profile before starting execution.

## Models and effort

The Worker · Codex picker uses the account/client's actual `model/list` response, including
supported and default reasoning efforts. It does not contain a fixed list of
model names or effort levels. A previously configured model that is unavailable
is shown as a mismatch requiring a deliberate selection. An unavailable catalog
or required login blocks Start while preserving the goal draft.

The startup request checks the catalog again before saving configuration or
admitting work. The executable must match the project's configured audited Codex
protocol version; setup does not silently upgrade or change this version.

Protocol reference: [Codex App Server model discovery](https://learn.chatgpt.com/docs/app-server#list-models-modellist).

## Retry and recovery

Duplicate clicks share a request key. Task 0, the first run, and the setup receipt
commit together. An exact replay returns the same task/run identities and never
re-arms or resumes execution. A different goal or selection cannot reuse that key.

If the connection is lost, use **Refresh status** before deciding what to do.
The pending request retains its original data and key. The setup commit marks
the run as driven, so a service restart after that commit resumes it. A service
started with `--no-resume` leaves it prepared; where no worker or turn has begun,
**Start prepared run** starts it explicitly. Existing execution recovery and
uncertain-outcome checks still apply.

Unsubmitted drafts and pending request keys stay in the current browser page's
memory. Reloading the page reads saved setup state without recovering an unsent
draft. Switching to another project clears the previous project's form state;
the browser binds each Start request to the project identity it read.

Local configuration and repository preparation precede the atomic task/run
creation. If a later preparation step fails, valid local settings or an initialized
repository may remain for the corrected retry, but no worker is started by that
failed preparation. Unrecorded publication heads require the normal repository
reconciliation rather than being adopted by setup.

## Read and start endpoints

All routes need the project's operator token:

- `GET /v1/setup`: first-run availability, existing Task 0, chosen settings,
  latest run/activation status, and the saved setup receipt.
- `GET /v1/setup/models`: available model/effort choices and whether login is
  required; account identifiers and credentials are not returned.
- `POST /v1/setup/start`: `key`, `goal`, `model`, `effort`, and, for an existing
  Task 0, its `task_id` and `task_revision`. Creates and activates a first run
  only after validation and local preparation. The browser also supplies
  `project_id` to reject submissions against a changed service/project.

An explicit prepared-run recovery uses the existing
`POST /v1/runs/{run_id}/drive` endpoint. The task and run protocols otherwise
remain unchanged.
