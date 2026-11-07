from .talk import PEER_TALK_SCHEMA

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_info(version INTEGER PRIMARY KEY);
INSERT OR IGNORE INTO schema_info VALUES(1);
CREATE TABLE IF NOT EXISTS dispatch_hold(
 id INTEGER PRIMARY KEY CHECK(id=1),code TEXT NOT NULL,message TEXT NOT NULL,
 subject_id TEXT,worker_id TEXT,created_at INTEGER NOT NULL,hold_id TEXT);
CREATE TABLE IF NOT EXISTS claude_dispatch_hold(
 id INTEGER PRIMARY KEY CHECK(id=1),code TEXT NOT NULL,message TEXT NOT NULL,
 subject_id TEXT,worker_id TEXT,created_at INTEGER NOT NULL,hold_id TEXT);
CREATE TABLE IF NOT EXISTS released_dispatch_holds(
 hold_id TEXT PRIMARY KEY,released_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS runtime_preferences(
 id INTEGER PRIMARY KEY CHECK(id=1),
 new_worker_backend TEXT NOT NULL CHECK(new_worker_backend IN ('codex','claude')),
 updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY,state TEXT NOT NULL,limits_json TEXT NOT NULL,
 turns_used INTEGER NOT NULL DEFAULT 0,tokens_used INTEGER NOT NULL DEFAULT 0,
 usage_uncertain INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,
 resume_on_start INTEGER NOT NULL DEFAULT 0,
 codex_tokens_used INTEGER NOT NULL DEFAULT 0,claude_tokens_used INTEGER NOT NULL DEFAULT 0,
 pause_after_turn INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS workers(
 id TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'idle',thread_id TEXT,
 worker_number INTEGER CHECK(worker_number>=0),
 reviewer_number INTEGER CHECK(reviewer_number>=1),
 context_epoch INTEGER NOT NULL DEFAULT 0,usage_epoch INTEGER NOT NULL DEFAULT 0,
 active_attempt TEXT,run_id TEXT,
 profile_json TEXT NOT NULL DEFAULT '{}',created_at INTEGER NOT NULL,
 kind TEXT NOT NULL DEFAULT 'ordinary',
 current_task_id TEXT,work_intention TEXT NOT NULL DEFAULT 'idle',
 pending_wake INTEGER NOT NULL DEFAULT 0,
 execution_mode TEXT NOT NULL DEFAULT 'work',
 backend TEXT NOT NULL DEFAULT 'codex' CHECK(backend IN ('codex','claude')),
 rotation_mode TEXT,instructions_sha256 TEXT);
CREATE TABLE IF NOT EXISTS worker_usage_holds(
 worker_id TEXT PRIMARY KEY REFERENCES workers(id),run_id TEXT NOT NULL REFERENCES runs(id),
 reason TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS helper_usage_holds(
 helper_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES runs(id),
 reason TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS tasks(
 id TEXT PRIMARY KEY,current_revision INTEGER NOT NULL,state TEXT NOT NULL,
 state_version INTEGER NOT NULL DEFAULT 1,fencing_counter INTEGER NOT NULL DEFAULT 0,
 active_attempt TEXT,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_revisions(
 task_id TEXT NOT NULL REFERENCES tasks(id),revision INTEGER NOT NULL,
 contract_json TEXT NOT NULL,contract_hash TEXT NOT NULL,
 PRIMARY KEY(task_id,revision));
CREATE INDEX IF NOT EXISTS contract_hash_idx ON task_revisions(contract_hash);
CREATE TABLE IF NOT EXISTS attempts(
 id TEXT PRIMARY KEY,task_id TEXT NOT NULL REFERENCES tasks(id),
 task_revision INTEGER NOT NULL,worker_id TEXT NOT NULL REFERENCES workers(id),
 run_id TEXT NOT NULL REFERENCES runs(id),fencing_token INTEGER NOT NULL,
 state TEXT NOT NULL,
 checkpoint_json TEXT,submission_json TEXT,continuation_id TEXT,snapshot TEXT,
 input_refs_json TEXT NOT NULL DEFAULT '[]',
 created_at INTEGER NOT NULL,ended_at INTEGER,execution_mode TEXT NOT NULL DEFAULT 'work');
CREATE TABLE IF NOT EXISTS attempt_workspaces(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),base_commit TEXT NOT NULL,prepared_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_worker
 ON attempts(worker_id) WHERE ended_at IS NULL;
-- A worker's attempts, latest first: without it each lookup read every attempt (in the Linux
-- pilot 12.5 ms of 23,000 rows, for each worker automatic recovery looked at).
CREATE INDEX IF NOT EXISTS attempts_worker_time ON attempts(worker_id, created_at);
CREATE TABLE IF NOT EXISTS continuations(
 id TEXT PRIMARY KEY,task_id TEXT NOT NULL,task_revision INTEGER NOT NULL,
 preferred_worker TEXT NOT NULL,predicate_json TEXT NOT NULL,
 checkpoint_json TEXT NOT NULL,snapshot TEXT,state TEXT NOT NULL,
 wake_reason TEXT,origin_attempt TEXT NOT NULL,resume_attempt TEXT,
 created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS continuation_state_idx ON continuations(state);
-- A worker's open continuations, which admission asks about for each idle worker on every pass.
CREATE INDEX IF NOT EXISTS continuation_worker_idx ON continuations(preferred_worker,state);
-- When a wait for a person's answer ends unanswered (waits.answer_deadline_minutes).
CREATE TABLE IF NOT EXISTS wait_deadlines(
 continuation_id TEXT PRIMARY KEY REFERENCES continuations(id),due_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS wait_deadline_due_idx ON wait_deadlines(due_at);
-- The operator's choice of waits.answer_deadline_minutes, which overrides project.toml.
CREATE TABLE IF NOT EXISTS answer_deadline_preferences(
 id INTEGER PRIMARY KEY CHECK(id=1),minutes INTEGER NOT NULL CHECK(minutes BETWEEN 1 AND 43200),
 updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS interruption_checkpoints(
 id TEXT PRIMARY KEY,task_id TEXT NOT NULL REFERENCES tasks(id),task_revision INTEGER NOT NULL,
 worker_id TEXT NOT NULL REFERENCES workers(id),origin_attempt TEXT NOT NULL UNIQUE REFERENCES attempts(id),
 turn_intent_id TEXT NOT NULL UNIQUE REFERENCES turn_intents(id),thread_id TEXT NOT NULL,
 usage_epoch INTEGER NOT NULL,workspace TEXT NOT NULL,codex_profile_json TEXT NOT NULL,
 snapshot TEXT NOT NULL,input_refs_json TEXT NOT NULL,checkpoint_json TEXT NOT NULL,
 proof_json TEXT NOT NULL,stop_reason TEXT NOT NULL,state TEXT NOT NULL,
 resume_attempt TEXT,created_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_parked_interruption_worker
 ON interruption_checkpoints(worker_id) WHERE state='parked';
CREATE TABLE IF NOT EXISTS artifacts(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,body_sha TEXT NOT NULL,snapshot TEXT,
 task_id TEXT,task_revision INTEGER,contract_hash TEXT,producer TEXT,
 status TEXT NOT NULL DEFAULT 'candidate',input_refs_json TEXT NOT NULL DEFAULT '[]',
 created_at INTEGER NOT NULL);
-- A task's artifacts: a requirement on a task (Hub._requirement) asks for its accepted ones, and
-- scanned all evidence for them (in the Linux pilot 5 ms each, for 634 waiting requirements on
-- every wake-up).
CREATE INDEX IF NOT EXISTS artifact_task_idx ON artifacts(task_id,task_revision);
-- A rejected or withdrawn artifact the operator recorded as replaced by an accepted one of the
-- same content (artifact.record_replacement): its next version (LATEST_VERSION_SQL).
CREATE TABLE IF NOT EXISTS artifact_replacements(
 original_id TEXT PRIMARY KEY REFERENCES artifacts(id),replacement_id TEXT NOT NULL REFERENCES artifacts(id),
 reason TEXT NOT NULL,recorded_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS evidence(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,
 artifact_id TEXT NOT NULL REFERENCES artifacts(id),policy TEXT NOT NULL,
 verdict TEXT NOT NULL,details_json TEXT NOT NULL,qualifications_json TEXT NOT NULL,
 issuer TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS evidence_lookup_idx ON evidence(artifact_id,policy,sequence);
CREATE TABLE IF NOT EXISTS events(
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,
 object_id TEXT NOT NULL,task_id TEXT,worker_id TEXT,
 payload_json TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS event_task_idx ON events(task_id,sequence);
CREATE INDEX IF NOT EXISTS event_object_idx ON events(object_id,kind,sequence);
-- Events of one kind, by worker: automatic recovery looks for workers' latest admission failures
-- several times a minute, and scanned every event for them (in the Linux pilot 3 s of 234,000
-- events a pass, half the coordination thread, for 80 such events).
CREATE INDEX IF NOT EXISTS event_kind_worker_idx ON events(kind,worker_id,sequence);
CREATE TABLE IF NOT EXISTS receipts(
 principal TEXT NOT NULL,command TEXT NOT NULL,key TEXT NOT NULL,
 request_hash TEXT NOT NULL,response_json TEXT NOT NULL,
 PRIMARY KEY(principal,command,key));
CREATE TABLE IF NOT EXISTS turn_intents(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,attempt_id TEXT NOT NULL,run_id TEXT NOT NULL,
 remote_turn_id TEXT,state TEXT NOT NULL,created_at INTEGER NOT NULL,thread_id TEXT,rotation_mode TEXT,
 UNIQUE(worker_id,remote_turn_id));
CREATE UNIQUE INDEX IF NOT EXISTS one_live_turn
 ON turn_intents(worker_id) WHERE state IN ('prepared','sending','bound','uncertain');
CREATE TABLE IF NOT EXISTS usage_samples(
 worker_id TEXT NOT NULL,epoch INTEGER NOT NULL,total INTEGER NOT NULL,
 sample_json TEXT NOT NULL,created_at INTEGER NOT NULL,
 PRIMARY KEY(worker_id,epoch));
CREATE TABLE IF NOT EXISTS turn_usage_totals(
 intent_id TEXT PRIMARY KEY REFERENCES turn_intents(id),worker_id TEXT NOT NULL,epoch INTEGER NOT NULL,
 start_total INTEGER NOT NULL,end_total INTEGER,updated_at INTEGER NOT NULL,
 first_input_tokens INTEGER,first_cached_tokens INTEGER,max_input_tokens INTEGER,
 input_tokens INTEGER,cached_tokens INTEGER,output_tokens INTEGER,requests INTEGER);
CREATE TABLE IF NOT EXISTS deliveries(
 id TEXT PRIMARY KEY,worker_id TEXT NOT NULL,context_epoch INTEGER NOT NULL,
 sequence INTEGER NOT NULL,body_sha TEXT NOT NULL,state TEXT NOT NULL,
 created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS notifications(
 id TEXT PRIMARY KEY,worker_id TEXT,task_id TEXT,object_id TEXT NOT NULL,
 reason TEXT NOT NULL,critical INTEGER NOT NULL DEFAULT 1,resolved INTEGER NOT NULL DEFAULT 0,
 created_at INTEGER NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
 object_id UNINDEXED,kind UNINDEXED,title,body);

CREATE TABLE IF NOT EXISTS task_collaboration(
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),mode TEXT NOT NULL,
 host_task_id TEXT REFERENCES tasks(id),governance TEXT NOT NULL,
 created_by TEXT NOT NULL,completion_artifact_id TEXT,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_members(
 task_id TEXT NOT NULL REFERENCES tasks(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 executor INTEGER NOT NULL DEFAULT 0,runnable INTEGER NOT NULL DEFAULT 0,
 work_reason TEXT,checkpoint_json TEXT,snapshot TEXT,joined_at INTEGER NOT NULL,
 membership_state TEXT NOT NULL DEFAULT 'member',contribution_plan TEXT,
 PRIMARY KEY(task_id,worker_id));
CREATE TABLE IF NOT EXISTS task_publishers(
 task_id TEXT NOT NULL REFERENCES tasks(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 created_at INTEGER NOT NULL,PRIMARY KEY(task_id,worker_id));
-- Where a published task came from: the task whose attempt published it (NULL: the operator or
-- the system published it). Display only; it grants no authority over the task.
CREATE TABLE IF NOT EXISTS task_origins(
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),source_task_id TEXT REFERENCES tasks(id),
 worker_id TEXT,attempt_id TEXT,recorded_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS task_origin_source ON task_origins(source_task_id);
-- One-time fills of records added after a project began, each run once at startup.
CREATE TABLE IF NOT EXISTS ledger_backfills(name TEXT PRIMARY KEY,completed_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_subscriptions(
 task_id TEXT NOT NULL REFERENCES tasks(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 task_revision INTEGER,reason TEXT NOT NULL,created_at INTEGER NOT NULL,
 PRIMARY KEY(task_id,worker_id));
CREATE TABLE IF NOT EXISTS candidate_bindings(
 artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id),task_id TEXT NOT NULL REFERENCES tasks(id),
 task_revision INTEGER NOT NULL,contract_hash TEXT NOT NULL,body_sha TEXT NOT NULL,
 snapshot TEXT NOT NULL,input_refs_json TEXT NOT NULL,policy TEXT NOT NULL,
 author_id TEXT NOT NULL,coauthors_json TEXT NOT NULL,completion INTEGER NOT NULL,
 review_task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS review_requests(
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),candidate_id TEXT NOT NULL UNIQUE REFERENCES artifacts(id),
 host_task_id TEXT NOT NULL REFERENCES tasks(id),state TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS peer_review_receipts(
 id TEXT PRIMARY KEY,review_task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 candidate_id TEXT NOT NULL REFERENCES artifacts(id),reviewer_id TEXT NOT NULL REFERENCES workers(id),
 binding_json TEXT NOT NULL,verdict TEXT NOT NULL,rationale TEXT NOT NULL,
 evidence_json TEXT NOT NULL,qualifications_json TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS private_records(
 artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id),worker_id TEXT NOT NULL REFERENCES workers(id),
 attempt_id TEXT NOT NULL REFERENCES attempts(id));
CREATE INDEX IF NOT EXISTS members_runnable_idx ON task_members(runnable,worker_id);
CREATE INDEX IF NOT EXISTS review_host_idx ON review_requests(host_task_id,state);
CREATE TABLE IF NOT EXISTS attempt_stop_requests(
 attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),reason TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS repository_bases(
 worker_id TEXT PRIMARY KEY REFERENCES workers(id),base_commit TEXT NOT NULL,
 initialized_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS candidate_repositories(
 artifact_id TEXT PRIMARY KEY REFERENCES artifacts(id),base_commit TEXT NOT NULL,
 paths_json TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS review_integrations(
 id TEXT PRIMARY KEY,workspace_id TEXT NOT NULL,review_task_id TEXT NOT NULL REFERENCES tasks(id),
 candidate_id TEXT NOT NULL REFERENCES artifacts(id),reviewer_id TEXT NOT NULL REFERENCES workers(id),
 prepare_attempt TEXT NOT NULL REFERENCES attempts(id),workspace_relative_path TEXT NOT NULL,
 base_commit TEXT NOT NULL,target_head TEXT NOT NULL,candidate_commit TEXT NOT NULL,
 commit_sha TEXT,snapshot_sha TEXT,state TEXT NOT NULL,validation_json TEXT,
 publication_attempt TEXT,created_at INTEGER NOT NULL,published_at INTEGER);
CREATE INDEX IF NOT EXISTS integrations_review_idx ON review_integrations(review_task_id,created_at);
CREATE TABLE IF NOT EXISTS repository_publications(
 integration_id TEXT PRIMARY KEY REFERENCES review_integrations(id),candidate_id TEXT NOT NULL,
 reviewer_id TEXT NOT NULL,commit_sha TEXT NOT NULL,snapshot_sha TEXT NOT NULL,
 previous_head TEXT NOT NULL,review_json TEXT NOT NULL,created_at INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS review_acceptances(
 review_task_id TEXT PRIMARY KEY REFERENCES tasks(id),candidate_id TEXT NOT NULL REFERENCES artifacts(id),
 reviewer_id TEXT NOT NULL REFERENCES workers(id),attempt_id TEXT NOT NULL REFERENCES attempts(id),
 report_json TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS worker_controls(
 worker_id TEXT PRIMARY KEY REFERENCES workers(id),run_id TEXT NOT NULL REFERENCES runs(id),
 control TEXT NOT NULL CHECK(control IN ('paused','resumed')),created_at INTEGER NOT NULL,
 after_turn INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS group_controls(
 scope TEXT PRIMARY KEY CHECK(scope IN ('codex','codex.workers','codex.reviewers','claude')),
 run_id TEXT NOT NULL REFERENCES runs(id),
 control TEXT NOT NULL CHECK(control IN ('paused','resumed')),created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS native_task_files(
 task_id TEXT NOT NULL REFERENCES tasks(id),revision INTEGER NOT NULL,contract_sha256 TEXT NOT NULL,
 PRIMARY KEY(task_id,revision));
CREATE TABLE IF NOT EXISTS native_draft_heads(
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),revision INTEGER NOT NULL,
 content_key TEXT NOT NULL,manifest_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_dag_head(
 id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL,
 content_key TEXT NOT NULL,manifest_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_record_files(
 record_id TEXT PRIMARY KEY REFERENCES artifacts(id),path TEXT NOT NULL,sha256 TEXT NOT NULL,
 category TEXT NOT NULL,owner_id TEXT);
CREATE TABLE IF NOT EXISTS native_artifact_manifests(
 record_id TEXT PRIMARY KEY REFERENCES artifacts(id),manifest_sha256 TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS archive_relocations(
 record_id TEXT PRIMARY KEY REFERENCES artifacts(id),area TEXT NOT NULL,
 receipt_json TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_tree_intents(
 task_id TEXT NOT NULL REFERENCES tasks(id),revision INTEGER NOT NULL,created_at INTEGER NOT NULL,
 PRIMARY KEY(task_id,revision));
""" + PEER_TALK_SCHEMA

# One step along an artifact's versions, from `chain.id`: the replacement the operator recorded
# for it, else the candidate that names it in `replaces` (artifact_obligations).
NEXT_VERSION_SQL = ("COALESCE(r.replacement_id,o.artifact_id),chain.depth+1 FROM chain "
                    "LEFT JOIN artifact_replacements r ON r.original_id=chain.id "
                    "LEFT JOIN artifact_obligations o ON o.predecessor_id=chain.id "
                    "WHERE COALESCE(r.replacement_id,o.artifact_id) IS NOT NULL AND chain.depth<64")
# The newest version of an artifact, the one a requirement naming it stands for: the artifact
# itself until it is corrected. In the Linux pilot 189 of 207 rejected or withdrawn candidates
# were corrected with `replaces`, yet a requirement naming one could never be met again.
LATEST_VERSION_SQL = ("WITH RECURSIVE chain(id,depth) AS (SELECT ?,0 UNION ALL SELECT " + NEXT_VERSION_SQL
                      + ") SELECT id FROM chain ORDER BY depth DESC LIMIT 1")
