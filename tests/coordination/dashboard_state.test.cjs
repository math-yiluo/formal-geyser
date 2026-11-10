const {test} = require('node:test');
const assert = require('node:assert/strict');
const {WorkerHistoryStore, requestResolutionPayload, projectStatus, personControl, controlConsole} = require('../../src/formal_geyser/coordination/dashboard.js');

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function turn(ordinal, text = 'Reply') {
  return {key: 'turn-' + ordinal, ordinal, inputs: [{input_id: 'input-' + ordinal, text: 'Prompt ' + ordinal}],
    replies: [{key: 'reply-' + ordinal, text}], actions: []};
}
function page(first, last, total = last, extras = {}) {
  return {session: {key: 'main', turn_count: total, context_version: 1},
    turns: Array.from({length: last - first + 1}, (_, index) => turn(first + index)),
    context: [{type: 'instructions', text: 'Standing instructions'}],
    has_more: first > 1, next_before: first > 1 ? first : null, ...extras};
}
const chat = () => ({messages: [], delivery_updates: [], next_after: 0, has_more: false});

test('overlapping polls and double clicks share one request and never duplicate turns', async () => {
  const response = deferred(); let calls = 0;
  const store = new WorkerHistoryStore(() => { calls++; return response.promise; });
  const first = store.loadSession('worker-a', 'main');
  const second = store.loadSession('worker-a', 'main');
  assert.equal(calls, 1);
  response.resolve(page(61, 80));
  await Promise.all([first, second]);
  const view = store.session('worker-a', 'main');
  assert.equal(view.turns.size, 20);
  assert.equal(view.before, 61);
  assert.equal(view.turns.get('turn-80').inputs[0].text, 'Prompt 80');
});

test('older paging and a live poll serialize without losing either range', async () => {
  const replies = [deferred(), deferred(), deferred()]; let calls = 0;
  const store = new WorkerHistoryStore(() => replies[calls++].promise);
  const initial = store.loadSession('a', 'main'); replies[0].resolve(page(61, 80)); await initial;
  const older = store.loadSession('a', 'main', 61);
  const poll = store.loadSession('a', 'main');
  assert.equal(calls, 2);
  replies[1].resolve(page(41, 60, 80)); await older;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls, 3);
  replies[2].resolve(page(62, 81)); await poll;
  const view = store.session('a', 'main');
  assert.equal(view.turns.size, 41);
  assert.equal(view.before, 41);
  assert.equal(view.turns.get('turn-81').ordinal, 81);
  assert.deepEqual(view.ranges, [[41, 81]]);
});

test('response replacement is idempotent and helpers never enter the main session', async () => {
  let result = page(1, 1);
  const store = new WorkerHistoryStore(async () => result);
  store.worker('a').sessions = [{key: 'main', context_version: 1}];
  await store.loadSession('a', 'main');
  result = page(1, 1, 1, {turns: [turn(1, 'The full response')]});
  await store.loadSession('a', 'main'); await store.loadSession('a', 'main');
  assert.equal(store.session('a', 'main').turns.size, 1);
  assert.equal(store.session('a', 'main').turns.get('turn-1').replies[0].text, 'The full response');
  await store.loadSession('a', 'helper');
  assert.notEqual(store.session('a', 'main').turns, store.session('a', 'helper').turns);
  assert.equal(store.session('b', 'main').turns.size, 0);
});

test('late native binding removes superseded orphan turns from an existing view', async () => {
  let result = page(1, 2);
  const store = new WorkerHistoryStore(async () => result);
  await store.loadSession('a', 'main');
  result = page(1, 1, 1, {superseded_turn_keys: ['turn-2']});
  await store.loadSession('a', 'main');
  assert.equal(store.session('a', 'main').turns.size, 1);
});

test('context is requested once per version, not with every poll', async () => {
  const paths = [];
  const store = new WorkerHistoryStore(async path => { paths.push(path); return page(1, 1); });
  store.worker('a').sessions = [{key: 'main', context_version: 1}];
  await store.loadSession('a', 'main'); await store.loadSession('a', 'main');
  assert.match(paths[0], /include_context=true/);
  assert.match(paths[1], /include_context=false/);
  store.worker('a').sessions[0].context_version = 2;
  await store.loadSession('a', 'main');
  assert.match(paths[2], /include_context=true/);
});

test('a delayed send never clears another worker draft or a newer draft for its sender', async () => {
  const send = deferred(); const commands = [];
  const store = new WorkerHistoryStore((path, body) => {
    if (body) { commands.push(body); return send.promise; }
    return Promise.resolve(chat());
  });
  store.worker('a').draft = 'First message';
  const pending = store.send('a');
  store.worker('b').draft = 'Keep this separate draft';
  store.worker('a').draft = 'A new thought while the first message is sending';
  send.resolve({ok: true}); await pending;
  assert.equal(commands[0].payload.worker_id, 'a');
  assert.equal(commands[0].payload.text, 'First message');
  assert.equal(store.worker('a').draft, 'A new thought while the first message is sending');
  assert.equal(store.worker('b').draft, 'Keep this separate draft');
  assert.equal(store.worker('a').sending, false);
});

test('an ambiguous send retains its idempotency key for explicit retry', async () => {
  const commands = []; let fail = true;
  const store = new WorkerHistoryStore(async (_path, body) => {
    if (!body) return chat();
    commands.push(body);
    if (fail) throw Error('Connection lost');
    return {ok: true};
  });
  store.worker('a').draft = 'Please inspect the source';
  await assert.rejects(store.send('a'), /Connection lost/);
  assert.equal(store.worker('a').draft, 'Please inspect the source');
  fail = false; await store.send('a');
  assert.equal(commands[0].key, commands[1].key);
  assert.equal(store.worker('a').draft, '');
});

test('message polling de-duplicates records and applies delayed delivery receipts', async () => {
  const message = {id: 'message-1', sequence: 1, text: 'Exact text', acknowledged_at: null};
  let result = {messages: [message], next_after: 1, has_more: false};
  const store = new WorkerHistoryStore(async () => result);
  await store.loadChat('a'); await store.loadChat('a');
  result = {messages: [], next_after: 1, has_more: false, delivery_updates: [{id: 'message-1', acknowledged_at: 42}]};
  await store.loadChat('a');
  assert.equal(store.worker('a').chats.size, 1);
  assert.equal(store.worker('a').chats.get('message-1').acknowledged_at, 42);
});

test('a failed request releases its lock and preserves previously loaded history', async () => {
  let fail = false;
  const store = new WorkerHistoryStore(async () => { if (fail) throw Error('Offline'); return page(1, 2); });
  await store.loadSession('a', 'main'); fail = true;
  await assert.rejects(store.loadSession('a', 'main'), /Offline/);
  assert.equal(store.session('a', 'main').turns.size, 2);
  assert.equal(store.session('a', 'main').inflight, null);
  fail = false; await store.loadSession('a', 'main');
  assert.equal(store.session('a', 'main').error, '');
});

test('returning after many new turns refreshes an old cached partial response', async () => {
  const paths = [];
  let advanced = false;
  const store = new WorkerHistoryStore(async path => {
    paths.push(path);
    if (!advanced) return page(1, 1, 1, {turns: [{...turn(1, 'Partial'), status: 'in_progress'}]});
    if (path.includes('before=2')) return page(1, 1, 21, {turns: [{...turn(1, 'Completed response'), status: 'completed'}]});
    return page(2, 21, 21, {turns: Array.from({length: 20}, (_, index) => ({...turn(index + 2), status: 'completed'}))});
  });
  await store.loadSession('a', 'main');
  advanced = true;
  await store.loadSession('a', 'main');
  const history = store.session('a', 'main');
  assert.equal(history.turns.size, 21);
  assert.equal(history.turns.get('turn-1').status, 'completed');
  assert.equal(history.turns.get('turn-1').replies[0].text, 'Completed response');
  assert.equal(history.older, false);
  assert(paths.some(path => path.includes('before=2')));
  const checked = paths.length;
  await store.loadSession('a', 'main');
  assert.equal(paths.length, checked + 1);
});

test('literature response validates before any command and submits one atomic resolution', async () => {
  const request = {id: 'request-a', kind: 'literature', title: 'Reference'};
  const commands = [];
  async function submit(text, path) {
    const payload = requestResolutionPayload(request, 'fulfilled', text, path, 'response-a');
    commands.push({name: 'request.resolve', payload});
  }
  await assert.rejects(submit('  ', 'wrong.txt'), /Enter a reply/);
  await assert.rejects(submit('Reply', ' '), /Supply a readable/);
  await assert.rejects(submit('λ'.repeat(65537), 'corrected.txt'), /131072 UTF-8 bytes/);
  assert.equal(commands.length, 0);
  await submit('Exact reply.', ' corrected.txt ');
  assert.deepEqual(commands, [{name: 'request.resolve', payload: {
    request_id: 'request-a', outcome: 'fulfilled', text: 'Exact reply.', message_id: 'response-a',
    source_path: 'corrected.txt'
  }}]);
});

test('pending and declined literature replies do not register the drafted path', () => {
  for (const outcome of ['pending', 'declined']) {
    const payload = requestResolutionPayload({id: 'request-a', kind: 'literature'}, outcome, 'Exact reply.', 'draft.txt', 'response');
    assert.equal(payload.text, 'Exact reply.');
    assert(!('source_path' in payload));
    assert(!('source_id' in payload));
  }
});

test('pause after turn counts the turns still finishing and offers both pauses', () => {
  const service = {armed_runs: ['r']};
  const running = {id: 'r', state: 'running', pause_after_turn: 0};
  assert.deepEqual(projectStatus({runs: [running], workers: [], service}).actions, ['pause_after_turn', 'pause']);
  const run = {id: 'r', state: 'paused', pause_after_turn: 1};
  const live = {id: 'a', active_attempt: 'x', state: 'running', run_control: null};
  const quiet = {id: 'b', active_attempt: null, state: 'suspended', run_control: null};
  const pausing = projectStatus({runs: [run], workers: [live, quiet], service});
  assert.deepEqual(pausing.actions, ['resume', 'pause']);  // Pause all now still interrupts it.
  assert.match(pausing.text, /1 turn in progress finishes on its own/);
  assert.deepEqual(personControl(run, live), {individual: null, group: null, running: false, afterTurn: true, finishing: true});
  assert.equal(personControl(run, quiet).finishing, false);
  const paused = projectStatus({runs: [run], workers: [quiet], service});
  assert.deepEqual(paused.actions, ['resume']);
  assert.match(paused.text, /^Paused\. No new turns start/);
  const hard = {id: 'r', state: 'paused', pause_after_turn: 0};
  assert.equal(personControl(hard, live).afterTurn, false);
  // One person paused after its turn while the run continues.
  const alone = {id: 'c', active_attempt: 'y', state: 'running', run_control: 'paused', run_control_after_turn: 1};
  assert.deepEqual(personControl(running, alone), {individual: 'paused', group: null, running: false, afterTurn: true, finishing: true});
  assert.equal(personControl(running, {...alone, run_control_after_turn: 0}).afterTurn, false);
  assert.equal(personControl(running, {...alone, active_attempt: null}).finishing, false);
});

test('the controls console pauses after turn or resumes Everyone and each group apart', () => {
  const service = {armed_runs: ['r']};
  const groups = {codex: {control: null, state: 'mixed', pause_mode: null},
    'codex.workers': {control: 'paused', state: 'paused', pause_mode: 'after_turn'},
    'codex.reviewers': {control: null, state: 'running', pause_mode: null},
    claude: {control: null, state: 'running', pause_mode: null}};
  const run = {id: 'r', state: 'running', pause_after_turn: 0, groups};
  const finishing = {id: 'a', backend: 'codex', kind: 'ordinary', active_attempt: 'x', state: 'running', group_control: 'paused'};
  const reviewer = {id: 'b', backend: 'codex', kind: 'review', active_attempt: 'y', state: 'running', group_control: null};
  const claude = {id: 'c', backend: 'claude', kind: 'ordinary', active_attempt: null, state: 'idle', group_control: null};
  const integrator = {id: 'i', backend: 'codex', kind: 'integrator', active_attempt: 'z', state: 'running'};
  const rows = Object.fromEntries(controlConsole(run, [finishing, reviewer, claude, integrator]).map(row => [row.key, row]));
  assert.deepEqual(Object.keys(rows), ['run', 'codex', 'codex.workers', 'codex.reviewers', 'claude', 'integrator']);
  assert.deepEqual(rows.run.actions, ['pause_after_turn', 'pause']);
  assert.equal(rows.run.text, 'Running · 3 turns in progress');
  assert.deepEqual(rows['codex.workers'].actions, ['resume']);
  assert.equal(rows['codex.workers'].text, 'Paused after turns · 1 turn still running');
  assert.deepEqual(rows['codex.reviewers'].actions, ['pause_after_turn']);
  assert.deepEqual(rows.codex.actions, ['pause_after_turn', 'resume']);  // Its workers and reviewers apart.
  assert.equal(rows.codex.live, 2);  // The Integrator is in no Codex group, only its own.
  assert.equal(rows.claude.text, 'Running');
  // The Integrator has its own row, apart from the Codex reviewers.
  assert.equal(rows.integrator.label, 'Integrator');
  assert.equal(rows.integrator.live, 1);
  assert.deepEqual(rows.integrator.actions, ['pause_after_turn']);
  const held = {...run, groups: {...groups, integrator: {control: 'paused', state: 'paused', pause_mode: 'after_turn'}}};
  const integratorHeld = Object.fromEntries(controlConsole(held, [integrator]).map(row => [row.key, row]));
  assert.equal(integratorHeld.integrator.text, 'Paused after turns · 1 turn still running');
  assert.deepEqual(integratorHeld.integrator.actions, ['resume']);
  assert.equal(integratorHeld['codex.reviewers'].text, 'Running');
  assert.deepEqual(personControl(run, finishing), {individual: null, group: 'paused', running: false, afterTurn: true, finishing: true});
  assert.match(projectStatus({runs: [run], workers: [finishing], service}).text, /paused after turns: Codex workers/);
  // While everyone is paused, Claude alone was resumed.
  const paused = {id: 'r', state: 'paused', pause_after_turn: 1, groups: {...groups,
    codex: {control: null, state: 'paused', pause_mode: 'after_turn'},
    'codex.workers': {control: null, state: 'paused', pause_mode: 'after_turn'},
    'codex.reviewers': {control: null, state: 'paused', pause_mode: 'after_turn'},
    claude: {control: 'resumed', state: 'running', pause_mode: null}}};
  const resumed = Object.fromEntries(controlConsole(paused, [claude]).map(row => [row.key, row]));
  assert.equal(resumed.claude.text, 'Resumed while others are paused');
  assert.deepEqual(resumed.claude.actions, ['pause_after_turn']);
  assert.equal(resumed.codex.text, 'Paused with everyone');
  assert.deepEqual(resumed.run.actions, ['resume']);
  const status = projectStatus({runs: [paused], workers: [{...claude, group_control: 'resumed'}], service});
  assert.equal(status.text, 'Paused · resumed: All Claude.');
  assert.deepEqual(personControl(paused, {...claude, group_control: 'resumed'}).running, true);
  assert.deepEqual(controlConsole({id: 'r', state: 'stopped'}, []), []);
});
