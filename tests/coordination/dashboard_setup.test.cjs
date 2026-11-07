'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');
const {ProjectSetupStore, AGENT_ROWS} = require('../../src/formal_geyser/coordination/dashboard.js');

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function state(extras = {}) {
  return {project_id: 'project-a', can_configure: true, reason: null, task: null,
    settings: {model: 'catalog-model', effort: 'careful'}, repository: {enabled: true, initialized: false},
    run: null, can_drive: false, receipt: null, ...extras};
}
function models(extras = {}) {
  return {models: [
    {id: 'catalog-model', name: 'Catalog Model', description: 'Main model from the profile.',
      efforts: [{id: 'quick', description: 'A quick check.'}, {id: 'careful', description: 'A careful check.'}],
      default_effort: 'quick', is_default: true},
    {id: 'different-model', name: 'Different Model', description: 'Another model.',
      efforts: [{id: 'thorough', description: 'A thorough check.'}], default_effort: 'thorough', is_default: false},
  ], authentication_required: false, source: 'codex/model/list', ...extras};
}
function started(extras = {}) {
  return {task_id: 'task-zero', run_id: 'run-zero', replayed: false, armed: true, driving: true, ...extras};
}
function prepared(key = 'setup-key', extras = {}) {
  return state({can_configure: false, reason: 'This project already has a run. Use existing controls.',
    task: {id: 'task-zero', title: 'Task 0', objective: 'The exact goal.', revision: 1},
    run: {id: 'run-zero', state: 'running', armed: false, driving: false}, can_drive: true,
    receipt: {key, task_id: 'task-zero', run_id: 'run-zero', model: 'catalog-model', effort: 'careful'}, ...extras});
}
function harness(snapshot = state(), catalog = models(), submit = async () => started()) {
  const calls = [];
  let keys = 0;
  const store = new ProjectSetupStore(async (url, body) => {
    calls.push({url, body});
    if (body !== undefined) return submit(url, body);
    if (url === '/v1/setup') return snapshot instanceof Error ? Promise.reject(snapshot) : snapshot;
    if (url === '/v1/setup/models') return catalog instanceof Error ? Promise.reject(catalog) : catalog;
    throw Error('Unexpected request ' + url);
  }, () => {}, () => 'setup-key' + (keys++ ? '-' + keys : ''));
  return {store, calls, setState: value => { snapshot = value; }, setModels: value => { catalog = value; }};
}

test('opening setup loads the catalog once; status polling and closing never start execution or poll models', async () => {
  const {store, calls} = harness();
  assert.equal(calls.length, 0);
  await store.show();
  assert.deepEqual(calls.map(call => call.url), ['/v1/setup', '/v1/setup/models']);
  store.setGoal('Keep my unsent goal.');
  store.selectEffort('quick');
  await store.refresh(); await store.refresh();
  assert.equal(store.goal, 'Keep my unsent goal.');
  assert.equal(store.effort, 'quick');
  assert.equal(calls.filter(call => call.url === '/v1/setup/models').length, 1);
  assert.ok(calls.every(call => call.body === undefined));
  store.setOpen(false);
  const before = calls.length;
  await store.refresh(); await store.loadModels();
  assert.equal(calls.length, before);
  await store.show();
  assert.equal(store.goal, 'Keep my unsent goal.');
  assert.equal(store.effort, 'quick');
  assert.equal(calls.filter(call => call.url === '/v1/setup/models').length, 2);
});

test('overlapping setup and catalog reads share requests', async () => {
  const status = deferred(), catalog = deferred(), calls = [];
  const store = new ProjectSetupStore(url => { calls.push(url); return url === '/v1/setup' ? status.promise : catalog.promise; });
  const first = store.show(), second = store.show();
  assert.deepEqual(calls, ['/v1/setup']);
  status.resolve(state());
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(calls, ['/v1/setup', '/v1/setup/models']);
  catalog.resolve(models()); await Promise.all([first, second]);
  assert.equal(store.stateLoad, null); assert.equal(store.modelsLoad, null);
});

test('saved model and effort survive catalog defaults and effort choices depend on the selected model', async () => {
  const {store} = harness();
  await store.show();
  assert.equal(store.model, 'catalog-model');
  assert.equal(store.effort, 'careful');
  store.selectModel('different-model');
  assert.equal(store.effort, 'thorough');
  store.selectEffort('careful');
  assert.equal(store.effort, 'thorough');
  store.selectModel('catalog-model');
  assert.equal(store.effort, 'quick');
  store.selectModel('invented-model');
  assert.equal(store.model, 'catalog-model');
});

test('only missing settings use catalog defaults; unsupported settings require an explicit selection', async () => {
  const fresh = harness(state({settings: {model: null, effort: ''}}));
  await fresh.store.show();
  assert.equal(fresh.store.model, 'catalog-model'); assert.equal(fresh.store.effort, 'quick');
  const {store, calls} = harness(state({settings: {model: 'unavailable-model', effort: 'unsupported'}}));
  await store.show(); store.setGoal('A valid goal.');
  assert.equal(store.model, 'unavailable-model'); assert.equal(store.effort, 'unsupported');
  assert.match(store.selectionIssue, /unavailable-model.*unavailable/);
  await assert.rejects(store.start(), /unavailable-model/);
  assert.equal(calls.filter(call => call.body).length, 0);
  store.selectModel('catalog-model');
  assert.equal(store.effort, 'quick'); assert.equal(store.selectionIssue, '');
  const unsupportedEffort = harness(state({settings: {model: 'catalog-model', effort: 'unsupported'}}));
  await unsupportedEffort.store.show();
  assert.equal(unsupportedEffort.store.effort, 'unsupported');
  assert.match(unsupportedEffort.store.selectionIssue, /Effort.*unsupported/);
  unsupportedEffort.store.selectEffort('careful');
  assert.equal(unsupportedEffort.store.selectionIssue, '');
});

test('authentication-required and empty catalogs block Start with profile guidance', async () => {
  const {store, calls, setModels} = harness(state(), models({authentication_required: true}));
  await store.show(); store.setGoal('A valid goal.');
  assert.match(store.startBlockedReason, /Sign in.*configured worker profile/);
  await assert.rejects(store.start(), /Sign in/);
  setModels(models({models: [], authentication_required: false})); await store.loadModels();
  assert.match(store.startBlockedReason, /No models/);
  setModels(models()); await store.loadModels();
  assert.equal(store.startBlockedReason, '');
  assert.equal(calls.filter(call => call.body).length, 0);
});

test('catalog failures retain the goal and selections until an explicit model retry succeeds', async () => {
  const {store, calls, setModels} = harness();
  await store.show(); store.setGoal('Retain this draft.'); store.selectEffort('quick');
  setModels(Error('Profile is offline'));
  await assert.rejects(store.loadModels(), /offline/);
  assert.equal(store.goal, 'Retain this draft.'); assert.equal(store.effort, 'quick');
  await assert.rejects(store.start(), /Load available models/);
  await store.refresh();
  assert.equal(calls.filter(call => call.url === '/v1/setup/models').length, 2);
  setModels(models()); await store.loadModels();
  assert.equal(store.modelsError, ''); assert.equal(store.goal, 'Retain this draft.');
  assert.equal(store.effort, 'quick');
});

test('an existing Task 0 is read-only and submitted with its exact goal, id, and revision', async () => {
  const goal = '  Keep the original goal.\nAnd its exact formatting.\n';
  const {store, calls} = harness(state({task: {id: 'existing-task', title: 'Task 0', objective: goal, revision: 7}}));
  await store.show(); store.setGoal('An unauthorized replacement.');
  assert.equal(store.goal, goal);
  await store.start();
  assert.deepEqual(calls.at(-1), {url: '/v1/setup/start', body: {key: 'setup-key', project_id: 'project-a', goal,
    model: 'catalog-model', effort: 'careful', task_id: 'existing-task', task_revision: 7}});
});

test('existing execution and role locks prevent catalog loading, edits, Start, and unauthorized drive', async () => {
  const {store, calls} = harness(prepared('different-key', {can_drive: false, reason: 'Only an operator can configure this project.'}));
  await store.show();
  assert.deepEqual(calls.map(call => call.url), ['/v1/setup']);
  store.setGoal('Change'); store.selectModel('different-model'); store.selectEffort('quick');
  assert.equal(store.goal, 'The exact goal.'); assert.equal(store.model, 'catalog-model'); assert.equal(store.effort, 'careful');
  await assert.rejects(store.start(), /Only an operator/);
  await assert.rejects(store.drive(), /not available/);
  assert.equal(calls.length, 1);
});

test('a capability change during an open form prevents a previously valid Start', async () => {
  const {store, calls, setState} = harness();
  await store.show(); store.setGoal('A valid goal.');
  setState(state({can_configure: false, reason: 'The current token is read-only.'}));
  await store.refresh();
  assert.equal(store.editable, false);
  await assert.rejects(store.start(), /read-only/);
  assert.equal(calls.filter(call => call.body).length, 0);
});

test('goal validation blocks empty and oversized inputs and permits the boundary', async () => {
  const {store, calls} = harness();
  await store.show(); store.setGoal(' \n ');
  await assert.rejects(store.start(), /Enter the Task 0 goal/);
  store.setGoal('x'.repeat(20001));
  await assert.rejects(store.start(), /20,000/);
  assert.equal(calls.filter(call => call.body).length, 0);
  store.setGoal('x'.repeat(20000)); await store.start();
  assert.equal(calls.at(-1).body.goal.length, 20000);
});

test('double clicks coalesce into one Start and successful setup cannot be resubmitted', async () => {
  const response = deferred();
  const {store, calls} = harness(state(), models(), () => response.promise);
  await store.show(); store.setGoal('A valid goal.');
  const first = store.start(), second = store.start();
  assert.equal(first, second); assert.equal(store.editable, false);
  assert.equal(calls.filter(call => call.body).length, 1);
  response.resolve(started()); await Promise.all([first, second]);
  assert.equal(store.pending, null); assert.equal(store.submission, null);
  await assert.rejects(store.start(), /saved/);
  assert.equal(calls.filter(call => call.body).length, 1);
});

test('ambiguous failures preserve the exact request and key for explicit retry without an automatic POST', async () => {
  let fail = true;
  const {store, calls} = harness(state(), models(), async () => { if (fail) throw Error('Connection closed'); return started({replayed: true}); });
  await store.show(); store.setGoal('The exact goal.');
  await assert.rejects(store.start(), /Connection closed/);
  const pending = store.pending;
  assert.match(store.submitError, /could not be confirmed/);
  store.setGoal('Another goal'); store.selectModel('different-model');
  assert.equal(store.goal, 'The exact goal.'); assert.equal(store.model, 'catalog-model');
  await store.refresh(); await store.refresh();
  assert.equal(calls.filter(call => call.body).length, 1);
  assert.equal(store.pending, pending);
  fail = false; await store.start();
  const submissions = calls.filter(call => call.body);
  assert.equal(submissions.length, 2); assert.deepEqual(submissions[0].body, submissions[1].body);
  assert.equal(store.pending, null);
});

test('a read reconciles only the matching receipt and leaves a prepared run waiting for explicit drive', async () => {
  const {store, calls, setState} = harness(state(), models(), async () => { throw Object.assign(Error('Activation unavailable'), {status: 503}); });
  await store.show(); store.setGoal('The exact goal.');
  await assert.rejects(store.start(), /Activation unavailable/);
  setState(prepared('unrelated-key')); await store.refresh();
  assert.ok(store.pending); assert.equal(store.result, null);
  setState(prepared('setup-key', {receipt: {...prepared().receipt, model: 'different-model'}})); await store.refresh();
  assert.ok(store.pending); assert.equal(store.result, null);
  setState(prepared()); await store.refresh();
  assert.equal(store.pending, null); assert.equal(store.submitError, '');
  assert.deepEqual(store.result, started({replayed: true, armed: false, driving: false}));
  assert.equal(store.canDrive, true);
  assert.deepEqual(calls.filter(call => call.body).map(call => call.url), ['/v1/setup/start']);
});

test('a late POST failure cannot undo a successful receipt read', async () => {
  const response = deferred();
  const {store, setState} = harness(state(), models(), () => response.promise);
  await store.show(); store.setGoal('The exact goal.');
  const pending = store.start();
  setState(prepared()); await store.refresh();
  response.reject(Error('Late transport failure'));
  assert.equal((await pending).run_id, 'run-zero');
  assert.equal(store.submitError, ''); assert.equal(store.pending, null);
});

test('definite validation rejections permit correction and use a new key; timeout responses retain their key', async () => {
  let rejection = Object.assign(Error('The task revision changed'), {status: 409});
  const {store, calls} = harness(state(), models(), async () => { throw rejection; });
  await store.show(); store.setGoal('First goal.');
  await assert.rejects(store.start(), /revision changed/);
  assert.equal(store.pending, null); assert.equal(store.editable, true);
  store.setGoal('Corrected goal.');
  rejection = Object.assign(Error('Request timed out'), {status: 408});
  await assert.rejects(store.start(), /timed out/);
  const posts = calls.filter(call => call.body);
  assert.notEqual(posts[0].body.key, posts[1].body.key);
  assert.equal(store.pending.key, posts[1].body.key);
  assert.equal(store.goal, 'Corrected goal.');
});

test('status failures retain drafts but require a successful read before Start', async () => {
  const {store, setState} = harness();
  await store.show(); store.setGoal('Keep the draft.');
  setState(Error('Status unavailable')); await assert.rejects(store.refresh(), /unavailable/);
  assert.equal(store.goal, 'Keep the draft.'); assert.match(store.startBlockedReason, /Refresh project status/);
  setState(state()); await store.refresh();
  assert.equal(store.stateError, ''); assert.equal(store.goal, 'Keep the draft.'); assert.equal(store.startBlockedReason, '');
});

test('prepared run recovery is explicit and duplicate drive clicks share one request', async () => {
  const response = deferred();
  const {store, calls} = harness(prepared('saved-key', {run: {id: 'run / saved', state: 'running', armed: false, driving: false}}),
    models(), () => response.promise);
  await store.show(); await store.refresh();
  assert.ok(calls.every(call => call.body === undefined));
  const first = store.drive(), second = store.drive();
  assert.equal(first, second);
  assert.equal(calls.at(-1).url, '/v1/runs/run%20%2F%20saved/drive');
  assert.deepEqual(calls.at(-1).body, {});
  response.resolve({run_id: 'run / saved', armed: true, driving: true});
  await Promise.all([first, second]);
  assert.equal(store.canDrive, false);
  await assert.rejects(store.drive(), /not available/);
  assert.equal(calls.filter(call => call.body).length, 1);
});

test('an uncertain drive requires a new status read and never retries automatically', async () => {
  const {store, calls} = harness(prepared(), models(), async () => { throw Error('Connection lost'); });
  await store.show(); await assert.rejects(store.drive(), /Connection lost/);
  assert.equal(store.canDrive, false);
  await assert.rejects(store.drive(), /not available/);
  await store.refresh();
  assert.equal(store.canDrive, true);
  assert.equal(calls.filter(call => call.body).length, 1);
});

test('a stale status read cannot restore a prepared run after explicit drive succeeds', async () => {
  const stale = deferred();
  const {store, setState} = harness(prepared(), models(), async () => ({run_id: 'run-zero', armed: true, driving: true}));
  await store.show(); setState(stale.promise);
  const read = store.refresh(); await store.drive();
  stale.resolve(prepared()); await read;
  assert.equal(store.canDrive, false); assert.equal(store.result.driving, true); assert.equal(store.snapshot.run.armed, true);
});

test('an activated run seen by a later status read clears an uncertain drive error without another POST', async () => {
  const {store, calls, setState} = harness(prepared(), models(), async () => { throw Error('Response lost'); });
  await store.show(); await assert.rejects(store.drive(), /Response lost/);
  setState(prepared('saved-key', {can_drive: false, run: {id: 'run-zero', state: 'running', armed: true, driving: true}}));
  await store.refresh();
  assert.equal(store.driveError, ''); assert.equal(store.result.driving, true);
  assert.equal(calls.filter(call => call.body).length, 1);
});

test('a project switch clears the prior goal, choices, catalog, and ambiguous submission', async () => {
  const {store, calls, setState} = harness(state(), models(), async () => { throw Error('Response lost'); });
  await store.show(); store.setGoal('Private goal for project A.');
  await assert.rejects(store.start(), /Response lost/);
  setState(state({project_id: 'project-b', settings: {model: 'different-model', effort: 'thorough'}}));
  await store.refresh();
  assert.equal(store.snapshot.project_id, 'project-b'); assert.equal(store.goal, '');
  assert.equal(store.model, 'different-model'); assert.equal(store.effort, 'thorough');
  assert.equal(store.pending, null); assert.equal(store.result, null); assert.equal(store.catalog, null);
  assert.equal(store.submitError, ''); assert.equal(store.editable, true);
  assert.equal(calls.filter(call => call.url === '/v1/setup/models').length, 1);
  assert.match(store.startBlockedReason, /Load available models/);
});

test('a project switch also clears successful setup and adopts the new saved Task 0', async () => {
  const {store, setState} = harness(); await store.show(); store.setGoal('Project A.'); await store.start();
  setState(state({project_id: 'project-b', task: {id: 'b-task', title: 'Task 0', objective: 'Project B.', revision: 3}}));
  await store.refresh();
  assert.equal(store.goal, 'Project B.'); assert.equal(store.result, null); assert.equal(store.catalog, null);
});

test('late model catalogs and errors from a previous project cannot replace a new project catalog', async () => {
  for (const fail of [false, true]) {
    const oldCatalog = deferred(); let snapshot = state(), catalogCalls = 0;
    const newCatalog = models({models: [models().models[1]]});
    const store = new ProjectSetupStore(url => url === '/v1/setup' ? Promise.resolve(snapshot)
      : catalogCalls++ === 0 ? oldCatalog.promise : Promise.resolve(newCatalog));
    store.setOpen(true); await store.loadState(); const oldRead = store.loadModels();
    snapshot = state({project_id: 'project-b', settings: {model: 'different-model', effort: 'thorough'}});
    await store.refresh(); await store.loadModels();
    if (fail) oldCatalog.reject(Error('Old profile failed')); else oldCatalog.resolve(models());
    assert.equal(await oldRead, null);
    assert.equal(store.catalog, newCatalog); assert.equal(store.modelsError, '');
    assert.equal(store.model, 'different-model'); assert.equal(store.modelsLoad, null);
  }
});

test('late starts from a previous project do not replace the new project or release its active submission', async () => {
  for (const fail of [false, true]) {
    const oldStart = deferred(), newStart = deferred(); let submits = 0;
    const {store, calls, setState} = harness(state(), models(), () => submits++ === 0 ? oldStart.promise : newStart.promise);
    await store.show(); store.setGoal('Project A.'); const oldPending = store.start();
    setState(state({project_id: 'project-b'})); await store.refresh(); await store.loadModels();
    store.setGoal('Project B.'); const newPending = store.start(), active = store.submission;
    if (fail) oldStart.reject(Error('Old start failed')); else oldStart.resolve(started());
    assert.equal(await oldPending, null);
    assert.equal(store.submission, active); assert.equal(store.result, null); assert.equal(store.submitError, '');
    assert.equal(store.goal, 'Project B.');
    newStart.resolve(started({task_id: 'b-task', run_id: 'b-run'})); await newPending;
    assert.equal(store.result.run_id, 'b-run');
    assert.deepEqual(calls.filter(call => call.body).map(call => call.body.project_id), ['project-a', 'project-b']);
  }
});

test('late drive results and failures from a previous project cannot alter the current run', async () => {
  for (const fail of [false, true]) {
    const oldDrive = deferred();
    const {store, setState} = harness(prepared(), models(), () => oldDrive.promise);
    await store.show(); const pending = store.drive();
    setState(prepared('b-key', {project_id: 'project-b', run: {id: 'b-run', state: 'running', armed: false, driving: false}}));
    await store.refresh();
    if (fail) oldDrive.reject(Error('Old drive failed'));
    else oldDrive.resolve({run_id: 'run-zero', armed: true, driving: true});
    assert.equal(await pending, null);
    assert.equal(store.snapshot.run.id, 'b-run'); assert.equal(store.snapshot.run.armed, false);
    assert.equal(store.driveError, ''); assert.equal(store.result, null); assert.equal(store.canDrive, true);
  }
});

class Element {
  constructor(tag, text) {
    this.tag = tag; this.textContent = text || ''; this.children = []; this.dataset = {};
    this.value = ''; this.open = false; this.disabled = false; this.readOnly = false;
    this.classes = new Set(); this.classList = {toggle: (name, on) => on ? this.classes.add(name) : this.classes.delete(name)};
  }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  querySelectorAll() { return []; }
}
const source = fs.readFileSync(path.resolve(__dirname, '../../src/formal_geyser/coordination/dashboard.js'), 'utf8');
function setupRenderer(store) {
  const nodes = new Map();
  const el = id => { if (!nodes.has(id)) nodes.set(id, new Element('div')); return nodes.get(id); };
  el('setup-dialog').open = true;
  // No run yet: the Agents and sessions panel shows, its Worker · Codex row being the setup choice.
  const context = vm.createContext({setupStore: store, el, node: (tag, text) => new Element(tag, text), data: {runs: []}, AGENT_ROWS});
  const first = source.indexOf('  function setupOptions('), last = source.indexOf('  async function finishSetup(', first);
  assert.ok(first >= 0 && last > first);
  vm.runInContext(source.slice(first, last), context);
  return {el, render: () => context.renderSetup()};
}

test('ordinary status refresh renders fields in place and preserves a draft and dropdown nodes', async () => {
  const {store} = harness(); await store.show(); store.setGoal('An unfinished goal.'); store.selectEffort('quick');
  const {el, render} = setupRenderer(store); render();
  const model = el('agent-worker-codex-model'), effort = el('agent-worker-codex-effort');
  const goal = el('setup-goal'), modelOption = model.children[0], effortOption = effort.children[0];
  await store.refresh(); render();
  assert.equal(el('setup-goal'), goal); assert.equal(goal.value, 'An unfinished goal.'); assert.equal(goal.readOnly, false);
  assert.equal(model.children[0], modelOption); assert.equal(effort.children[0], effortOption);
  assert.equal(model.value, 'catalog-model'); assert.equal(model.disabled, false);
  assert.equal(effort.value, 'quick'); assert.equal(el('setup-start').disabled, false);
  assert.equal(el('agent-settings').classes.has('hidden'), false); assert.equal(el('agent-settings-setup').classes.has('hidden'), false);
  // No worker exists yet: nothing offers to change existing workers.
  for (const [role, backend] of AGENT_ROWS) assert.equal(el('agent-' + role + '-' + backend + '-existing-row').classes.has('hidden'), true);
  assert.equal(el('setup-model-state').textContent, 'Main model from the profile. · A quick check.');
});

test('the setup view shows unavailable settings, profile sign-in guidance, and read-only recovery controls', async () => {
  const missing = harness(state({settings: {model: 'not-in-catalog', effort: 'removed'}}));
  await missing.store.show(); missing.store.setGoal('A valid goal.');
  const unavailable = setupRenderer(missing.store); unavailable.render();
  assert.match(unavailable.el('agent-worker-codex-model').children[0].textContent, /not-in-catalog.*unavailable/);
  assert.match(unavailable.el('setup-model-state').textContent, /Choose a model/);
  assert.equal(unavailable.el('setup-start').disabled, true);
  const auth = harness(state(), models({authentication_required: true})); await auth.store.show();
  const signIn = setupRenderer(auth.store); signIn.render();
  assert.match(signIn.el('setup-model-state').textContent, /Sign in.*worker profile/);
  assert.equal(signIn.el('setup-start').disabled, true); assert.equal(signIn.el('setup-model-retry').classes.has('hidden'), false);
  const locked = harness(prepared()); await locked.store.show();
  const recovery = setupRenderer(locked.store); recovery.render();
  // Once a run exists the panel's rows are the ordinary role choices, not this setup choice.
  assert.equal(recovery.el('setup-goal').readOnly, true); assert.equal(recovery.el('agent-worker-codex-model').children.length, 0);
  assert.equal(recovery.el('setup-start').classes.has('hidden'), true);
  assert.equal(recovery.el('setup-drive').classes.has('hidden'), false); assert.equal(recovery.el('setup-drive').disabled, false);
});

test('setup markup has labelled native fields and keeps all existing task views and history controls', () => {
  const html = fs.readFileSync(path.resolve(__dirname, '../../src/formal_geyser/coordination/dashboard.html'), 'utf8');
  assert.ok(html.includes('for="setup-goal"'));
  // The model and effort are chosen only in Agents and sessions, before the first run as after it.
  assert.ok(!html.includes('id="setup-model"') && !html.includes('id="setup-effort"'));
  for (const id of ['agent-worker-codex-model', 'agent-worker-codex-effort', 'agent-settings-setup']) assert.ok(html.includes('id="' + id + '"'));
  assert.match(html, /id="setup-goal"[^>]*maxlength="20000"/);
  for (const id of ['tasks-all', 'tasks-running', 'tasks-pool', 'tasks-history', 'session-select', 'transcript', 'compose', 'notice']) {
    assert.ok(html.includes('id="' + id + '"'));
  }
  assert.match(html, /<html lang="en">/);
});
