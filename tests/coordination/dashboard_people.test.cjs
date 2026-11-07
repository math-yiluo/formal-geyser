'use strict';

const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const {test} = require('node:test');
const vm = require('node:vm');
const {arrangeMembers, agentDraft, agentSettingCommands, profileRows, profileBadge, profileHistoryRows, durationLabel,
  pollsToSkip, projectStatus} = require('../../src/formal_geyser/coordination/dashboard.js');

const directory = path.resolve(__dirname, '../../src/formal_geyser/coordination');
const source = fs.readFileSync(path.join(directory, 'dashboard.js'), 'utf8');
const markup = fs.readFileSync(path.join(directory, 'dashboard.html'), 'utf8');

// Enough DOM to run the dashboard script and its handlers; requests are synthetic.
class Element {
  constructor(tag = 'div') {
    this.tag = tag; this.children = []; this.parent = null; this.dataset = {}; this.style = {};
    this.attributes = {}; this.events = {}; this.className = ''; this.value = '';
    this.open = false; this.disabled = false; this.scrollTop = 0; this.scrollLeft = 0;
    this.scrollHeight = 500; this.clientHeight = 500;
    this.classList = {
      contains: name => this.className.split(' ').includes(name),
      toggle: (name, force) => {
        const enabled = force ?? !this.classList.contains(name);
        const classes = new Set(this.className.split(' ').filter(Boolean));
        if (enabled) classes.add(name); else classes.delete(name);
        this.className = [...classes].join(' '); return enabled;
      },
      add: name => this.classList.toggle(name, true),
      remove: name => this.classList.toggle(name, false),
    };
  }
  get textContent() { return this.children.map(child => child instanceof Element ? child.textContent : String(child)).join(''); }
  set textContent(value) { this.replaceChildren(String(value)); }
  append(...children) {
    for (const child of children) if (child instanceof Element) child.parent = this;
    this.children.push(...children);
  }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  addEventListener(name, callback) { (this.events[name] ||= []).push(callback); }
  dispatchEvent(event) { for (const callback of this.events[event.type] || []) callback(event); }
  getBoundingClientRect() { return {top: 0}; }
  close() { this.open = false; this.dispatchEvent({type: 'close'}); }
  showModal() { this.open = true; }
  matches(selector) { return this.tag === selector; }
  querySelectorAll(selector) {
    return this.children.flatMap(child => child instanceof Element
      ? [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)] : []);
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest() { return null; }
}

const settle = () => new Promise(resolve => setImmediate(resolve));
const person = (number, extra = {}) => ({id: 'w' + number, kind: 'ordinary', worker_number: number,
  display_name: 'Worker ' + number, state: 'idle', work_intention: 'idle', backend: 'codex', ...extra});
const backendStatus = () => ({new_worker_backend: 'codex', new_worker_source: 'plan',
  next_worker: {number: 9, backend: 'codex'},
  role_models: {worker: {codex: {model: 'gpt-6-astra', effort: 'xhigh', source: 'config'},
    claude: {model: 'claude-opus-5-5', effort: 'xhigh', source: 'config'}},
  review: {codex: {model: 'gpt-6-astra', effort: 'max', source: 'config'}},
  integrator: {codex: {model: 'gpt-6-astra', effort: 'max', source: 'config'}}},
  decision_model: {model: 'gpt-6.1-sol', effort: 'high', source: 'config'},
  waker_model: {model: 'gpt-6.1-sol', effort: 'low', source: 'config'},
  answer_deadline: {minutes: 60, source: 'config'}, missing_choices: [],
  sessions: {codex: {live: 3, limit: 60, source: 'config'}, claude: {live: 0, limit: 12, source: 'config'}},
  rotation: {mode: 'aggressive', idle_by_backend: {codex: {minutes: 40, source: 'config'}, claude: {minutes: 40, source: 'config'}}}});
const catalog = () => ({models: [
  {id: 'gpt-6-astra', name: 'GPT-6 Astra', efforts: [{id: 'high'}, {id: 'xhigh'}, {id: 'max'}], default_effort: 'xhigh'},
  {id: 'gpt-6.1-sol', name: 'GPT-6.1 Sol', efforts: [{id: 'low'}, {id: 'high'}], default_effort: 'high'}],
authentication_required: false});

function harness(workers, {setup = false} = {}) {
  const elements = new Map();
  for (const [, tag, attributes, id] of markup.matchAll(/<([a-z]+)([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const element = new Element(tag);
    element.id = id; element.className = attributes.match(/class="([^"]*)"/)?.[1] || '';
    element.open = /\bopen(?:\s|$)/.test(attributes); elements.set(id, element);
  }
  const el = id => { assert.ok(elements.has(id), 'Missing markup element: ' + id); return elements.get(id); };
  const state = {overview: {name: 'Synthetic project', task_count: 0, tasks: [], workers, requests: [],
    runs: setup ? [] : [{id: 'run', state: 'running'}], drivers: {}, delivery_holds: []}, backend: backendStatus(), starts: []};
  const commands = []; let refresh;
  const context = vm.createContext({document: {getElementById: el,
    createElement: tag => new Element(tag), querySelectorAll: () => [], body: new Element('body'),
    addEventListener() {}},
    sessionStorage: {getItem: () => '', setItem() {}, removeItem() {}},
    crypto: {randomUUID}, TextEncoder, Event: class { constructor(type) { this.type = type; } },
    setInterval(callback) { refresh = callback; }, setTimeout, clearTimeout, location: {reload() {}}, confirm: () => true,
    async fetch(url, options) {
      let result;
      if (options.method === 'POST' && url === '/v1/setup/start') {
        state.starts.push(JSON.parse(options.body));
        result = {task_id: 't0', run_id: 'run', replayed: false, armed: true, driving: true};
      } else if (options.method === 'POST') {
        assert.equal(url, '/v1/commands');
        const body = JSON.parse(options.body);
        commands.push([body.name, body.payload]);
        result = {};
      } else if (url === '/v1/overview?compact=true') result = state.overview;
      else if (url === '/v1/runtime/backend') result = state.backend;
      else if (url === '/v1/runtime/claude-profiles') result = state.claude;
      else if (url === '/v1/runtime/codex-profiles') result = state.codex;
      else if (url === '/v1/retired-workers') { state.retiredReads = (state.retiredReads || 0) + 1; result = state.retired; }
      else if (state.details?.[url]) result = state.details[url];
      else if (url === '/v1/setup/models') result = catalog();
      else if (url === '/v1/setup' && setup) result = {project_id: 'p', can_configure: true, reason: null, task: null,
        settings: {model: 'gpt-6-astra', effort: 'xhigh'}, repository: {enabled: true, initialized: false}, run: null,
        can_drive: false, receipt: null};
      else if (url === '/v1/setup') result = {project_id: 'p', can_configure: false, reason: 'This project already has a run.',
        task: {id: 't0', objective: 'Goal', revision: 1}, settings: {model: 'gpt-6-astra', effort: 'xhigh'},
        repository: {enabled: true, initialized: true}, run: {id: 'run', state: 'running'}, can_drive: false, receipt: null};
      else if (/^\/v1\/workers\//.test(url)) result = url.includes('/messages')
        ? {messages: [], delivery_updates: [], next_after: 0, has_more: false} : {sessions: []};
      else if (url.startsWith('/v1/')) result = {};
      return {ok: true, json: async () => result, headers: {get: () => null}};
    },
  });
  vm.runInContext(source, context);
  return {el, state, commands, load: settle, refresh: () => refresh(),
    names: () => el('worker-list').querySelectorAll('strong').map(item => item.textContent)};
}

test('members sort either way within a group and filter by state, backend and text', () => {
  const members = [person(3), person(1, {backend: 'claude', active_attempt: 'a', state: 'running'}), person(10), person(2, {backend: 'claude'})];
  const names = list => list.map(item => item.display_name);
  assert.deepEqual(names(arrangeMembers(members)), ['Worker 1', 'Worker 2', 'Worker 3', 'Worker 10']);
  assert.deepEqual(names(arrangeMembers(members, {descending: true})), ['Worker 10', 'Worker 3', 'Worker 2', 'Worker 1']);
  assert.deepEqual(names(arrangeMembers(members, {backend: 'claude'})), ['Worker 1', 'Worker 2']);
  assert.deepEqual(names(arrangeMembers(members, {state: 'Working'})), ['Worker 1']);
  assert.deepEqual(names(arrangeMembers(members, {state: 'Idle', backend: 'codex', descending: true})), ['Worker 10', 'Worker 3']);
  assert.deepEqual(names(arrangeMembers(members, {query: 'worker 1'})), ['Worker 1', 'Worker 10']);
  // A worker recorded before backends were reported is on Codex.
  assert.equal(arrangeMembers([{...person(4), backend: undefined}], {backend: 'codex'}).length, 1);
});

test('saving sends only what changed and refuses an invalid session limit', () => {
  const status = backendStatus(), draft = agentDraft(status);
  assert.deepEqual(agentSettingCommands(status, draft), []);
  draft.nextBackend = 'claude';
  draft.models.worker_claude = {model: ' claude-sonnet-5-5 ', effort: 'high'};
  draft.models.review_codex.effort = 'xhigh';
  draft.models.integrator_codex.model = 'gpt-6.1-sol';
  draft.dispatcher = {model: 'gpt-6-astra', effort: 'high'};
  draft.waker = {model: 'gpt-6-astra', effort: 'high'};
  draft.deadline = ' 45 ';
  draft.sessions = {codex: '40', claude: '12'};
  draft.idle = {codex: '0', claude: '40'};
  assert.deepEqual(agentSettingCommands(status, draft), [
    ['runtime.select_backend', {backend: 'claude'}],
    ['runtime.set_role_model', {role: 'worker', backend: 'claude', model: 'claude-sonnet-5-5', effort: 'high', apply_to_existing: false}],
    ['runtime.set_role_model', {role: 'review', backend: 'codex', model: 'gpt-6-astra', effort: 'xhigh', apply_to_existing: false}],
    ['runtime.set_role_model', {role: 'integrator', backend: 'codex', model: 'gpt-6.1-sol', effort: 'max', apply_to_existing: false}],
    ['runtime.set_decision_model', {model: 'gpt-6-astra', effort: 'high'}],
    ['runtime.set_waker_model', {model: 'gpt-6-astra', effort: 'high'}],
    ['runtime.set_answer_deadline', {minutes: 45}],
    ['runtime.set_concurrency', {backend: 'codex', sessions: 40}],
    ['runtime.set_rotation_idle', {backend: 'codex', minutes: 0}]]);
  // The Waker needs both choices, and the deadline whole minutes.
  assert.throws(() => agentSettingCommands(status, {...agentDraft(status), waker: {model: 'gpt-6-astra', effort: ''}}), /Waker/);
  for (const deadline of ['', '0', '1.5', '43201']) {
    assert.throws(() => agentSettingCommands(status, {...agentDraft(status), deadline}), /answer deadline/);
  }
  // An emptied Claude model leaves the choice to project.toml; no limit is -1, and 0 starts no worker.
  const cleared = agentDraft(status);
  cleared.models.worker_claude.model = '';
  cleared.sessions = {codex: '0', claude: '-1'};
  assert.deepEqual(agentSettingCommands(status, cleared), [
    ['runtime.set_role_model', {role: 'worker', backend: 'claude', model: null, effort: 'xhigh', apply_to_existing: false}],
    ['runtime.set_concurrency', {backend: 'codex', sessions: 0}],
    ['runtime.set_concurrency', {backend: 'claude', sessions: -1}]]);
  for (const value of ['-2', '-0', '2.5', 'ten', '2000']) {
    assert.throws(() => agentSettingCommands(status, {...agentDraft(status), sessions: {codex: value, claude: '12'}}));
  }
  for (const value of ['-1', '2.5', 'ten', '20000']) {
    assert.throws(() => agentSettingCommands(status, {...agentDraft(status), idle: {codex: '40', claude: value}}));
  }
  // Ticked alone, it is sent only while existing workers keep earlier choices.
  const ticked = agentDraft(status);
  ticked.models.worker_codex.existing = true;
  assert.deepEqual(agentSettingCommands(status, ticked), []);
  const keeping = {...status, role_models: {...status.role_models, worker: {...status.role_models.worker,
    codex: {...status.role_models.worker.codex, kept_by_existing: 4}}}};
  assert.deepEqual(agentSettingCommands(keeping, ticked), [
    ['runtime.set_role_model', {role: 'worker', backend: 'codex', model: 'gpt-6-astra', effort: 'xhigh', apply_to_existing: true}]]);
  // The project having no limit reads as -1; a limit of 0 stays 0.
  assert.equal(agentDraft({...status, sessions: {codex: {limit: null}, claude: {limit: 12}}}).sessions.codex, '-1');
  assert.equal(agentDraft({...status, sessions: {codex: {limit: 0}, claude: {limit: 12}}}).sessions.codex, '0');
});

test("the Integrator's batch rule is saved, emptied to its default, and checked", () => {
  const status = {...backendStatus(), integrator_batch: {delay_minutes: 13, threshold: 5,
    source: {delay_minutes: 'config', threshold: 'config'}}};
  const draft = agentDraft(status);
  assert.deepEqual(draft.batch, {delay: '13', threshold: '5'});
  assert.deepEqual(agentSettingCommands(status, draft), []);
  draft.batch = {delay: '30', threshold: '8'};
  assert.deepEqual(agentSettingCommands(status, draft), [
    ['runtime.set_integrator_batch', {delay_minutes: 30, threshold: 8}]]);
  draft.batch = {delay: '', threshold: '5'};
  assert.deepEqual(agentSettingCommands(status, draft), [
    ['runtime.set_integrator_batch', {delay_minutes: null, threshold: 5}]]);
  for (const batch of [{delay: '-1', threshold: '5'}, {delay: '20000', threshold: '5'},
    {delay: '13', threshold: '0'}, {delay: '13', threshold: '2.5'}]) {
    assert.throws(() => agentSettingCommands(status, {...agentDraft(status), batch}));
  }
});

test('the sidebar applies its order and filters to every refresh', async () => {
  const app = harness([person(3), person(1, {backend: 'claude'}), person(2, {active_attempt: 'a', state: 'running',
    work_intention: 'working', active_since: Date.now()})]);
  await app.load();
  assert.deepEqual(app.names(), ['Worker 1', 'Worker 2', 'Worker 3']);
  // Only states someone is in are offered.
  assert.deepEqual(app.el('worker-state-filter').children.map(option => option.value), ['', 'Working', 'Idle']);
  app.el('worker-sort').onclick();
  assert.deepEqual(app.names(), ['Worker 3', 'Worker 2', 'Worker 1']);
  assert.equal(app.el('worker-sort').attributes['aria-pressed'], 'true');
  app.el('worker-state-filter').value = 'Idle'; app.el('worker-state-filter').onchange();
  assert.deepEqual(app.names(), ['Worker 3', 'Worker 1']);
  assert.equal(app.el('worker-count').textContent, '(2/3)');
  app.el('worker-backend-filter').value = 'claude'; app.el('worker-backend-filter').onchange();
  assert.deepEqual(app.names(), ['Worker 1']);
  assert.equal(app.el('worker-count').textContent, '(1/3)');
  // A group the filters empty says so and offers to show everyone.
  app.el('worker-state-filter').value = 'Working'; app.el('worker-state-filter').onchange();
  const empty = app.el('worker-list').children[0];
  assert.match(empty.textContent, /^Hidden by the state or backend filter\. Show everyone$/);
  empty.children.at(-1).onclick();
  assert.deepEqual(app.names(), ['Worker 3', 'Worker 2', 'Worker 1']);
  assert.equal(app.el('worker-count').textContent, '(3)');
  assert.deepEqual(app.commands, []);
});

test('a worker switches backend from its next new conversation', async () => {
  const app = harness([person(1, {thread_id: 't', session_settings: {model: 'gpt-6-astra', effort: 'xhigh'}}),
    {...person(0), id: 'r1', kind: 'review', reviewer_number: 1, display_name: 'Reviewer 1'}]);
  await app.load();
  app.el('worker-list').children[0].onclick(); await settle();
  assert.equal(app.el('worker-backend-text').textContent, 'Backend: Codex · gpt-6-astra · xhigh effort');
  assert.equal(app.el('worker-backend-button').textContent, 'Switch to Claude');
  await app.el('worker-backend-button').onclick();
  assert.deepEqual(app.commands, [['worker.select_backend', {worker_id: 'w1', backend: 'claude'}]]);
  app.state.overview = {...app.state.overview, workers: [{...app.state.overview.workers[0], requested_backend: 'claude'},
    app.state.overview.workers[1]]};
  await app.refresh(); await settle();
  assert.match(app.el('worker-backend-text').textContent, /moves to Claude at its next new conversation$/);
  assert.equal(app.el('worker-backend-button').textContent, 'Stay on Codex');
  // Reviewers stay on Codex: no switch is offered.
  app.el('reviewer-list').children[0].onclick(); await settle();
  assert.equal(app.el('worker-backend-button').classList.contains('hidden'), true);
});

test('project settings save choices for new conversations and session limits', async () => {
  const app = harness([person(1)]);
  await app.load();
  app.el('show-setup').onclick(); await settle(); await settle();
  assert.equal(app.el('agent-settings').classList.contains('hidden'), false);
  assert.equal(app.el('agent-worker-codex-model').value, 'gpt-6-astra');
  assert.deepEqual(app.el('agent-worker-codex-effort').children.map(option => option.value), ['high', 'xhigh', 'max']);
  assert.equal(app.el('agent-sessions-codex').value, '60');
  assert.equal(app.el('agent-sessions-codex-live').textContent, '3 running · project limit');
  assert.equal(app.el('agent-next-source').textContent, 'from the assignment plan');
  assert.equal(app.el('agent-idle-codex').value, '40');
  assert.equal(app.el('agent-idle-codex-source').textContent, 'project default');
  // A model without the chosen effort takes its default.
  app.el('agent-worker-codex-model').value = 'gpt-6.1-sol'; app.el('agent-worker-codex-model').onchange();
  assert.equal(app.el('agent-worker-codex-effort').value, 'high');
  app.el('agent-sessions-claude').value = '20'; app.el('agent-sessions-claude').oninput();
  app.el('agent-next-backend').value = 'claude'; app.el('agent-next-backend').onchange();
  app.el('agent-idle-codex').value = '720'; app.el('agent-idle-codex').oninput();
  // Unticked by default; ticked, the new choice reaches existing workers too.
  assert.equal(app.el('agent-worker-codex-existing').checked, false);
  app.el('agent-worker-codex-existing').checked = true; app.el('agent-worker-codex-existing').onchange();
  await app.el('agent-settings-save').onclick(); await settle(); await settle();
  assert.deepEqual(app.commands, [
    ['runtime.select_backend', {backend: 'claude'}],
    ['runtime.set_role_model', {role: 'worker', backend: 'codex', model: 'gpt-6.1-sol', effort: 'high', apply_to_existing: true}],
    ['runtime.set_concurrency', {backend: 'claude', sessions: 20}],
    ['runtime.set_rotation_idle', {backend: 'codex', minutes: 720}]]);
  assert.match(app.el('agent-settings-state').textContent, /^Saved\. .*only where you ticked/);
  assert.equal(app.el('agent-worker-codex-existing').checked, false);  // Unticked again after saving.
  app.el('agent-sessions-codex').value = '-3'; app.el('agent-sessions-codex').oninput();
  await app.el('agent-settings-save').onclick(); await settle();
  assert.match(app.el('agent-settings-error').textContent, /whole number/);
  assert.equal(app.commands.length, 4);
});

test('before the first run, setup chooses every role in Agents and sessions and Start saves them', async () => {
  const app = harness([], {setup: true});
  app.state.backend.missing_choices = ['The Waker: model', 'The answer deadline'];
  await app.load();
  app.el('show-setup').onclick(); for (let i = 0; i < 6; i += 1) await settle();
  assert.equal(app.el('agent-settings').classList.contains('hidden'), false);
  assert.equal(app.el('agent-settings-setup').classList.contains('hidden'), false);
  assert.equal(app.el('agent-worker-codex-existing-row').classList.contains('hidden'), true);
  assert.equal(app.el('agent-settings-missing').textContent,
    'Still to choose before a run starts: The Waker: model; The answer deadline.');
  // Worker · Codex is the project's own model and effort; every other role has its own choice.
  assert.equal(app.el('agent-worker-codex-model').value, 'gpt-6-astra');
  assert.equal(app.el('agent-worker-codex-effort').value, 'xhigh');
  assert.equal(app.el('agent-review-codex-model').value, 'gpt-6-astra');
  assert.equal(app.el('agent-review-codex-effort').value, 'max');
  app.el('agent-worker-codex-model').value = 'gpt-6.1-sol'; app.el('agent-worker-codex-model').onchange();
  assert.equal(app.el('agent-worker-codex-effort').value, 'high');  // The model's default effort.
  assert.equal(app.el('agent-review-codex-model').value, 'gpt-6-astra');  // Reviewers do not follow it.
  app.el('agent-review-codex-effort').value = 'high'; app.el('agent-review-codex-effort').onchange();
  app.el('agent-integrator-codex-model').value = 'gpt-6.1-sol'; app.el('agent-integrator-codex-model').onchange();
  assert.equal(app.el('agent-integrator-codex-effort').value, 'high');
  app.el('agent-waker-effort').value = 'high'; app.el('agent-waker-effort').onchange();
  app.el('agent-answer-deadline').value = '45'; app.el('agent-answer-deadline').oninput();
  app.el('agent-sessions-claude').value = '7'; app.el('agent-sessions-claude').oninput();
  assert.deepEqual(app.commands, []);  // Nothing is sent before Start.
  app.el('setup-goal').value = 'Prove it.'; app.el('setup-goal').oninput();
  app.el('setup-form').onsubmit({preventDefault() {}}); for (let i = 0; i < 8; i += 1) await settle();
  assert.deepEqual(app.commands, [
    ['runtime.set_role_model', {role: 'review', backend: 'codex', model: 'gpt-6-astra', effort: 'high', apply_to_existing: false}],
    ['runtime.set_role_model', {role: 'integrator', backend: 'codex', model: 'gpt-6.1-sol', effort: 'high', apply_to_existing: false}],
    ['runtime.set_waker_model', {model: 'gpt-6.1-sol', effort: 'high'}],
    ['runtime.set_answer_deadline', {minutes: 45}],
    ['runtime.set_concurrency', {backend: 'claude', sessions: 7}]]);
  assert.equal(app.state.starts.length, 1);
  assert.deepEqual([app.state.starts[0].goal, app.state.starts[0].model, app.state.starts[0].effort], ['Prove it.', 'gpt-6.1-sol', 'high']);
});

test('a setup save that fails starts nothing', async () => {
  const app = harness([], {setup: true});
  await app.load();
  app.el('show-setup').onclick(); for (let i = 0; i < 6; i += 1) await settle();
  app.el('agent-sessions-codex').value = 'many'; app.el('agent-sessions-codex').oninput();
  app.el('setup-goal').value = 'Prove it.'; app.el('setup-goal').oninput();
  app.el('setup-form').onsubmit({preventDefault() {}}); for (let i = 0; i < 6; i += 1) await settle();
  assert.match(app.el('agent-settings-error').textContent, /whole number/);
  assert.deepEqual(app.commands, []); assert.equal(app.state.starts.length, 0);
});

const HOUR = 3600000, NOW = Date.UTC(2026, 9, 25, 18, 0);
const claudeView = (overrides = {}) => ({selected: 'second', held_until: null, history: [], profiles: [
  {name: 'main', home: '/h', selected: false, signed_in: true, problem: null, limited_until: NOW + 2 * HOUR + 15 * 60000,
    limit_type: '5h', last_limited_at: NOW - HOUR, usage: [
      {window: 'five_hour', status: 'rejected', utilization: 1, resets_at: NOW + 2 * HOUR + 15 * 60000, observed_at: NOW - HOUR}]},
  {name: 'second', home: '/h/profiles/second', selected: true, signed_in: true, problem: null, limited_until: null,
    limit_type: null, last_limited_at: null, usage: [
      {window: 'seven_day', status: 'allowed_warning', utilization: 0.82, resets_at: NOW + 3 * 24 * HOUR, observed_at: NOW}]},
  {name: 'third', home: '/h/profiles/third', selected: false, signed_in: false, problem: null, limited_until: null,
    limit_type: null, last_limited_at: null, usage: []},
  {name: 'fourth', home: '/h/profiles/fourth', selected: false, signed_in: true, problem: 'keeps conversations of its own',
    limited_until: null, limit_type: null, last_limited_at: null, usage: []}], ...overrides});

test('the Claude tab says which profile is in use, until when the others are limited, and their usage', () => {
  const rows = profileRows(claudeView(), NOW);
  assert.match(rows[0].status, /^At its 5-hour limit until .* \(in 2 h 15 min\)$/);
  assert.match(rows[0].usage[0], /^5-hour: at its limit, 100% used · resets .* \(in 2 h 15 min\) · reported /);
  assert.match(rows[0].last_limited, /^Last at a limit /);
  assert.match(rows[1].usage[0], /^Weekly: approaching its limit, 82% used · resets .* \(in 3 d\) · reported /);
  assert.deepEqual(rows.slice(1).map(row => [row.status, row.usable, row.attention]), [
    ['In use', false, false], ['Not signed in', false, true], ['Cannot be used: keeps conversations of its own', false, true]]);
  assert.equal(rows[0].usable, true);  // Selecting a limited profile clears its limit.
  assert.deepEqual(profileRows(null), []);
  assert.deepEqual([durationLabel(-5), durationLabel(45 * 60000), durationLabel(HOUR), durationLabel(26 * HOUR + 1)],
    ['now', '45 min', '1 h', '1 d 2 h']);
  assert.deepEqual(profileBadge({selected: 'second', held_until: null}), {text: 'Claude: second', held: false});
  assert.match(profileBadge({selected: 'third', held_until: NOW}).text, /^Claude: every profile at its limit until /);
  assert.equal(profileBadge(null), null);
});

test('the Claude tab lists recent limits and switches, newest first', () => {
  const history = profileHistoryRows({history: [
    {at: NOW, kind: 'all_limited', profile: null, until: NOW + HOUR},
    {at: NOW - 60000, kind: 'selected', profile: 'third', previous: 'second', reason: 'limit'},
    {at: NOW - 120000, kind: 'limited', profile: 'second', limit_type: 'weekly', until: NOW + 72 * HOUR},
    {at: NOW - 180000, kind: 'selected', profile: 'main', previous: 'main', reason: 'operator'}]});
  assert.deepEqual(history.map(row => row.text.replace(/ resets .*| until .*/, '')), [
    'Every profile at its limit; Claude waits', 'Switched to third from second: the previous one reached its limit',
    'second reached its weekly limit;', 'Switched to main: selected by you']);
  assert.deepEqual(profileHistoryRows(null), []);
});

test('the header opens the Claude tab, which selects another profile', async () => {
  const app = harness([person(1)]);
  await app.load();
  assert.equal(app.el('claude-profile').classList.contains('hidden'), true);  // No Claude profiles reported.
  assert.equal(app.el('show-claude').disabled, true);
  app.state.overview = {...app.state.overview, claude_profile: {selected: 'second', held_until: null, count: 4}};
  app.state.claude = claudeView({history: [{at: NOW, kind: 'selected', profile: 'second', previous: 'main', reason: 'limit'}]});
  await app.refresh(); await settle();
  assert.equal(app.el('claude-profile').textContent, 'Claude: second');
  assert.equal(app.el('show-claude').disabled, false);
  app.el('claude-profile').onclick(); await settle(); await settle();
  assert.equal(app.el('claude-dialog').open, true);
  const list = app.el('claude-profile-list').children;
  assert.deepEqual(list.filter(item => item.tag === 'label' && item.className === 'profile-name').map(item => item.textContent),
    ['main', 'second', 'third', 'fourth']);
  const buttons = list.filter(item => item.tag === 'button');
  assert.deepEqual(buttons.map(button => button.disabled), [false, true, true, true]);
  assert.match(app.el('claude-history').textContent, /Switched to second from main/);
  await buttons[0].onclick(); await settle(); await settle();
  assert.deepEqual(app.commands, [['runtime.select_claude_profile', {profile: 'main'}]]);
  assert.match(app.el('claude-state').textContent, /use main from their next activation/);
  // Every profile at its limit: the tab says until when Claude waits.
  app.state.claude = claudeView({held_until: Date.now() + HOUR});
  app.el('show-claude').onclick(); await settle(); await settle();
  assert.equal(app.el('claude-held').classList.contains('hidden'), false);
  assert.match(app.el('claude-held').textContent, /start again at .*, in (1 h|60 min), without Resume/);
});

test('a profile out of the rotation is said so, and its box takes it out or puts it back', async () => {
  const view = claudeView();
  view.profiles[0] = {...view.profiles[0], in_rotation: false, limited_until: null, usage: []};
  const rows = profileRows(view, NOW);
  assert.deepEqual([rows[0].rotation, rows[0].status, rows[0].usable, rows[1].rotation], [false, 'Not in the rotation · Available', true, true]);
  // Selecting it again is offered even while it is in use, to put it back in.
  view.profiles[0].selected = true;
  assert.equal(profileRows(view, NOW)[0].usable, true);
  assert.deepEqual(profileHistoryRows({history: [{at: NOW, kind: 'selected', profile: 'second', previous: 'main', reason: 'excluded'},
    {at: NOW, kind: 'excluded', profile: 'main'}, {at: NOW, kind: 'included', profile: 'third'}]}).map(row => row.text), [
    'Switched to second from main: you took the previous one out of the rotation', 'main taken out of the rotation',
    'third put in the rotation']);
  assert.deepEqual(profileBadge({selected: 'main', held_until: null, held: 'claudeProfilesUnavailable'}),
    {text: 'Claude: no usable profile in the rotation', held: true});

  const app = harness([person(1)]);
  await app.load();
  app.state.overview = {...app.state.overview, claude_profile: {selected: 'second', held_until: null, count: 4}};
  app.state.claude = claudeView();
  await app.refresh(); await settle();
  app.el('show-claude').onclick(); await settle(); await settle();
  const boxes = app.el('claude-profile-list').children.filter(item => item.tag === 'label')
    .map(label => label.children.find(child => child.tag === 'input'));
  assert.deepEqual(boxes.map(box => box.checked), [true, true, true, true]);
  boxes[1].checked = false; await boxes[1].onchange(); await settle(); await settle();
  assert.deepEqual(app.commands, [['runtime.set_claude_profile_rotation', {profile: 'second', in_rotation: false}]]);
  assert.match(app.el('claude-state').textContent, /second is out of the rotation/);
  // No profile in the rotation can be used: the tab says why Claude waits.
  app.state.claude = claudeView({hold: {code: 'claudeProfilesUnavailable', message: 'No Claude profile is in the rotation: tick one in the Claude tab.', until: null}});
  app.el('show-claude').onclick(); await settle(); await settle();
  assert.equal(app.el('claude-held').textContent, 'No Claude profile is in the rotation: tick one in the Claude tab.');
});

test('the Codex tab works as the Claude tab, with Codex commands and its own usage windows', async () => {
  const codexView = () => ({...claudeView(), profiles: claudeView().profiles.map((profile, index) =>
    ({...profile, usage: [], priority: index + 1}))});
  const view = codexView();
  view.profiles[1].usage = [{window: '5h', status: 'allowed', utilization: 0.4, resets_at: NOW + HOUR, observed_at: NOW},
    {window: 'weekly', status: 'rejected', utilization: 1, resets_at: NOW + 50 * HOUR, observed_at: NOW}];
  assert.deepEqual(profileRows(view, NOW)[1].usage.map(line => line.replace(/ · resets .*/, '')),
    ['5-hour: 40% used', 'Weekly: at its limit, 100% used']);
  assert.deepEqual(profileBadge({selected: 'main', held_until: null}, 'Codex'), {text: 'Codex: main', held: false});
  assert.deepEqual(profileBadge({selected: 'main', held_until: null, held: 'codexProfilesUnavailable'}, 'Codex'),
    {text: 'Codex: no usable profile in the rotation', held: true});
  assert.match(profileHistoryRows({history: [{at: NOW, kind: 'all_limited', profile: null, until: NOW + HOUR}]}, 'Codex')[0].text,
    /^Every profile at its limit; Codex waits until /);
  // A hold while every Codex profile is at its limit ends by itself: Resume is not offered for it.
  const held = {name: 'p', task_count: 0, runs: [{id: 'run', state: 'running', dispatch_holds: {codex: {
    backend: 'codex', code: 'codexProfilesLimited', message: 'Every Codex profile in the rotation is at its usage limit.'}}}]};
  assert.equal(projectStatus(held).actions.includes('resume'), false);

  const app = harness([person(1)]);
  await app.load();
  assert.equal(app.el('show-codex').disabled, true);  // No Codex profiles configured.
  app.state.overview = {...app.state.overview, codex_profile: {selected: 'second', held_until: null, count: 4}};
  app.state.codex = codexView();
  await app.refresh(); await settle();
  assert.equal(app.el('codex-profile').textContent, 'Codex: second');
  assert.equal(app.el('claude-profile').classList.contains('hidden'), true);  // Claude's badge is apart.
  app.el('show-codex').onclick(); await settle(); await settle();
  assert.equal(app.el('codex-dialog').open, true);
  const list = app.el('codex-profile-list').children;
  await list.filter(item => item.tag === 'button')[0].onclick(); await settle(); await settle();
  const box = list.filter(item => item.tag === 'label')[2].children.find(child => child.tag === 'input');
  box.checked = false; await box.onchange(); await settle(); await settle();
  assert.deepEqual(app.commands, [['runtime.select_codex_profile', {profile: 'main'}],
    ['runtime.set_codex_profile_rotation', {profile: 'third', in_rotation: false}]]);
  assert.match(app.el('codex-state').textContent, /third is out of the rotation: Codex never switches to it/);
  // The priority: numbered, and changed with the arrows beside each profile.
  const rows = app.el('codex-profile-list').children;
  assert.deepEqual(rows.filter(item => item.tag === 'label').map(item => item.textContent),
    ['1. main', '2. second', '3. third', '4. fourth']);
  const arrows = rows.filter(item => item.className === 'profile-order').map(item => item.children);
  assert.deepEqual(arrows.map(([up, down]) => [up.disabled, down.disabled]),
    [[true, false], [false, false], [false, false], [false, true]]);
  await arrows[0][1].onclick(); await settle(); await settle();
  assert.deepEqual(app.commands.at(-1), ['runtime.set_codex_profile_priority', {order: ['second', 'main', 'third', 'fourth']}]);
  assert.match(app.el('codex-state').textContent, /^Priority: second → main → third → fourth\./);
  assert.equal(profileHistoryRows({history: [{at: NOW, kind: 'priority', reason: 'second,main'},
    {at: NOW, kind: 'selected', profile: 'second', previous: 'main', reason: 'priority'}]}, 'Codex').map(row => row.text).join(' | '),
  'Priority set: second → main | Switched to second from main: it is ahead in the priority');
});

test('retired people are read apart once, named everywhere, and listed only under their state', async () => {
  // In the Linux pilot 3,424 of 4,109 workers were retired, half of a 7 MB overview each poll.
  const app = harness([person(1), person(2)]);
  app.state.overview = {...app.state.overview, retired_count: 2,
    requests: [{id: 'q', kind: 'question', state: 'pending', workers: ['w9'], created_at: 1}]};
  app.state.retired = {count: 2, workers: [['r7', 'Reviewer 7', 'review', 'codex'], ['w9', 'Worker 9', 'ordinary', 'claude']]};
  await app.load(); await app.refresh(); await settle(); await settle();
  assert.equal(app.state.retiredReads, 1);
  assert.deepEqual(app.names(), ['Worker 1', 'Worker 2']);  // Any state: the retired are left out.
  assert.match(app.el('counts').textContent, /2 workers · 0 reviewers · 2 retired$/);
  await app.refresh(); await settle();
  assert.equal(app.state.retiredReads, 1);  // The same count: not read again.
  app.el('worker-state-filter').value = 'Retired'; app.el('worker-state-filter').onchange();
  assert.deepEqual(app.names(), ['Worker 9']);
  assert.deepEqual(app.el('reviewer-list').querySelectorAll('strong').map(item => item.textContent), ['Reviewer 7']);
  // Opened, a retired person's own record fills in what the list leaves out.
  app.state.details = {'/v1/workers/w9': {id: 'w9', kind: 'ordinary', backend: 'claude', state: 'retired',
    work_intention: 'retired', thread_id: 'thread-9', created_at: 1, profile_json: '{"large": true}'}};
  app.el('worker-list').children[0].onclick(); await settle(); await settle();
  assert.match(app.el('worker-ids').textContent, /Native thread ID: thread-9/);
  assert.equal(app.el('conversation-title').textContent, 'Worker 9');
  app.state.overview = {...app.state.overview, retired_count: 3};
  app.state.retired = {count: 3, workers: [...app.state.retired.workers, ['w10', 'Worker 10', 'ordinary', 'codex']]};
  await app.refresh(); await settle(); await settle();
  assert.equal(app.state.retiredReads, 2);
  assert.deepEqual(app.names(), ['Worker 9', 'Worker 10']);
});

test('after a slow overview the dashboard asks less often, at most 20 s apart', () => {
  assert.deepEqual([0, 500, 1000, 3000, 7000, 60000].map(pollsToSkip), [0, 0, 1, 4, 9, 9]);
});
