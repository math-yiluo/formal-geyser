'use strict';

// Data and in-flight work belong to a worker/session, never to the current selection.
// This class has no DOM dependency so the concurrency contract can be tested directly.
class WorkerHistoryStore {
  constructor(request, onChange = () => {}) {
    this.request = request;
    this.onChange = onChange;
    this.workers = new Map();
  }

  worker(id) {
    if (!this.workers.has(id)) this.workers.set(id, {
      draft: '', messageKey: null, sending: false, messageError: '',
      sessions: [], selectedSession: null, sessionLoad: null, sessionError: '',
      histories: new Map(), chats: new Map(), chatAfter: 0, chatMore: false,
      chatLoad: null, chatError: '', chatVersion: 0
    });
    return this.workers.get(id);
  }

  session(id, key) {
    const worker = this.worker(id);
    if (!worker.histories.has(key)) worker.histories.set(key, {
      turns: new Map(), meta: null, context: [], contextVersion: null,
      loaded: false, older: false, before: null, inflight: null, error: '', version: 0, ranges: [], checkedRevision: new Map()
    });
    return worker.histories.get(key);
  }

  async loadSessions(id) {
    const worker = this.worker(id);
    if (worker.sessionLoad) return worker.sessionLoad;
    worker.sessionLoad = (async () => {
      try {
        const result = await this.request('/v1/workers/' + encodeURIComponent(id) + '/sessions');
        worker.sessions = result.sessions;
        if (!worker.sessions.some(s => s.key === worker.selectedSession)) {
          worker.selectedSession = (worker.sessions.find(s => s.kind === 'main') || worker.sessions[0])?.key || null;
        }
        worker.sessionError = '';
      } catch (error) { worker.sessionError = error.message; throw error; }
      finally { worker.sessionLoad = null; this.onChange(id, 'sessions'); }
    })();
    return worker.sessionLoad;
  }

  async loadSession(id, key, before = null) {
    if (!key) return;
    const history = this.session(id, key);
    const intent = before === null ? 'latest' : 'before:' + before;
    if (history.inflight) {
      if (history.inflight.intent === intent) return history.inflight.promise;
      // Serialize different page reads. Identical overlapping reads share a promise.
      await history.inflight.promise.catch(() => {});
      return this.loadSession(id, key, before);
    }
    const operation = {intent, promise: null};
    history.inflight = operation;
    this.onChange(id, 'loading');
    operation.promise = (async () => {
      try {
        const known = this.worker(id).sessions.find(s => s.key === key);
        const needContext = !history.loaded || history.contextVersion !== known?.context_version;
        const query = '?limit=20&include_context=' + needContext + (before === null ? '' : '&before=' + before);
        const endpoint = '/v1/workers/' + encodeURIComponent(id) + '/sessions/' + encodeURIComponent(key);
        const result = await this.request(endpoint + query);
        this.applyPage(history, result, before, needContext);
        // A worker can finish an old turn while this view is closed and advance
        // beyond the latest page. Refresh cached nonterminal turns as well, so
        // "all turns loaded" never means an old partial response is frozen forever.
        if (before === null && result.turns.length) {
          const first = result.turns[0].ordinal;
          const revision = String(result.session.updated_at || '') + ':' + result.session.turn_count;
          const terminal = new Set(['completed', 'failed', 'interrupted', 'cancelled', 'not_delivered']);
          const stale = [...history.turns.values()].filter(turn => turn.ordinal < first &&
            turn.status && !terminal.has(turn.status) && history.checkedRevision.get(turn.key) !== revision).sort((a, b) => b.ordinal - a.ordinal);
          for (const turn of stale) {
            if (history.checkedRevision.get(turn.key) === revision) continue;
            const refreshBefore = turn.ordinal + 1;
            const refreshed = await this.request(endpoint + '?limit=20&include_context=false&before=' + refreshBefore);
            this.applyPage(history, refreshed, refreshBefore, false);
            for (const checked of refreshed.turns) history.checkedRevision.set(checked.key, revision);
          }
        }
        history.error = '';
      } catch (error) { history.error = error.message; throw error; }
      finally {
        history.inflight = null;
        this.onChange(id, 'history');
      }
    })();
    return operation.promise;
  }

  applyPage(history, result, before, needContext) {
    const oldest = Math.min(...[...history.turns.values()].map(t => t.ordinal), Infinity);
    const first = result.turns[0]?.ordinal ?? Infinity;
    // Server projections replace a complete turn; deltas are never appended twice.
    let changed = !history.loaded || JSON.stringify(history.meta) !== JSON.stringify(result.session);
    for (const key of result.superseded_turn_keys || []) {
      if (history.turns.delete(key)) changed = true;
    }
    for (const turn of result.turns) {
      if (JSON.stringify(history.turns.get(turn.key)) !== JSON.stringify(turn)) changed = true;
      history.turns.set(turn.key, turn);
    }
    if (result.turns.length) {
      const range = [first, before === null ? result.turns.at(-1).ordinal : before - 1];
      history.ranges.push(range);
      history.ranges.sort((a, b) => a[0] - b[0]);
      history.ranges = history.ranges.reduce((ranges, next) => {
        const last = ranges.at(-1);
        if (last && next[0] <= last[1] + 1) last[1] = Math.max(last[1], next[1]);
        else ranges.push(next);
        return ranges;
      }, []);
    }
    if (first <= oldest || !history.loaded) {
      history.before = result.next_before;
      history.older = result.has_more;
    }
    if (needContext) {
      history.context = result.context || [];
      history.contextVersion = result.session.context_version;
      changed = true;
    }
    history.meta = result.session;
    history.loaded = true;
    if (changed) history.version += 1;
  }

  async loadChat(id) {
    const worker = this.worker(id);
    if (worker.chatLoad) return worker.chatLoad;
    worker.chatLoad = (async () => {
      try {
        const result = await this.request('/v1/workers/' + encodeURIComponent(id) + '/messages?after=' + worker.chatAfter);
        let changed = false;
        for (const message of result.messages) {
          if (JSON.stringify(worker.chats.get(message.id)) !== JSON.stringify(message)) changed = true;
          worker.chats.set(message.id, message);
        }
        for (const update of result.delivery_updates || []) {
          const message = worker.chats.get(update.id);
          if (message && message.acknowledged_at !== update.acknowledged_at) {
            message.acknowledged_at = update.acknowledged_at;
            changed = true;
          }
        }
        for (const update of result.withdrawals || []) {
          const message = worker.chats.get(update.id);
          if (message && message.withdrawn_at !== update.withdrawn_at) {
            message.withdrawn_at = update.withdrawn_at;
            changed = true;
          }
        }
        worker.chatAfter = result.next_after;
        worker.chatMore = result.has_more;
        worker.chatError = '';
        if (changed) worker.chatVersion += 1;
      } catch (error) { worker.chatError = error.message; throw error; }
      finally { worker.chatLoad = null; this.onChange(id, 'chat'); }
    })();
    return worker.chatLoad;
  }

  async send(id) {
    const worker = this.worker(id);
    if (worker.sending || !worker.draft.trim()) return;
    const text = worker.draft;
    if (!worker.messageKey || worker.messageKey.text !== text) worker.messageKey = {text, id: crypto.randomUUID()};
    const key = worker.messageKey.id;
    worker.sending = true;
    worker.messageError = '';
    this.onChange(id, 'compose');
    try {
      await this.request('/v1/commands', {name: 'message.send', key, payload: {worker_id: id, text, message_id: key}});
      // A user may have edited this draft or switched to another worker during the send.
      if (worker.draft === text) worker.draft = '';
      if (worker.messageKey?.id === key) worker.messageKey = null;
      await this.loadChat(id).catch(error => { worker.messageError = 'Sent; message history could not be refreshed: ' + error.message; });
    } catch (error) { worker.messageError = error.message; throw error; }
    finally { worker.sending = false; this.onChange(id, 'compose'); }
  }

  // Withdraw your queued message before any turn receives it.
  async withdraw(id, messageId) {
    const worker = this.worker(id);
    worker.messageError = '';
    try {
      await this.request('/v1/commands', {name: 'message.withdraw', key: crypto.randomUUID(), payload: {message_id: messageId}});
    } catch (error) { worker.messageError = error.message; this.onChange(id, 'compose'); throw error; }
    await this.loadChat(id);
  }
}

function correspondencePresentation(message, workerName) {
  const human = message.sender_role === 'operator';
  const sender = human ? 'You' : message.sender_display_name || 'Unknown sender';
  const recipient = message.direction === 'to_worker' ? workerName : 'You';
  return {title: sender + ' → ' + recipient, style: human ? 'user' : 'assistant'};
}

function requestResolutionPayload(request, outcome, text, sourcePath, messageId) {
  if (!text.trim() || new TextEncoder().encode(text).length > 131072) {
    throw Error('Enter a reply within 131072 UTF-8 bytes before submitting.');
  }
  const payload = {request_id: request.id, outcome, text, message_id: messageId};
  if (request.kind === 'literature' && outcome === 'fulfilled') {
    if (!sourcePath.trim()) throw Error('Supply a readable local file or publication folder. Links alone remain pending.');
    payload.source_path = sourcePath.trim();
  }
  return payload;
}

// The task list and frozen contracts have separate state from live execution.
// A response may update only the selection and request that originally asked for it.
class TaskBrowserStore {
  constructor(request, onChange = () => {}) {
    this.request = request;
    this.onChange = onChange;
    this.open = false;
    this.view = 'all'; this.query = ''; this.offset = 0; this.limit = 25;
    // Work tasks by default; reviews are counted on their task. `until` holds the list still:
    // tasks published after the first page of a view was read wait behind "newer".
    this.kind = 'work'; this.sort = 'newest'; this.until = null;
    this.page = null; this.pageLoad = null; this.error = '';
    this.counts = {all: null, running: null, pool: null, history: null};
    this.selectedTask = null;
    this.details = new Map();
    this.titles = new Map();
    this.numbers = new Map();  // A work task's number: it is shown as Task N.
  }

  rememberTasks(items) {
    for (const item of items || []) {
      const title = item.contract?.title || item.title;
      if (title) this.titles.set(item.id, title);
      if (Number.isInteger(item.task_number)) this.numbers.set(item.id, item.task_number);
    }
  }

  detail(id = this.selectedTask) {
    if (!id) return null;
    if (!this.details.has(id)) this.details.set(id, {
      current: null, selectedRevision: null, history: new Map(),
      currentLoad: null, historicalLoad: null, currentError: '', historicalError: '',
      revisions: null, revisionOffset: 0, revisionsLoad: null, revisionsError: '',
      relations: null, relationsLoad: null, relationsError: '', relationsAt: 0
    });
    return this.details.get(id);
  }

  setOpen(open) {
    this.open = open;
    if (!open) {
      this.pageLoad = null;
      for (const detail of this.details.values()) {
        detail.currentLoad = detail.historicalLoad = detail.revisionsLoad = detail.relationsLoad = null;
      }
    }
  }

  async browse({view = this.view, query = this.query, offset = 0, limit = this.limit, kind = this.kind, sort = this.sort} = {}) {
    if (!['all', 'running', 'pool', 'history'].includes(view)) throw Error('Unknown task view.');
    if (!['work', 'review', 'both'].includes(kind) || !['newest', 'oldest', 'title'].includes(sort)) throw Error('Unknown task filter.');
    if (!Number.isInteger(limit) || limit < 1 || limit > 100 || !Number.isInteger(offset) || offset < 0) {
      throw Error('Invalid task page.');
    }
    query = query.trim();
    // Another view, search, filter or order starts from what exists now; another page keeps it.
    if (view !== this.view || query !== this.query || kind !== this.kind || sort !== this.sort) this.until = null;
    if (view !== this.view || query !== this.query || offset !== this.offset || limit !== this.limit
        || kind !== this.kind || sort !== this.sort) {
      this.view = view; this.query = query; this.offset = offset; this.limit = limit; this.kind = kind; this.sort = sort;
      this.page = null; this.pageLoad = null; this.error = '';
    }
    return this.loadPage();
  }

  // Show the tasks published since the list was held still, from its first page.
  async showNewer() {
    this.until = null; this.offset = 0; this.pageLoad = null;
    return this.loadPage();
  }

  async loadPage() {
    if (!this.open) return;
    if (this.pageLoad) return this.pageLoad.promise;
    const operation = {promise: null};
    this.pageLoad = operation;
    this.onChange('page');
    operation.promise = (async () => {
      try {
        const params = new URLSearchParams({view: this.view, query: this.query, offset: this.offset, limit: this.limit,
          kind: this.kind, sort: this.sort});
        if (this.until !== null) params.set('until', this.until);
        const result = await this.request('/v1/task-browser?' + params);
        if (!this.open || this.pageLoad !== operation) return;
        this.page = result; this.counts = result.counts; this.offset = result.offset; this.error = '';
        if (this.until === null && Number.isInteger(result.now)) this.until = result.now;
        this.rememberTasks(result.items);
        // A shrinking result set can leave the last page empty.
        if (!result.items.length && result.total > 0 && this.offset >= result.total) {
          this.offset = Math.floor((result.total - 1) / this.limit) * this.limit;
          this.pageLoad = null;
          return await this.loadPage();
        }
      } catch (error) {
        if (this.open && this.pageLoad === operation) this.error = error.message;
        throw error;
      } finally {
        if (this.pageLoad === operation) { this.pageLoad = null; this.onChange('page'); }
      }
    })();
    return operation.promise;
  }

  async selectTask(id) {
    if (this.selectedTask !== id) {
      const previous = this.detail();
      if (previous) previous.currentLoad = previous.historicalLoad = previous.revisionsLoad = null;
      this.selectedTask = id;
      this.onChange('detail', id);
    }
    const detail = this.detail(id);
    return Promise.allSettled([this.loadCurrent(id), this.loadRevisions(id), this.loadRelations(id, {force: true}),
      ...(detail.selectedRevision === null ? [] : [this.loadHistorical(id)])]);
  }

  // What the task relates to: its origin, what it published, its reviews, needs and dependents.
  // They change slowly and cost more to read, so a refresh reads them at most every 15 s.
  async loadRelations(id = this.selectedTask, {force = false, now = Date.now()} = {}) {
    if (!this.open || !id || id !== this.selectedTask) return;
    const detail = this.detail(id);
    if (detail.relationsLoad) return detail.relationsLoad.promise;
    if (!force && detail.relations && now - detail.relationsAt < 15000) return;
    const operation = {promise: null}; detail.relationsLoad = operation;
    this.onChange('detail', id);
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/tasks/' + encodeURIComponent(id) + '/relations');
        if (!this.open || id !== this.selectedTask || detail.relationsLoad !== operation) return;
        detail.relations = result; detail.relationsError = ''; detail.relationsAt = now;
        const tasks = Object.values(result.tasks || {}).concat(result.children?.items || []);
        this.rememberTasks(tasks.map(task => ({id: task.id, title: task.title, task_number: task.task_number})));
      } catch (error) {
        if (this.open && id === this.selectedTask && detail.relationsLoad === operation) detail.relationsError = error.message;
        throw error;
      } finally {
        if (detail.relationsLoad === operation) { detail.relationsLoad = null; this.onChange('detail', id); }
      }
    })();
    return operation.promise;
  }

  async loadCurrent(id = this.selectedTask) {
    if (!this.open || !id || id !== this.selectedTask) return;
    const detail = this.detail(id);
    if (detail.currentLoad) return detail.currentLoad.promise;
    const operation = {promise: null}; detail.currentLoad = operation;
    this.onChange('detail', id);
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/tasks/' + encodeURIComponent(id));
        if (!this.open || id !== this.selectedTask || detail.currentLoad !== operation) return;
        detail.current = result; detail.currentError = '';
        this.rememberTasks([result]);
      } catch (error) {
        if (this.open && id === this.selectedTask && detail.currentLoad === operation) detail.currentError = error.message;
        throw error;
      } finally {
        if (detail.currentLoad === operation) { detail.currentLoad = null; this.onChange('detail', id); }
      }
    })();
    return operation.promise;
  }

  async selectRevision(revision) {
    const detail = this.detail();
    if (!detail) return;
    if (revision !== null && (!Number.isInteger(revision) || revision < 1)) throw Error('Invalid contract revision.');
    if (detail.selectedRevision !== revision) {
      detail.selectedRevision = revision; detail.historicalLoad = null; detail.historicalError = '';
      this.onChange('detail', this.selectedTask);
    }
    return revision === null ? this.loadCurrent() : this.loadHistorical();
  }

  async loadHistorical(id = this.selectedTask) {
    if (!this.open || !id || id !== this.selectedTask) return;
    const detail = this.detail(id), revision = detail.selectedRevision;
    if (revision === null || detail.history.has(revision)) return;
    if (detail.historicalLoad) return detail.historicalLoad.promise;
    const operation = {promise: null}; detail.historicalLoad = operation;
    this.onChange('detail', id);
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/tasks/' + encodeURIComponent(id) + '?revision=' + revision);
        if (!this.open || id !== this.selectedTask || detail.historicalLoad !== operation || detail.selectedRevision !== revision) return;
        detail.history.set(revision, result); detail.historicalError = '';
      } catch (error) {
        if (this.open && id === this.selectedTask && detail.historicalLoad === operation) detail.historicalError = error.message;
        throw error;
      } finally {
        if (detail.historicalLoad === operation) { detail.historicalLoad = null; this.onChange('detail', id); }
      }
    })();
    return operation.promise;
  }

  async loadRevisions(id = this.selectedTask, offset = null) {
    if (!this.open || !id || id !== this.selectedTask) return;
    const detail = this.detail(id);
    if (offset !== null && offset !== detail.revisionOffset) {
      detail.revisionOffset = offset; detail.revisions = null; detail.revisionsLoad = null;
    }
    if (detail.revisionsLoad) return detail.revisionsLoad.promise;
    const operation = {promise: null}; detail.revisionsLoad = operation;
    this.onChange('detail', id);
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/tasks/' + encodeURIComponent(id) + '/revisions?offset=' + detail.revisionOffset + '&limit=20');
        if (!this.open || id !== this.selectedTask || detail.revisionsLoad !== operation) return;
        detail.revisions = result; detail.revisionOffset = result.offset; detail.revisionsError = '';
      } catch (error) {
        if (this.open && id === this.selectedTask && detail.revisionsLoad === operation) detail.revisionsError = error.message;
        throw error;
      } finally {
        if (detail.revisionsLoad === operation) { detail.revisionsLoad = null; this.onChange('detail', id); }
      }
    })();
    return operation.promise;
  }

  // After a change the operator made: reads begun before it are dropped for new ones.
  async reload() {
    this.pageLoad = null;
    const detail = this.detail();
    if (detail) detail.currentLoad = null;
    return this.refresh();
  }

  // `page`: whether the list is shown, so its page is read again too.
  async refresh({page = true} = {}) {
    if (!this.open) return;
    await Promise.allSettled([...(page ? [this.loadPage()] : []), this.loadCurrent(), this.loadRelations(),
      ...(this.detail()?.selectedRevision === null ? [] : [this.loadHistorical()])]);
    const detail = this.detail();
    if (this.open && detail && (!detail.revisions || detail.revisions.current_revision !== detail.current?.current_revision)) {
      await this.loadRevisions().catch(() => {});
    }
  }
}

// Setup owns its draft and one immutable submission until its outcome is known.
// Catalog reads and execution requests are always explicit, never part of polling.
class ProjectSetupStore {
  constructor(request, onChange = () => {}, makeKey = () => crypto.randomUUID()) {
    this.request = request; this.onChange = onChange; this.makeKey = makeKey;
    this.open = false; this.snapshot = null; this.catalog = null;
    this.goal = ''; this.model = ''; this.effort = ''; this.settingsLoaded = false;
    this.stateLoad = null; this.modelsLoad = null; this.submission = null; this.driveLoad = null;
    this.stateError = ''; this.modelsError = ''; this.submitError = ''; this.driveError = '';
    this.pending = null; this.result = null; this.uncertainDrive = null; this.actionRevision = 0;
  }

  setOpen(open) { this.open = open; this.onChange(); }

  resetProject() {
    this.goal = ''; this.model = ''; this.effort = ''; this.settingsLoaded = false; this.catalog = null;
    this.modelsLoad = null; this.submission = null; this.driveLoad = null;
    this.modelsError = ''; this.submitError = ''; this.driveError = '';
    this.pending = null; this.result = null; this.uncertainDrive = null;
    this.actionRevision += 1;
  }

  async show() {
    this.setOpen(true);
    await this.loadState();
    if (this.open && this.snapshot?.can_configure) await this.loadModels();
  }

  async refresh() { if (this.open) await this.loadState(); }

  async loadState() {
    if (this.stateLoad) return this.stateLoad.promise;
    const operation = {promise: null, revision: this.actionRevision}; this.stateLoad = operation; this.onChange();
    operation.promise = (async () => {
      try {
        const snapshot = await this.request('/v1/setup');
        // A read started before an execution request cannot undo its outcome.
        const changedProject = this.snapshot && this.snapshot.project_id !== snapshot.project_id;
        if (operation.revision !== this.actionRevision && !changedProject) return this.snapshot;
        if (changedProject) this.resetProject();
        this.snapshot = snapshot; this.stateError = '';
        if (snapshot.task) this.goal = snapshot.task.objective;
        if (!this.settingsLoaded || !snapshot.can_configure) {
          this.model = snapshot.settings.model || ''; this.effort = snapshot.settings.effort || '';
          this.settingsLoaded = true;
          this.applyDefaults();
        }
        const receipt = snapshot.receipt, submitted = this.pending;
        if (receipt && submitted && receipt.key === submitted.key && receipt.model === submitted.model &&
            receipt.effort === submitted.effort && (!submitted.task_id || receipt.task_id === submitted.task_id)) {
          this.result = {task_id: receipt.task_id, run_id: receipt.run_id, replayed: true,
            armed: Boolean(snapshot.run?.id === receipt.run_id && snapshot.run.armed),
            driving: Boolean(snapshot.run?.id === receipt.run_id && snapshot.run.driving)};
          this.pending = null; this.submitError = '';
        }
        if (this.result && snapshot.run?.id === this.result.run_id) {
          this.result = {...this.result, armed: snapshot.run.armed, driving: snapshot.run.driving};
        }
        if (this.uncertainDrive === snapshot.run?.id && (snapshot.run.armed || snapshot.run.driving)) {
          this.result = {...this.result, task_id: snapshot.task?.id, run_id: snapshot.run.id,
            armed: snapshot.run.armed, driving: snapshot.run.driving};
          this.uncertainDrive = null; this.driveError = '';
        }
        return snapshot;
      } catch (error) {
        if (operation.revision !== this.actionRevision) return this.snapshot;
        this.stateError = error.message; throw error;
      }
      finally { this.stateLoad = null; this.onChange(); }
    })();
    return operation.promise;
  }

  async loadModels() {
    if (!this.open || !this.snapshot?.can_configure) return;
    if (this.modelsLoad) return this.modelsLoad.promise;
    const operation = {promise: null, projectId: this.snapshot.project_id}; this.modelsLoad = operation; this.onChange();
    operation.promise = (async () => {
      try {
        const catalog = await this.request('/v1/setup/models');
        if (this.modelsLoad !== operation || this.snapshot?.project_id !== operation.projectId) return null;
        this.catalog = catalog; this.modelsError = ''; this.applyDefaults();
        return catalog;
      } catch (error) {
        if (this.modelsLoad !== operation || this.snapshot?.project_id !== operation.projectId) return null;
        this.modelsError = error.message; throw error;
      } finally { if (this.modelsLoad === operation) { this.modelsLoad = null; this.onChange(); } }
    })();
    return operation.promise;
  }

  selectedModel() { return this.catalog?.models.find(model => model.id === this.model); }

  applyDefaults() {
    // A missing setting can use a catalog default. An unsupported saved value
    // stays visible until the operator explicitly chooses a replacement.
    if (!this.snapshot?.can_configure) return;
    if (!this.model) this.model = this.catalog?.models.find(model => model.is_default)?.id || '';
    const model = this.selectedModel();
    if (!this.effort && model?.efforts.some(effort => effort.id === model.default_effort)) this.effort = model.default_effort;
  }

  get editable() {
    return Boolean(this.snapshot?.can_configure && !this.pending && !this.submission && !this.result);
  }

  setGoal(goal) {
    if (!this.editable || this.snapshot.task) return;
    this.goal = goal; this.submitError = ''; this.onChange();
  }

  selectModel(id) {
    if (!this.editable) return;
    const model = this.catalog?.models.find(item => item.id === id);
    if (!model) return;
    this.model = id;
    if (!model.efforts.some(item => item.id === this.effort)) {
      this.effort = model.efforts.some(item => item.id === model.default_effort) ? model.default_effort : '';
    }
    this.submitError = ''; this.onChange();
  }

  selectEffort(id) {
    if (!this.editable || !this.selectedModel()?.efforts.some(item => item.id === id)) return;
    this.effort = id; this.submitError = ''; this.onChange();
  }

  get selectionIssue() {
    if (!this.catalog) return '';
    if (this.catalog.authentication_required) return 'Sign in to Codex in the configured worker profile, then retry models.';
    if (!this.catalog.models.length) return 'No models are available in the configured worker profile. Retry models after checking the profile.';
    const model = this.selectedModel();
    if (!model) return this.model ? 'Model "' + this.model + '" is unavailable. Choose a model from the current catalog.' : 'Choose a model.';
    if (!model.efforts.some(item => item.id === this.effort)) return this.effort
      ? 'Effort "' + this.effort + '" is unavailable for this model. Choose an available effort.' : 'Choose an effort.';
    return '';
  }

  get startBlockedReason() {
    if (this.result) return 'Project setup has been saved.';
    if (!this.snapshot || this.stateError) return 'Refresh project status before starting.';
    if (!this.snapshot.can_configure) return this.snapshot.reason || 'Project setup is read-only. Use the existing run controls.';
    if (this.submission) return 'Starting the project…';
    if (this.modelsLoad) return 'Loading available models…';
    if (!this.catalog || this.modelsError) return 'Load available models before starting.';
    if (this.selectionIssue) return this.selectionIssue;
    if (!this.goal.trim()) return 'Enter the Task 0 goal.';
    if (this.goal.length > 20000) return 'Keep the Task 0 goal within 20,000 characters.';
    return '';
  }

  start() {
    if (this.submission) return this.submission.promise;
    if (this.startBlockedReason) return Promise.reject(Error(this.startBlockedReason));
    if (!this.pending) this.pending = {key: this.makeKey(), project_id: this.snapshot.project_id,
      goal: this.goal, model: this.model, effort: this.effort,
      task_id: this.snapshot.task?.id || null, task_revision: this.snapshot.task?.revision ?? null};
    const submitted = this.pending, operation = {promise: null}; this.submission = operation;
    this.actionRevision += 1;
    this.submitError = ''; this.onChange();
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/setup/start', submitted);
        if (this.submission !== operation || this.snapshot?.project_id !== submitted.project_id) return null;
        this.result = result; this.pending = null; this.submitError = '';
        return result;
      } catch (error) {
        if (this.submission !== operation || this.snapshot?.project_id !== submitted.project_id) return null;
        // A simultaneous status read may already have confirmed this request.
        if (this.result) return this.result;
        const rejected = error.status >= 400 && error.status < 500 && ![408, 429].includes(error.status);
        if (rejected) this.pending = null;
        this.submitError = rejected ? 'Start was rejected: ' + error.message
          : 'Start could not be confirmed: ' + error.message + ' Refresh status or retry the same request.';
        throw error;
      } finally {
        if (this.submission === operation) { this.actionRevision += 1; this.submission = null; this.onChange(); }
      }
    })();
    return operation.promise;
  }

  get canDrive() {
    return Boolean(this.snapshot?.can_drive && this.snapshot.run && !this.stateError && !this.driveLoad && !this.submission);
  }

  drive() {
    if (this.driveLoad) return this.driveLoad.promise;
    if (!this.canDrive) return Promise.reject(Error('This run is not available to start. Refresh project status.'));
    const runId = this.snapshot.run.id, taskId = this.snapshot.task?.id;
    const operation = {promise: null, projectId: this.snapshot.project_id}; this.driveLoad = operation;
    this.actionRevision += 1;
    this.driveError = ''; this.onChange();
    operation.promise = (async () => {
      try {
        const result = await this.request('/v1/runs/' + encodeURIComponent(runId) + '/drive', {});
        if (this.driveLoad !== operation || this.snapshot?.project_id !== operation.projectId) return null;
        if (this.snapshot.run?.id === runId) this.snapshot = {...this.snapshot, can_drive: false,
          run: {...this.snapshot.run, armed: result.armed, driving: result.driving}};
        this.result = {task_id: taskId, ...result}; this.uncertainDrive = null;
        return this.result;
      } catch (error) {
        if (this.driveLoad !== operation || this.snapshot?.project_id !== operation.projectId) return null;
        if (this.snapshot.run?.id === runId && (this.snapshot.run.armed || this.snapshot.run.driving)) {
          this.result = {task_id: taskId, run_id: runId, armed: this.snapshot.run.armed, driving: this.snapshot.run.driving};
          return this.result;
        }
        this.driveError = 'The run start could not be confirmed: ' + error.message + ' Refresh status before trying again.';
        // The service might have received the request. A read must establish
        // whether starting remains available before an explicit second click.
        if (this.snapshot.run?.id === runId) this.snapshot = {...this.snapshot, can_drive: false};
        this.uncertainDrive = runId;
        throw error;
      } finally {
        if (this.driveLoad === operation) { this.actionRevision += 1; this.driveLoad = null; this.onChange(); }
      }
    })();
    return operation.promise;
  }
}

function workerStateLabel(value) {
  if (value.state === 'recovery_required') return 'Recovering';
  if (value.state === 'draining') return 'Finishing turn';
  if (value.work_intention === 'retired' || value.state === 'retired') return 'Retired';
  if (value.active_attempt && ['message', 'message_only'].includes(value.execution_mode)) return 'Handling message';
  if (value.state === 'suspended' && value.work_intention !== 'waiting') return 'Paused';
  if (value.active_attempt) return 'Working';
  return {waiting: 'Waiting', idle: 'Idle', sleeping: 'Sleeping', working: 'Ready'}[value.work_intention]
    || String(value.state || 'Status not recorded').replaceAll('_', ' ');
}

function workerTaskPrefix(value, tasks) {
  const task = tasks.find(item => item.id === value.current_task_id);
  const member = task?.collaboration?.members?.find(item => item.worker_id === value.id || item.worker_id === value.display_name);
  const status = member?.execution_status || workerStateLabel(value);
  return {'Working': 'Working on ', 'Finishing turn': 'Finishing turn on ',
    'Handling message': 'Handling messages for ', 'Waiting': 'Waiting on ',
    'Ready': 'Ready to continue ', 'Recovering': 'Automatically recovering ', 'Paused': 'Paused on ',
    'Left': 'Previous task: ', 'Ended': 'Previous task: '}[status] || 'Retained task: ';
}

function selectedRun(runs) {
  return runs.find(r => r.state === 'running') || runs.find(r => r.state === 'paused') || runs.at(-1) || null;
}

// One sentence about current work, with applicable project-wide controls.
function projectStatus(data) {
  const run = selectedRun(data?.runs || []), service = data?.service || {}, workers = data?.workers || [];
  if (!run) return {tone: 'idle', text: 'No run yet. Open Project setup to start Task 0.', actions: ['setup']};
  if (run.state === 'complete') return {tone: 'idle', text: 'Project complete.', actions: []};
  if (run.state === 'stopped') return {tone: 'idle', text: 'Project stopped.', actions: []};
  const resumed = workers.filter(w => w.run_control === 'resumed').length;
  // Codex and Claude bill separate accounts: a hold stops only its own backend's workers.
  const holds = Object.values(run.dispatch_holds || (run.dispatch_hold ? {codex: run.dispatch_hold} : {}));
  if (holds.length) {
    // A hold on the machine (both backends, one message) is said once and ends without Resume.
    const shown = holds.every(hold => hold.message === holds[0].message) ? [holds[0]] : holds;
    const named = shown.length > 1 || (holds.length === 1 && holds[0].backend === 'claude');
    const text = shown.map(hold => (named ? (hold.backend === 'claude' ? 'Claude: ' : 'Codex: ') : '') + hold.message).join(' ');
    // A sandbox hold and one while every profile of a backend is at its limit end by themselves;
    // one while no profile in the rotation can be used ends in the backend's tab.
    const resume = holds.some(hold => !['sandboxUnavailable', 'claudeProfilesLimited', 'claudeProfilesUnavailable',
      'codexProfilesLimited', 'codexProfilesUnavailable'].includes(hold.code))
      || run.state !== 'running' ? ['resume'] : [];
    return {tone: 'attention', text: text + ' Turns already running finish on their own.',
      actions: run.state === 'running' ? [...resume, 'pause_after_turn', 'pause'] : resumed ? [...resume, 'pause'] : resume};
  }
  // The groups the operator paused after turn, or resumed while everyone else is paused.
  const groups = CONTROL_GROUPS.filter(group => run.groups?.[group.key]?.control === (run.state === 'paused' ? 'resumed' : 'paused'))
    .map(group => group.label);
  if (run.state === 'paused') {
    // Pause all after turn: turns already under way finish on their own; Pause all now still interrupts them.
    const finishing = run.pause_after_turn ? workers.filter(w => w.active_attempt && w.state === 'running'
      && w.run_control !== 'resumed' && w.group_control !== 'resumed').length : 0;
    if (finishing) {
      return {tone: 'attention', actions: ['resume', 'pause'], text: 'Pausing after turns · ' + (finishing === 1
        ? '1 turn in progress finishes on its own' : finishing + ' turns in progress finish on their own')
        + '; no new turns start until you resume.' + (groups.length ? ' Resumed: ' + groups.join(', ') + '.' : '')};
    }
    if (groups.length) {
      return {tone: 'attention', actions: ['resume', 'pause'], text: 'Paused · resumed: ' + groups.join(', ') + '.'};
    }
    return {tone: 'attention', actions: resumed ? ['resume', 'pause'] : ['resume'], text: resumed
      ? 'Paused · ' + resumed + (resumed === 1 ? ' person continues' : ' people continue') + ' only their own current work.'
      : 'Paused. No new turns start until you resume.'};
  }
  const startup = service.startup_resume;
  if (startup?.retrying) {
    return {tone: 'attention', text: 'Startup recovery is retrying automatically' + (startup.error ? ': ' + startup.error : '.'), actions: []};
  }
  if (!(service.armed_runs || []).includes(run.id)) {
    return {tone: 'attention', text: 'Not running. This service is not driving the project, so no turns start.', actions: ['start']};
  }
  const live = workers.filter(w => w.active_attempt && w.state !== 'recovery_required'), active = live.length;
  const recovering = workers.filter(w => w.state === 'recovery_required').length;
  // Codex and Claude are separate accounts with separate session limits.
  const codex = live.filter(w => (w.backend || 'codex') === 'codex').length, claude = active - codex;
  const accounts = [codex && 'Codex ' + codex, claude && 'Claude ' + claude].filter(Boolean).join(', ');
  const parts = [active ? active + (active === 1 ? ' turn' : ' turns') + ' in progress (' + accounts + ')' : 'idle, waiting for work'];
  if (recovering) parts.push(recovering + ' recovering');
  const paused = workers.filter(w => w.run_control === 'paused').length;
  if (paused) parts.push(paused + (paused === 1 ? ' person paused' : ' people paused'));
  if (groups.length) parts.push('paused after turns: ' + groups.join(', '));
  if (run.usage_uncertain) parts.push('usage needs reconciliation');
  return {tone: active ? 'active' : 'idle', text: 'Running · ' + parts.join(' · '), actions: ['pause_after_turn', 'pause']};
}

// The run's state as it applies to one person: paused or resumed on its own, else with its group.
function personControl(run, value) {
  if (!run || !['running', 'paused'].includes(run.state) || value?.work_intention === 'retired') return null;
  const individual = value?.run_control === 'resumed' || value?.run_control === 'paused' ? value.run_control : null;
  const group = !individual && ['resumed', 'paused'].includes(value?.group_control) ? value.group_control : null;
  const control = individual || group;
  const running = control ? control === 'resumed' : run.state === 'running';
  // Pause after turn: a turn under way finishes on its own, and nothing new starts after it.
  // A group pauses only after turns.
  const afterTurn = !running && Boolean(individual === 'paused' ? value?.run_control_after_turn
    : group === 'paused' || run.pause_after_turn);
  const live = Boolean(value?.active_attempt) && value?.state === 'running';
  return {individual, group, running, afterTurn, finishing: afterTurn && live};
}

// The groups paused after turn or resumed together (Hub CONTROL_GROUPS). The Integrator and the
// Dispatcher are in none: they follow Everyone.
const CONTROL_GROUPS = [
  {key: 'codex', label: 'All Codex', backend: 'codex', kinds: ['ordinary', 'review']},
  {key: 'codex.workers', label: 'Codex workers', backend: 'codex', kinds: ['ordinary']},
  {key: 'codex.reviewers', label: 'Codex reviewers', backend: 'codex', kinds: ['review']},
  {key: 'claude', label: 'All Claude', backend: 'claude', kinds: ['ordinary', 'review']},
];

function turnsLabel(count) {
  return count + (count === 1 ? ' turn' : ' turns');
}

// The run controls console: Everyone, then each group, with what applies now and its actions.
function controlConsole(run, workers = []) {
  if (!run || !['running', 'paused'].includes(run.state)) return [];
  const live = value => Boolean(value.active_attempt) && value.state === 'running';
  const everyone = workers.filter(live).length;
  const rows = [{key: 'run', label: 'Everyone', state: run.state, control: null, live: everyone,
    text: run.state === 'running' ? 'Running' + (everyone ? ' · ' + turnsLabel(everyone) + ' in progress' : '')
      : (run.pause_after_turn ? 'Paused after turns' : 'Paused') + (everyone ? ' · ' + turnsLabel(everyone) + ' still running' : ''),
    actions: run.state === 'running' ? ['pause_after_turn', 'pause'] : run.pause_after_turn && everyone ? ['resume', 'pause'] : ['resume']}];
  for (const group of CONTROL_GROUPS) {
    const view = run.groups?.[group.key] || {control: null, state: run.state === 'running' ? 'running' : 'paused'};
    const members = workers.filter(value => (value.backend || 'codex') === group.backend
      && group.kinds.includes(value.kind || 'ordinary') && value.work_intention !== 'retired');
    const running = members.filter(live).length;
    let text;
    if (view.state === 'running') {
      text = (view.control === 'resumed' && run.state === 'paused' ? 'Resumed while others are paused' : 'Running')
        + (running ? ' · ' + turnsLabel(running) + ' in progress' : '');
    } else if (view.state === 'paused') {
      // Its members' turns under way finish on their own; someone resumed on its own runs on.
      text = (view.control === 'paused' ? 'Paused after turns' : 'Paused with everyone')
        + (running ? ' · ' + turnsLabel(running) + ' still running' : '');
    } else {
      text = 'Partly paused: workers and reviewers apart' + (running ? ' · ' + turnsLabel(running) + ' running' : '');
    }
    rows.push({key: group.key, label: group.label, state: view.state, control: view.control, live: running, text,
      actions: view.state === 'running' ? ['pause_after_turn'] : view.state === 'paused' ? ['resume'] : ['pause_after_turn', 'resume']});
  }
  return rows;
}

function elapsedLabel(since, now = Date.now()) {
  const minutes = Math.floor(Math.max(0, now - since) / 60000);
  if (minutes < 1) return 'under 1 min';
  if (minutes < 60) return minutes + ' min';
  const hours = Math.floor(minutes / 60), rest = minutes % 60;
  return hours + ' h' + (rest ? ' ' + rest + ' min' : '');
}

// Specific about what a person is doing or waiting for, with elapsed time for live turns.
function workerActivity(value, {asking = [], awaitingReview = false, now = Date.now()} = {}) {
  const base = workerStateLabel(value);
  if (['Working', 'Handling message', 'Finishing turn'].includes(base)) {
    return value.active_since ? base + ' · ' + elapsedLabel(value.active_since, now) : base;
  }
  if (base === 'Recovering') return base;
  if (asking.length && value.work_intention === 'waiting') {
    return asking.every(kind => kind === 'literature') ? 'Waiting for a source from you' : 'Waiting for your answer';
  }
  if (value.kind === 'dispatcher') return 'Assigns open tasks';
  if (value.kind === 'waker') return 'Ends waits on published tasks';
  if (base === 'Retired') return value.kind === 'review' ? 'Review finished' : 'Finished';
  if (awaitingReview) return base + ' · submission in review';
  return base;
}

const ACTION_PHRASES = {
  command: n => n === 1 ? 'ran a command' : 'ran ' + n + ' commands',
  file: n => n === 1 ? 'edited a file' : 'made ' + n + ' file edits',
  search: n => n === 1 ? 'searched the web' : 'ran ' + n + ' web searches',
  external: n => n === 1 ? 'called an external tool' : 'called external tools ' + n + ' times',
  lean: n => n === 1 ? 'ran a Lean check' : 'ran ' + n + ' Lean checks',
  message_send: n => n === 1 ? 'messaged you' : 'sent you ' + n + ' messages',
  question: n => n === 1 ? 'asked you a question' : 'asked you ' + n + ' questions',
  literature: n => n === 1 ? 'asked you for a source' : 'asked you for ' + n + ' sources',
  talk_send: n => n === 1 ? 'messaged a teammate' : 'sent ' + n + ' messages to teammates',
  stage: () => 'staged a candidate',
  contribute: () => 'submitted work for review',
  withdraw: () => 'withdrew a submission',
  review: () => 'recorded a review verdict',
  publish: n => n === 1 ? 'published a task' : 'published ' + n + ' tasks',
  propose_revision: () => 'proposed a task revision',
  checkpoint: () => 'saved a checkpoint',
  pause: () => 'paused its task',
  resume_work: () => 'resumed work',
  leave_task: () => 'left its task',
  invitation_update: () => 'updated its collaboration invitation',
  invitation_withdraw: () => 'withdrew its collaboration invitation',
  complete: () => 'declared the task complete',
  completion_feedback: () => 'gave completion feedback',
  completion_finalize: () => 'finalized task completion',
  wiki_submit: () => 'submitted a wiki edit',
  literature_register: () => 'registered a source',
  request_resolve: () => 'answered a request',
  dag_update: () => 'updated the shared plan',
  draft_update: () => 'updated a shared draft',
  integration: () => 'worked on integration',
  lookup: n => n === 1 ? 'looked up project state' : 'made ' + n + ' project lookups',
};
const LOOKUP_OPERATIONS = new Set(['actions', 'search', 'inspect', 'fetch', 'public_read', 'talk_read',
  'dag_status', 'sync', 'subscribe', 'batch_status']);

// A one-line, human-readable account of what a turn did; details stay in the action panel.
function actionSummary(actions) {
  const counts = new Map();
  let failed = 0;
  const add = key => counts.set(key, (counts.get(key) || 0) + 1);
  for (const action of actions || []) {
    if (['failed', 'error'].includes(action.status)) failed += 1;
    const operation = action.operation || action.arguments?.operation;
    if (action.tool === 'taskhub_actions') add('lookup');
    else if (action.tool === 'commandExecution') add('command');
    else if (action.tool === 'fileChange') add('file');
    else if (action.tool === 'webSearch') add('search');
    else if (action.tool === 'mcpToolCall') add('external');
    else if (action.tool === 'lean_check' || operation === 'lean_check') add('lean');
    else if (operation === 'request') add(action.arguments?.arguments?.kind === 'literature' ? 'literature' : 'question');
    else if (operation?.startsWith('batch_') && operation !== 'batch_status') add('integration');
    else if (LOOKUP_OPERATIONS.has(operation)) add('lookup');
    else if (operation && ACTION_PHRASES[operation]) add(operation);
    else add('other:' + (operation || action.tool || 'action'));
  }
  const parts = [...counts].map(([key, count]) => key.startsWith('other:')
    ? 'used ' + key.slice(6).replaceAll('_', ' ') + (count > 1 ? ' ' + count + ' times' : '')
    : ACTION_PHRASES[key](count));
  if (failed) parts.push(failed + (failed === 1 ? ' action failed' : ' actions failed'));
  const text = parts.join(' · ');
  return text ? text[0].toUpperCase() + text.slice(1) : '';
}

function parsePacket(text) {
  try {
    const value = JSON.parse(text);
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
  } catch { return null; }
}

// Directed messages travel as file references inside a model input.
function packetMessages(packet) {
  if (!packet) return [];
  return [packet.messages, packet.human_messages?.messages, packet.private_talk?.messages]
    .flatMap(group => Array.isArray(group) ? group : []);
}

// Transport delivery confirms input to the model, not that it read a referenced file.
function inputDelivery(input) {
  const state = input.status || 'unknown';
  const text = {delivered: 'Delivered to model', prepared: 'Prepared · not yet delivered',
    not_delivered: 'Not delivered', uncertain: 'Delivery uncertain'}[state] || 'Delivery not confirmed';
  return {state, text, tone: state === 'delivered' ? '' : state === 'not_delivered' ? 'failure' : 'attention'};
}

// One chronological stream for a turn: what the worker said and did (left) and
// inputs with their delivery evidence (right). Consecutive tool calls fold together.
// Message bodies come from the server's inline copies, else from messageText(path).
function turnStream(turn, {messageText = () => null} = {}) {
  const items = [];
  const push = item => items.push({...item, order: items.length});
  for (const input of turn.inputs || []) {
    const packet = parsePacket(input.text);
    const references = packetMessages(packet);
    const messages = (Array.isArray(input.messages) ? input.messages : references).map((message, index) => {
      const reference = references[index] || {};
      const path = message.path || message.file?.path || reference.file?.path || null;
      const text = typeof message.text === 'string' ? message.text : messageText(path);
      return {sender: message.sender || reference.sender || null, senderRole: message.sender_role || reference.sender_role || null,
        text: typeof text === 'string' ? text : null, path};
    });
    const at = input.at || '';
    if (input.kind !== 'steer') push({kind: 'prompt', side: 'right', at, input, packet});
    else if (!messages.length) push({kind: 'context', side: 'right', at, input, packet,
      title: input.source === 'context_refresh' ? 'Context refreshed after compaction' : 'Additional input'});
    for (const [index, message] of messages.entries()) push({kind: 'message', side: 'right', at, input, ...message,
      delivery: inputDelivery(input), showInput: input.kind === 'steer' && index === 0});
  }
  for (const reply of turn.replies || []) push({kind: 'reply', side: 'left', at: reply.started_at || reply.at || '', reply});
  for (const action of turn.actions || []) {
    const operation = action.operation || action.arguments?.operation;
    const args = (action.tool === 'taskhub_call' && action.arguments?.arguments) || {};
    const at = action.started_at || action.updated_at || '';
    if (action.tool === 'taskhub_call' && operation === 'request') {
      push({kind: 'request', side: 'left', at, action, requestKind: args.kind === 'literature' ? 'literature' : 'question',
        title: args.title || 'Request', text: args.text || '', requestId: action.result?.id || null});
    } else if (action.tool === 'taskhub_call' && ['message_send', 'talk_send'].includes(operation)) {
      push({kind: 'outgoing', side: 'left', at, action, text: args.text || '',
        recipient: operation === 'talk_send' ? args.recipient_id || args.recipient || null : null});
    } else push({kind: 'tool', side: 'left', at, action});
  }
  // ISO timestamps order lexically; ties keep inputs, then replies, then actions.
  items.sort((a, b) => a.at < b.at ? -1 : a.at > b.at ? 1 : a.order - b.order);
  const stream = [];
  for (const item of items) {
    const last = stream.at(-1);
    if (item.kind !== 'tool') stream.push(item);
    else if (last?.kind === 'tools') last.actions.push(item.action);
    else stream.push({kind: 'tools', side: 'left', at: item.at, actions: [item.action]});
  }
  return stream;
}

// A task's current executors: the outline's compact list, or a summary's members.
function taskExecutors(task) {
  if (Array.isArray(task.executors)) return task.executors;
  return (task.collaboration?.members || []).filter(m => m.executor && (!m.membership_state || m.membership_state === 'member'));
}
function openReviews(task) { return task.reviews ? task.reviews.open || 0 : task.reviews_pending || 0; }

// Plain status for the task overview; review tasks keep their verdict wording.
function taskOutlineState(task) {
  if (task.state === 'done') return {text: 'Done', tone: 'done'};
  if (task.state === 'retired') return {text: 'Retired', tone: 'done'};
  const executors = taskExecutors(task);
  if (task.contract?.kind === 'review') {
    const review = task.collaboration?.review || {};
    if (['reject', 'rejected', 'request_changes'].includes(review.feedback?.verdict)) return {text: 'Changes requested', tone: 'attention'};
    if (review.state === 'integrating' || review.acceptance) return {text: 'Accepted · integrating', tone: 'active'};
    return task.running || executors.length ? {text: 'Being reviewed', tone: 'active'} : {text: 'Waiting for a reviewer', tone: 'waiting'};
  }
  if (openReviews(task)) return {text: 'Submitted · awaiting review', tone: 'waiting'};
  if (task.running) return {text: 'In progress', tone: 'active'};
  if (!executors.length) return {text: 'Waiting for a worker', tone: 'waiting'};
  if (executors.some(m => m.execution_status === 'Recovering')) return {text: 'Recovering', tone: 'attention'};
  if (executors.every(m => m.execution_status === 'Waiting')) return {text: 'Waiting', tone: 'waiting'};
  return {text: 'Ready', tone: 'idle'};
}

// Reviews are part of their task's progress, not separate work items.
function outlineProgress(items) {
  const work = (items || []).filter(item => item.contract?.kind !== 'review' && item.state !== 'retired');
  return {total: work.length, done: work.filter(item => item.state === 'done').length,
    review: work.filter(item => item.state !== 'done' && openReviews(item)).length,
    active: work.filter(item => item.state !== 'done' && item.running && !openReviews(item)).length};
}

// Reviews of a task in a few words: "3 reviews · 1 open · 1 changes requested".
function reviewSummary(counts) {
  if (!counts?.total) return '';
  const parts = [counts.total + (counts.total === 1 ? ' review' : ' reviews')];
  if (counts.open) parts.push(counts.open + ' open');
  if (counts.changes) parts.push(counts.changes + ' changes requested');
  return parts.join(' · ');
}

// The publication tree: each work task under the task it came from (`parent`), Task 0 first,
// then in publication order. A parent that is missing, or that would close a cycle, makes a root.
// `below` counts each task's descendants (retired ones left out) and how many are done.
function publicationTree(items, rootId = null) {
  const byId = new Map((items || []).map(item => [item.id, item]));
  const parentOf = id => { const parent = byId.get(id)?.parent; return parent && byId.has(parent) ? parent : null; };
  const children = new Map(), roots = [];
  for (const item of byId.values()) {
    let parent = parentOf(item.id);
    // A cycle in recorded origins never hides a task: walk up, and cut where it repeats.
    for (let seen = new Set([item.id]), up = parent; up; up = parentOf(up)) {
      if (seen.has(up)) { parent = null; break; }
      seen.add(up);
    }
    if (parent) { if (!children.has(parent)) children.set(parent, []); children.get(parent).push(item.id); }
    else roots.push(item.id);
  }
  roots.sort((a, b) => (b === rootId) - (a === rootId));
  const below = new Map(), order = [], stack = [...roots];
  while (stack.length) { const id = stack.pop(); order.push(id); stack.push(...(children.get(id) || [])); }
  for (const id of order.reverse()) {
    const counts = {total: 0, done: 0};
    for (const child of children.get(id) || []) {
      const task = byId.get(child), inner = below.get(child);
      counts.total += inner.total + (task.state === 'retired' ? 0 : 1);
      counts.done += inner.done + (task.state === 'done' ? 1 : 0);
    }
    below.set(id, counts);
  }
  return {byId, roots, children, below};
}

// The ancestors of a task in the publication tree, nearest first.
function treeAncestors(tree, id) {
  const found = [], seen = new Set([id]);
  for (let up = tree.byId.get(id)?.parent; up && tree.byId.has(up) && !seen.has(up); up = tree.byId.get(up).parent) {
    found.push(up); seen.add(up);
  }
  return found;
}

// A small, safe Markdown reading for task texts: headings, lists, quotes, fenced code, rules and
// paragraphs, with inline code, bold, italics and http(s) links. It returns plain data; the page
// builds nodes from it as text, never as HTML.
function markdownInline(text) {
  const spans = [], pattern = /(`+)([\s\S]*?)\1|\*\*([^*]+)\*\*|__([^_]+)__|\*([^*\s][^*]*)\*|\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g;
  let last = 0;
  for (const match of String(text).matchAll(pattern)) {
    if (match.index > last) spans.push({type: 'text', text: text.slice(last, match.index)});
    if (match[1]) spans.push({type: 'code', text: match[2].trim() || match[2]});
    else if (match[3] || match[4]) spans.push({type: 'strong', text: match[3] || match[4]});
    else if (match[5]) spans.push({type: 'em', text: match[5]});
    else spans.push({type: 'link', text: match[6], href: match[7]});
    last = match.index + match[0].length;
  }
  if (last < text.length) spans.push({type: 'text', text: text.slice(last)});
  return spans;
}
function markdownBlocks(text) {
  const lines = String(text || '').replace(/\r\n?/g, '\n').split('\n'), blocks = [];
  let paragraph = [];
  const flush = () => { if (paragraph.length) blocks.push({type: 'paragraph', inline: markdownInline(paragraph.join(' '))}); paragraph = []; };
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const fence = line.match(/^\s*(```|~~~)/);
    if (fence) {
      flush();
      const code = [];
      for (index += 1; index < lines.length && !lines[index].trimStart().startsWith(fence[1]); index += 1) code.push(lines[index]);
      blocks.push({type: 'code', text: code.join('\n')});
      continue;
    }
    const heading = line.match(/^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$/);
    if (heading) { flush(); blocks.push({type: 'heading', level: heading[1].length, inline: markdownInline(heading[2])}); continue; }
    if (/^\s{0,3}([-*_])(\s*\1){2,}\s*$/.test(line)) { flush(); blocks.push({type: 'rule'}); continue; }
    const item = line.match(/^(\s*)([-*+]|\d+[.)])\s+(.*)$/);
    if (item) {
      flush();
      const ordered = /\d/.test(item[2]), list = {type: 'list', ordered, items: []};
      for (; index < lines.length; index += 1) {
        const next = lines[index].match(/^(\s*)([-*+]|\d+[.)])\s+(.*)$/);
        if (next && /\d/.test(next[2]) === ordered) list.items.push(markdownInline(next[3]));
        else if (list.items.length && /^\s{2,}\S/.test(lines[index])) {
          list.items[list.items.length - 1].push({type: 'text', text: ' '}, ...markdownInline(lines[index].trim()));
        } else break;
      }
      index -= 1;
      blocks.push(list);
      continue;
    }
    const quote = line.match(/^\s{0,3}>\s?(.*)$/);
    if (quote) { flush(); blocks.push({type: 'quote', inline: markdownInline(quote[1])}); continue; }
    if (!line.trim()) { flush(); continue; }
    paragraph.push(line.trim());
  }
  flush();
  return blocks;
}

// ---- The dependency graph (after Geyser's task graph): what is needed stands left of what
// needs it. One edge per pair of tasks, styled by its most pressing unmet link.
const GRAPH = {nodeWidth: 210, nodeHeight: 58, gapX: 72, gapY: 16, pad: 12};
function graphEdges(links, deadlocks = []) {
  const cycle = new Map(deadlocks.flatMap((group, index) => group.map(id => [id, index])));
  const pairs = new Map();
  for (const link of links || []) {
    if (link.source === link.target) continue;
    const key = link.source + '>' + link.target;
    if (!pairs.has(key)) pairs.set(key, []);
    pairs.get(key).push(link);
  }
  return [...pairs.values()].map(group => {
    const {source, target} = group[0], unmet = group.filter(link => !link.met);
    const deadlock = unmet.length > 0 && cycle.has(source) && cycle.get(source) === cycle.get(target);
    const kind = deadlock ? 'deadlock' : !unmet.length ? 'met'
      : unmet.some(link => link.kind === 'subscription') ? 'subscription'
      : unmet.some(link => link.kind === 'wait') ? 'wait' : 'requirement';
    return {source, target, links: group, kind};
  });
}
// Columns by the longest chain of needs; an edge across several columns passes an empty slot in
// each, so none runs behind a node; edges closing a cycle are drawn as loops back. `ids` give the
// order within a column when nothing else decides it.
function layeredLayout(ids, edges, size = GRAPH) {
  const rank = new Map(ids.map((id, index) => [id, id === 'operator' ? -1 : index]));
  const order = id => rank.get(id) ?? Number.MAX_SAFE_INTEGER;
  const nodes = [...ids].sort((a, b) => order(a) - order(b));
  const outgoing = new Map();
  for (const edge of edges) { if (!outgoing.has(edge.source)) outgoing.set(edge.source, []); outgoing.get(edge.source).push(edge); }
  const back = new Set(), visiting = new Map();
  for (const start of nodes) {
    if (visiting.has(start)) continue;
    // Iterative depth-first search: a long chain never exhausts the stack.
    const stack = [[start, 0]]; visiting.set(start, true);
    while (stack.length) {
      const top = stack[stack.length - 1], out = outgoing.get(top[0]) || [];
      if (top[1] >= out.length) { visiting.set(top[0], false); stack.pop(); continue; }
      const edge = out[top[1]++];
      if (visiting.get(edge.target) === true) back.add(edge);
      else if (!visiting.has(edge.target)) { visiting.set(edge.target, true); stack.push([edge.target, 0]); }
    }
  }
  const forward = edges.filter(edge => !back.has(edge));
  const column = new Map(nodes.map(id => [id, 0])), pending = new Map(nodes.map(id => [id, 0]));
  for (const edge of forward) pending.set(edge.target, pending.get(edge.target) + 1);
  const ready = nodes.filter(id => pending.get(id) === 0), forwardOut = new Map();
  for (const edge of forward) { if (!forwardOut.has(edge.source)) forwardOut.set(edge.source, []); forwardOut.get(edge.source).push(edge); }
  while (ready.length) {
    const id = ready.shift();
    for (const edge of forwardOut.get(id) || []) {
      column.set(edge.target, Math.max(column.get(edge.target), column.get(id) + 1));
      pending.set(edge.target, pending.get(edge.target) - 1);
      if (pending.get(edge.target) === 0) ready.push(edge.target);
    }
  }
  const columns = [], chains = new Map(), before = new Map();
  const place = (id, index) => { column.set(id, index); (columns[index] ||= []).push(id); };
  for (const id of nodes) place(id, column.get(id));
  for (const edge of forward) {
    const chain = [edge.source];
    for (let index = column.get(edge.source) + 1; index < column.get(edge.target); index += 1) {
      const waypoint = edge.source + '>' + edge.target + '@' + index;
      place(waypoint, index); chain.push(waypoint);
    }
    chain.push(edge.target);
    chains.set(edge, chain);
    chain.slice(1).forEach((point, index) => { if (!before.has(point)) before.set(point, []); before.get(point).push(chain[index]); });
  }
  const row = new Map();
  const meanRow = id => {
    const rows = (before.get(id) || []).map(point => row.get(point)).filter(value => value !== undefined);
    return rows.length ? rows.reduce((sum, value) => sum + value, 0) / rows.length : Number.MAX_SAFE_INTEGER;
  };
  columns.forEach((members, index) => {
    if (index > 0) members.sort((a, b) => meanRow(a) - meanRow(b) || order(a) - order(b));
    members.forEach((id, position) => row.set(id, position));
  });
  const position = new Map([...column.keys()].map(id => [id, {
    x: size.pad + column.get(id) * (size.nodeWidth + size.gapX), y: size.pad + row.get(id) * (size.nodeHeight + size.gapY)}]));
  const tallest = Math.max(1, ...columns.filter(Boolean).map(members => members.length));
  return {position, back, chains, width: size.pad * 2 + Math.max(1, columns.length) * (size.nodeWidth + size.gapX) - size.gapX,
    height: size.pad * 2 + tallest * (size.nodeHeight + size.gapY) - size.gapY};
}
// An edge's path: curves between columns, straight across each empty slot; a loop back runs from
// the left of its source to the right of its target, below the nodes' middles.
function edgeRoute(edge, layout, size = GRAPH) {
  if (layout.back.has(edge)) {
    const from = layout.position.get(edge.source), to = layout.position.get(edge.target);
    const x1 = from.x, y1 = from.y + size.nodeHeight / 2 + 14, x2 = to.x + size.nodeWidth, y2 = to.y + size.nodeHeight / 2 + 14;
    const bend = Math.max(30, (x1 - x2) / 2);
    return {d: `M${x1},${y1} C${x1 - bend},${y1} ${x2 + bend},${y2} ${x2},${y2}`, mid: {x: (x1 + x2) / 2, y: (y1 + y2) / 2 + 17}};
  }
  const points = layout.chains.get(edge).map(id => layout.position.get(id));
  let x = points[0].x + size.nodeWidth, y = points[0].y + size.nodeHeight / 2, d = `M${x},${y}`, mid = null;
  points.slice(1).forEach((point, index) => {
    const tx = point.x, ty = point.y + size.nodeHeight / 2, bend = Math.max(30, (tx - x) / 2);
    d += ` C${x + bend},${y} ${tx - bend},${ty} ${tx},${ty}`;
    mid ??= {x: (x + tx) / 2, y: (y + ty) / 2};
    x = tx; y = ty;
    if (index < points.length - 2) { x = point.x + size.nodeWidth; d += ` L${x},${y}`; }
  });
  return {d, mid};
}
// The tasks one step from `id` along these edges, and `id` itself.
function graphNeighbours(edges, id) {
  const found = new Set([id]);
  for (const edge of edges) {
    if (edge.source === id) found.add(edge.target);
    if (edge.target === id) found.add(edge.source);
  }
  return found;
}

function principalName(value, reference = '') {
  if (value?.kind === 'integrator' || reference === 'Integrator') return 'Integrator';
  if (value?.kind === 'dispatcher' || reference === 'Dispatcher') return 'Dispatcher';
  if (value?.kind === 'waker' || reference === 'Waker') return 'Waker';
  const reviewer = value?.kind === 'review';
  const pattern = value ? reviewer ? /^Reviewer [1-9]\d*$/ : /^Worker \d+$/ : /^(?:Worker \d+|Reviewer [1-9]\d*)$/;
  const recorded = value?.display_name || reference;
  if (pattern.test(recorded)) return recorded;
  const number = reviewer ? value?.reviewer_number : value?.worker_number;
  if (Number.isInteger(number) && number >= (reviewer ? 1 : 0)) return (reviewer ? 'Reviewer ' : 'Worker ') + number;
  return value ? reviewer ? 'Unnamed reviewer' : 'Unnamed worker' : 'Unregistered contributor';
}

function principalNumber(value) {
  const number = value.kind === 'review' ? value.reviewer_number : value.worker_number;
  if (Number.isInteger(number) && number >= (value.kind === 'review' ? 1 : 0)) return number;
  const recorded = principalName(value).match(/ (\d+)$/);
  return recorded ? Number(recorded[1]) : Number.MAX_SAFE_INTEGER;
}

function preserveDisclosures(target, render, scroller = target) {
  const states = new Map([...target.querySelectorAll('details')].filter(item => item.dataset.key)
    .map(item => [item.dataset.key, item.open]));
  const top = scroller.scrollTop, left = scroller.scrollLeft;
  render();
  // Restoring a lazy parent creates its descendants; restore those in turn.
  const restored = new Set();
  while (true) {
    const pending = [...target.querySelectorAll('details')].filter(item => states.has(item.dataset.key) && !restored.has(item));
    if (!pending.length) break;
    for (const item of pending) {
      restored.add(item); item.open = states.get(item.dataset.key);
      if (item.open) item.dispatchEvent(new Event('toggle'));
    }
  }
  scroller.scrollTop = top; scroller.scrollLeft = left;
}

// The states the sidebar can show, in its filter's order.
const PEOPLE_STATES = ['Working', 'Handling message', 'Finishing turn', 'Waiting', 'Ready', 'Idle', 'Sleeping',
  'Paused', 'Recovering', 'Retired'];

// The 2-second polls to skip after an overview that took `took` ms: about three times as long as
// it took, so the dashboard asks a busy service less often, and at most 20 s in all.
function pollsToSkip(took) {
  return Math.min(9, Math.max(0, Math.ceil(took * 3 / 2000) - 1));
}

// One sidebar group in the operator's order, narrowed by text, current state and backend.
function arrangeMembers(members, {query = '', state = '', backend = '', descending = false} = {}, describe = principalName) {
  const order = descending ? -1 : 1;
  return members
    .filter(item => (!state || workerStateLabel(item) === state) && (!backend || (item.backend || 'codex') === backend)
      && (!query || describe(item).toLowerCase().includes(query)))
    .sort((a, b) => order * (principalNumber(a) - principalNumber(b) || principalName(a).localeCompare(principalName(b))));
}

const AGENT_ROWS = [['worker', 'codex'], ['worker', 'claude'], ['review', 'codex'], ['integrator', 'codex']];
const CLAUDE_EFFORTS = ['low', 'medium', 'high', 'xhigh', 'max'];

// The page's editable copy of the choices for new conversations and the session limits.
function agentDraft(status) {
  const models = {};
  for (const [role, backend] of AGENT_ROWS) {
    const saved = status?.role_models?.[role]?.[backend] || {};
    // Unticked unless the operator ticks it: a change reaches only workers created afterwards.
    models[role + '_' + backend] = {model: saved.model || '', effort: saved.effort || '', existing: false};
  }
  return {nextBackend: status?.next_worker?.backend || status?.new_worker_backend || 'codex', models,
    dispatcher: {model: status?.decision_model?.model || '', effort: status?.decision_model?.effort || ''},
    waker: {model: status?.waker_model?.model || '', effort: status?.waker_model?.effort || ''},
    deadline: String(status?.answer_deadline?.minutes ?? ''),
    // No limit (null) reads as -1; 0 starts no worker session.
    sessions: Object.fromEntries(['codex', 'claude'].map(backend => [backend, String(status?.sessions?.[backend]?.limit ?? -1)])),
    idle: Object.fromEntries(['codex', 'claude'].map(backend => [backend,
      String(status?.rotation?.idle_by_backend?.[backend]?.minutes ?? '')])),
    batch: {delay: String(status?.integrator_batch?.delay_minutes ?? ''),
      threshold: String(status?.integrator_batch?.threshold ?? '')}};
}

// A reset time in the viewer's own clock.
function momentLabel(ms) {
  return new Date(ms).toLocaleString([], {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'});
}

// How long until a time: "2 h 15 min", "45 min", "3 d 4 h"; "now" once it has passed.
function durationLabel(ms) {
  const minutes = Math.ceil(ms / 60000);
  if (minutes <= 0) return 'now';
  if (minutes < 60) return minutes + ' min';
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return hours + ' h' + (minutes % 60 ? ' ' + (minutes % 60) + ' min' : '');
  return Math.floor(hours / 24) + ' d' + (hours % 24 ? ' ' + (hours % 24) + ' h' : '');
}

// A reset time with how long until it.
function resetLabel(ms, now) {
  return momentLabel(ms) + ' (in ' + durationLabel(ms - now) + ')';
}

// A usage window as Claude Code names it (five_hour, seven_day, seven_day_opus, ...), or as
// TaskHub names Codex's (5h, weekly, or by its length).
function windowLabel(window) {
  return {five_hour: '5-hour', seven_day: 'Weekly', seven_day_opus: 'Weekly (Opus)', seven_day_sonnet: 'Weekly (Sonnet)',
    overage: 'Extra usage', '5h': '5-hour', weekly: 'Weekly'}[window] || window;
}

// A profile tab's profiles (Claude's or Codex's): whether each is in the rotation, the one in use,
// those at a limit and until when, those that cannot be used, and each usage window last reported.
// Selecting the one in use is offered only to clear its limit or to put it back in the rotation.
function profileRows(view, now = Date.now()) {
  return (view?.profiles || []).map(profile => {
    const limited = Boolean(profile.limited_until), rotation = profile.in_rotation !== false;
    const kind = {'5h': '5-hour', weekly: 'weekly', overage: 'plan'}[profile.limit_type] || 'usage';
    const status = (rotation ? '' : 'Not in the rotation · ') + (profile.problem ? 'Cannot be used: ' + profile.problem
      : profile.signed_in === false ? 'Not signed in'
      : limited ? 'At its ' + kind + ' limit until ' + resetLabel(profile.limited_until, now) + (profile.selected ? ' · in use' : '')
      : profile.selected ? 'In use' : 'Available');
    const usage = (profile.usage || []).map(item => windowLabel(item.window) + ': '
      + ({allowed_warning: 'approaching its limit, ', rejected: 'at its limit, '}[item.status] || '')
      + (item.utilization !== null && item.utilization !== undefined ? Math.round(item.utilization * 100) + '% used' : 'usage not reported')
      + (item.resets_at ? ' · resets ' + resetLabel(item.resets_at, now) : '')
      + ' · reported ' + momentLabel(item.observed_at));
    return {name: profile.name, home: profile.home, status, usage, selected: Boolean(profile.selected), rotation,
      priority: profile.priority,
      last_limited: profile.last_limited_at ? 'Last at a limit ' + momentLabel(profile.last_limited_at) : '',
      attention: Boolean(rotation && (profile.problem || profile.signed_in === false || limited)),
      usable: !profile.problem && profile.signed_in !== false && !(profile.selected && !limited && rotation)};
  });
}

// A profile tab's recent limits and switches, newest first.
function profileHistoryRows(view, label = 'Claude') {
  const kinds = {'5h': '5-hour', weekly: 'weekly', overage: 'plan'};
  const reasons = {limit: 'the previous one reached its limit', unusable: 'the previous one could not be used',
    operator: 'selected by you', excluded: 'you took the previous one out of the rotation',
    priority: 'it is ahead in the priority'};
  return (view?.history || []).map(item => ({when: momentLabel(item.at), text:
    item.kind === 'limited' ? item.profile + ' reached its ' + (kinds[item.limit_type] || 'usage') + ' limit; resets '
      + momentLabel(item.until)
    : item.kind === 'all_limited' ? 'Every profile at its limit; ' + label + ' waits until ' + momentLabel(item.until)
    : item.kind === 'excluded' ? item.profile + ' taken out of the rotation'
    : item.kind === 'included' ? item.profile + ' put in the rotation'
    : item.kind === 'priority' ? 'Priority set: ' + (item.reason || '').split(',').join(' → ')
    : 'Switched to ' + item.profile + (item.previous && item.previous !== item.profile ? ' from ' + item.previous : '')
      + (reasons[item.reason] ? ': ' + reasons[item.reason] : '')}));
}

// A header badge: the profile in use, until when every profile is at its limit, or that none in
// the rotation can be used.
function profileBadge(summary, label = 'Claude') {
  if (!summary?.selected) return null;
  if (summary.held_until) return {text: label + ': every profile at its limit until ' + momentLabel(summary.held_until), held: true};
  if (summary.held?.endsWith('ProfilesUnavailable')) return {text: label + ': no usable profile in the rotation', held: true};
  return {text: label + ': ' + summary.selected, held: false};
}

// The commands that save what changed; an invalid session limit is an error, not a command.
function agentSettingCommands(status, draft) {
  const commands = [], saved = agentDraft(status);
  if (draft.nextBackend !== saved.nextBackend) commands.push(['runtime.select_backend', {backend: draft.nextBackend}]);
  for (const [role, backend] of AGENT_ROWS) {
    const key = role + '_' + backend, value = draft.models[key], before = saved.models[key];
    const model = value.model.trim(), kept = status?.role_models?.[role]?.[backend]?.kept_by_existing || 0;
    // Ticked alone, it gives the current choice to existing workers that kept earlier ones.
    if (model !== before.model || value.effort !== before.effort || (value.existing && kept)) {
      commands.push(['runtime.set_role_model', {role, backend, model: model || null, effort: value.effort || null,
        apply_to_existing: Boolean(value.existing)}]);
    }
  }
  if (draft.dispatcher.model !== saved.dispatcher.model || draft.dispatcher.effort !== saved.dispatcher.effort) {
    commands.push(['runtime.set_decision_model', {model: draft.dispatcher.model, effort: draft.dispatcher.effort}]);
  }
  if (draft.waker.model !== saved.waker.model || draft.waker.effort !== saved.waker.effort) {
    if (!draft.waker.model || !draft.waker.effort) throw Error("Choose both the Waker's model and its effort.");
    commands.push(['runtime.set_waker_model', {model: draft.waker.model, effort: draft.waker.effort}]);
  }
  const deadline = String(draft.deadline).trim();
  if (deadline !== saved.deadline) {
    if (!/^\d+$/.test(deadline) || Number(deadline) < 1 || Number(deadline) > 43200) {
      throw Error('The answer deadline must be a whole number of minutes from 1 to 43200.');
    }
    commands.push(['runtime.set_answer_deadline', {minutes: Number(deadline)}]);
  }
  for (const backend of ['codex', 'claude']) {
    const text = String(draft.sessions[backend]).trim();
    if (text === saved.sessions[backend]) continue;
    const sessions = Number(text);
    if (!/^(-1|\d+)$/.test(text) || sessions > 1024) throw Error('Sessions at once must be a whole number from 0 to 1024, or -1 for no limit.');
    commands.push(['runtime.set_concurrency', {backend, sessions}]);
  }
  for (const backend of ['codex', 'claude']) {
    const text = String(draft.idle[backend]).trim();
    if (text === saved.idle[backend]) continue;
    const minutes = Number(text);
    if (!/^\d+$/.test(text) || minutes > 10080) throw Error('Idle rotation must be a whole number of minutes from 0 to 10080.');
    commands.push(['runtime.set_rotation_idle', {backend, minutes}]);
  }
  const delay = String(draft.batch.delay).trim(), threshold = String(draft.batch.threshold).trim();
  if (delay !== saved.batch.delay || threshold !== saved.batch.threshold) {
    // An emptied field returns to the default.
    if (delay && (!/^\d+$/.test(delay) || Number(delay) > 10080)) {
      throw Error("The Integrator's wait must be a whole number of minutes from 0 to 10080.");
    }
    if (threshold && (!/^\d+$/.test(threshold) || Number(threshold) < 1 || Number(threshold) > 1000)) {
      throw Error("The Integrator's batch size must be a whole number from 1 to 1000.");
    }
    commands.push(['runtime.set_integrator_batch', {delay_minutes: delay ? Number(delay) : null,
      threshold: threshold ? Number(threshold) : null}]);
  }
  return commands;
}

// A task's place in admission (task.set_priority, task.set_pinned), for an unfinished task
// that is not a review; '' otherwise. A pin is shown apart, as a badge.
function admissionLabel(task) {
  const admission = task?.admission;
  if (!admission || task.contract?.kind === 'review' || ['done', 'retired'].includes(task.state)) return '';
  return 'Priority ' + admission.priority + (admission.operator_priority === null ? ''
    : ' · set by the operator (contract ' + admission.contract_priority + ')');
}

if (typeof module !== 'undefined' && module.exports) module.exports = {
  WorkerHistoryStore, TaskBrowserStore, ProjectSetupStore, correspondencePresentation, requestResolutionPayload,
  workerStateLabel, workerTaskPrefix, principalName, principalNumber, preserveDisclosures,
  selectedRun, projectStatus, personControl, controlConsole, CONTROL_GROUPS, elapsedLabel, workerActivity, actionSummary, taskOutlineState, outlineProgress,
  turnStream, arrangeMembers, agentDraft, agentSettingCommands, AGENT_ROWS, PEOPLE_STATES, profileRows, profileBadge,
  profileHistoryRows, durationLabel, pollsToSkip, admissionLabel, taskExecutors, reviewSummary, publicationTree,
  treeAncestors, markdownInline, markdownBlocks, GRAPH, graphEdges, layeredLayout, edgeRoute, graphNeighbours
};

if (typeof document !== 'undefined') {
  const el = id => document.getElementById(id);
  let selected = null, data = null, refreshing = false;
  // Retired workers are left out of each poll (in the Linux pilot 3,424 of 4,109 workers, half of a
  // 7 MB overview) and read apart when the overview's retired_count changes, so links and names
  // still find them. Every worker is indexed by ID and name.
  const retired = {count: null, records: [], load: null};
  let workerIndex = new Map();
  let workerSignature = '', sessionSignature = '', historySignature = '', pinnedSignature = '';
  let historyViewKey = '', taskSignature = '', taskDetailSignature = '', taskFilterTimer = null;
  let taskMode = 'outline', outline = null, outlineLoad = null, outlineError = '', outlineSignature = '';
  let drawerOffered = false;
  const scrollPositions = new Map(), expanded = new Map(), requestViews = new Map(), pinnedViews = new Map();
  // Per person and session: the turn shown, or '' to follow the latest turn.
  const turnChoices = new Map();
  // Unsent request replies, shared by the Requests dialog and the person's page.
  const requestDrafts = new Map();
  const queueHistory = {publications: [], loaded: false, key: null, inflight: null, error: ''};
  const notices = {interaction: '', health: ''};
  const libraryStats = {attemptedAt: null, inflight: null, value: null, error: ''};
  const leanClock = {status: null, at: 0};
  const store = new WorkerHistoryStore(api, changed);
  const taskStore = new TaskBrowserStore(api, taskChanged);
  const setupStore = new ProjectSetupStore(api, renderSetup);
  const cause = {initial: 'Start task', continuation: 'Continue task', review: 'Review artifact',
    message: 'Incoming message', steer: 'Message during turn', assignment: 'Assign task',
    recruitment: 'Decide collaboration', input: 'Prompt'};
  const statusLabels = {inProgress: 'In progress', in_progress: 'In progress', completed: 'Completed', interrupted: 'Interrupted',
    failed: 'Failed', prepared: 'Prepared', uncertain: 'Uncertain', delivered: 'Delivered',
    not_delivered: 'Not delivered', streaming: 'Streaming', pending: 'Pending', unknown: 'Status not recorded'};

  function node(tag, text, cls) {
    const value = document.createElement(tag);
    if (text !== undefined) value.textContent = String(text);
    if (cls) value.className = cls;
    return value;
  }
  function notice(text, channel = 'interaction') {
    notices[channel] = text || '';
    const message = Object.values(notices).filter(Boolean).join(' · ');
    el('notice').textContent = message;
    document.body.classList.toggle('has-notice', Boolean(message));
  }
  function time(value) {
    if (value === null || value === undefined || value === '') return '';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '' : date.toLocaleString();
  }
  function label(value) { return statusLabels[value] || String(value || 'Status not recorded').replaceAll('_', ' '); }
  function worker(id) { return workerIndex.get(id); }
  // The overview's workers, then the retired ones read apart; the first of each ID or name wins.
  function indexWorkers() {
    workerIndex = new Map();
    for (const item of (data?.workers || []).concat(retired.records)) {
      if (!workerIndex.has(item.id)) workerIndex.set(item.id, item);
      if (item.display_name && !workerIndex.has(item.display_name)) workerIndex.set(item.display_name, item);
    }
  }
  async function loadRetired(count) {
    if (retired.load) return;
    retired.load = api('/v1/retired-workers');
    try {
      const value = await retired.load;
      const known = new Map(retired.records.map(item => [item.id, item]));
      retired.records = (value.workers || []).map(([id, displayName, kind, backend]) => known.get(id)
        || {id, display_name: displayName, kind, backend, state: 'retired', work_intention: 'retired', summary: true});
      retired.count = count;
      indexWorkers();
      if (data) renderWorkers();
    } catch { /* Their names show as IDs until a later poll reads them again. */ }
    finally { retired.load = null; }
  }
  // A retired person opened from the list: its thread, creation time and controls.
  async function loadRetiredDetails(id) {
    try {
      const {profile_json: _profile, ...details} = await api('/v1/workers/' + encodeURIComponent(id));
      const record = retired.records.find(item => item.id === id);
      if (!record) return;
      Object.assign(record, details, {summary: false});
      if (selected === id) renderWorkerHead();
    } catch { /* The head keeps the name, kind and backend. */ }
  }
  function name(id) { return principalName(worker(id), id); }
  function taskTitle(id) {
    return taskStore.titles.get(id) || 'Task details unavailable';
  }
  function pendingKinds(id) {
    return (data?.requests || []).filter(r => r.state === 'pending' && (r.workers || []).includes(id)).map(r => r.kind);
  }
  function stateLabel(value) {
    const task = data?.tasks.find(item => item.id === value.current_task_id);
    return workerActivity(value, {asking: pendingKinds(value.id), awaitingReview: Boolean(task?.reviews_pending)});
  }
  // "Seen" is this browser's convenience only: viewing never acknowledges anything on the server.
  const seenMemory = new Map();
  let seenVersion = 0;
  function seenSequence(id) {
    let stored = 0;
    try { stored = Number(localStorage.getItem('taskhub-seen:' + id)) || 0; } catch { stored = 0; }
    return Math.max(stored, seenMemory.get(id) || 0);
  }
  function markChatSeen() {
    if (!selected) return;
    const sequences = [...store.worker(selected).chats.values()].filter(m => m.direction !== 'to_worker').map(m => m.sequence);
    const latest = Math.max(0, ...sequences, data?.messages_to_you?.[selected] || 0);
    if (latest <= seenSequence(selected)) return;
    seenMemory.set(selected, latest);
    try { localStorage.setItem('taskhub-seen:' + selected, String(latest)); } catch { /* memory fallback */ }
    seenVersion += 1;
    renderWorkers();
  }
  function empty(target, title, text) {
    const container = node('div', undefined, 'empty');
    container.append(node('h2', title));
    if (text) container.append(node('p', text));
    target.replaceChildren(container);
  }
  const responseTags = new Map();
  // A conditional read resolves to null when the service answers 304 (unchanged).
  async function api(path, body, {conditional = false} = {}) {
    const headers = body === undefined ? {} : {'Content-Type': 'application/json'};
    if (conditional && responseTags.has(path)) headers['If-None-Match'] = responseTags.get(path);
    const response = await fetch(path, {method: body === undefined ? 'GET' : 'POST', cache: 'no-store', headers,
      ...(body === undefined ? {} : {body: JSON.stringify(body)})});
    if (conditional && response.status === 304) return null;
    let value;
    try { value = await response.json(); } catch { throw Error('The service returned an unreadable response.'); }
    if (!response.ok) {
      const error = Error(value.error?.message || value.detail || ('Request failed: ' + response.status));
      error.status = response.status; error.code = value.error?.code;
      throw error;
    }
    if (conditional) {
      const tag = response.headers?.get?.('ETag');
      if (tag) responseTags.set(path, tag); else responseTags.delete(path);
    }
    return value;
  }
  function command(commandName, payload, key = crypto.randomUUID()) { return api('/v1/commands', {name: commandName, payload, key}); }
  function changed(id, area) {
    if (id !== selected) return;
    if (area === 'sessions') renderSessions();
    if (['history', 'loading', 'sessions'].includes(area)) renderHistory();
    if (area === 'chat') { markChatSeen(); renderQueued(); renderHistory(); }
    if (area === 'compose') renderComposer();
  }
  function workerButton(id, text) {
    const button = node('button', text || name(id), 'link');
    button.disabled = !worker(id);
    button.onclick = () => {
      for (const dialog of document.querySelectorAll('dialog[open]')) dialog.close();
      selectWorker(worker(id).id);
    };
    return button;
  }
  function taskButton(id, text) {
    const button = node('button', text || taskTitle(id), 'link');
    button.onclick = () => showTask(id).catch(error => notice(error.message));
    return button;
  }

  function renderWorkers() {
    const query = el('worker-filter').value.trim().toLowerCase();
    renderPeopleTools();
    const signature = selected + query + JSON.stringify(peopleView) + JSON.stringify(data.workers)
      + ':' + retired.records.length + JSON.stringify(data.tasks.map(t => [t.id, t.contract?.title, t.reviews_pending]))
      + JSON.stringify(data.requests.filter(r => r.state === 'pending').map(r => [r.id, r.kind, r.workers]))
      + JSON.stringify(data.messages_to_you || {}) + ':' + seenVersion + ':' + Math.floor(Date.now() / 60000);
    if (signature === workerSignature) return;
    workerSignature = signature;
    // Keep each native disclosure in place so polling, filtering, and selection
    // cannot change either group's independent open/closed state.
    for (const [kind, prefix] of [['ordinary', 'worker'], ['review', 'reviewer'], ['integrator', 'integrator']]) {
      const list = el(prefix + '-list');
      list.replaceChildren();
      // The Dispatcher and the Waker, which own assignment and wake decisions, are listed with the Integrator.
      const kinds = kind === 'integrator' ? ['integrator', 'dispatcher', 'waker'] : [kind];
      // Retired people are listed only when that state is chosen: thousands of reviewers retire.
      const pool = peopleView.state === 'Retired' ? [...new Set(workerIndex.values())] : data.workers;
      const members = pool.filter(item => kinds.includes(item.kind || 'ordinary')
        && (peopleView.state === 'Retired' || item.state !== 'retired'));
      const visible = arrangeMembers(members, {query, ...peopleView}, item => name(item.id) + ' ' + stateLabel(item) + ' ' +
        (item.current_task_id ? taskTitle(item.current_task_id) : ''));
      const narrowed = query || peopleView.state || peopleView.backend;
      el(prefix + '-count').textContent = '(' + (narrowed ? visible.length + '/' : '') + members.length + ')';
      for (const item of visible) {
        const button = node('button', undefined, 'worker' + (item.id === selected ? ' selected' : ''));
        const asking = pendingKinds(item.id), unread = (data.messages_to_you?.[item.id] || 0) > seenSequence(item.id);
        const attention = asking.length ? (asking.includes('question') ? 'Asked you a question' : 'Needs a source from you')
          : unread ? 'New message for you' : '';
        button.setAttribute('aria-pressed', String(item.id === selected));
        button.setAttribute('aria-label', name(item.id) + ', ' + stateLabel(item) + (attention ? ', ' + attention : ''));
        button.dataset.worker = item.id;
        const live = item.active_attempt && item.state !== 'recovery_required';
        const dot = node('span', undefined, 'worker-state' + (live ? ' active' : item.runtime_stop_reason || item.state === 'recovery_required' || attention ? ' alert' : ''));
        button.append(dot, node('strong', name(item.id)), node('span', stateLabel(item), 'muted'));
        if (attention) button.append(node('span', attention, 'attention-line'));
        if (item.run_control) button.append(node('span', item.run_control === 'paused' ? 'Paused by you' : 'Resumed by you', 'control-line'));
        if (item.current_task_id) button.append(node('span', taskTitle(item.current_task_id), 'muted'));
        else if (item.wait_reason) button.append(node('span', item.wait_reason, 'muted'));
        button.onclick = () => selectWorker(item.id);
        list.append(button);
      }
      if (!visible.length) {
        const filtered = members.length && (peopleView.state || peopleView.backend);
        const empty = node('p', !members.length ? 'No ' + prefix + 's yet'
          : filtered ? 'Hidden by the state or backend filter. ' : 'No matches', 'group-empty muted');
        if (filtered) {
          const clear = node('button', 'Show everyone', 'link');
          clear.type = 'button';
          clear.onclick = () => changePeopleView({state: '', backend: ''});
          empty.append(clear);
        }
        list.append(empty);
      }
    }
    const waiting = data.workers.filter(item => pendingKinds(item.id).length
      || (data.messages_to_you?.[item.id] || 0) > seenSequence(item.id)).length;
    el('people-toggle').textContent = 'People' + (waiting ? ' · ' + waiting : '');
    el('people-toggle').classList.toggle('attention', waiting > 0);
  }

  // The sidebar's order and filters are this browser's convenience.
  const peopleView = {descending: false, state: '', backend: ''};
  try {
    const stored = JSON.parse(localStorage.getItem('taskhub-people-view') || '{}');
    peopleView.descending = stored.descending === true;
    if (PEOPLE_STATES.includes(stored.state)) peopleView.state = stored.state;
    if (['codex', 'claude'].includes(stored.backend)) peopleView.backend = stored.backend;
  } catch { /* defaults */ }
  function changePeopleView(change) {
    Object.assign(peopleView, change);
    try { localStorage.setItem('taskhub-people-view', JSON.stringify(peopleView)); } catch { /* this page only */ }
    if (data) renderWorkers();
  }
  function renderPeopleTools() {
    // Only states someone is in, and the chosen one, so the list never jumps.
    const present = new Set(data.workers.map(workerStateLabel));
    if (retired.records.length) present.add('Retired');
    const states = PEOPLE_STATES.filter(state => present.has(state) || state === peopleView.state);
    const filter = el('worker-state-filter');
    if (filter.dataset.states !== states.join('|')) {
      filter.dataset.states = states.join('|');
      const all = node('option', 'Any state'); all.value = '';
      filter.replaceChildren(all, ...states.map(state => { const option = node('option', state); option.value = state; return option; }));
    }
    filter.value = peopleView.state;
    el('worker-backend-filter').value = peopleView.backend;
    const sort = el('worker-sort');
    sort.textContent = peopleView.descending ? '9→1' : '1→9';
    sort.setAttribute('aria-pressed', String(peopleView.descending));
    sort.title = peopleView.descending ? 'Highest number first. Select to show the lowest first.'
      : 'Lowest number first. Select to show the highest first.';
  }

  function backendName(backend) { return backend === 'claude' ? 'Claude' : 'Codex'; }
  // A worker's backend applies to its conversations from the next new one on.
  function renderBackendControl(current) {
    const line = el('worker-backend'), button = el('worker-backend-button');
    const now = current.backend || 'codex', requested = current.requested_backend, own = current.session_settings;
    line.classList.toggle('hidden', current.kind === 'dispatcher' || current.kind === 'waker');
    el('worker-backend-text').textContent = 'Backend: ' + backendName(now)
      + (own ? ' · ' + (own.model || 'default model') + ' · ' + own.effort + ' effort' : '')
      + (requested ? ' · moves to ' + backendName(requested) + ' at its next turn' : '');
    const switchable = (current.kind || 'ordinary') === 'ordinary' && current.work_intention !== 'retired' && current.state !== 'retired';
    button.classList.toggle('hidden', !switchable);
    if (!switchable) return;
    const target = requested ? now : now === 'claude' ? 'codex' : 'claude';
    button.textContent = requested ? 'Stay on ' + backendName(now) : 'Switch to ' + backendName(target);
    button.title = requested ? 'Cancels the requested switch.'
      : 'Its next turn continues its work in a new conversation on ' + backendName(target)
        + ', once that account has a free session; it takes no further turn on ' + backendName(now) + '.';
    button.onclick = async () => {
      button.disabled = true;
      try { await command('worker.select_backend', {worker_id: current.id, backend: target}); await refresh(); }
      catch (error) { notice(error.message); }
      finally { button.disabled = false; }
    };
  }

  function renderWorkerHead() {
    const current = worker(selected);
    if (!current) return;
    el('conversation-title').textContent = name(selected);
    el('history-heading').textContent = current.kind === 'integrator' ? 'INTEGRATOR HISTORY' : current.kind === 'dispatcher' ? 'DISPATCHER HISTORY' : current.kind === 'waker' ? 'WAKER HISTORY' : current.kind === 'review' ? 'REVIEWER HISTORY' : 'WORKER HISTORY';
    el('worker-kind').textContent = current.kind === 'integrator' ? 'Integrator' : current.kind === 'dispatcher' ? 'Dispatcher' : current.kind === 'waker' ? 'Waker' : current.kind === 'review' ? 'Reviewer' : 'Worker';
    el('worker-status').textContent = stateLabel(current);
    el('worker-kind').classList.remove('hidden');
    el('worker-status').classList.remove('hidden');
    const taskLine = el('worker-task');
    taskLine.replaceChildren();
    if (current.current_task_id) taskLine.append(workerTaskPrefix(current, data.tasks), taskButton(current.current_task_id));
    else {
      const memberships = data.tasks.filter(t => !['done', 'retired', 'cancelled'].includes(t.state) &&
        (t.collaboration?.members || []).some(m => (m.worker_id === selected || m.worker_id === current.display_name)
          && m.executor && (!m.membership_state || m.membership_state === 'member')));
      if (memberships.length) {
        taskLine.append('Retained tasks: ');
        memberships.forEach((task, index) => { if (index) taskLine.append(' · '); taskLine.append(taskButton(task.id)); });
      } else taskLine.textContent = current.work_intention === 'retired' ? name(selected) + ' has finished its role. Its history remains available.' : 'No current execution task.';
    }
    const reason = current.runtime_stop_reason || current.wait_reason;
    el('worker-reason').textContent = reason ? 'Reason: ' + reason : '';
    el('worker-technical').classList.remove('hidden');
    el('worker-ids').textContent = 'Internal ' + (current.kind === 'review' ? 'reviewer' : 'worker') + ' ID: ' + current.id + '\nNative thread ID: ' + (current.thread_id || 'Not started') + '\nCreated: ' + time(current.created_at)
      + '\nBackend: ' + (current.backend === 'claude' ? 'Claude' : 'Codex')
      + (current.kind === 'dispatcher' || current.kind === 'waker' ? '' : '\nConversation rotation: ' + (current.rotation_mode ? current.rotation_mode + ' (set for this person)' : 'project mode'));
    renderPersonControl(current);
    renderBackendControl(current);
    renderComposer(); renderPinnedRequests();
  }
  // Pause or resume this person alone. While everyone is paused, a resumed
  // person continues only its own current work; Pause all / Resume all end this.
  function renderPersonControl(current) {
    // A service that predates individual controls does not report them.
    const run = selectedRun(data.runs), control = 'run_control' in current ? personControl(run, current) : null;
    const badge = el('worker-control'), button = el('worker-control-button'), later = el('worker-control-after-turn');
    badge.classList.toggle('hidden', !(control?.individual || control?.group || control?.finishing));
    badge.textContent = control?.finishing ? 'Finishing its turn, then paused'
      : control?.individual === 'paused' ? (control.afterTurn ? 'Paused by you after its turn' : 'Paused by you')
      : control?.individual === 'resumed' ? 'Resumed by you'
      : control?.group === 'paused' ? 'Paused with its group (Controls)' : control?.group === 'resumed' ? 'Resumed with its group (Controls)' : '';
    button.classList.toggle('hidden', !control);
    // A running person may pause after its turn; one finishing its turn may still be stopped now.
    later.classList.toggle('hidden', !(control?.running || control?.finishing));
    if (!control) return;
    const who = name(current.id);
    button.textContent = control.running ? 'Pause ' + who + ' now' : 'Resume ' + who;
    button.title = control.running ? 'Interrupts its turn in progress; everyone else continues.'
      : run.state === 'paused' ? 'It continues only its own current work while everyone else stays paused.' : 'It continues with the run.';
    later.textContent = control.running ? 'Pause ' + who + ' after turn' : 'Pause ' + who + ' now';
    later.title = control.running ? 'Its turn in progress finishes on its own; nothing new starts after it.'
      : 'Interrupts the turn it is finishing.';
    const apply = async (target, name, drive) => {
      target.disabled = true;
      try {
        await command(name, {worker_id: current.id, run_id: run.id});
        if (drive) await api('/v1/runs/' + encodeURIComponent(run.id) + '/drive', {});
        el('status-bar').dataset.actions = '';
        await refresh();
      } catch (error) { notice(error.message); }
      finally { target.disabled = false; }
    };
    button.onclick = async () => {
      const live = current.active_attempt && current.state !== 'recovery_required';
      if (control.running && live && !confirm('Pause ' + who + ' now? Its turn in progress is interrupted; everyone else continues.')) return;
      await apply(button, control.running ? 'worker.pause' : 'worker.resume', !control.running);
    };
    later.onclick = async () => {
      if (!control.running && !confirm('Pause ' + who + ' now? The turn it is finishing is interrupted.')) return;
      await apply(later, control.running ? 'worker.pause_after_turn' : 'worker.pause', false);
    };
  }

  function rememberView() {
    if (!historyViewKey) return;
    const pane = el('transcript');
    if (!pane.querySelector('.turn')) return;
    scrollPositions.set(historyViewKey, {top: pane.scrollTop, bottom: pane.scrollHeight - pane.scrollTop - pane.clientHeight < 90});
    expanded.set(historyViewKey, new Set([...pane.querySelectorAll('details[open]')].map(d => d.dataset.key)));
  }
  function sessionName(session, index) {
    const kind = {main: 'Main conversation', assignment: 'Assignment decision', recruitment: 'Collaboration decision', helper: 'Decision'}[session.kind] || 'Session';
    return kind + ' · Session ' + (index + 1);
  }
  function renderSessions() {
    if (!selected) return;
    const current = store.worker(selected);
    const sessions = [...current.sessions].sort((a, b) => String(a.started_at).localeCompare(String(b.started_at)) || a.key.localeCompare(b.key));
    const signature = selected + current.selectedSession + JSON.stringify(sessions);
    if (signature === sessionSignature) return;
    sessionSignature = signature;
    const select = el('session-select');
    select.replaceChildren();
    sessions.forEach((session, index) => {
      const option = node('option', sessionName(session, index) + ' · ' + session.turn_count + ' turn' + (session.turn_count === 1 ? '' : 's'));
      option.value = session.key;
      option.selected = session.key === current.selectedSession;
      select.append(option);
    });
    if (!sessions.length) select.append(node('option', current.sessionError ? 'History unavailable' : 'No captured sessions yet'));
    select.disabled = !sessions.length;
  }

  // Render structured prompts without escaped newlines, while retaining an exact-input view.
  function structured(value, key, depth = 0) {
    if (value === null || typeof value !== 'object') return node('pre', typeof value === 'string' ? value : JSON.stringify(value));
    const root = node('div', undefined, 'structured');
    for (const [field, item] of Object.entries(value)) {
      const path = key + '/' + field;
      if (item !== null && typeof item === 'object') {
        const group = node('details');
        group.dataset.key = path;
        const title = Array.isArray(value) ? 'Item ' + (Number(field) + 1) : field.replaceAll('_', ' ');
        group.append(node('summary', title + ' · ' + (Array.isArray(item) ? item.length + ' items' : Object.keys(item).length + ' fields')));
        let loaded = false;
        const fill = () => { if (!loaded) { loaded = true; group.append(structured(item, path, depth + 1)); } };
        if (depth < 3 && (['task', 'contract', 'human_messages', 'messages', 'private_talk'].includes(field) || (Array.isArray(value) && value.length <= 5))) { fill(); group.open = true; }
        group.addEventListener('toggle', () => { if (group.open) fill(); });
        root.append(group);
      } else {
        const line = node('div', undefined, 'field');
        line.append(node('span', field.replaceAll('_', ' '), 'field-label'));
        if (String(item).length < 90 && !String(item).includes('\n') && !['text', 'objective', 'instructions', 'message'].includes(field)) {
          line.style.display = 'flex'; line.style.gap = '10px';
          line.append(node('span', item === null ? 'null' : String(item), 'small'));
        } else line.append(structured(item, path, depth + 1));
        root.append(line);
      }
    }
    return root;
  }
  function disclosure(title, value, key) {
    const group = node('details');
    group.dataset.key = key;
    group.append(node('summary', title));
    let loaded = false;
    group.addEventListener('toggle', () => {
      if (group.open && !loaded) { loaded = true; group.append(structured(value, key)); }
    });
    return group;
  }
  function entry(title, text, cls, timestamp) {
    const article = node('article', undefined, 'entry ' + cls);
    const head = node('div', undefined, 'entry-heading');
    head.append(node('strong', title));
    if (timestamp) head.append(node('span', time(timestamp), 'right'));
    article.append(head);
    if (text !== undefined) article.append(node('pre', text));
    return article;
  }
  // The model's input is long and technical; it is collapsed and rendered only when opened.
  function inputPanel(input) {
    const panel = node('details', undefined, 'input-panel');
    panel.dataset.key = 'input-panel:' + input.input_id;
    const delivery = input.status === 'uncertain' ? 'delivery uncertain' : label(input.status).toLowerCase();
    panel.append(node('summary', 'Input to the model · ' + (cause[input.kind] || input.kind || 'Input') + ' · ' + delivery
      + (input.at ? ' · ' + time(input.at) : '')));
    let loaded = false;
    panel.addEventListener('toggle', () => {
      if (!panel.open || loaded) return;
      loaded = true;
      let parsed;
      try { parsed = JSON.parse(input.text); } catch { parsed = input.text; }
      const raw = node('details', undefined, 'exact-input');
      raw.dataset.key = 'exact:' + input.input_id;
      raw.append(node('summary', 'Exact input text'), node('pre', input.text));
      panel.append(structured(parsed, 'input:' + input.input_id), raw);
    });
    return panel;
  }
  function contextPanel(history) {
    if (!history.context.length) return null;
    const group = node('details', undefined, 'context');
    group.dataset.key = 'session-context';
    group.append(node('summary', 'Session context · instructions'));
    for (const item of history.context) {
      if (item.type === 'instructions') {
        group.append(disclosure('Standing instructions · ' + time(item.at), item.instructions, 'instructions:' + item.id));
      }
    }
    return group;
  }
  function actionRow(action) {
    const row = node('div', undefined, 'action');
    const operation = action.operation || action.arguments?.operation;
    const actionName = action.tool === 'taskhub_call' && operation ? 'TaskHub · ' + operation : action.tool || action.type || 'Tool';
    row.append(node('strong', actionName), node('span', ' · ' + label(action.status), 'muted'));
    if (action.summary) row.append(node('pre', action.summary));
    if (typeof action.details?.command === 'string') row.append(node('pre', action.details.command));
    if (action.arguments !== undefined) row.append(disclosure('Arguments', action.arguments, 'arguments:' + action.key));
    if (action.result !== undefined) row.append(disclosure('Result', action.result, 'result:' + action.key));
    if (action.details !== undefined) row.append(disclosure('Details', action.details, 'details:' + action.key));
    return row;
  }
  // The person's own words sit on the left; everything it received sits on the right.
  function bubble(side, cls, parts, timestamp) {
    const article = node('article', undefined, 'bubble ' + side + (cls ? ' ' + cls : ''));
    const head = node('div', undefined, 'bubble-head');
    for (const part of parts) if (part) head.append(typeof part === 'string' ? node('strong', part) : part);
    if (timestamp) head.append(node('span', time(timestamp), 'muted bubble-time'));
    article.append(head);
    return article;
  }
  function excerpt(text, limit) {
    const value = String(text || '').trim();
    return value.length > limit ? value.slice(0, limit).trimEnd() + '…' : value;
  }
  // The model's input is long and technical: show its gist; the exact input stays folded.
  function promptGist(packet, input) {
    if (!packet) return excerpt(input.text, 400);
    const lines = [];
    const instruction = packet.context_refresh_instruction || packet.instruction;
    if (typeof instruction === 'string') lines.push(excerpt(instruction, 400));
    const next = packet.checkpoint?.next_action;
    if (typeof next === 'string') lines.push('Next: ' + excerpt(next, 300));
    return lines.join('\n');
  }
  function senderName(item) {
    if (item.senderRole === 'operator') return 'You';
    if (!item.sender) return 'Unknown sender';
    return worker(item.sender) ? name(item.sender) : item.sender;
  }
  function messageText(path) {
    if (!path || !selected) return null;
    for (const message of store.worker(selected).chats.values()) if (message.path === path) return message.text;
    return null;
  }
  function streamItem(item, turn, index) {
    const person = name(selected);
    if (item.kind === 'prompt') {
      const article = bubble('right', 'system', ['System prompt', node('span', cause[item.input.kind] || label(item.input.kind || 'input'), 'badge')], item.at);
      const task = item.packet?.task;
      if (task?.title) {
        const line = node('p', undefined, 'small');
        line.append('Task: ', task.id ? taskButton(task.id, task.title) : task.title);
        article.append(line);
      }
      const gist = promptGist(item.packet, item.input);
      if (gist) article.append(node('pre', gist));
      article.append(inputPanel(item.input));
      return article;
    }
    if (item.kind === 'context') {
      const article = bubble('right', 'system', [item.title], item.at);
      article.append(inputPanel(item.input));
      return article;
    }
    if (item.kind === 'message') {
      const article = bubble('right', item.senderRole === 'operator' ? 'from-you' : 'from-peer',
        [senderName(item) + ' → ' + person, node('span', item.delivery.text, 'badge ' + item.delivery.tone)], item.at);
      article.append(item.text === null
        ? node('p', 'The message text is not available here' + (item.path ? ': ' + item.path : '.'), 'muted small')
        : node('pre', item.text));
      // A steer can contain several messages; retain one exact-input disclosure per input.
      if (item.showInput) article.append(inputPanel(item.input));
      return article;
    }
    if (item.kind === 'reply') {
      const reply = item.reply, final = ['final', 'finalAnswer', 'final_answer'].includes(reply.phase);
      const partial = ['streaming', 'incomplete'].includes(reply.status) ? ' · ' + reply.status : '';
      const article = bubble('left', final ? 'answer' : '', [person, node('span', (final ? 'Answer' : 'Progress') + partial, 'muted')], item.at);
      article.dataset.reply = reply.key;
      article.append(node('pre', reply.text));
      return article;
    }
    if (item.kind === 'request') {
      const request = (data?.requests || []).find(entry => entry.id === item.requestId);
      const article = bubble('left', 'request', [person + ' asks you',
        node('span', item.requestKind === 'literature' ? 'Source needed' : 'Question', 'badge attention'),
        request ? node('span', label(request.state), 'muted') : null], item.at);
      article.append(node('strong', item.title), node('pre', item.text));
      if (request?.state === 'pending') article.append(node('p', 'Answer it in the pinned request above.', 'muted small'));
      return article;
    }
    if (item.kind === 'outgoing') {
      const target = item.recipient ? (worker(item.recipient) ? name(item.recipient) : item.recipient) : 'You';
      const article = bubble('left', target === 'You' ? 'to-you' : '', [person + ' → ' + target], item.at);
      article.append(node('pre', item.text));
      return article;
    }
    // Tool calls stay folded; their details render when opened.
    const group = node('details', undefined, 'tools');
    group.dataset.key = 'tools:' + turn.key + ':' + index;
    group.append(node('summary', item.actions.length + ' tool call' + (item.actions.length === 1 ? '' : 's') + ' · ' + actionSummary(item.actions)));
    let loaded = false;
    group.addEventListener('toggle', () => {
      if (!group.open || loaded) return;
      loaded = true;
      for (const action of item.actions) group.append(actionRow(action));
    });
    return group;
  }
  function turnView(turn) {
    const section = node('section', undefined, 'turn');
    section.dataset.turn = turn.key;
    const head = node('div', undefined, 'turn-heading');
    const kind = turn.inputs[0]?.kind;
    head.append(node('h3', 'Turn ' + turn.ordinal + (kind ? ' · ' + (cause[kind] || kind) : '')),
      node('span', turn.status === 'uncertain' ? 'Outcome uncertain' : label(turn.status), 'badge' + (['failed', 'interrupted', 'uncertain'].includes(turn.status) ? ' attention' : '')),
      node('span', time(turn.started_at), 'muted right'));
    section.append(head);
    if (turn.task?.id) {
      const taskLine = node('p', undefined, 'turn-task');
      taskLine.append('Task: ', taskButton(turn.task.id, turn.task.title || taskTitle(turn.task.id)));
      section.append(taskLine);
    }
    const stream = node('div', undefined, 'stream');
    turnStream(turn, {messageText}).forEach((item, index) => stream.append(streamItem(item, turn, index)));
    section.append(stream);
    if (!turn.replies.length) {
      section.append(node('p', ['in_progress', 'pending', 'prepared'].includes(turn.status)
        ? name(selected) + ' has not said anything in this turn yet.' : 'No visible response was recorded for this turn.', 'muted'));
    }
    for (const input of turn.inputs) {
      if (input.status === 'uncertain' || input.error || input.reconciliation) {
        section.append(node('p', input.error || input.reconciliation || 'Delivery of this input is uncertain; it is never resent automatically.', 'muted'));
      }
    }
    return section;
  }
  function loadBefore(id, key, before) {
    store.loadSession(id, key, before).catch(error => notice(error.message));
  }
  function turnLabel(turn) {
    const kind = turn.inputs[0]?.kind;
    return 'Turn ' + turn.ordinal + (kind ? ' · ' + (cause[kind] || kind) : '') + ' · '
      + (turn.status === 'uncertain' ? 'Outcome uncertain' : label(turn.status)) + (turn.started_at ? ' · ' + time(turn.started_at) : '');
  }
  function renderTurnPicker(turns, history, viewKey) {
    const select = el('turn-select'), choice = turnChoices.get(viewKey) || '';
    select.replaceChildren();
    const latest = node('option', turns.length ? 'Latest turn (follows new turns)' : 'No turns yet');
    latest.value = ''; latest.selected = !choice;
    select.append(latest);
    for (const turn of [...turns].reverse()) {
      const option = node('option', turnLabel(turn));
      option.value = turn.key; option.selected = turn.key === choice;
      select.append(option);
    }
    if (history.older) {
      const older = node('option', 'Load older turns…');
      older.value = '__older__';
      select.append(older);
    }
    select.disabled = !turns.length || Boolean(history.inflight);
  }

  function renderHistory() {
    if (!selected) return;
    const current = store.worker(selected), key = current.selectedSession, pane = el('transcript');
    if (!key) {
      historyViewKey = ''; historySignature = '';
      el('turn-select').replaceChildren(node('option', 'No turns yet')); el('turn-select').disabled = true;
      empty(pane, current.sessionError ? 'History unavailable' : 'No captured sessions', current.sessionError || name(selected) + ' has no captured prompt or visible response yet.');
      el('transcript-state').textContent = 'Viewing this page does not start a model turn.';
      return;
    }
    const history = store.session(selected, key), viewKey = selected + '/' + key;
    const turns = [...history.turns.values()].sort((a, b) => a.ordinal - b.ordinal);
    const total = history.meta?.turn_count ?? current.sessions.find(s => s.key === key)?.turn_count ?? 0;
    const choice = turnChoices.get(viewKey) || '';
    const shown = (choice && history.turns.get(choice)) || turns.at(-1) || null;
    el('transcript-state').textContent = history.error || current.sessionError || (history.inflight ? 'Updating history…'
      : (shown ? 'Turn ' + shown.ordinal + ' of ' + total : 'No turns') + (choice ? '' : ' · following the latest turn')
        + ' · Updated ' + time(history.meta?.updated_at));
    // Message bodies and request states feed the stream, so they are part of what it shows.
    const signature = viewKey + ':' + history.version + ':' + history.error + ':' + (shown?.key || '') + ':' + choice
      + ':' + current.chatVersion + ':' + Boolean(history.inflight) + ':' + JSON.stringify((data?.requests || []).map(r => [r.id, r.state]));
    if (signature === historySignature) return;
    const shownKey = viewKey + '#' + (shown?.key || '');
    const same = historyViewKey === shownKey;
    if (same) rememberView();
    historySignature = signature;
    historyViewKey = shownKey;
    renderTurnPicker(turns, history, viewKey);
    const position = scrollPositions.get(shownKey);
    const previousOpen = expanded.get(shownKey);
    pane.replaceChildren();
    const context = contextPanel(history);
    if (context) pane.append(context);
    if (shown) pane.append(turnView(shown));
    else pane.append(node('p', history.loaded ? 'No turns have been captured in this session.' : 'Loading session…', 'empty'));
    if (previousOpen) {
      let count = -1;
      while (count !== pane.querySelectorAll('details[data-key]').length) {
        count = pane.querySelectorAll('details[data-key]').length;
        for (const group of pane.querySelectorAll('details[data-key]')) {
          group.open = previousOpen.has(group.dataset.key);
          if (group.open) group.dispatchEvent(new Event('toggle'));
        }
      }
    }
    // A chosen turn opens at its start; the latest turn follows its newest entries.
    if (position) pane.scrollTop = position.bottom ? pane.scrollHeight : position.top;
    else pane.scrollTop = choice ? 0 : pane.scrollHeight;
  }

  // Messages to this person that no turn has received yet; yours can be withdrawn until then.
  function renderQueued() {
    if (!selected) return;
    const list = el('queued-messages'), id = selected;
    const queued = [...store.worker(id).chats.values()]
      .filter(message => message.direction === 'to_worker' && !message.acknowledged_at && !message.withdrawn_at)
      .sort((a, b) => a.sequence - b.sequence);
    list.replaceChildren();
    for (const message of queued) {
      const item = bubble('right', 'from-you', ['You → ' + name(id), node('span', 'Queued · not yet delivered', 'badge')], message.created_at);
      item.append(node('pre', message.text));
      if (message.sender_role === 'operator' && !message.request_id) {
        const withdraw = node('button', 'Withdraw');
        withdraw.type = 'button';
        withdraw.title = 'Withdraw this message: no turn will receive it';
        withdraw.onclick = () => {
          withdraw.disabled = true;
          store.withdraw(id, message.id).catch(() => { withdraw.disabled = false; });
        };
        item.append(withdraw);
      }
      list.append(item);
    }
    list.classList.toggle('hidden', !queued.length);
  }

  function renderComposer() {
    if (!selected) return;
    const current = store.worker(selected), person = worker(selected);
    const retired = person?.work_intention === 'retired' || person?.state === 'retired';
    el('compose').classList.remove('hidden');
    if (el('message').value !== current.draft) el('message').value = current.draft;
    el('message').disabled = retired;
    el('message-label').textContent = 'Message ' + name(selected);
    el('send').disabled = retired || current.sending || !current.draft.trim();
    el('send').textContent = current.sending ? 'Sending…' : 'Send';
    el('message-state').textContent = current.messageError || (retired ? name(selected) + ' is retired. Its messages remain readable.' : current.sending ? 'Sending to ' + name(selected) + '…' : 'Drafts stay with each person. A paused project keeps messages queued; under Pause after turn a turn under way still receives them.');
  }
  async function loadWorker(id) {
    await Promise.allSettled([store.loadSessions(id), store.loadChat(id)]);
    if (store.worker(id).selectedSession) await store.loadSession(id, store.worker(id).selectedSession).catch(() => {});
  }
  function setDrawer(open) {
    document.body.classList.toggle('drawer-open', open);
    el('people-toggle').setAttribute('aria-expanded', String(open));
  }
  function selectWorker(id) {
    setDrawer(false);
    if (selected === id) return;
    rememberView();
    selected = id;
    historySignature = ''; sessionSignature = ''; pinnedSignature = '';
    renderWorkers(); renderWorkerHead(); renderSessions(); renderHistory(); renderQueued(); markChatSeen();
    loadWorker(id).catch(error => notice(error.message));
    if (worker(id)?.summary) void loadRetiredDetails(id);
  }

  function reviewResult(task) {
    if (task.current_evidence?.state === 'invalid') return label(task.state) + ' · evidence invalidated';
    const collaboration = task.collaboration || {};
    const review = collaboration.review || task.review || {};
    const feedback = (review.feedback && typeof review.feedback === 'object' ? review.feedback : null) || task.review_feedback || review;
    if (review.state === 'invalidated') return 'Review invalidated';
    if (['reject', 'rejected', 'request_changes'].includes(feedback?.verdict) || feedback?.decision === 'reject') return 'Rejected · changes requested';
    if (feedback?.verdict === 'uncertain') return 'Review needs attention';
    if (review.state === 'resolved' && feedback?.verdict === 'approve' && !(feedback.qualifications || []).length) return 'Accepted & published';
    if (review.acceptance) return 'Accepted · integrating';
    return label(task.state);
  }
  // ---- Task center: the overview (publication tree), the dependency graph, the paged lists, and
  // the selected task beside them.
  const outlineView = {expanded: new Set(), touched: false, filter: '', hideDone: false, loadedAt: 0, shown: new Map()};
  const graphView = {data: null, load: null, error: '', loadedAt: 0, includeMet: false, scrolledTo: null, signature: ''};
  let outlineTree = null, outlineFilterTimer = null;
  const OUTLINE_CHILDREN = 150;
  function plural(count, word, many = word + 's') { return count.toLocaleString() + ' ' + (count === 1 ? word : many); }
  function shortId(id) { return String(id).slice(0, 8); }
  // A work task by its name, Task N; a review, or a task not yet seen, by its short id.
  function taskName(id) {
    const number = taskStore.numbers.get(id);
    return Number.isInteger(number) ? 'Task ' + number : shortId(id);
  }
  function chip(text, tone = '') { return node('span', text, 'chip' + (tone ? ' ' + tone : '')); }
  function stateChip(task) {
    if (task.contract?.kind === 'review') {
      const text = reviewResult(task);
      return chip(text, /changes|Rejected|attention|invalid/i.test(text) ? 'attention' : /Accepted|done/i.test(text) ? 'done' : 'active');
    }
    const state = taskOutlineState(task);
    return chip(state.text, state.tone);
  }
  function dateLabel(value) {
    if (!value) return '';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '' : date.toLocaleString(undefined, {dateStyle: 'medium', timeStyle: 'short'});
  }
  function shortDate(value) {
    if (!value) return '';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '' : date.toLocaleString(undefined, {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'});
  }
  function taskChanged(area, id) {
    if (!el('tasks-dialog').open) return;
    if (area === 'page') renderTasks();
    if (area === 'detail' && id === taskStore.selectedTask) {
      renderTasks(); renderTaskDetail();
      if (taskMode === 'outline') renderOutline();
      if (taskMode === 'graph') renderGraph();
    }
  }
  function taskMembers(task) {
    const members = node('div', undefined, 'task-members'), seen = new Set();
    for (const member of taskExecutors(task)) {
      if (seen.has(member.worker_id)) continue;
      seen.add(member.worker_id);
      const line = node('span');
      line.append(workerButton(member.worker_id), node('span', ' · ' + (member.execution_status || 'Retained'), 'muted'));
      members.append(line);
    }
    if (!members.children.length) members.append(node('span', 'No executors', 'muted'));
    return members;
  }
  function executorText(executors) {
    return executors.map(member => name(member.worker_id) + ' ' + String(member.execution_status || 'retained').toLowerCase()).join(' · ');
  }
  function renderTotals() {
    const target = el('tasks-totals');
    if (outline) {
      const progress = outlineProgress(outline.items);
      target.textContent = plural(progress.total, 'work task') + ' · ' + progress.done.toLocaleString() + ' done · '
        + plural(outline.review_count || 0, 'review');
    } else if (taskStore.counts.all !== null && taskStore.kind === 'work') {
      target.textContent = plural(taskStore.counts.all, 'work task') + ' · ' + taskStore.counts.history.toLocaleString() + ' finished';
    }
  }

  // The overview reads every work task (compactly): at most every 30 s while it is shown.
  async function loadOutline({force = false} = {}) {
    if (!el('tasks-dialog').open || taskMode !== 'outline') return;
    if (outlineLoad) return outlineLoad;
    if (!force && outline && Date.now() - outlineView.loadedAt < 30000) return;
    outlineLoad = (async () => {
      try {
        outline = await api('/v1/task-outline'); outlineError = ''; outlineView.loadedAt = Date.now();
        taskStore.rememberTasks(outline.items);
        outlineTree = publicationTree(outline.items, outline.root_task_id);
        if (!outlineView.touched && outline.root_task_id) outlineView.expanded.add(outline.root_task_id);
      } catch (error) { outlineError = error.message; }
      finally { outlineLoad = null; renderOutline(); renderTotals(); }
    })();
    return outlineLoad;
  }
  function finished(tree, id) {
    const task = tree.byId.get(id), below = tree.below.get(id);
    return ['done', 'retired'].includes(task.state) && below.done >= below.total;
  }
  function outlineRow(task, tree, open, forced) {
    const item = node('li'), row = node('div', undefined, 'tree-row' + (task.id === taskStore.selectedTask ? ' selected' : ''));
    const children = tree.children.get(task.id) || [], below = tree.below.get(task.id);
    const toggle = node('button', open ? '▾' : '▸', 'tree-toggle');
    toggle.disabled = !children.length || forced;
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-label', (open ? 'Collapse ' : 'Expand ') + (task.contract?.title || 'task'));
    toggle.onclick = () => {
      outlineView.touched = true;
      if (outlineView.expanded.has(task.id)) outlineView.expanded.delete(task.id); else outlineView.expanded.add(task.id);
      renderOutline();
    };
    const main = node('div', undefined, 'tree-main'), marks = node('span', undefined, 'tree-marks');
    const link = taskButton(task.id, task.contract?.title || 'Untitled task');
    link.setAttribute('aria-current', String(task.id === taskStore.selectedTask));
    marks.append(stateChip(task));
    if (task.id === outline.root_task_id) marks.prepend(chip('Task 0', 'active'));
    if (task.relation === 'repair') marks.append(chip('Repair', 'bad'));
    const meta = task.id === outline.root_task_id ? [] : [taskName(task.id)];  // Task 0 has its chip.
    const executors = taskExecutors(task);
    if (executors.length) meta.push(executorText(executors));
    if (children.length) meta.push(below.done + '/' + below.total + ' below done');
    const reviews = reviewSummary(task.reviews);
    if (reviews) meta.push(reviews);
    main.append(link, marks);
    if (meta.length) main.append(node('span', meta.join(' · '), 'tree-meta'));
    row.append(toggle, main);
    item.append(row);
    return item;
  }
  function outlineLevel(ids, tree, visible, forced, parentKey) {
    const list = node('ul', undefined, 'tree');
    const shown = ids.filter(id => (!visible || visible.has(id)) && !(outlineView.hideDone && !forced.has(id) && finished(tree, id)));
    const limit = outlineView.shown.get(parentKey) || OUTLINE_CHILDREN;
    for (const id of shown.slice(0, limit)) {
      const task = tree.byId.get(id), children = tree.children.get(id) || [];
      const open = children.length > 0 && (forced.has(id) || outlineView.expanded.has(id));
      const item = outlineRow(task, tree, open, forced.has(id));
      if (open) item.append(outlineLevel(children, tree, visible, forced, id));
      list.append(item);
    }
    if (shown.length > limit) {
      const rest = shown.length - limit, next = Math.min(OUTLINE_CHILDREN, rest);
      const more = node('button', 'Show ' + next + ' more' + (rest > next ? ' of ' + rest.toLocaleString() : ''), 'small tree-more');
      more.onclick = () => { outlineView.shown.set(parentKey, limit + OUTLINE_CHILDREN); renderOutline(); };
      const item = node('li'); item.append(more); list.append(item);
    }
    return list;
  }
  function renderOutline() {
    const target = el('task-outline');
    const signature = JSON.stringify([outlineView.loadedAt, outlineError, taskStore.selectedTask, [...outlineView.expanded],
      outlineView.filter, outlineView.hideDone, [...outlineView.shown], data?.workers.map(w => [w.id, w.display_name]), retired.records.length]);
    if (signature === outlineSignature) return;
    outlineSignature = signature;
    const pane = el('task-list-pane'), top = pane.scrollTop;
    target.replaceChildren();
    if (!outline || !outlineTree) {
      target.append(node('p', outlineError ? 'Tasks could not be loaded: ' + outlineError : 'Loading tasks…', outlineError ? 'request-error' : 'muted'));
      return;
    }
    if (outlineError) target.append(node('p', 'Showing the last loaded overview. ' + outlineError, 'request-error'));
    const progress = outlineProgress(outline.items), line = node('p', undefined, 'task-progress');
    line.append(node('strong', progress.done.toLocaleString() + ' of ' + progress.total.toLocaleString()), ' work tasks done'
      + (progress.review ? ' · ' + progress.review + ' awaiting review' : '') + (progress.active ? ' · ' + progress.active + ' in progress' : ''));
    const track = node('div', undefined, 'progress-track'), fill = node('span');
    fill.style.width = (progress.total ? Math.round(100 * progress.done / progress.total) : 0) + '%';
    track.append(fill);
    target.append(line, track);
    const tree = outlineTree;
    let visible = null, forced = new Set();
    const query = outlineView.filter.trim().toLowerCase();
    if (query) {
      const matches = outline.items.filter(item => item.id.toLowerCase().startsWith(query)
        || (item.contract?.title || '').toLowerCase().includes(query));
      visible = new Set();
      for (const item of matches.slice(0, 300)) {
        visible.add(item.id);
        for (const up of treeAncestors(tree, item.id)) { visible.add(up); forced.add(up); }
      }
      target.append(node('p', matches.length ? plural(matches.length, 'task') + ' match' + (matches.length === 1 ? 'es, shown with the tasks above it.' : '')
        + (matches.length > 300 ? '; the first 300 are shown with the tasks above them.' : matches.length > 1 ? ', shown with the tasks above them.' : '')
        : 'No task matches “' + outlineView.filter.trim() + '”.', 'muted outline-head'));
    }
    if (!tree.roots.length) { target.append(node('p', 'No tasks yet.', 'muted')); return; }
    target.append(outlineLevel(tree.roots, tree, visible, forced, ''));
    pane.scrollTop = top;
  }

  // The dependency graph reads every open task's needs: at most every 30 s while it is shown.
  async function loadGraph({force = false} = {}) {
    if (!el('tasks-dialog').open || taskMode !== 'graph') return;
    if (graphView.load) return graphView.load;
    const current = graphView.data && graphView.data.include_met === graphView.includeMet;
    if (!force && current && Date.now() - graphView.loadedAt < 30000) return;
    graphView.load = (async () => {
      try {
        graphView.data = await api('/v1/task-graph?include_met=' + graphView.includeMet);
        graphView.error = ''; graphView.loadedAt = Date.now();
        taskStore.rememberTasks(graphView.data.tasks.map(task => ({id: task.id, title: task.title,
                                                                    task_number: task.task_number})));
      } catch (error) { graphView.error = error.message; }
      finally { graphView.load = null; renderGraph(); }
    })();
    return graphView.load;
  }
  function graphTask(id) { return graphView.data?.tasks.find(task => task.id === id); }
  function agentName(id) { return id ? graphView.data?.agents?.[id] || name(id) : ''; }
  function linkSentence(link, titleOf, agents) {
    const needer = titleOf(link.target), needed = link.source === 'operator' ? 'you' : titleOf(link.source);
    const who = link.agent ? (agents[link.agent] || name(link.agent)) + ' on “' + needer + '”' : '“' + needer + '”';
    const what = link.kind === 'requirement' ? who + ' requires “' + needed + '” (' + link.detail + ')'
      : link.kind === 'subscription' ? who + ' waits for “' + needed + '”: ' + link.detail
      : who + ' waits for ' + link.detail;
    return what + (link.met ? ' · met' : ' · not met');
  }
  function noteSentence(note, agents) {
    const who = note.agent ? agents[note.agent] || name(note.agent) : 'Its contract';
    return who + ' waits for ' + note.detail + (note.met ? ' (met)' : '');
  }
  function svgNode(tag, attributes = {}, children = []) {
    const value = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [key, attribute] of Object.entries(attributes)) value.setAttribute(key, String(attribute));
    for (const child of children) if (child) value.append(child);
    return value;
  }
  function graphBanner(tone, parts) {
    const banner = node('div', undefined, 'graph-banner ' + tone);
    for (const part of parts) banner.append(part);
    return banner;
  }
  function taskList(ids) {
    const parts = [];
    ids.forEach((id, index) => { if (index) parts.push(', '); parts.push(taskButton(id, taskTitle(id))); });
    return parts;
  }
  function renderGraph() {
    const value = graphView.data;
    const signature = JSON.stringify([graphView.loadedAt, graphView.error, Boolean(graphView.load), taskStore.selectedTask,
      data?.workers.map(w => [w.id, w.display_name]), retired.records.length]);
    if (signature === graphView.signature) return;
    graphView.signature = signature;
    const banners = el('graph-banners'), canvas = el('graph-canvas'), notes = el('graph-notes');
    banners.replaceChildren(); notes.replaceChildren();
    el('graph-legend').replaceChildren();
    if (!value) {
      canvas.replaceChildren(node('p', graphView.error ? 'The dependency graph could not be loaded: ' + graphView.error : 'Loading the dependency graph…',
        graphView.error ? 'request-error graph-empty' : 'graph-empty'));
      el('graph-summary').textContent = '';
      return;
    }
    const agents = value.agents || {}, titleOf = id => graphTask(id)?.title || taskTitle(id);
    el('graph-summary').textContent = plural(value.tasks.length, 'task') + ' · ' + plural(value.counts.unmet, 'unmet link')
      + ' of ' + value.counts.links.toLocaleString() + (graphView.error ? ' · refresh failed: ' + graphView.error : '');
    for (const group of value.deadlocks) {
      banners.append(graphBanner('bad', [node('strong', 'Deadlock: '), ...taskList(group),
        ' need each other through unmet links, so none of them can go on. Revise a contract, or message a worker who waits.']));
    }
    if (value.waiting_on_operator.length) {
      const open = node('button', 'Open the requests', 'link');
      open.onclick = () => { el('tasks-dialog').close(); el('show-requests').click(); };
      banners.append(graphBanner('attention', [plural(value.waiting_on_operator.length, 'task') + ' wait for your answer, directly or through other tasks: ',
        ...taskList(value.waiting_on_operator), '. ', open]));
    }
    const legend = el('graph-legend');
    legend.append(node('span', 'Arrows run from what is needed to what needs it.'));
    for (const [kind, text] of [['', 'contract requirement'], ['subscription', 'subscription'], ['wait', 'wait for your answer'],
      ['met', 'met'], ['deadlock', 'deadlock']]) {
      const item = node('span'); item.append(node('span', undefined, 'swatch ' + kind), text); legend.append(item);
    }
    const edges = graphEdges(value.links, value.deadlocks);
    const ids = [...new Set(edges.flatMap(edge => [edge.source, edge.target]))];
    if (!ids.length) {
      canvas.replaceChildren(node('p', value.counts.links ? 'Every link between open tasks is met. Show met links to see them.'
        : 'No open task needs another task, and none waits for your answer.', 'graph-empty'));
    } else {
      const order = new Map(value.tasks.map((task, index) => [task.id, index]));
      ids.sort((a, b) => (order.get(a) ?? -1) - (order.get(b) ?? -1));
      const layout = layeredLayout(ids, edges);
      const selected = ids.includes(taskStore.selectedTask) ? taskStore.selectedTask : null;
      const near = selected ? graphNeighbours(edges, selected) : null;
      const deadlocked = new Set(value.deadlocks.flat()), onYou = new Set(value.waiting_on_operator);
      const drawing = node('div', undefined, 'graph');
      drawing.style.width = layout.width + 'px'; drawing.style.height = layout.height + 'px';
      const heads = ['plain', 'subscription', 'wait', 'met', 'deadlock'].map(kind => svgNode('marker', {
        id: 'task-head-' + kind, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse'},
      [svgNode('path', {d: 'M0,0 L10,5 L0,10 z', class: 'head ' + kind})]));
      const paths = edges.map(edge => {
        const {d} = edgeRoute(edge, layout);
        const focus = !selected ? '' : edge.source === selected || edge.target === selected ? ' focus' : ' dim';
        const kind = edge.kind === 'requirement' ? '' : edge.kind;
        const title = svgNode('title');
        title.textContent = edge.links.map(link => linkSentence(link, titleOf, agents)).join('\n');
        return svgNode('path', {d, class: 'edge ' + kind + focus, 'marker-end': 'url(#task-head-' + (kind || 'plain') + ')'}, [title]);
      });
      drawing.append(svgNode('svg', {width: layout.width, height: layout.height, 'aria-hidden': 'true'}, [svgNode('defs', {}, heads), ...paths]));
      for (const id of ids) {
        const position = layout.position.get(id);
        const button = node('button', undefined, 'gnode');
        button.style.left = position.x + 'px'; button.style.top = position.y + 'px';
        button.style.width = GRAPH.nodeWidth + 'px'; button.style.height = GRAPH.nodeHeight + 'px';
        if (id === 'operator') {
          const asks = value.links.filter(link => link.source === 'operator' && !link.met).length;
          button.classList.add('operator');
          button.append(node('span', 'You', 'line title'), node('span', plural(asks, 'answer') + ' awaited', 'line'), node('span', 'Open the requests', 'line'));
          button.onclick = () => { el('tasks-dialog').close(); el('show-requests').click(); };
        } else {
          const task = graphTask(id) || {id, title: taskTitle(id), state: 'ready', executors: []};
          const state = taskOutlineState(task);
          const first = node('span', undefined, 'line');
          first.append(state.text + ' · ' + taskName(id) + (onYou.has(id) ? ' · waits on you' : ''));
          const waiting = (value.notes || []).filter(note => note.task === id && !note.met).map(note => noteSentence(note, agents));
          const third = waiting.length ? waiting.join('; ') : task.executors.length ? executorText(task.executors) : 'No executors';
          button.append(first, node('span', task.title || 'Untitled task', 'line title'), node('span', third, 'line muted'));
          button.title = (task.title || id) + '\n' + third;
          button.onclick = () => showTask(id).catch(error => notice(error.message));
          if (deadlocked.has(id)) button.classList.add('deadlock');
          if (['done', 'retired'].includes(task.state)) button.classList.add('done');
        }
        if (id === selected) button.classList.add('selected');
        else if (near && !near.has(id)) button.classList.add('dim');
        drawing.append(button);
      }
      const scrollTop = canvas.scrollTop, scrollLeft = canvas.scrollLeft;
      canvas.replaceChildren(drawing);
      canvas.scrollTop = scrollTop; canvas.scrollLeft = scrollLeft;
      if (selected && graphView.scrolledTo !== selected) {
        graphView.scrolledTo = selected;
        const position = layout.position.get(selected);
        canvas.scrollLeft = Math.max(0, position.x - canvas.clientWidth / 2 + GRAPH.nodeWidth / 2);
        canvas.scrollTop = Math.max(0, position.y - canvas.clientHeight / 2 + GRAPH.nodeHeight / 2);
      }
    }
    const drawn = new Set(ids), inner = new Map();
    for (const note of value.notes || []) {
      if (note.met || drawn.has(note.task)) continue;
      if (!inner.has(note.task)) inner.set(note.task, []);
      inner.get(note.task).push(note);
    }
    if (inner.size) {
      const section = node('section', undefined, 'graph-notes');
      section.append(node('h3', 'Waits within a task'),
        node('p', 'These waits involve no other task: a worker waits on its own submission, a decision, or a peer\'s answer.', 'muted'));
      const list = node('ul', undefined, 'relation-list');
      for (const [id, items] of inner) {
        const entry = node('li');
        entry.append(taskButton(id, titleOf(id)), node('span', items.map(note => noteSentence(note, agents)).join('; '), 'muted'));
        list.append(entry);
      }
      section.append(list); notes.append(section);
    }
  }

  function renderTasks() {
    const page = taskStore.page, loading = Boolean(taskStore.pageLoad);
    for (const [mode, button, view] of [['outline', 'tasks-outline', 'task-outline-view'], ['graph', 'tasks-graph', 'task-graph-view']]) {
      el(button).classList.toggle('active', taskMode === mode);
      el(button).setAttribute('aria-pressed', String(taskMode === mode));
      el(view).classList.toggle('hidden', taskMode !== mode);
    }
    el('task-table-view').classList.toggle('hidden', taskMode !== 'list');
    el('tasks-dialog').classList.toggle('graph-mode', taskMode === 'graph');
    const viewNames = {all: 'All', running: 'Running', pool: 'Task pool', history: 'History'};
    for (const [view, title] of Object.entries(viewNames)) {
      const button = el('tasks-' + view), count = taskStore.counts[view];
      button.textContent = title + (count === null || count === undefined ? '' : ' (' + count.toLocaleString() + ')');
      button.classList.toggle('active', taskMode === 'list' && taskStore.view === view);
      button.setAttribute('aria-pressed', String(taskMode === 'list' && taskStore.view === view));
    }
    renderTotals();
    if (taskMode === 'outline') {
      el('task-view-description').textContent = 'Each work task under the task it was published from, Task 0 first; a repair under the task it repairs. Reviews are counted on their task.';
      renderOutline();
      return;
    }
    if (taskMode === 'graph') {
      el('task-view-description').textContent = 'Open tasks that need other tasks: contract requirements, and workers waiting on another task or on your answer. Select a task to bring out its neighbours.';
      renderGraph();
      return;
    }
    const kinds = {work: 'work tasks', review: 'reviews', both: 'tasks'}[taskStore.kind];
    el('task-view-description').textContent = {
      all: 'Every ' + (taskStore.kind === 'review' ? 'review' : taskStore.kind === 'work' ? 'work task' : 'task') + ', unfinished and finished.',
      running: 'Tasks with a worker executing or finishing task work.',
      pool: 'Tasks with an available execution opportunity, in the order TaskHub takes them: reviews, then pinned tasks, then by priority (0 first) and the earliest published. A running task may also be recruiting.',
      history: 'Completed and retired tasks.'
    }[taskStore.view];
    el('task-kind').value = taskStore.kind; el('task-sort').value = taskStore.sort;
    el('task-sort').disabled = taskStore.view === 'pool';
    const start = page?.items.length ? page.offset + 1 : 0;
    el('task-count').textContent = page ? start.toLocaleString() + '–' + (page.offset + page.items.length).toLocaleString() + ' of '
      + page.total.toLocaleString() + ' ' + kinds : 'Loading tasks…';
    el('task-newer').classList.toggle('hidden', !page?.newer);
    el('task-newer').textContent = page?.newer ? plural(page.newer, 'newer task') + ' · Show' : '';
    el('task-list-state').textContent = taskStore.error ? 'Tasks could not be refreshed: ' + taskStore.error
      : loading && !page ? 'Loading tasks…' : '';
    el('task-retry').classList.toggle('hidden', !taskStore.error);
    el('task-previous').disabled = !page || taskStore.offset === 0;
    el('task-next').disabled = !page?.has_more;
    const pages = page ? Math.max(1, Math.ceil(page.total / taskStore.limit)) : 1, current = Math.floor(taskStore.offset / taskStore.limit) + 1;
    el('task-page-label').textContent = page ? 'Page ' + current.toLocaleString() + ' of ' + pages.toLocaleString() : '';
    el('task-page').max = String(pages);
    if (document.activeElement !== el('task-page')) el('task-page').value = page ? String(current) : '';
    const signature = JSON.stringify([taskStore.view, taskStore.kind, taskStore.sort, taskStore.query, taskStore.offset, taskStore.selectedTask,
      page?.items, Boolean(taskStore.error), data?.workers.map(w => [w.id, w.display_name]), retired.records.length]);
    if (signature === taskSignature) return;
    taskSignature = signature;
    const list = el('task-list'), pane = el('task-list-pane'), top = pane.scrollTop;
    list.replaceChildren();
    for (const task of page?.items || []) {
      const contract = task.contract || task, review = contract.kind === 'review';
      const row = node('tr', undefined, task.id === taskStore.selectedTask ? 'selected-task' : '');
      const title = node('td'), state = node('td'), members = node('td'), reviews = node('td'), published = node('td');
      const open = taskButton(task.id, contract.title || 'Untitled task');
      open.setAttribute('aria-current', String(task.id === taskStore.selectedTask));
      const meta = [taskName(task.id)];
      if (review) meta.push('review');
      if (task.revision > 1) meta.push('revision ' + task.revision);
      const place = admissionLabel(task);
      // Priority 1 is every contract's default: only another one, or the operator's, is news.
      if (place && (task.admission.priority !== 1 || task.admission.operator_priority !== null)) meta.push(place);
      title.append(open, node('span', meta.join(' · '), 'task-meta'));
      const cell = node('div', undefined, 'task-state-cell');
      cell.append(stateChip(task));
      if (place && task.admission.pinned_at) cell.append(chip('Pinned', 'active'));
      if (task.running && !['done', 'retired'].includes(task.state)) cell.append(chip('Running', 'active'));
      state.append(cell);
      if (task.pool_opportunity) {
        const opportunity = task.pool_opportunity, line = node('span', undefined, 'task-opportunity');
        line.append(opportunity.opportunity === 'collaborator' ? 'Open to a collaborator'
          : opportunity.opportunity === 'review' ? 'Needs a reviewer' : 'Needs an executor');
        if (opportunity.invitation) {
          const invitation = node('details'); invitation.dataset.key = 'invitation:' + task.id;
          invitation.append(node('summary', 'Invitation'), node('div', opportunity.invitation, 'small'));
          line.append(invitation);
        }
        state.append(line);
      }
      members.append(taskMembers(task));
      reviews.textContent = review ? '—' : reviewSummary(task.reviews) || '—';
      published.textContent = shortDate(task.created_at);
      published.title = dateLabel(task.created_at);
      state.dataset.label = 'State'; members.dataset.label = 'Executors'; reviews.dataset.label = 'Reviews'; published.dataset.label = 'Published';
      row.append(title, state, members, reviews, published); list.append(row);
    }
    if (!list.children.length) {
      const row = node('tr'), cell = node('td', page ? 'No ' + kinds + ' match this view and search.' : taskStore.error ? 'Task list unavailable.' : 'Loading tasks…', 'muted empty-cell');
      cell.colSpan = 5; row.append(cell); list.append(row);
    }
    pane.scrollTop = top;
  }
  async function showTask(id) {
    openTasks();
    el('tasks-dialog').classList.add('detail-open');
    const changed = taskStore.selectedTask !== id;
    const pending = taskStore.selectTask(id);
    if (changed) el('task-detail-pane').scrollTop = 0;
    await pending;
  }
  function openTasks() {
    if (!el('tasks-dialog').open) el('tasks-dialog').showModal();
    taskStore.setOpen(true);
    renderTasks(); renderTaskDetail();
    taskStore.loadPage().catch(() => {});
    loadOutline(); loadGraph();
  }
  function inlineNodes(spans) {
    return spans.map(span => {
      if (span.type === 'text') return document.createTextNode(span.text);
      if (span.type === 'link') {
        const link = node('a', span.text); link.href = span.href; link.target = '_blank'; link.rel = 'noopener noreferrer';
        return link;
      }
      return node({code: 'code', strong: 'strong', em: 'em'}[span.type], span.text);
    });
  }
  function markdown(text) {
    const root = node('div', undefined, 'markdown');
    for (const block of markdownBlocks(text)) {
      if (block.type === 'heading') { const heading = node('h' + Math.min(6, block.level + 2)); heading.append(...inlineNodes(block.inline)); root.append(heading); }
      else if (block.type === 'paragraph') { const paragraph = node('p'); paragraph.append(...inlineNodes(block.inline)); root.append(paragraph); }
      else if (block.type === 'quote') { const quote = node('blockquote'); quote.append(...inlineNodes(block.inline)); root.append(quote); }
      else if (block.type === 'code') { const pre = node('pre'); pre.append(node('code', block.text)); root.append(pre); }
      else if (block.type === 'rule') root.append(node('hr'));
      else if (block.type === 'list') {
        const list = node(block.ordered ? 'ol' : 'ul');
        for (const item of block.items) { const entry = node('li'); entry.append(...inlineNodes(item)); list.append(entry); }
        root.append(list);
      }
    }
    return root;
  }
  function markdownDisclosure(title, text, key) {
    const group = node('details'); group.dataset.key = key;
    group.append(node('summary', title));
    let loaded = false;
    group.addEventListener('toggle', () => { if (group.open && !loaded) { loaded = true; group.append(markdown(text)); } });
    return group;
  }
  function relationTask(relations, id) {
    if (id === 'operator') return node('strong', 'you');
    return taskButton(id, relations.tasks?.[id]?.title || taskTitle(id));
  }
  function relationLinks(relations, title, links, other) {
    if (!links.length) return null;
    const section = node('div'), list = node('ul', undefined, 'relation-list');
    section.append(node('div', title, 'eyebrow'));
    for (const link of links) {
      const item = node('li'), body = node('div'), who = link.agent ? relations.agents?.[link.agent] || name(link.agent) : '';
      body.append(relationTask(relations, other(link)));
      const kind = link.kind === 'requirement' ? 'contract requirement' : link.kind === 'subscription' ? 'subscription of ' + who : 'wait of ' + who;
      const state = other(link) === 'operator' ? '' : relations.tasks?.[other(link)]?.state;
      body.append(node('span', kind + (link.met ? ' · met' : ' · not met') + (state ? ' · task ' + label(state).toLowerCase() : '')
        + (link.detail ? ' — ' + link.detail : ''), 'muted'));
      item.append(node('span', link.met ? '✓' : '○', 'relation-mark ' + (link.met ? 'met' : 'unmet')), body);
      list.append(item);
    }
    section.append(list);
    return section;
  }
  function relationsSection(view, id) {
    const box = node('section', undefined, 'detail-section'), relations = view.relations;
    box.append(node('h4', 'Relations'));
    if (!relations) {
      box.append(node('p', view.relationsError ? 'Relations could not be loaded: ' + view.relationsError : 'Loading relations…',
        view.relationsError ? 'request-error' : 'muted'));
      return box;
    }
    const origin = node('p');
    if (relations.host) origin.append('Reviews ', relationTask(relations, relations.host.id), '.');
    else if (relations.parent) {
      origin.append(relations.relation === 'repair' ? 'Repairs the evidence of ' : 'Published from ', relationTask(relations, relations.parent.id));
      if (relations.publisher && relations.relation !== 'repair') origin.append(' by ', workerButton(relations.publisher.worker_id, relations.publisher.display_name));
      origin.append('.');
    } else if (relations.publisher) origin.append('Published by ', workerButton(relations.publisher.worker_id, relations.publisher.display_name), ' outside a task.');
    else origin.append('A top-level task: published by you or by the system.');
    box.append(origin);
    for (const group of relations.deadlocks || []) {
      box.append(graphBanner('bad', [node('strong', 'Deadlock: '), ...taskList(group), ' need each other through unmet links.']));
    }
    const needs = relationLinks(relations, 'Needs', relations.needs, link => link.source);
    const neededBy = relationLinks(relations, 'Needed by', relations.needed_by, link => link.target);
    if (needs) box.append(needs);
    if (neededBy) box.append(neededBy);
    if (relations.notes?.length) {
      const waits = node('div'), list = node('ul', undefined, 'relation-list');
      waits.append(node('div', 'Waits within this task', 'eyebrow'));
      for (const note of relations.notes) {
        const item = node('li');
        item.append(node('span', note.met ? '✓' : '○', 'relation-mark ' + (note.met ? 'met' : 'unmet')), noteSentence(note, relations.agents || {}));
        list.append(item);
      }
      waits.append(list); box.append(waits);
    }
    if (!needs && !neededBy && !relations.notes?.length) box.append(node('p', 'It needs no other task, and no task needs it.', 'muted'));
    if (relations.children.total) {
      const group = node('details'); group.dataset.key = 'published-here';
      group.append(node('summary', 'Published from this task · ' + relations.children.total.toLocaleString()));
      const list = node('ul', undefined, 'relation-list');
      for (const child of relations.children.items) {
        const item = node('li');
        item.append(taskButton(child.id, child.title || 'Untitled task'), ' ', chip(label(child.state), ['done', 'retired'].includes(child.state) ? 'done' : 'idle'));
        if (child.relation === 'repair') item.append(' ', chip('Repair', 'bad'));
        list.append(item);
      }
      if (relations.children.total > relations.children.items.length) {
        list.append(node('li', 'The first ' + relations.children.items.length + ' are listed; find the rest in the overview.', 'muted'));
      }
      group.append(list); box.append(group);
    }
    if (relations.reviews.counts.total) {
      const group = node('details'); group.dataset.key = 'reviews-of-task';
      group.append(node('summary', reviewSummary(relations.reviews.counts)));
      const list = node('ul', undefined, 'relation-list');
      for (const review of relations.reviews.items) {
        const item = node('li');
        const verdict = review.verdict === 'approve' ? chip('Approved', 'done') : review.verdict === 'request_changes' ? chip('Changes requested', 'attention')
          : ['pending', 'integrating'].includes(review.state) ? chip(review.state === 'pending' ? 'Open' : 'Integrating', 'active') : chip(label(review.state), 'idle');
        item.append(taskButton(review.id, review.reviewer_name ? 'Review by ' + review.reviewer_name : 'Review ' + shortId(review.id)), ' ', verdict,
          node('span', dateLabel(review.created_at), 'muted'));
        list.append(item);
      }
      group.append(list); box.append(group);
    }
    return box;
  }
  // Workers that left the task, and the operator's invitation back (task.invite_back). The draft
  // and the outcome belong to the task and worker they were written for.
  const inviteState = {task: null, worker: null, draft: '', busy: false, note: '', error: ''};
  function departedMembers(task, departed) {
    const box = node('div', undefined, 'departed-members'), list = node('ul', undefined, 'relation-list');
    box.append(node('div', 'Left this task', 'eyebrow'));
    const open = !['done', 'retired'].includes(task.state) && task.contract?.kind !== 'review';
    for (const member of departed) {
      const item = node('li'), body = node('div'), mine = inviteState.task === task.id && inviteState.worker === member.worker_id;
      body.append(workerButton(member.worker_id));
      if (member.work_reason) body.append(node('span', 'Left: ' + member.work_reason, 'muted'));
      if (open && !mine) {
        const invite = node('button', 'Invite back', 'small');
        invite.onclick = () => { Object.assign(inviteState, {task: task.id, worker: member.worker_id, draft: '', note: '', error: ''}); taskDetailSignature = ''; renderTaskDetail(); };
        body.append(invite);
      }
      if (mine && (inviteState.note || inviteState.error) && !inviteState.draft && !inviteState.busy) {
        body.append(node('span', inviteState.error || inviteState.note, inviteState.error ? 'request-error' : 'muted'));
      } else if (mine && open) {
        const form = node('form', undefined, 'invite-form'), text = document.createElement('textarea');
        text.id = 'invite-reason'; text.rows = 3; text.maxLength = 4000; text.value = inviteState.draft;
        text.placeholder = 'Why ' + name(member.worker_id) + ' is needed again (it reads this)';
        text.setAttribute('aria-label', 'Reason to invite ' + name(member.worker_id) + ' back');
        text.oninput = () => { inviteState.draft = text.value; send.disabled = inviteState.busy || !text.value.trim(); };
        const send = node('button', inviteState.busy ? 'Inviting…' : 'Invite back', 'small primary'), cancel = node('button', 'Cancel', 'small');
        send.type = 'submit'; cancel.type = 'button';
        send.disabled = inviteState.busy || !inviteState.draft.trim();
        cancel.onclick = () => { Object.assign(inviteState, {task: null, worker: null, draft: ''}); taskDetailSignature = ''; renderTaskDetail(); };
        form.onsubmit = event => { event.preventDefault(); void inviteBack(task.id, member.worker_id); };
        const actions = node('div', undefined, 'row'); actions.append(send, cancel);
        form.append(text, actions);
        if (inviteState.error) form.append(node('p', inviteState.error, 'request-error'));
        body.append(form);
      }
      item.append(node('span', '↩', 'relation-mark'), body);
      list.append(item);
    }
    box.append(list);
    return box;
  }
  async function inviteBack(taskId, workerId) {
    Object.assign(inviteState, {busy: true, error: '', note: ''});
    taskDetailSignature = ''; renderTaskDetail();
    try {
      await command('task.invite_back', {task_id: taskId, worker_id: workerId, reason: inviteState.draft.trim()});
      Object.assign(inviteState, {draft: '', note: name(workerId) + ' is a member again and resumes this task in its next turn.'});
    } catch (error) { inviteState.error = error.message; }
    inviteState.busy = false;
    await taskStore.reload();
    taskDetailSignature = ''; renderTaskDetail();
  }
  function renderTaskDetail() {
    const view = taskStore.detail(), detail = el('task-detail'), id = taskStore.selectedTask;
    el('task-detail-empty').classList.toggle('hidden', Boolean(view));
    if (!view) { detail.classList.add('hidden'); return; }
    detail.classList.remove('hidden');
    const task = view.current, pinned = view.selectedRevision !== null;
    const record = pinned ? view.history.get(view.selectedRevision) : task;
    const signature = JSON.stringify([id, task, view.selectedRevision, record, view.revisions, view.revisionOffset,
      view.currentError, view.historicalError, view.revisionsError, admissionState, view.relations, view.relationsError,
      Boolean(view.historicalLoad), Boolean(view.revisionsLoad), data?.workers.map(w => [w.id, w.display_name]), retired.records.length]);
    if (signature === taskDetailSignature) return;
    taskDetailSignature = signature;
    const sameTask = detail.dataset.task === id;
    const render = () => {
      detail.dataset.task = id;
      detail.replaceChildren();
      const contract = record?.contract, review = (contract || task?.contract)?.kind === 'review';
      detail.append(node('div', (review ? 'Review' : 'Work task') + ' · ' + taskName(id)
        + (pinned ? ' · contract frozen at revision ' + view.selectedRevision : ''), 'eyebrow'));
      detail.append(node('h3', contract?.title || taskTitle(id)));
      if (task) {
        const head = node('div', undefined, 'detail-head');
        head.append(stateChip(task));
        if (task.running && !['done', 'retired'].includes(task.state)) head.append(chip('Running', 'active'));
        if (task.admission?.pinned_at && admissionLabel(task)) head.append(chip('Pinned', 'active'));
        if (task.current_revision > 1 || pinned) head.append(node('span', 'Revision ' + (pinned ? view.selectedRevision + ' of ' : '') + task.current_revision, 'muted'));
        detail.append(head, taskMembers(task));
        const departed = (task.collaboration?.members || []).filter(member => member.executor && member.membership_state === 'left');
        if (departed.length) detail.append(departedMembers(task, departed));
        if (inviteState.task === task.id && inviteState.note && !departed.some(member => member.worker_id === inviteState.worker)) {
          detail.append(node('p', inviteState.note, 'muted'));  // It returned: its row has moved to the executors.
        }
        if (pinned) detail.append(node('p', 'The contract below is frozen at the selected revision. Execution, members, relations and evidence show the current state.', 'muted'));
      }
      if (view.currentError || view.historicalError) {
        detail.append(node('p', 'Task details could not be refreshed: ' + (view.historicalError || view.currentError), 'request-error'));
        const retry = node('button', 'Retry task details');
        retry.onclick = () => taskStore.selectTask(id);
        detail.append(retry);
      }
      if (task) detail.append(relationsSection(view, id));
      if (contract) {
        const objective = node('section', undefined, 'detail-section');
        objective.append(node('h4', 'Objective'), markdown(contract.objective || ''));
        if (contract.context) objective.append(markdownDisclosure('Task context', contract.context, 'task-context'));
        if (contract.constraints?.length) objective.append(disclosure('Acceptance constraints', contract.constraints, 'constraints'));
        if (contract.requirements?.length) objective.append(disclosure('Requirements', contract.requirements, 'requirements'));
        detail.append(objective);
      } else if (!view.historicalError && !view.currentError) detail.append(node('p', 'Loading contract…', 'muted'));
      if (task) {
        const execution = node('section', undefined, 'detail-section');
        execution.append(node('h4', 'Evidence'));
        if (task.current_evidence?.state === 'invalid') {
          execution.append(node('p', 'Historical completion is retained. Its current evidence is no longer valid; the linked repair work has a separate lifecycle.'));
          const repairs = new Set((task.current_evidence.invalidations || []).map(item => item.repair_task_id).filter(Boolean));
          for (const repair of repairs) execution.append(taskButton(repair, 'Repair: ' + taskTitle(repair)));
          execution.append(disclosure('Invalidated evidence', task.current_evidence.invalidations || [], 'invalidated-evidence'));
        } else execution.append(node('p', task.state === 'done' ? 'Completed; its evidence is current.' : 'No completion yet.', 'muted'));
        if (task.repair_of?.task_id) execution.append(taskButton(task.repair_of.task_id, 'Original task: ' + taskTitle(task.repair_of.task_id)));
        detail.append(execution);
        if (admissionLabel(task)) detail.append(admissionControls(task));
      }
      const revisions = node('details'); revisions.dataset.key = 'task-revisions';
      revisions.append(node('summary', 'Contract revisions' + (view.revisions ? ' · ' + view.revisions.total : '')));
      const controls = node('div', undefined, 'row revision-controls');
      const current = node('button', 'Current' + (task ? ' · ' + task.current_revision : ''));
      current.classList.toggle('primary', !pinned);
      current.setAttribute('aria-pressed', String(!pinned));
      current.onclick = () => taskStore.selectRevision(null).catch(() => {});
      controls.append(current);
      for (const revision of view.revisions?.items || []) {
        const button = node('button', 'Revision ' + revision.revision);
        button.title = revision.title;
        button.setAttribute('aria-pressed', String(view.selectedRevision === revision.revision));
        button.classList.toggle('primary', view.selectedRevision === revision.revision);
        button.onclick = () => taskStore.selectRevision(revision.revision).catch(() => {});
        controls.append(button);
      }
      revisions.append(controls);
      const pager = node('div', undefined, 'row revision-controls');
      const newer = node('button', 'Newer'), older = node('button', 'Older');
      newer.disabled = Boolean(view.revisionsLoad) || view.revisionOffset === 0;
      older.disabled = Boolean(view.revisionsLoad) || !view.revisions?.has_more;
      newer.onclick = () => taskStore.loadRevisions(id, Math.max(0, view.revisionOffset - 20)).catch(() => {});
      older.onclick = () => taskStore.loadRevisions(id, view.revisions.next_offset).catch(() => {});
      pager.append(newer, older);
      if (view.revisionsError) {
        pager.append(node('span', view.revisionsError, 'request-error'));
        const retry = node('button', 'Retry revisions');
        retry.onclick = () => taskStore.loadRevisions(id).catch(() => {}); pager.append(retry);
      } else if (view.revisionsLoad) pager.append(node('span', 'Loading revisions…', 'muted'));
      revisions.append(pager);
      const technical = node('section', undefined, 'detail-section');
      technical.append(node('h4', 'History and records'), revisions);
      if (contract) technical.append(disclosure(pinned ? 'Frozen contract · technical details' : 'Contract · technical details',
        {revision: record.revision, contract_hash: record.contract_hash, contract}, 'task-contract'));
      if (task) technical.append(disclosure('Current task record · technical details', task, 'task-record'));
      detail.append(technical);
    };
    // A refresh while the operator types an invitation keeps the text and the cursor.
    const typing = document.activeElement?.id === 'invite-reason' ? document.activeElement.selectionStart : null;
    if (sameTask) preserveDisclosures(detail, render, el('task-detail-pane'));
    else render();
    if (typing !== null && el('invite-reason')) { el('invite-reason').focus(); el('invite-reason').setSelectionRange(typing, typing); }
  }
  // The operator's place for a task in admission: its priority, and a pin to the top. Neither
  // revises the task. The note or error belongs to the task it was set for.
  const admissionState = {task: null, busy: false, note: '', error: ''};
  function admissionControls(task) {
    const admission = task.admission, mine = admissionState.task === task.id;
    const box = node('div', undefined, 'task-admission'), row = node('div', undefined, 'row');
    box.append(node('h4', 'Admission'));
    const priority = document.createElement('select');
    priority.setAttribute('aria-label', 'Priority of this task');
    for (const value of [0, 1, 2, 3]) {
      const option = node('option', 'Priority ' + value + (value === 0 ? ' (first)' : value === 3 ? ' (last)' : ''));
      option.value = String(value); priority.append(option);
    }
    priority.value = String(admission.priority);
    priority.disabled = admissionState.busy;
    priority.onchange = () => { void placeTask(task.id, 'task.set_priority', {priority: Number(priority.value)},
      'Priority ' + priority.value + ': new tasks are taken by it from the next admission.'); };
    row.append(priority);
    if (admission.operator_priority !== null) {
      const reset = node('button', 'Use the contract\'s (' + admission.contract_priority + ')', 'small');
      reset.disabled = admissionState.busy;
      reset.onclick = () => { void placeTask(task.id, 'task.set_priority', {priority: null},
        'The contract\'s priority ' + admission.contract_priority + ' applies again.'); };
      row.append(reset);
    }
    const pin = node('button', admission.pinned_at ? 'Unpin' : 'Pin to top', admission.pinned_at ? 'small' : 'small primary');
    pin.disabled = admissionState.busy;
    pin.onclick = () => { void placeTask(task.id, 'task.set_pinned', {pinned: !admission.pinned_at}, admission.pinned_at
      ? 'Unpinned: it is taken by its priority again.' : 'Pinned to the top: the next free session takes it.'); };
    row.append(pin);
    box.append(row, node('p', admission.pinned_at
      ? 'Pinned to the top since ' + momentLabel(admission.pinned_at) + ': its saved work and its opening for a worker go before every unpinned task\'s, and its reviews before other reviews. Only a due Integrator batch, reviews, resumptions and messages go first; pinned tasks go in the order they were pinned.'
      : 'Saved work and new tasks are taken by priority, 0 first, then the earliest published; '
        + (admission.operator_priority === null ? 'this is the contract\'s priority.' : 'the contract asks for ' + admission.contract_priority + '.')
        + ' Pin to top puts it before every unpinned task. Neither revises the task.', 'muted'));
    if (mine && admissionState.busy) box.append(node('p', 'Saving…', 'muted'));
    if (mine && admissionState.note) box.append(node('p', admissionState.note, 'muted'));
    if (mine && admissionState.error) box.append(node('p', admissionState.error, 'request-error'));
    return box;
  }
  async function placeTask(id, name, payload, done) {
    Object.assign(admissionState, {task: id, busy: true, note: '', error: ''});
    renderTaskDetail();
    try {
      await command(name, {task_id: id, ...payload});
      admissionState.note = done;
    } catch (error) { admissionState.error = error.message; }
    admissionState.busy = false;
    await taskStore.reload();
    renderTasks(); renderTaskDetail();
  }
  function setupOptions(select, choices, value, placeholder) {
    const signature = JSON.stringify([choices, value, placeholder]);
    if (select.dataset.options === signature) return;
    select.dataset.options = signature;
    select.replaceChildren();
    if (!choices.some(choice => choice.id === value)) {
      const missing = node('option', value ? value + ' (unavailable)' : placeholder);
      missing.value = value; missing.disabled = true; select.append(missing);
    }
    for (const choice of choices) {
      const option = node('option', choice.name || choice.id);
      option.value = choice.id; select.append(option);
    }
    select.value = value;
  }

  function renderSetup() {
    if (!el('setup-dialog').open) return;
    const snapshot = setupStore.snapshot, catalog = setupStore.catalog, selectedModel = setupStore.selectedModel();
    const configurable = Boolean(snapshot?.can_configure), editable = setupStore.editable;
    const busy = Boolean(setupStore.submission || setupStore.driveLoad);
    el('setup-summary').textContent = !snapshot ? 'Loading project status…' : !configurable
      ? snapshot.reason || 'Project setup is read-only. Use the existing task and run controls.'
      : snapshot.task ? 'Task 0 is ready. Choose the agents\' models and sessions below, then Start.'
      : 'Describe the goal for Task 0, choose the agents\' models and sessions below, then Start.';
    el('setup-repository').textContent = !snapshot ? '' : snapshot.repository.enabled
      ? snapshot.repository.initialized ? 'Workers use this project\'s local managed Git repository.'
        : 'Starting will initialize a local managed Git repository in this project\'s folder.'
      : configurable ? 'Starting prepares a local review repository and starts the project run.'
        : 'The local review repository is not enabled.';
    const goal = el('setup-goal');
    if (goal.value !== setupStore.goal) goal.value = setupStore.goal;
    goal.readOnly = !editable || Boolean(snapshot?.task);
    el('setup-goal-help').textContent = snapshot?.task ? 'Saved Task 0 goal · revision ' + snapshot.task.revision
      : setupStore.goal.length.toLocaleString() + ' / 20,000 characters.';
    // The model and effort are chosen in Agents and sessions, on its Worker · Codex row.
    const described = [selectedModel?.description,
      selectedModel?.efforts.find(effort => effort.id === setupStore.effort)?.description].filter(Boolean).join(' · ');
    el('setup-model-state').textContent = configurable ? setupStore.modelsLoad ? 'Loading models from the configured worker profile…'
      : setupStore.selectionIssue ? 'Worker · Codex: ' + setupStore.selectionIssue : described : '';
    el('setup-error').textContent = [setupStore.stateError && 'Project status could not be refreshed: ' + setupStore.stateError,
      setupStore.modelsError && 'Models could not be loaded: ' + setupStore.modelsError,
      setupStore.submitError, setupStore.driveError].filter(Boolean).join('\n');
    const result = setupStore.result;
    el('setup-result').textContent = result ? result.driving ? 'Project start accepted. Follow the workers for progress.'
      : snapshot?.can_drive ? 'Setup is saved. Start the prepared run when you are ready.'
        : 'Project setup is saved. Current run state: ' + (snapshot?.run?.state || 'Refreshing…') + '.' : '';
    el('setup-refresh').disabled = Boolean(setupStore.stateLoad || busy);
    el('setup-refresh').textContent = setupStore.stateLoad ? 'Refreshing…' : 'Refresh status';
    el('setup-model-retry').classList.toggle('hidden', !configurable || !(setupStore.modelsError || setupStore.selectionIssue || !catalog));
    el('setup-model-retry').disabled = Boolean(setupStore.modelsLoad || busy);
    el('setup-start').classList.toggle('hidden', Boolean(snapshot && !configurable));
    el('setup-start').disabled = Boolean(setupStore.startBlockedReason);
    el('setup-start').title = setupStore.startBlockedReason;
    el('setup-start').textContent = setupStore.submission ? 'Starting…' : setupStore.pending ? 'Retry Start' : 'Start';
    el('setup-drive').classList.toggle('hidden', !snapshot?.can_drive && !setupStore.driveLoad);
    el('setup-drive').disabled = !setupStore.canDrive || busy;
    el('setup-drive').textContent = setupStore.driveLoad ? 'Starting…' : 'Start prepared run';
    renderAgentSettings();  // Before the first run, its Worker · Codex row is this choice.
  }

  // Choices for conversations that begin later and each account's session limit. Read when
  // the dialog opens, never while polling; saving sends only what changed.
  const agents = {status: null, catalog: null, draft: null, load: null, saving: false, error: '', state: ''};
  function loadAgentSettings() {
    if (agents.load) return agents.load;
    agents.load = (async () => {
      try {
        const [status, catalog] = await Promise.all([api('/v1/runtime/backend'),
          api('/v1/setup/models').catch(error => ({models: [], error: error.message}))]);
        agents.status = status; agents.catalog = catalog; agents.draft = agentDraft(status); agents.error = '';
      } catch (error) { agents.error = 'Settings could not be loaded: ' + error.message; }
      finally { agents.load = null; renderAgentSettings(); }
    })();
    renderAgentSettings();
    return agents.load;
  }
  function agentOptions(select, values, value, empty) {
    const choices = [...values];
    if (value && !choices.some(choice => choice.id === value)) choices.unshift({id: value, name: value + ' (current)'});
    setupOptions(select, choices, value, empty);
  }
  function codexEfforts(model) {
    return (agents.catalog?.models.find(item => item.id === model)?.efforts || []).map(effort => ({id: effort.id}));
  }
  // Before the first run (Project setup can still configure it), the Worker · Codex row is the
  // project's own model and effort, which Start saves in project.toml.
  function settingUp() { return Boolean(setupStore.snapshot?.can_configure); }
  function renderSetupChoice() {
    if (!settingUp()) return;
    const catalog = setupStore.catalog, selected = setupStore.selectedModel();
    const model = el('agent-worker-codex-model'), effort = el('agent-worker-codex-effort');
    setupOptions(model, catalog ? catalog.models : setupStore.model ? [{id: setupStore.model}] : [], setupStore.model,
      setupStore.modelsLoad ? 'Loading models…' : 'Choose a model');
    setupOptions(effort, selected?.efforts || [], setupStore.effort, 'Choose an effort');
    model.disabled = !setupStore.editable || !catalog
      || Boolean(setupStore.modelsLoad || setupStore.modelsError || catalog.authentication_required);
    effort.disabled = model.disabled || !selected;
  }
  function renderAgentSettings() {
    const setup = settingUp(), section = el('agent-settings'), visible = Boolean(data?.runs?.length) || setup;
    section.classList.toggle('hidden', !visible);
    if (!visible || !el('setup-dialog').open) return;
    const status = agents.status, draft = agents.draft, ready = Boolean(status && draft) && !agents.saving;
    for (const control of section.querySelectorAll('select, input')) control.disabled = !ready;
    el('agent-settings-setup').classList.toggle('hidden', !setup);
    // No worker exists yet to keep earlier choices.
    for (const [role, backend] of AGENT_ROWS) el('agent-' + role + '-' + backend + '-existing-row').classList.toggle('hidden', setup);
    renderSetupChoice();
    el('agent-settings-save').disabled = !ready;
    el('agent-settings-reload').disabled = Boolean(agents.load || agents.saving);
    el('agent-settings-state').textContent = agents.load ? 'Loading…' : agents.saving ? 'Saving…' : agents.state
      || (agents.catalog?.error ? 'The Codex model list is unavailable (' + agents.catalog.error + '); current choices are shown.' : '');
    el('agent-settings-error').textContent = agents.error;
    // Each role's model and effort, and the answer deadline: a run starts only once all are chosen.
    const missing = status?.missing_choices || [];
    el('agent-settings-missing').textContent = missing.length ? 'Still to choose before a run starts: ' + missing.join('; ') + '.' : '';
    if (!status || !draft) return;
    el('agent-next-backend').value = draft.nextBackend;
    el('agent-next-source').textContent = {operator: 'your choice', plan: 'from the assignment plan', config: 'project default'}[status.new_worker_source] || '';
    const catalog = (agents.catalog?.models || []).map(model => ({id: model.id, name: model.name || model.id}));
    for (const key of ['worker_codex', 'review_codex', 'integrator_codex']) {
      if (setup && key === 'worker_codex') continue;  // Project setup's choice (renderSetupChoice).
      const value = draft.models[key], prefix = 'agent-' + key.replace('_', '-');
      agentOptions(el(prefix + '-model'), catalog, value.model, 'Choose a model');
      agentOptions(el(prefix + '-effort'), codexEfforts(value.model), value.effort, 'Choose an effort');
    }
    const claude = draft.models.worker_claude;
    if (el('agent-worker-claude-model').value !== claude.model) el('agent-worker-claude-model').value = claude.model;
    agentOptions(el('agent-worker-claude-effort'), CLAUDE_EFFORTS.map(id => ({id})), claude.effort, 'Choose an effort');
    for (const [role, backend] of AGENT_ROWS) {
      const prefix = 'agent-' + role + '-' + backend, kept = status.role_models?.[role]?.[backend]?.kept_by_existing || 0;
      el(prefix + '-existing').checked = draft.models[role + '_' + backend].existing;
      el(prefix + '-kept').textContent = kept ? kept + ' existing keep' + (kept === 1 ? 's' : '') + ' earlier choices' : '';
    }
    agentOptions(el('agent-dispatcher-model'), catalog, draft.dispatcher.model, 'Choose a model');
    agentOptions(el('agent-dispatcher-effort'), codexEfforts(draft.dispatcher.model).filter(effort => CLAUDE_EFFORTS.includes(effort.id)),
      draft.dispatcher.effort, 'Choose an effort');
    agentOptions(el('agent-waker-model'), catalog, draft.waker.model, 'Choose a model');
    agentOptions(el('agent-waker-effort'), codexEfforts(draft.waker.model).filter(effort => CLAUDE_EFFORTS.includes(effort.id)),
      draft.waker.effort, 'Choose an effort');
    const deadline = el('agent-answer-deadline');
    if (deadline.value !== draft.deadline) deadline.value = draft.deadline;
    el('agent-answer-deadline-source').textContent = {operator: 'your choice', config: 'project.toml'}[status.answer_deadline?.source] || 'not chosen';
    for (const backend of ['codex', 'claude']) {
      const input = el('agent-sessions-' + backend), session = status.sessions?.[backend] || {};
      if (input.value !== draft.sessions[backend]) input.value = draft.sessions[backend];
      el('agent-sessions-' + backend + '-live').textContent = (session.live || 0) + ' running'
        + (session.source === 'operator' ? ' · your limit' : ' · project limit');
      const idle = el('agent-idle-' + backend), rotation = status.rotation?.idle_by_backend?.[backend] || {};
      if (idle.value !== draft.idle[backend]) idle.value = draft.idle[backend];
      el('agent-idle-' + backend + '-source').textContent = (rotation.minutes === 0 ? 'off · ' : '')
        + (rotation.source === 'operator' ? 'your choice' : 'project default');
    }
    for (const [key, field] of [['delay', 'delay_minutes'], ['threshold', 'threshold']]) {
      const input = el('agent-batch-' + key);
      if (input.value !== draft.batch[key]) input.value = draft.batch[key];
      el('agent-batch-' + key + '-source').textContent =
        status.integrator_batch?.source?.[field] === 'operator' ? 'your choice' : 'default';
    }
  }

  // The Claude and Codex tabs: each read when it opens and again at most every 30 s while it stays
  // open; the countdowns move with every overview refresh.
  function profileTab(backend, label) {
    const tab = {backend, view: null, loadedAt: 0, load: null, busy: false, error: '', state: ''};
    const part = name => el(backend + '-' + name);
    tab.reload = () => {
      if (tab.load) return tab.load;
      tab.load = (async () => {
        try { tab.view = await api('/v1/runtime/' + backend + '-profiles'); tab.error = ''; tab.loadedAt = Date.now(); }
        catch (error) { tab.error = label + ' profiles could not be loaded: ' + error.message; }
        finally { tab.load = null; tab.render(); }
      })();
      return tab.load;
    };
    tab.open = () => {
      tab.state = '';
      void tab.reload();
      tab.render();
      if (!part('dialog').open) part('dialog').showModal();
    };
    tab.render = () => {
      const view = tab.view, now = Date.now(), list = part('profile-list'), history = part('history');
      part('held').classList.toggle('hidden', !view?.held_until && !view?.hold);
      if (view?.held_until) {
        part('held').textContent = 'Every profile in the rotation is at its limit: ' + label + ' sessions start again at '
          + momentLabel(view.held_until) + ', in ' + durationLabel(view.held_until - now) + ', without Resume.';
      } else if (view?.hold) {
        part('held').textContent = view.hold.message;
      }
      part('state').textContent = tab.load && !view ? 'Loading…' : tab.busy ? 'Saving…' : tab.state;
      part('error').textContent = tab.error;
      list.replaceChildren();
      const rows = profileRows(view, now);
      rows.forEach((row, index) => {
        const state = node('span', row.status, row.attention ? '' : 'muted');
        for (const line of row.usage) state.append(node('span', line, 'profile-usage'));
        if (row.last_limited) state.append(node('span', row.last_limited, 'profile-usage muted'));
        state.append(node('code', row.home, 'profile-home muted'));
        const button = node('button', 'Use this profile', 'small');
        button.disabled = !row.usable || tab.busy;
        button.title = row.selected ? 'Clears its recorded limit: ' + label + ' conversations use it again from their next activation.'
          : 'Puts it first in the priority: ' + label + ' conversations use it from their next activation.';
        // The order of preference: the first usable profile in it is used.
        const order = node('span', undefined, 'profile-order');
        for (const [text, target, title] of [['↑', index - 1, 'Earlier in the priority'], ['↓', index + 1, 'Later in the priority']]) {
          const move = node('button', text, 'small');
          move.disabled = tab.busy || target < 0 || target >= rows.length;
          move.title = title;
          move.setAttribute('aria-label', title + ': ' + row.name);
          move.onclick = () => { void reorder(rows.map(item => item.name), index, target); };
          order.append(move);
        }
        button.onclick = () => { void use(row.name); };
        const tick = document.createElement('input');
        tick.type = 'checkbox';
        tick.checked = row.rotation;
        tick.disabled = tab.busy;
        tick.onchange = () => { void setRotation(row, tick.checked); };
        const name = node('label', '', 'profile-name');
        name.title = 'In the rotation: ' + label + ' may switch to it';
        name.append(tick, (row.priority ? row.priority + '. ' : '') + row.name);
        list.append(name, state, order, button);
      });
      history.replaceChildren();
      const entries = profileHistoryRows(view, label);
      if (!entries.length) history.append(node('span', 'No limit or switch recorded yet.', 'muted'));
      for (const row of entries) history.append(node('span', row.when, 'muted'), node('span', row.text));
    };
    // The header's badge, and the tab while it is open.
    tab.renderHeader = summary => {
      const badge = profileBadge(summary, label), profile = part('profile');
      profile.classList.toggle('hidden', !badge);
      if (badge) { profile.textContent = badge.text; profile.classList.toggle('held', badge.held); }
      el('show-' + backend).disabled = !summary;
      if (part('dialog').open) {
        if (Date.now() - tab.loadedAt > 30000) void tab.reload();
        tab.render();
      }
    };
    async function setRotation(row, included) {
      if (!included && row.selected && !confirm('Take ' + row.name + ', the profile in use, out of the rotation? ' + label + ' conversations, '
          + 'new and continued, move to the next usable ticked profile from their next activation; with none, ' + label + ' sessions wait.')) {
        tab.render();
        return;
      }
      tab.busy = true; tab.error = ''; tab.render();
      try {
        await command('runtime.set_' + backend + '_profile_rotation', {profile: row.name, in_rotation: included});
        tab.state = row.name + (included ? ' is in the rotation.' : ' is out of the rotation: ' + label + ' never switches to it.');
      } catch (error) { tab.error = error.message; }
      tab.busy = false;
      await tab.reload();
      await refresh();
    }
    async function reorder(names, from, to) {
      const order = [...names];
      [order[from], order[to]] = [order[to], order[from]];
      tab.busy = true; tab.error = ''; tab.render();
      try {
        await command('runtime.set_' + backend + '_profile_priority', {order});
        tab.state = 'Priority: ' + order.join(' → ') + '. ' + label + ' conversations use the first usable one from their next activation.';
      } catch (error) { tab.error = error.message; }
      tab.busy = false;
      await tab.reload();
      await refresh();
    }
    async function use(profile) {
      if (!confirm('Use the ' + label + ' profile ' + profile + '? It is put first in the priority and in the rotation, and a limit recorded for it is cleared; ' + label + ' conversations, new and continued, use it from their next activation.')) return;
      tab.busy = true; tab.error = ''; tab.render();
      try {
        await command('runtime.select_' + backend + '_profile', {profile});
        tab.state = label + ' conversations use ' + profile + ' from their next activation.';
      } catch (error) { tab.error = error.message; }
      tab.busy = false;
      await tab.reload();
      await refresh();
    }
    return tab;
  }
  const claudeTab = profileTab('claude', 'Claude'), codexTab = profileTab('codex', 'Codex');
  function editAgents(change) {
    if (!agents.draft) return;
    change(agents.draft); agents.state = ''; agents.error = ''; renderAgentSettings();
  }
  // Whether everything changed was saved. Before the first run the Worker · Codex row is not a
  // role choice: Start saves it as the project's own (setupStore).
  async function saveAgentSettings({starting = false} = {}) {
    let commands;
    const draft = settingUp() ? {...agents.draft, models: {...agents.draft.models,
      worker_codex: agentDraft(agents.status).models.worker_codex}} : agents.draft;
    try { commands = agentSettingCommands(agents.status, draft); }
    catch (error) { agents.error = error.message; renderAgentSettings(); return false; }
    if (!commands.length) {
      if (!starting) agents.state = 'Nothing has changed.';
      renderAgentSettings(); return true;
    }
    agents.saving = true; agents.error = ''; renderAgentSettings();
    let saved = 0, failure = '';
    try {
      for (const [commandName, payload] of commands) { await command(commandName, payload); saved += 1; }
    } catch (error) {
      failure = (saved ? saved + ' of ' + commands.length + ' changes were saved. ' : '') + error.message;
    }
    agents.saving = false;
    const edited = agents.draft;
    await loadAgentSettings();
    if (failure) { agents.error = failure; agents.draft = edited; }  // Keep what was not saved.
    else agents.state = starting ? 'Saved; starting the project…'
      : 'Saved. Conversations that begin from now on use these; a new model or effort reaches existing workers only where you ticked “Also existing”.';
    renderAgentSettings();
    return !failure;
  }

  // Start saves what changed in Agents and sessions, then starts the project with the Worker ·
  // Codex row as its own model and effort. A save that fails starts nothing.
  async function startSetup() {
    if (agents.saving) return;
    if (!setupStore.startBlockedReason && agents.status && agents.draft && !await saveAgentSettings({starting: true})) return;
    await setupStore.start().then(finishSetup).catch(() => {});
  }

  async function finishSetup(result, projectId = setupStore.snapshot?.project_id) {
    if (!result) return;
    void loadAgentSettings();  // The project's own model and effort may have changed.
    await refresh();
    // A start must never take someone away from the history they are reading.
    if (selected || !el('setup-dialog').open || !result.driving || setupStore.snapshot?.project_id !== projectId) return;
    const initial = data?.workers.find(item => item.current_task_id === result.task_id);
    if (initial) { el('setup-dialog').close(); selectWorker(initial.id); }
  }

  async function refreshLibraryStats() {
    if (libraryStats.inflight) return;
    const now = Date.now();
    if (libraryStats.attemptedAt !== null && now - libraryStats.attemptedAt < 60000) return;
    libraryStats.attemptedAt = now;
    libraryStats.inflight = api('/v1/library/stats');
    try {
      const value = await libraryStats.inflight;
      if (!Number.isSafeInteger(value.lean_lines) || value.lean_lines < 0 ||
          !Number.isSafeInteger(value.lean_files) || value.lean_files < 0) {
        throw Error('Invalid Library statistics');
      }
      libraryStats.value = value; libraryStats.error = '';
    } catch (error) {
      libraryStats.error = error.message;
    } finally {
      libraryStats.inflight = null;
      {
        const target = el('library-stats'), value = libraryStats.value;
        // The service counts every hour on the hour and serves its last count; its `error` says
        // the latest count failed, so the count shown is older.
        const failed = libraryStats.error ? 'Refresh failed: ' + libraryStats.error + '. Retrying every minute.'
          : typeof value?.error === 'string' ? 'The latest hourly count failed: ' + value.error + '.' : '';
        target.textContent = value
          ? 'Library · ' + value.lean_lines.toLocaleString() + ' Lean lines' + (failed ? ' · stale' : '')
          : 'Library · Lean lines unavailable';
        target.title = (value ? value.lean_files.toLocaleString() + ' published Lean files. ' : '') +
          'Includes comments and blank lines; excludes drafts, old snapshots and dependency caches. ' +
          (Number.isSafeInteger(value?.counted_at)
            ? 'Counted ' + new Date(value.counted_at).toLocaleString() + '; recounted every hour on the hour. ' : '') +
          (failed || 'Checked ' + new Date(now).toLocaleTimeString() + '.');
      }
    }
  }

  // The header's badges (the Codex and Claude profiles in use) and the tabs, which a project
  // without a backend's profiles does not offer.
  function renderProfiles() {
    for (const tab of [claudeTab, codexTab]) tab.renderHeader(data[tab.backend + '_profile']);
  }
  function renderStatus() {
    const status = projectStatus(data), run = selectedRun(data.runs), bar = el('status-bar');
    bar.classList.remove('hidden', 'active', 'attention', 'idle');
    bar.classList.add(status.tone);
    el('status-text').textContent = status.text;
    const signature = JSON.stringify([status.actions, run?.id]);
    if (bar.dataset.actions === signature) return;
    bar.dataset.actions = signature;
    const actions = el('status-actions');
    actions.replaceChildren();
    for (const kind of status.actions) {
      const pausing = kind === 'pause' || kind === 'pause_after_turn';
      const button = node('button', {setup: 'Open project setup', start: 'Start', pause: 'Pause all now',
        pause_after_turn: 'Pause all after turn', resume: 'Resume all'}[kind], pausing ? '' : 'primary');
      if (kind === 'pause') button.title = 'Interrupts turns in progress.';
      if (kind === 'pause_after_turn') button.title = 'Turns in progress finish on their own; nothing new starts.';
      button.onclick = async () => {
        if (kind === 'setup') { el('show-setup').click(); return; }
        // Pause all now interrupts running turns; Pause all after turn, an account hold or Start never does.
        if (kind === 'pause' && !confirm('Pause everyone now? Turns in progress are interrupted, and no new turns start until you resume.')) return;
        if (kind === 'pause_after_turn' && !confirm('Pause everyone after their turns? Turns in progress finish on their own, and no new turns start until you resume.')) return;
        button.disabled = true;
        try {
          if (kind === 'pause') await command('run.pause', {run_id: run.id});
          if (kind === 'pause_after_turn') await command('run.pause_after_turn', {run_id: run.id});
          if (kind === 'resume') await command('run.resume', {run_id: run.id});
          if (!pausing) await api('/v1/runs/' + encodeURIComponent(run.id) + '/drive', {});
          bar.dataset.actions = '';
          await refresh();
        } catch (error) { notice(error.message); button.disabled = false; }
      };
      actions.append(button);
    }
  }
  // The run controls console: Everyone and each group, paused after turn or resumed apart.
  function renderControls() {
    const run = selectedRun(data?.runs || []), rows = controlConsole(run, data?.workers || []), list = el('controls-list');
    const signature = JSON.stringify(rows);
    if (list.dataset.rows === signature) return;
    list.dataset.rows = signature;
    list.replaceChildren();
    if (!rows.length) { list.append(node('p', 'No running or paused run.', 'muted')); return; }
    for (const row of rows) {
      const actions = node('div', '', 'row');
      for (const kind of row.actions) {
        const everyone = row.key === 'run';
        const button = node('button', {pause_after_turn: 'Pause after turn', pause: 'Pause now', resume: 'Resume'}[kind],
          kind === 'resume' ? 'primary small' : 'small');
        button.title = kind === 'pause' ? 'Interrupts turns in progress.'
          : kind === 'pause_after_turn' ? 'Turns in progress finish on their own; nothing new starts for them.'
          : everyone ? 'Everyone runs again; every group control ends.' : 'This group runs, new work and new members included, also while others are paused.';
        button.onclick = async () => {
          const who = everyone ? 'everyone' : row.label;
          if (kind === 'pause' && !confirm('Pause everyone now? Turns in progress are interrupted, and no new turns start until you resume.')) return;
          if (kind === 'pause_after_turn' && !confirm('Pause ' + who + ' after their turns? Turns in progress finish on their own, and no new turns start for them until you resume.')) return;
          button.disabled = true;
          try {
            if (everyone) await command({pause: 'run.pause', pause_after_turn: 'run.pause_after_turn', resume: 'run.resume'}[kind], {run_id: run.id});
            else await command(kind === 'resume' ? 'group.resume' : 'group.pause_after_turn', {group: row.key, run_id: run.id});
            if (kind === 'resume') await api('/v1/runs/' + encodeURIComponent(run.id) + '/drive', {});
            el('status-bar').dataset.actions = '';
            list.dataset.rows = '';
            await refresh();
          } catch (error) { notice(error.message); button.disabled = false; }
        };
        actions.append(button);
      }
      const state = node('span', row.text, 'muted');
      state.classList.toggle('control-line', row.state !== 'running');
      list.append(node('span', row.label, 'agent-role'), state, actions);
    }
  }
  function bytesLabel(value) {
    return value >= 2 ** 30 ? (value / 2 ** 30).toFixed(1) + ' GiB' : Math.max(1, Math.round(value / 2 ** 20)) + ' MiB';
  }
  // The Lean service's checks in its scheduler's order: what runs, what waits, and for how long.
  function renderLean() {
    const service = data.lean_resources, list = el('lean-list'), stats = el('lean-stats');
    // Not a Lean project, or a TaskHub older than the Lean service (it reports no queue).
    const shown = Boolean(service) && (service.running === false || Array.isArray(service.jobs));
    el('lean-group').classList.toggle('hidden', !shown);
    stats.classList.toggle('hidden', !shown);
    if (!shown) return;
    list.replaceChildren();
    if (service.running === false) {
      el('lean-count').textContent = '(off)';
      stats.textContent = 'Lean service · off';
      list.append(node('p', service.error || 'Not running. It starts with the next Lean check.', 'muted group-empty'));
      return;
    }
    // Ages count from when this status arrived, so an unchanged overview keeps them right.
    if (leanClock.status !== service) Object.assign(leanClock, {status: service, at: Date.now()});
    const jobs = service.jobs || [], age = job => elapsedLabel(leanClock.at - job.seconds * 1000);
    const running = jobs.filter(job => job.state === 'running'), waiting = jobs.filter(job => job.state === 'queued');
    const suspended = jobs.filter(job => job.state === 'suspended');
    const summary = jobs.length ? [running.length + ' running', waiting.length + ' waiting',
      ...(suspended.length ? [suspended.length + ' suspended'] : [])].join(' · ') : 'idle';
    el('lean-count').textContent = '(' + (jobs.length ? jobs.length + (jobs.length === 1 ? ' check' : ' checks') : 'idle') + ')';
    stats.textContent = 'Lean service · ' + summary;
    const sessions = service.sessions || [];
    list.append(node('p', [...(service.threads == null ? [] : [service.threads_in_use + ' of ' + service.threads + ' threads in use']),
      ...(service.memory_available == null ? [] : [bytesLabel(service.memory_available) + ' free']),
      ...(sessions.length ? [sessions.length + ' open Lean session' + (sessions.length === 1 ? '' : 's')] : [])].join(' · '),
      'muted group-empty'));
    const lane = job => ({official: 'Validation', interactive: 'Dev check', integrator: 'Integration'}[job.lane] || job.lane)
      + (job.blocking ? ', priority' : '');
    const usage = job => [job.threads ? job.threads + (job.threads === 1 ? ' thread' : ' threads') : '',
      job.private_memory ? bytesLabel(job.private_memory) : ''].filter(Boolean).join(' · ');
    const group = (title, items, detail, emptyText, limit = 40) => {
      list.append(node('h3', title + ' (' + items.length + ')', 'eyebrow queue-heading'));
      for (const [index, job] of items.slice(0, limit).entries()) {
        const row = node('div', undefined, 'worker'), lines = [job.label, detail(job, index), usage(job)].filter(Boolean);
        row.title = lines.join('\n') + (job.blocking ? '\nA review or a publication waits for it, so it runs before other validations.' : '');
        row.append(worker(job.owner) ? workerButton(job.owner)
          : node('span', {operator: 'You', integrator: 'Integrator', validation: 'Validation'}[job.owner] || job.owner || 'Unknown'),
          ...lines.map(line => node('span', line, 'muted')));
        list.append(row);
      }
      if (items.length > limit) list.append(node('p', (items.length - limit) + ' more ' + title.toLowerCase(), 'muted group-empty'));
      if (!items.length && emptyText) list.append(node('p', emptyText, 'muted group-empty'));
    };
    group('Running', running, job => lane(job) + ' · running ' + age(job), 'No check is running.');
    group('Waiting', waiting, (job, index) => '#' + (index + 1) + ' · ' + lane(job) + ' · waiting ' + age(job),
      'No check is waiting.');
    if (suspended.length) group('Suspended', suspended, job => lane(job) + ' · started ' + age(job) + ' ago');
  }
  function updateRequestAuthors(target, workers) {
    target.replaceChildren();
    (workers || []).forEach((id, index) => { if (index) target.append(' · '); target.append(workerButton(id)); });
  }
  function requestAge(request) {
    return request.created_at ? elapsedLabel(request.created_at) + ' ago' : '';
  }
  function renderQueue() {
    const queue = data.integration_queue, list = el('queue-list');
    el('queue-group').classList.toggle('hidden', !queue);
    if (!queue) return;
    const reviews = queue.reviews || [];
    const isHolding = item => queue.mode === 'batch' ? item.holding : item.review_task_id === queue.holder?.review_task_id;
    const active = reviews.filter(isHolding);
    // Presentation order must not move suspended earlier acceptances above the holder.
    // Preserve the scheduler's positions within the waiting group.
    const waiting = reviews.filter(item => !isHolding(item)).sort((a, b) =>
      (a.position ?? Infinity) - (b.position ?? Infinity) || a.accepted_at - b.accepted_at);
    const pending = new Set(reviews.map(item => item.reviewer_id));
    const completed = queueHistory.publications.filter(item => !pending.has(item.reviewer_id))
      .slice().sort((a, b) => b.created_at - a.created_at);
    el('queue-count').textContent = '(' + reviews.length + ' unpublished)';
    list.replaceChildren();
    const group = (title, items, render, emptyText) => {
      list.append(node('h3', title + ' (' + items.length + ')', 'eyebrow queue-heading'));
      for (const item of items) {
        const row = node('div', undefined, 'worker');
        if (item.review_task_id) row.title = taskTitle(item.review_task_id);
        row.append(workerButton(item.integrator_id || item.reviewer_id), node('span', render(item), 'muted'));
        if (item.integrator_id) row.append(node('span', 'Reviewed by ' + name(item.reviewer_id), 'muted'));
        if (item.deferred_reason) {
          row.append(node('span', 'Deferred ' + elapsedLabel(item.deferred_at) + ' ago', 'muted'));
          const retry = node('button', 'Retry integration');
          retry.onclick = async () => {
            const reason = window.prompt('What changed or what evidence resolves this blocker?');
            if (!reason?.trim()) return;
            retry.disabled = true;
            try {
              await command('integrator.retry', {candidate_id: item.candidate_id, reason: reason.trim()});
              retry.textContent = 'Requeued';
            } catch (error) {
              row.append(node('span', error.message, 'request-error'));
              retry.disabled = false;
            }
          };
          row.append(retry);
        }
        list.append(row);
      }
      if (!items.length) list.append(node('p', emptyText, 'muted group-empty'));
    };
    group('In progress', active, () => 'Integrating · ' + elapsedLabel(queue.holder.granted_at),
      'No integration is running.');
    group('Waiting', waiting, item => '#' + item.position + ' · ' + (item.deferred_reason ? 'Deferred: ' + item.deferred_reason : item.validation_reason || (item.can_run ? 'ready' : 'waiting'))
      + (item.validation_retry_at ? ' · local retry after ' + new Date(item.validation_retry_at).toLocaleTimeString() : '')
      + ' · accepted ' + elapsedLabel(item.accepted_at) + ' ago', 'No accepted review is waiting.');
    group('Completed', completed, item => 'Published · ' + elapsedLabel(item.created_at) + ' ago',
      queueHistory.loaded ? 'No published integrations.' : 'Loading publication history…');
    if (queueHistory.loaded && queueHistory.publications.length >= 20) {
      list.append(node('p', 'Latest 20 publications; older reviews remain in task history.', 'muted group-empty'));
    }
    if (queue.mode === 'batch') list.append(node('p', 'One integrator. A fixed batch starts at ' + queue.threshold + ' ready contributions, or ' + queue.delay_minutes + ' minutes after the previous batch of mathematics finishes. Deferred items require an explicit retry by the integrator or you, with a reason explaining what changed.', 'muted group-empty'));
    if (queueHistory.error) list.append(node('p', queueHistory.error, 'muted group-empty'));
  }
  async function refreshQueueHistory() {
    const queue = data?.integration_queue;
    if (!queue || queueHistory.inflight) return;
    // A publication removes its review from this list. Fetch the small, existing
    // publication endpoint on membership changes, not on every dashboard poll.
    const key = JSON.stringify((queue.reviews || []).map(item => item.review_task_id).sort());
    if (queueHistory.loaded && queueHistory.key === key && !queueHistory.error) return;
    queueHistory.inflight = api('/v1/repository');
    try {
      const result = await queueHistory.inflight;
      queueHistory.publications = result.publications || [];
      queueHistory.loaded = true; queueHistory.key = key; queueHistory.error = '';
    } catch {
      queueHistory.error = 'Publication history could not refresh; retrying automatically.';
    } finally {
      queueHistory.inflight = null;
      renderQueue();
    }
  }
  function updateRequestCard(card, request) {
    card.querySelector('.request-title').textContent = request.title;
    card.querySelector('.request-text').textContent = request.text;
    card.querySelector('.request-age').textContent = requestAge(request);
    updateRequestAuthors(card.querySelector('.request-workers'), request.workers);
  }
  // The same request can be open in the Requests dialog and on its author's page:
  // both cards share one unsent draft.
  function requestCard(request) {
    const card = node('article', undefined, 'request'), literature = request.kind === 'literature';
    const meta = node('div', undefined, 'row request-meta'), authors = node('span', undefined, 'muted request-workers');
    updateRequestAuthors(authors, request.workers);
    meta.append(node('span', literature ? 'Source needed' : 'Question', 'badge attention'), node('span', 'from', 'muted'),
      authors, node('span', requestAge(request), 'muted request-age'));
    card.append(meta, node('strong', request.title, 'request-title'), node('p', request.text, 'request-text'));
    const reply = node('textarea');
    reply.placeholder = literature ? 'A short note for the worker' : 'Your answer';
    reply.setAttribute('aria-label', 'Reply to ' + request.title);
    const path = node('input');
    path.placeholder = 'For example C:\\papers\\henniart-2000.pdf';
    path.setAttribute('aria-label', 'Literature file or folder path');
    const draft = requestDrafts.get(request.id) || {reply: '', path: ''};
    reply.value = draft.reply; path.value = draft.path;
    const save = () => {
      requestDrafts.set(request.id, {reply: reply.value, path: path.value});
      for (const other of [requestViews.get(request.id), pinnedViews.get(request.id)]) {
        if (!other || other === card) continue;
        other.querySelector('textarea').value = reply.value;
        const otherPath = other.querySelector('input');
        if (otherPath) otherPath.value = path.value;
      }
    };
    reply.oninput = save; path.oninput = save;
    if (literature) card.append(node('label', 'Source file or folder on this computer', 'small'), path);
    card.append(reply);
    const errorLine = node('p', undefined, 'request-error'), actions = node('div', undefined, 'row');
    let requestKey = null;
    for (const [text, outcome] of [['Reply', 'pending'], ['Answer and close', 'fulfilled'], ['Decline', 'declined']]) {
      const button = node('button', text);
      button.onclick = async () => {
        const submitted = reply.value, submittedPath = path.value;
        const fingerprint = JSON.stringify([outcome, submitted, submittedPath]);
        if (!requestKey || requestKey.fingerprint !== fingerprint) requestKey = {id: crypto.randomUUID(), fingerprint};
        for (const control of actions.children) control.disabled = true;
        errorLine.textContent = '';
        try {
          const payload = requestResolutionPayload(request, outcome, submitted, submittedPath, requestKey.id);
          await command('request.resolve', payload, requestKey.id);
          requestKey = null;
          if (reply.value === submitted) { reply.value = ''; save(); }
          await refresh();
        } catch (error) { errorLine.textContent = error.message; }
        finally { for (const control of actions.children) control.disabled = false; }
      };
      actions.append(button);
    }
    const help = 'Reply sends your text and keeps the request open. Answer and close resolves it'
      + (literature ? ' and needs the file or folder.' : '.') + ' Decline tells the worker it will not be provided.';
    card.append(actions, node('p', help, 'muted small'), errorLine);
    return card;
  }
  // A person's open requests stay pinned above its conversation.
  function renderPinnedRequests() {
    const panel = el('worker-requests'), list = el('worker-request-list');
    const pending = selected ? (data?.requests || []).filter(r => r.state === 'pending' && (r.workers || []).includes(selected)) : [];
    const ids = new Set(pending.map(r => r.id));
    for (const [id, card] of pinnedViews) {
      if (!ids.has(id) || card.dataset.worker !== selected) { card.remove(); pinnedViews.delete(id); }
    }
    for (const request of pending) {
      if (pinnedViews.has(request.id)) { updateRequestCard(pinnedViews.get(request.id), request); continue; }
      const card = requestCard(request); card.dataset.worker = selected;
      pinnedViews.set(request.id, card); list.append(card);
    }
    el('worker-requests-title').textContent = pending.length === 1 ? 'Waiting for your answer'
      : pending.length + ' requests waiting for your answer';
    panel.classList.toggle('hidden', !pending.length);
  }
  function renderRequests() {
    const pending = data.requests.filter(r => r.state === 'pending');
    el('show-requests').textContent = pending.length ? 'Requests · ' + pending.length + ' waiting' : 'Requests';
    el('show-requests').classList.toggle('attention', pending.length > 0);
    const list = el('requests-list'), ids = new Set(pending.map(r => r.id));
    for (const [id, view] of requestViews) if (!ids.has(id)) { view.remove(); requestViews.delete(id); }
    for (const id of [...requestDrafts.keys()]) if (!ids.has(id)) requestDrafts.delete(id);
    if (pending.length) for (const placeholder of list.querySelectorAll('.empty')) placeholder.remove();
    for (const request of pending) {
      if (requestViews.has(request.id)) { updateRequestCard(requestViews.get(request.id), request); continue; }
      const card = requestCard(request); requestViews.set(request.id, card); list.append(card);
    }
    if (!pending.length) empty(list, 'No unanswered requests', 'Workers and reviewers can send questions or request literature here.');
    const history = el('resolved-requests'); history.replaceChildren();
    for (const request of data.requests.filter(r => r.state !== 'pending')) {
      const row = node('div', undefined, 'request');
      row.append(node('strong', request.title), node('span', ' · ' + label(request.state), 'muted'), node('p', request.text));
      for (const id of request.workers || []) row.append(workerButton(id));
      row.append(disclosure('Request record', request, 'request:' + request.id)); history.append(row);
    }
    if (!history.children.length) history.append(node('p', 'No resolved requests.', 'muted'));
  }

  async function refresh() {
    if (refreshing) return;
    refreshing = true;
    try {
      // An unchanged overview (304) keeps the page; elapsed-time labels still advance.
      const next = await api('/v1/overview?compact=true', undefined, {conditional: data !== null});
      if (next === null && Date.now() - overviewRenderedAt < 30000) {
        el('connection').textContent = 'Connected · ' + new Date().toLocaleTimeString();
        if (selected) await loadWorker(selected);
        return;
      }
      if (next !== null) { data = next; indexWorkers(); }
      if (Number.isInteger(data.retired_count) && data.retired_count !== retired.count) void loadRetired(data.retired_count);
      overviewRenderedAt = Date.now();
      taskStore.rememberTasks(data.tasks);
      el('main').classList.remove('hidden');
      el('project-name').textContent = data.name;
      el('connection').textContent = 'Connected · ' + new Date().toLocaleTimeString();
      const live = data.workers.filter(item => item.state !== 'retired');
      const reviewerCount = live.filter(item => item.kind === 'review').length;
      const workerCount = live.filter(item => (item.kind || 'ordinary') === 'ordinary').length;
      el('counts').textContent = data.task_count + ' tasks · ' + workerCount + ' worker' + (workerCount === 1 ? '' : 's') +
        ' · ' + reviewerCount + ' reviewer' + (reviewerCount === 1 ? '' : 's') + (data.workers.some(item => item.kind === 'integrator') ? ' · 1 integrator' : '')
        + (data.retired_count ? ' · ' + data.retired_count + ' retired' : '');
      el('show-tasks').textContent = 'Tasks (' + data.task_count + ')';
      el('show-setup').disabled = false;
      el('show-controls').disabled = false;
      // After the first run the dialog shows saved settings rather than a fresh setup.
      el('show-setup').textContent = el('setup-heading').textContent = data.runs.length ? 'Project settings' : 'Project setup';
      if (typeof matchMedia === 'function' && !selected && !drawerOffered && matchMedia('(max-width: 580px)').matches) {
        drawerOffered = true; setDrawer(true);
      }
      renderWorkers(); renderStatus(); renderProfiles(); renderRequests(); renderQueue(); renderLean(); renderPinnedRequests();
      if (el('controls-dialog').open) renderControls();
      void refreshQueueHistory();
      void refreshLibraryStats();
      if (el('tasks-dialog').open) { await taskStore.refresh({page: taskMode === 'list'}); await Promise.all([loadOutline(), loadGraph()]); }
      if (el('setup-dialog').open) await setupStore.refresh().catch(() => {});
      if (selected) { renderWorkerHead(); await loadWorker(selected); }
      if (data.delivery_holds?.length) notice(data.delivery_holds.length + ' message delivery outcome(s) need reconciliation. They will not be resent automatically.', 'health');
      else if (data.delivery_recovery_error) notice('Transcript recovery needs attention: ' + data.delivery_recovery_error, 'health');
      else {
        const failure = Object.values(data.drivers || {}).find(d => d.state === 'failed');
        notice(failure ? 'Execution needs attention: ' + (failure.error?.message || 'Driver failed.') : '', 'health');
      }
    } catch (error) { el('connection').textContent = 'Connection error'; notice(error.message, 'health'); }
    finally { refreshing = false; }
  }
  let overviewRenderedAt = 0;

  el('show-setup').onclick = () => {
    if (!data) return;
    if (!el('setup-dialog').open) el('setup-dialog').showModal();
    setupStore.show().catch(() => {});
    agents.state = '';
    // Read before the first run too: Project setup chooses the agents' models here.
    void loadAgentSettings();
  };
  el('setup-dialog').addEventListener('close', () => setupStore.setOpen(false));
  el('setup-goal').oninput = () => setupStore.setGoal(el('setup-goal').value);
  el('setup-refresh').onclick = () => setupStore.loadState().catch(() => {});
  el('setup-model-retry').onclick = () => setupStore.loadModels().catch(() => {});
  el('setup-form').onsubmit = event => {
    event.preventDefault();
    void startSetup();
  };
  el('setup-drive').onclick = () => setupStore.drive().then(finishSetup).catch(() => {});
  el('worker-filter').oninput = () => { if (data) renderWorkers(); };
  el('worker-state-filter').onchange = () => changePeopleView({state: el('worker-state-filter').value});
  el('worker-backend-filter').onchange = () => changePeopleView({backend: el('worker-backend-filter').value});
  el('worker-sort').onclick = () => changePeopleView({descending: !peopleView.descending});
  for (const group of document.querySelectorAll('.principal-group')) {
    // A heading stays in view while its list scrolls; closing it there keeps the next group in view.
    group.addEventListener('toggle', () => {
      const scroller = group.closest('.principal-groups');
      if (!group.open && scroller && group.getBoundingClientRect().top < scroller.getBoundingClientRect().top) {
        scroller.scrollTop += group.getBoundingClientRect().top - scroller.getBoundingClientRect().top;
      }
    });
  }
  el('agent-next-backend').onchange = () => editAgents(draft => { draft.nextBackend = el('agent-next-backend').value; });
  for (const key of ['worker_codex', 'review_codex', 'integrator_codex']) {
    const prefix = 'agent-' + key.replace('_', '-'), setupChoice = () => key === 'worker_codex' && settingUp();
    el(prefix + '-model').onchange = () => setupChoice() ? setupStore.selectModel(el(prefix + '-model').value) : editAgents(draft => {
      const value = draft.models[key];
      const efforts = codexEfforts(el(prefix + '-model').value).map(effort => effort.id);
      value.model = el(prefix + '-model').value;
      if (efforts.length && !efforts.includes(value.effort)) {
        value.effort = agents.catalog.models.find(item => item.id === value.model)?.default_effort || efforts[0];
      }
    });
    el(prefix + '-effort').onchange = () => setupChoice() ? setupStore.selectEffort(el(prefix + '-effort').value)
      : editAgents(draft => { draft.models[key].effort = el(prefix + '-effort').value; });
  }
  el('agent-worker-claude-model').oninput = () => editAgents(draft => { draft.models.worker_claude.model = el('agent-worker-claude-model').value; });
  el('agent-worker-claude-effort').onchange = () => editAgents(draft => { draft.models.worker_claude.effort = el('agent-worker-claude-effort').value; });
  for (const [role, backend] of AGENT_ROWS) {
    const box = el('agent-' + role + '-' + backend + '-existing');
    box.onchange = () => editAgents(draft => { draft.models[role + '_' + backend].existing = box.checked; });
  }
  el('agent-dispatcher-model').onchange = () => editAgents(draft => { draft.dispatcher.model = el('agent-dispatcher-model').value; });
  el('agent-dispatcher-effort').onchange = () => editAgents(draft => { draft.dispatcher.effort = el('agent-dispatcher-effort').value; });
  el('agent-waker-model').onchange = () => editAgents(draft => { draft.waker.model = el('agent-waker-model').value; });
  el('agent-waker-effort').onchange = () => editAgents(draft => { draft.waker.effort = el('agent-waker-effort').value; });
  el('agent-answer-deadline').oninput = () => editAgents(draft => { draft.deadline = el('agent-answer-deadline').value; });
  for (const backend of ['codex', 'claude']) {
    el('agent-sessions-' + backend).oninput = () => editAgents(draft => { draft.sessions[backend] = el('agent-sessions-' + backend).value; });
    el('agent-idle-' + backend).oninput = () => editAgents(draft => { draft.idle[backend] = el('agent-idle-' + backend).value; });
  }
  for (const key of ['delay', 'threshold']) {
    el('agent-batch-' + key).oninput = () => editAgents(draft => { draft.batch[key] = el('agent-batch-' + key).value; });
  }
  el('agent-settings-save').onclick = () => { void saveAgentSettings(); };
  el('agent-settings-reload').onclick = () => { agents.state = ''; void loadAgentSettings(); };
  el('people-toggle').onclick = () => setDrawer(!document.body.classList.contains('drawer-open'));
  el('lean-stats').onclick = () => {
    const group = el('lean-group'), scroller = group.closest('.principal-groups');
    group.open = true;
    if (typeof matchMedia === 'function' && matchMedia('(max-width: 580px)').matches) setDrawer(true);
    // Only the sidebar scrolls; scrolling the page would also move a drawer that is sliding in.
    if (scroller) scroller.scrollTop += group.getBoundingClientRect().top - scroller.getBoundingClientRect().top;
  };
  el('drawer-backdrop').onclick = () => setDrawer(false);
  el('task-filter').oninput = () => {
    clearTimeout(taskFilterTimer);
    taskFilterTimer = setTimeout(() => taskStore.browse({query: el('task-filter').value}).catch(() => {}), 250);
  };
  for (const view of ['all', 'running', 'pool', 'history']) el('tasks-' + view).onclick = () => {
    clearTimeout(taskFilterTimer);
    taskMode = 'list';
    renderTasks();
    taskStore.browse({view, query: el('task-filter').value}).catch(() => {});
  };
  el('tasks-outline').onclick = () => { taskMode = 'outline'; renderTasks(); loadOutline(); };
  el('tasks-graph').onclick = () => { taskMode = 'graph'; renderTasks(); loadGraph(); };
  el('task-kind').onchange = () => taskStore.browse({kind: el('task-kind').value}).catch(() => {});
  el('task-sort').onchange = () => taskStore.browse({sort: el('task-sort').value}).catch(() => {});
  el('task-newer').onclick = () => taskStore.showNewer().catch(() => {});
  el('task-page').onchange = () => {
    const pages = Math.max(1, Math.ceil((taskStore.page?.total || 0) / taskStore.limit));
    const wanted = Math.min(pages, Math.max(1, Math.round(Number(el('task-page').value) || 1)));
    taskStore.browse({offset: (wanted - 1) * taskStore.limit}).catch(() => {});
  };
  el('task-previous').onclick = () => taskStore.browse({offset: Math.max(0, taskStore.offset - taskStore.limit)}).catch(() => {});
  el('task-next').onclick = () => { if (taskStore.page?.has_more) taskStore.browse({offset: taskStore.page.next_offset}).catch(() => {}); };
  el('task-retry').onclick = () => taskStore.loadPage().catch(() => {});
  el('outline-filter').oninput = () => {
    clearTimeout(outlineFilterTimer);
    outlineFilterTimer = setTimeout(() => { outlineView.filter = el('outline-filter').value; renderOutline(); }, 200);
  };
  el('outline-hide-done').onchange = () => { outlineView.hideDone = el('outline-hide-done').checked; renderOutline(); };
  el('outline-expand').onclick = () => {
    // Every task above an unfinished one opens, so all open work shows in its place.
    if (!outlineTree) return;
    outlineView.touched = true;
    for (const item of outline.items) {
      if (!['done', 'retired'].includes(item.state)) for (const up of treeAncestors(outlineTree, item.id)) outlineView.expanded.add(up);
    }
    renderOutline();
  };
  el('outline-collapse').onclick = () => { outlineView.touched = true; outlineView.expanded.clear(); outlineView.shown.clear(); renderOutline(); };
  el('graph-met').onchange = () => { graphView.includeMet = el('graph-met').checked; loadGraph({force: true}); };
  el('graph-refresh').onclick = () => loadGraph({force: true});
  el('task-back').onclick = () => el('tasks-dialog').classList.remove('detail-open');
  el('show-tasks').onclick = () => { if (data) { openTasks(); taskStore.refresh({page: taskMode === 'list'}); } };
  el('tasks-dialog').addEventListener('close', () => {
    taskStore.setOpen(false); clearTimeout(taskFilterTimer); clearTimeout(outlineFilterTimer);
    el('tasks-dialog').classList.remove('detail-open');
  });
  el('show-requests').onclick = () => { if (data) el('requests-dialog').showModal(); };
  el('show-controls').onclick = () => { if (data) { el('controls-list').dataset.rows = ''; renderControls(); el('controls-dialog').showModal(); } };
  for (const tab of [claudeTab, codexTab]) {
    el('show-' + tab.backend).onclick = tab.open;
    el(tab.backend + '-profile').onclick = tab.open;
    el(tab.backend + '-profile').onkeydown = event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); tab.open(); } };
  }
  for (const button of document.querySelectorAll('[data-close]')) button.onclick = () => el(button.dataset.close).close();
  el('session-select').onchange = () => {
    if (!selected) return;
    rememberView(); store.worker(selected).selectedSession = el('session-select').value;
    historySignature = ''; renderHistory(); store.loadSession(selected, store.worker(selected).selectedSession).catch(error => notice(error.message));
  };
  el('turn-select').onchange = () => {
    if (!selected) return;
    const id = selected, key = store.worker(id).selectedSession, viewKey = id + '/' + key, value = el('turn-select').value;
    rememberView();
    if (value === '__older__') {
      const history = store.session(id, key);
      historySignature = ''; renderHistory();
      if (history.older) loadBefore(id, key, history.before);
      return;
    }
    turnChoices.set(viewKey, value);
    historySignature = ''; renderHistory();
  };
  el('jump-latest').onclick = async () => {
    if (!selected) return;
    const id = selected, key = store.worker(id).selectedSession;
    turnChoices.set(id + '/' + key, '');
    scrollPositions.delete(historyViewKey);
    historySignature = ''; renderHistory();
    try {
      await store.loadSession(id, key);
      if (id === selected && key === store.worker(id).selectedSession) el('transcript').scrollTop = el('transcript').scrollHeight;
    } catch (error) { notice(error.message); }
  };
  el('message').oninput = () => { if (selected) { store.worker(selected).draft = el('message').value; renderComposer(); } };
  el('compose').onsubmit = event => { event.preventDefault(); if (selected) store.send(selected).catch(() => {}); };
  refresh();
  // A hidden page does not poll: each overview reads every worker and the tasks they refer to.
  // After a slow one, polls are skipped (pollsToSkip): a slow overview means a busy service.
  let skipPolls = 0;
  const poll = async () => {
    if (document.hidden) return;
    if (skipPolls > 0) { skipPolls -= 1; return; }
    const started = Date.now();
    await refresh();
    skipPolls = pollsToSkip(Date.now() - started);
  };
  setInterval(poll, 2000);
  document.addEventListener('visibilitychange', poll);
}
