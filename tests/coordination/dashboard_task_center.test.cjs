'use strict';

const assert = require('node:assert/strict');
const {test} = require('node:test');
const {
  TaskBrowserStore, taskOutlineState, outlineProgress, reviewSummary, publicationTree, treeAncestors,
  markdownBlocks, markdownInline, graphEdges, layeredLayout, edgeRoute, graphNeighbours, GRAPH,
} = require('../../src/formal_geyser/coordination/dashboard.js');

const item = (id, parent = null, state = 'ready', extra = {}) => ({id, parent, state, contract: {title: id, kind: 'work'},
  executors: [], reviews: {total: 0, open: 0, accepted: 0, changes: 0, ended: 0}, ...extra});

test('the publication tree puts Task 0 first and counts what lies below each task', () => {
  const items = [item('a'), item('root'), item('b', 'root', 'done'), item('c', 'root'), item('d', 'c', 'done'),
    item('e', 'c', 'retired'), item('orphan', 'missing')];
  const tree = publicationTree(items, 'root');
  assert.deepEqual(tree.roots, ['root', 'a', 'orphan']);
  assert.deepEqual(tree.children.get('root'), ['b', 'c']);
  assert.deepEqual(tree.below.get('root'), {total: 3, done: 2}); // b, c and d; e is retired.
  assert.deepEqual(tree.below.get('c'), {total: 1, done: 1});
  assert.deepEqual(tree.below.get('a'), {total: 0, done: 0});
  assert.deepEqual(treeAncestors(tree, 'd'), ['c', 'root']);
});

test('a cycle in recorded origins never hides a task', () => {
  const tree = publicationTree([item('x', 'y'), item('y', 'x'), item('z', 'x')]);
  const shown = new Set(tree.roots);
  for (const children of tree.children.values()) for (const id of children) shown.add(id);
  assert.deepEqual([...shown].sort(), ['x', 'y', 'z']);
  assert.ok(tree.roots.length >= 1);
});

test('outline items count their open reviews and executors like task summaries', () => {
  assert.equal(taskOutlineState(item('a', null, 'ready', {reviews: {total: 2, open: 1}})).text, 'Submitted · awaiting review');
  assert.equal(taskOutlineState(item('a', null, 'claimed', {executors: [{worker_id: 'w', execution_status: 'Waiting'}]})).text, 'Waiting');
  assert.equal(taskOutlineState(item('a')).text, 'Waiting for a worker');
  assert.deepEqual(outlineProgress([item('a', null, 'done'), item('b', null, 'ready', {reviews: {open: 1}}), item('c', null, 'retired')]),
    {total: 2, done: 1, review: 1, active: 0});
  assert.equal(reviewSummary({total: 3, open: 1, changes: 1}), '3 reviews · 1 open · 1 changes requested');
  assert.equal(reviewSummary({total: 0}), '');
});

test('task texts are read as safe Markdown blocks', () => {
  const blocks = markdownBlocks('## Goal\nProve the `twist` lemma\nfor **every** field.\n\n- first\n- second\n  continued\n\n```lean\ntheorem x : True := trivial\n```\n> quoted\n---\n1. one');
  assert.deepEqual(blocks.map(block => block.type), ['heading', 'paragraph', 'list', 'code', 'quote', 'rule', 'list']);
  assert.equal(blocks[0].level, 2);
  assert.deepEqual(blocks[1].inline.map(span => span.type), ['text', 'code', 'text', 'strong', 'text']);
  assert.equal(blocks[2].items.length, 2);
  assert.equal(blocks[2].items[1].map(span => span.text).join(''), 'second continued');
  assert.equal(blocks[3].text, 'theorem x : True := trivial');
  assert.equal(blocks[6].ordered, true);
  assert.deepEqual(markdownInline('[source](https://example.org/a)'), [{type: 'link', text: 'source', href: 'https://example.org/a'}]);
  // Only http(s) links become links; anything else stays text.
  assert.deepEqual(markdownInline('[x](javascript:alert(1))').map(span => span.type), ['text']);
});

test('dependency edges merge per pair and take the most pressing kind', () => {
  const links = [
    {source: 'a', target: 'b', kind: 'requirement', met: true}, {source: 'a', target: 'b', kind: 'subscription', met: false},
    {source: 'b', target: 'c', kind: 'requirement', met: true}, {source: 'operator', target: 'c', kind: 'wait', met: false},
    {source: 'c', target: 'd', kind: 'requirement', met: false}, {source: 'd', target: 'c', kind: 'requirement', met: false},
    {source: 'e', target: 'e', kind: 'requirement', met: false}];
  const edges = graphEdges(links, [['c', 'd']]);
  const kinds = Object.fromEntries(edges.map(edge => [edge.source + '>' + edge.target, edge.kind]));
  assert.deepEqual(kinds, {'a>b': 'subscription', 'b>c': 'met', 'operator>c': 'wait', 'c>d': 'deadlock', 'd>c': 'deadlock'});
  assert.equal(edges.find(edge => edge.source === 'a').links.length, 2);
});

test('the layered layout puts what is needed left of what needs it and loops cycles back', () => {
  const edges = graphEdges([{source: 'a', target: 'b', kind: 'requirement', met: false}, {source: 'b', target: 'c', kind: 'requirement', met: false},
    {source: 'a', target: 'c', kind: 'requirement', met: false}, {source: 'c', target: 'a', kind: 'subscription', met: false}]);
  const layout = layeredLayout(['a', 'b', 'c'], edges);
  const column = id => (layout.position.get(id).x - GRAPH.pad) / (GRAPH.nodeWidth + GRAPH.gapX);
  assert.deepEqual(['a', 'b', 'c'].map(column), [0, 1, 2]);
  const back = [...layout.back];
  assert.deepEqual(back.map(edge => edge.source + '>' + edge.target), ['c>a']);
  // The long edge a>c passes an empty slot in column 1.
  const long = edges.find(edge => edge.source === 'a' && edge.target === 'c');
  assert.equal(layout.chains.get(long).length, 3);
  const route = edgeRoute(long, layout);
  assert.match(route.d, new RegExp('^M' + (GRAPH.pad + GRAPH.nodeWidth) + ','));
  assert.ok(edgeRoute(back[0], layout).d.startsWith('M'));
  assert.ok(layout.width >= 3 * GRAPH.nodeWidth && layout.height >= GRAPH.nodeHeight);
  assert.deepEqual([...graphNeighbours(edges, 'b')].sort(), ['a', 'b', 'c']);
});

test('a long chain is laid out without exhausting the stack', () => {
  const ids = Array.from({length: 4000}, (_, index) => 't' + index);
  const edges = graphEdges(ids.slice(1).map((id, index) => ({source: ids[index], target: id, kind: 'requirement', met: false})));
  const layout = layeredLayout(ids, edges);
  assert.equal(layout.position.get('t3999').x, GRAPH.pad + 3999 * (GRAPH.nodeWidth + GRAPH.gapX));
});

function page(extra = {}) {
  return {items: [], view: 'all', query: '', total: 0, offset: 0, limit: 25, kind: 'work', sort: 'newest', newer: 0, now: 500,
    counts: {all: 0, running: 0, pool: 0, history: 0}, has_more: false, next_offset: null, ...extra};
}

test('the list asks for work tasks, holds still at the first read, and shows newer tasks on request', async () => {
  const urls = [];
  const store = new TaskBrowserStore(async path => { urls.push(new URL(path, 'http://local')); return page({now: 500 + urls.length}); });
  store.setOpen(true);
  await store.loadPage();
  assert.equal(urls[0].searchParams.get('kind'), 'work');
  assert.equal(urls[0].searchParams.get('sort'), 'newest');
  assert.equal(urls[0].searchParams.get('until'), null);
  assert.equal(store.until, 501);
  await store.browse({offset: 25});
  assert.equal(urls[1].searchParams.get('until'), '501'); // Another page keeps the list still.
  await store.browse({kind: 'review', sort: 'title'});
  assert.equal(urls[2].searchParams.get('until'), null); // Another filter starts from now.
  assert.equal(urls[2].searchParams.get('kind'), 'review');
  assert.equal(urls[2].searchParams.get('sort'), 'title');
  assert.equal(urls[2].searchParams.get('offset'), '0');
  await store.browse({offset: 25});
  await store.showNewer();
  assert.equal(urls[4].searchParams.get('until'), null);
  assert.equal(urls[4].searchParams.get('offset'), '0');
  await assert.rejects(store.browse({kind: 'everything'}), /Unknown task filter/);
});

test('a selected task reads its relations, and a refresh reads them again only after 15 s', async () => {
  const paths = [];
  const store = new TaskBrowserStore(async path => {
    paths.push(path);
    if (path.endsWith('/relations')) return {task_id: 'a', tasks: {b: {id: 'b', title: 'Needed task'}}, children: {total: 0, items: []}};
    if (path.includes('/revisions')) return {items: [], total: 1, offset: 0, current_revision: 1, has_more: false};
    if (path.startsWith('/v1/tasks/')) return {id: 'a', revision: 1, current_revision: 1, contract: {title: 'A'}};
    return page();
  });
  store.setOpen(true);
  await store.selectTask('a');
  const relationReads = () => paths.filter(path => path.endsWith('/relations')).length;
  assert.equal(relationReads(), 1);
  assert.equal(store.titles.get('b'), 'Needed task');
  await store.refresh({page: false});
  assert.equal(relationReads(), 1);
  assert.equal(paths.filter(path => path.startsWith('/v1/task-browser')).length, 0);
  await store.loadRelations('a', {now: Date.now() + 16000});
  assert.equal(relationReads(), 2);
});
