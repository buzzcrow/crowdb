// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';
import { deploymentState, observeDeploymentDiagnostics } from '../clients/console-ui/e2e/fixtures/deploymentDiagnostics.ts';

function request(node, kind = 'chunk-kv') {
  return { method: () => 'POST', url: () => `http://owned/api/nodes/${node}/services/deploy?token=private`,
    postDataJSON: () => ({ kind, token: 'private' }), failure: () => ({ errorText: 'net::ERR_ABORTED' }) };
}

test('deployment evidence distinguishes pending, rejected and failed requests without credentials', async () => {
  const page = new EventEmitter();
  let time = 0;
  const snapshot = observeDeploymentDiagnostics(page, () => time);
  const first = request(4);
  page.emit('request', first);
  time = 200;
  assert.equal(snapshot()[0].state, 'pending');
  page.emit('response', { request: () => first, status: () => 409,
    json: async () => ({ error: 'Waiting: unavailable token=private' }) });
  await new Promise(resolve => setImmediate(resolve));
  const second = request(5);
  page.emit('request', second);
  time = 350;
  page.emit('requestfailed', second);
  assert.deepEqual(snapshot().map(row => [row.node, row.service, row.state, row.status, row.elapsed_ms]),
    [[4, 'chunk-kv', 'responded', 409, 200], [5, 'chunk-kv', 'failed', undefined, 150]]);
  assert.match(snapshot()[0].error, /Waiting: unavailable/);
  assert.equal(snapshot()[1].failure, 'net::ERR_ABORTED');
  assert.doesNotMatch(JSON.stringify(snapshot()), /private/);
});

test('only bounded deployment history is collected', () => {
  const page = new EventEmitter();
  const snapshot = observeDeploymentDiagnostics(page);
  page.emit('request', { ...request(1), method: () => 'GET' });
  for (let node = 1; node <= 85; node++) page.emit('request', request(node));
  assert.equal(snapshot().length, 80);
  assert.equal(snapshot()[0].node, 6);
  assert.equal(snapshot().at(-1).node, 85);
});

test('state observations keep plan progress and process identity while excluding configuration secrets', async () => {
  const page = { request: { get: async (path, options) => {
    assert.equal(options.timeout, 1000);
    return { ok: () => true, status: () => 200, json: async () => path.endsWith('service-plans')
      ? { 4: { revision: 9, steps: { diskdb: { state: 'waiting', detail: 'token=private' } }, overrides: { password: 'private' } } }
      : [{ id: 'diskdb-4', node_id: 4, service_type: 'diskdb', pid: 42, health: 'up', token: 'private' }] };
  } } };
  const result = await deploymentState(page);
  assert.equal(result[0].value[4].steps.diskdb.state, 'waiting');
  assert.equal(result[0].value[4].revision, 9);
  assert.equal(result[1].value[0].pid, 42);
  assert.doesNotMatch(JSON.stringify(result), /private|overrides/);
});

test('an unavailable observation is recorded without losing the other observation', async () => {
  const page = { request: { get: async path => {
    if (path.endsWith('service-plans')) throw new Error('authority unavailable');
    return { ok: () => false, status: () => 502 };
  } } };
  const result = await deploymentState(page);
  assert.match(result[0].error, /authority unavailable/);
  assert.equal(result[1].status, 502);
});
