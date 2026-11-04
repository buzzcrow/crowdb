// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';
import { journalRuntimeResponse, matchesJournalRuntimeRequest } from '../clients/console-ui/e2e/fixtures/journalRuntimeRequest.ts';

const target = { origin: 'http://owned', id: 'a'.repeat(32), epoch: '9007199254740993', generation: '9007199254740997', page: 2, offset: 17 };
const url = () => new URL('/api/chunk-kv/runtime?' + new URLSearchParams({ id: target.id, epoch: target.epoch,
  generation: target.generation, page: String(target.page), offset: String(target.offset), stream_offset: '0' }), target.origin);
const request = (address = url(), method = 'GET') => ({ url: () => String(address), method: () => method,
  failure: () => ({ errorText: 'owned request aborted' }) });
const response = (owner, finished = async () => null) => ({ request: () => owner, finished });
const remaining = () => 1_000;

test('journal matcher rejects another partition and every old resource or cursor component', () => {
  assert.equal(matchesJournalRuntimeRequest(request(), target), true);
  for (const [name, value] of [['id', 'b'.repeat(32)], ['epoch', '1'], ['generation', '2'],
    ['page', '0'], ['offset', '0'], ['stream_offset', '20'], ['stream_generation', 'old'],
    ['page_path', ''], ['entry_offset', '0']]) {
    const changed = url(); changed.searchParams.set(name, value);
    assert.equal(matchesJournalRuntimeRequest(request(changed), target), false, name);
  }
  const duplicate = url(); duplicate.searchParams.append('id', target.id);
  assert.equal(matchesJournalRuntimeRequest(request(duplicate), target), false);
  const foreign = url(); foreign.hostname = 'foreign';
  assert.equal(matchesJournalRuntimeRequest(request(foreign), target), false);
  assert.equal(matchesJournalRuntimeRequest(request(url(), 'POST'), target), false);
});

test('an automatic response started before Refresh cannot satisfy its request observation', async () => {
  const page = new EventEmitter();
  const automatic = request();
  page.emit('request', automatic);
  const refreshed = request();
  const wrong = url(); wrong.searchParams.set('id', 'b'.repeat(32));
  const unrelated = request(wrong);
  const observed = await journalRuntimeResponse(page, target, async () => {
    page.emit('response', response(automatic));
    page.emit('request', unrelated); page.emit('response', response(unrelated));
    page.emit('request', refreshed); page.emit('response', response(refreshed));
  }, remaining);
  assert.equal(observed.request(), refreshed);
  assert.equal(page.listenerCount('request'), 0);
  assert.equal(page.listenerCount('response'), 0);
  assert.equal(page.listenerCount('requestfailed'), 0);
});

test('Refresh ignores an old cursor and waits for its exact response body to finish', async () => {
  const page = new EventEmitter();
  const oldUrl = url(); oldUrl.searchParams.set('stream_generation', 'old');
  const old = request(oldUrl), refreshed = request();
  let finish;
  const body = new Promise(resolve => { finish = resolve; });
  let completed = false;
  const pending = journalRuntimeResponse(page, target, async () => {
    page.emit('request', old); page.emit('response', response(old));
    page.emit('request', refreshed); page.emit('response', response(refreshed, () => body));
  }, remaining).then(value => { completed = true; return value; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(completed, false);
  finish(null);
  assert.equal((await pending).request(), refreshed);
});

test('request failure or action failure removes all listeners and keeps the real error', async () => {
  const page = new EventEmitter(), failed = request();
  await assert.rejects(journalRuntimeResponse(page, target, async () => {
    page.emit('request', failed); page.emit('requestfailed', failed);
  }, remaining), /owned request aborted/);
  await assert.rejects(journalRuntimeResponse(page, target, async () => { throw new Error('selection click failed'); }, remaining), /selection click failed/);
  for (const name of ['request', 'response', 'requestfailed']) assert.equal(page.listenerCount(name), 0);
});
