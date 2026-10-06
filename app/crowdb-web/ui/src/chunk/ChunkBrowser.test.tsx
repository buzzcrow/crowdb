// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { ChunkBrowser } from './ChunkBrowser';
import { DomainProvider } from '../contexts/DomainContext';
import { SelectionProvider } from '../contexts/SelectionContext';
import { Domain, GroupHealth, ReplicaRole, ReplicaState, type EnrichedStoreView, type Node } from '../types';

vi.mock('./ownership/OwnershipPanel', () => ({ OwnershipPanel: () => null }));
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

const nodes: Node[] = [{ id: 1, rack_id: 1, host: 'localhost', ssh: { type: 'KeyDefault', user: 'test' } }];
const stores: EnrichedStoreView[] = [{ store_id: '0', nodes: [1], groups: ['1', '2'].map(group_id => ({
  store_id: '0', group_id, state: GroupHealth.Healthy,
  replicas: [{ replica_id: group_id, node_id: 1, store_id: '0', group_id: String(group_id),
    role: ReplicaRole.Leader, state: ReplicaState.Running, engine_healthy: true }],
})) }];
const item = (id: string) => ({ chunk_id: id, key_hex: id, chunk_type: 1, state: 1, strip_count: 0 });
const json = (value: unknown) => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
function browser(topology = stores) {
  return <DomainProvider initialDomain={Domain.Chunk}><SelectionProvider>
    <ChunkBrowser active onPlacement={() => {}} racks={[{ id: 1, nodes }]} nodes={nodes} servers={[]} stores={topology} />
  </SelectionProvider></DomainProvider>;
}

it('finishes Node pagination through an unchanged topology refresh and skips exhausted groups', async () => {
  let finishPage!: (response: Response) => void;
  let pendingSignal: AbortSignal | undefined;
  const fetchMock = vi.fn(async (url: string, options?: RequestInit) => {
    const request = new URL(url, 'http://localhost');
    if (request.pathname === '/api/chunks') return json({ chunks: [], scanned: 0, next: null, owners: 0, failures: [], observed_at_ms: 0 });
    if (request.pathname.includes('/groups/2/')) return json({ items: [item('other-group')] });
    if (request.searchParams.has('start_after')) {
      pendingSignal = options?.signal as AbortSignal;
      return new Promise<Response>(resolve => { finishPage = resolve; });
    }
    return json({ items: [item('first-page')], next_start_after: 'cursor-1' });
  });
  vi.stubGlobal('fetch', fetchMock);
  const { rerender } = render(browser());
  fireEvent.click(screen.getByRole('button', { name: 'N-1' }));
  const rows = within(screen.getByRole('table', { name: 'Pxgroup chunks' }));
  await rows.findByRole('button', { name: 'first-page' });
  expect(rows.getByRole('button', { name: 'other-group' })).toBeVisible();
  const pages = within(screen.getByRole('navigation', { name: 'Pxgroup chunk pages' }));
  fireEvent.click(pages.getByRole('button', { name: 'Next' }));
  rerender(browser(structuredClone(stores)));
  expect(pendingSignal?.aborted).toBe(false);
  await act(async () => { finishPage(json({ items: [item('second-page')] })); });
  expect(rows.getByRole('button', { name: 'second-page' })).toBeVisible();
  expect(rows.queryByRole('button', { name: 'other-group' })).toBeNull();
  expect(pages.getByRole('button', { name: 'Next' })).toBeDisabled();
  expect(fetchMock.mock.calls.filter(([url]) => url.includes('/groups/2/'))).toHaveLength(1);
  fireEvent.click(pages.getByRole('button', { name: 'Previous' }));
  await rows.findByRole('button', { name: 'first-page' });
  expect(rows.getByRole('button', { name: 'other-group' })).toBeVisible();
});
