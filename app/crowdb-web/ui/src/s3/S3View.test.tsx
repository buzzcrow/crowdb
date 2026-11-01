// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { S3View } from '../views/S3View';
import { SelectionProvider } from '../contexts/SelectionContext';
import { DomainProvider } from '../contexts/DomainContext';
import { Domain } from '../types';

vi.mock('../contexts/ActivityContext', () => ({ useActivity: () => ({ log: vi.fn() }) }));
vi.mock('./useObjectLocations', () => ({ useObjectLocations: () => ({ selected: null, page: null, snapshot: () => null }) }));
afterEach(() => { vi.unstubAllGlobals(); });

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(complete => { resolve = complete; });
  return { promise, resolve };
}

const listing = (keys: string[]) => new Response(`<ListBucketResult><IsTruncated>false</IsTruncated>${keys.map(key =>
  `<Contents><Key>${key}</Key><Size>12</Size><ETag>etag</ETag><LastModified>today</LastModified></Contents>`).join('')}</ListBucketResult>`);

async function deletion() {
  const removed = deferred<Response>();
  const refreshed = deferred<Response>();
  let lists = 0;
  const fetcher = vi.fn(async (input: RequestInfo | URL, options?: RequestInit) => {
    const path = new URL(String(input), 'http://localhost').pathname;
    if (path === '/api/access/connections') return new Response(JSON.stringify({ s3: 'http://s3' }));
    if (path === '/api/access/s3/') return new Response('<ListAllMyBucketsResult><Buckets><Bucket><Name>photos</Name></Bucket></Buckets></ListAllMyBucketsResult>');
    if (path === '/api/access/s3/photos') return ++lists === 1 ? listing(['delete.txt', 'keep.txt', 'delete.txt.backup']) : refreshed.promise;
    if (path === '/api/access/s3/photos/delete.txt') {
      if (options?.method === 'DELETE') return removed.promise;
      if (options?.method === 'HEAD') return new Response(null, { headers: { 'content-length': '12' } });
    }
    throw new Error(`Unexpected request: ${options?.method} ${input}`);
  });
  vi.stubGlobal('fetch', fetcher);
  render(<DomainProvider initialDomain={Domain.S3}><SelectionProvider><S3View active readonly={false} onChunk={vi.fn()} /></SelectionProvider></DomainProvider>);
  const buckets = await screen.findByRole('table', { name: 'S3 bucket list' });
  fireEvent.click(await within(buckets).findByRole('button', { name: 'photos' }));
  fireEvent.click(await screen.findByRole('button', { name: 'delete.txt' }));
  await waitFor(() => expect(screen.getByRole('region', { name: 'Object metadata' })).toHaveTextContent('content length12'));
  fireEvent.contextMenu(screen.getByRole('heading', { level: 1, name: 'delete.txt' }));
  fireEvent.click(screen.getByRole('menuitem', { name: 'Delete object…' }));
  fireEvent.click(within(screen.getByRole('dialog', { name: 'Delete object' })).getByRole('button', { name: 'Delete object' }));
  return { removed, refreshed, fetcher, lists: () => lists };
}

it('removes only the confirmed object while its list refresh is still pending', async () => {
  const state = await deletion();
  await act(async () => state.removed.resolve(new Response(null, { status: 204 })));
  await waitFor(() => expect(state.lists()).toBe(2));
  const table = screen.getByRole('table', { name: 'S3 objects' });
  expect(within(table).queryByRole('button', { name: 'delete.txt' })).toBeNull();
  expect(within(table).getByRole('button', { name: 'keep.txt' })).toBeVisible();
  expect(within(table).getByRole('button', { name: 'delete.txt.backup' })).toBeVisible();
  await act(async () => state.refreshed.resolve(new Response('refresh unavailable', { status: 503 })));
  expect(screen.getByRole('alert')).toHaveTextContent('refresh unavailable');
  expect(within(table).queryByRole('button', { name: 'delete.txt' })).toBeNull();
  expect(state.fetcher.mock.calls.filter(([, options]) => options?.method === 'DELETE')).toHaveLength(1);
});

it.each([403, 503])('retains the selected row after a rejected or unknown delete (%s)', async status => {
  const state = await deletion();
  await act(async () => state.removed.resolve(new Response('delete unavailable', { status })));
  expect(screen.getByRole('alert')).toHaveTextContent('delete unavailable');
  expect(screen.getByRole('heading', { level: 1, name: 'delete.txt' })).toBeVisible();
  expect(state.lists()).toBe(1);
  expect(state.fetcher.mock.calls.filter(([, options]) => options?.method === 'DELETE')).toHaveLength(1);
});
