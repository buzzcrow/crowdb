// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Request, Response } from '@playwright/test';

export interface JournalRuntimeTarget {
  origin: string; id: string; epoch: string; generation: string; page: number; offset: number;
}

export function matchesJournalRuntimeRequest(request: Pick<Request, 'method' | 'url'>, target: JournalRuntimeTarget): boolean {
  if (request.method() !== 'GET') return false;
  const url = new URL(request.url());
  const query = { id: target.id, epoch: target.epoch, generation: target.generation,
    page: String(target.page), offset: String(target.offset), stream_offset: '0' };
  return url.origin === target.origin && url.pathname === '/api/chunk-kv/runtime'
    && [...url.searchParams.keys()].length === Object.keys(query).length
    && Object.entries(query).every(([name, value]) => url.searchParams.get(name) === value);
}

/** Observe the GET started by this action, never a late response to an earlier selection. */
export async function journalRuntimeResponse(page: Pick<Page, 'on' | 'off'>, target: JournalRuntimeTarget,
  action: () => Promise<unknown>, remaining: () => number): Promise<Response> {
  const budget = remaining();
  if (budget <= 0) throw new Error('Journal observation exceeded its three-second deadline');
  let owned: Request | null = null;
  let receive!: (response: Response) => void;
  let fail!: (error: Error) => void;
  const observed = new Promise<Response>((resolve, reject) => { receive = resolve; fail = reject; });
  const onRequest = (request: Request) => {
    if (!owned && matchesJournalRuntimeRequest(request, target)) owned = request;
  };
  const onResponse = (response: Response) => {
    if (response.request() !== owned) return;
    void response.finished().then(error => { if (error) fail(error); else receive(response); }, fail);
  };
  const onFailure = (request: Request) => {
    if (request === owned) fail(new Error(request.failure()?.errorText ?? 'Journal runtime request failed'));
  };
  const timeout = setTimeout(() => fail(new Error(`Journal runtime response timed out for ${target.id}, epoch ${target.epoch}, catalog ${target.generation}`)), budget);
  page.on('request', onRequest);
  page.on('response', onResponse);
  page.on('requestfailed', onFailure);
  try {
    const [response] = await Promise.all([observed, Promise.resolve().then(action)]);
    return response;
  } finally {
    clearTimeout(timeout);
    page.off('request', onRequest);
    page.off('response', onResponse);
    page.off('requestfailed', onFailure);
  }
}
