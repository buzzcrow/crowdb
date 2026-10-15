// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Response } from '@playwright/test';
import { expect } from './realBackend';

/** Observe actual automatic deployment mutations before checking durable progress. */
export function observeDefaultDeployments(page: Page) {
  const replies = new Map<string, Response[]>();
  function key(response: Response) {
    if (response.request().method() !== 'POST') return undefined;
    const path = new URL(response.url()).pathname;
    const match = path.match(/^\/api\/nodes\/(\d+)\/(server|diskdb|services)\/deploy$/);
    if (!match) return undefined;
    const kind = match[2] === 'server' ? 'paxos-kv' : match[2] === 'diskdb' ? 'diskdb' : response.request().postDataJSON().kind;
    return `${match[1]}:${kind}`;
  }
  page.on('response', response => {
    const identity = key(response);
    if (identity) replies.set(identity, [...(replies.get(identity) ?? []), response]);
  });
  return {
    async nextCompleted(nodes: number[], kind: string) {
      const expectedWait = (response: Response) => response.status() === 409 && ['chunkdb', 'chunk-kv'].includes(kind);
      const rejected = nodes.flatMap(node => replies.get(`${node}:${kind}`) ?? [])
        .find(response => response.status() !== 201 && !expectedWait(response));
      if (rejected) expect(rejected.status(), await rejected.text()).toBe(201);
      const completed = nodes.find(node => replies.get(`${node}:${kind}`)?.some(response => response.status() === 201));
      if (completed !== undefined) return completed;
      // The deploy request can be queued behind disk recovery, so its response
      // is not a useful readiness signal. Follow the durable service plan and
      // let the enclosing Playwright test budget report a genuine hang.
      for (;;) {
        const plans = await page.evaluate(async () => {
          const response = await fetch('/api/service-plans');
          return response.json();
        });
        const ready = nodes.find(node => plans[node]?.steps?.[kind]?.state === 'deployed');
        if (ready !== undefined) return ready;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    },
    async verify(node: number, kind: string) {
      const identity = `${node}:${kind}`;
      const response = replies.get(identity)?.find(response => response.status() === 201);
      if (!response) return;
      for (const rejected of replies.get(identity) ?? []) {
        if (rejected === response) break;
        const body = await rejected.json();
        expect(rejected.status(), JSON.stringify(body)).toBe(409);
        expect(['chunkdb', 'chunk-kv']).toContain(kind);
        expect(body.error).toMatch(/^Waiting: /);
      }
      expect(response.status(), `Node ${node} ${kind}: ${await response.text()}`).toBe(201);
    },
  };
}
