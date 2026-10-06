// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Response } from '@playwright/test';
import { expect } from './realBackend';

/** Observe actual automatic deployment mutations before checking durable progress. */
export function observeDefaultDeployments(page: Page) {
  const replies = new Map<string, Response>();
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
    if (identity && !replies.has(identity)) replies.set(identity, response);
  });
  return {
    async verify(node: number, kind: string) {
      const identity = `${node}:${kind}`;
      const response = replies.get(identity) ?? await page.waitForResponse(response => key(response) === identity);
      expect(response.status(), `Node ${node} ${kind}: ${await response.text()}`).toBe(201);
    },
  };
}
