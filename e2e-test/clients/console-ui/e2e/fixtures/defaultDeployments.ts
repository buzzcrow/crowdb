// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Response } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { expect, test } from './realBackend';
import { deploymentState, observeDeploymentDiagnostics } from './deploymentDiagnostics';

/** Observe actual automatic deployment mutations before checking durable progress. */
export function observeDefaultDeployments(page: Page) {
  const replies = new Map<string, Response[]>();
  const attempts = observeDeploymentDiagnostics(page);
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
  async function wait(nodes: number[], kind: string, predicate: (response: Response) => boolean) {
    const timeout = process.platform === 'darwin' ? 30_000 : 10_000;
    try {
      return await page.waitForResponse(predicate, { timeout });
    } catch (error) {
      const evidence = { nodes, service: kind, failed_at: new Date().toISOString(), timeout_ms: timeout, attempts: attempts(),
        observations: await deploymentState(page) };
      const text = JSON.stringify(evidence, null, 2);
      console.log(`[DEPLOYMENT FAILURE] ${text}`);
      try {
        const path = test.info().outputPath('deployment-diagnostics.json');
        await mkdir(dirname(path), { recursive: true });
        await writeFile(path, text);
        await test.info().attach('deployment-diagnostics', { path, contentType: 'application/json' });
      } catch (diagnosticError) {
        console.log(`[DEPLOYMENT DIAGNOSTIC WRITE FAILED] ${String(diagnosticError)}`);
      }
      if (error instanceof Error) error.message = `Deployment response missing: nodes=${nodes.join(',')} service=${kind}\n${error.message}`;
      throw error;
    }
  }
  return {
    async nextCompleted(nodes: number[], kind: string) {
      const pending = new Set(nodes.map(node => `${node}:${kind}`));
      const expectedWait = (response: Response) => response.status() === 409 && ['chunkdb', 'chunk-kv'].includes(kind);
      const rejected = nodes.flatMap(node => replies.get(`${node}:${kind}`) ?? [])
        .find(response => response.status() !== 201 && !expectedWait(response));
      if (rejected) expect(rejected.status(), await rejected.text()).toBe(201);
      const completed = nodes.find(node => replies.get(`${node}:${kind}`)?.some(response => response.status() === 201));
      if (completed !== undefined) return completed;
      const response = await wait(nodes, kind, response => pending.has(key(response) ?? '')
        && (response.status() === 201 || !expectedWait(response)));
      expect(response.status(), await response.text()).toBe(201);
      return Number(key(response)!.split(':')[0]);
    },
    async verify(node: number, kind: string) {
      const identity = `${node}:${kind}`;
      const response = replies.get(identity)?.find(response => response.status() === 201)
        ?? await wait([node], kind, response => key(response) === identity && response.status() === 201);
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
