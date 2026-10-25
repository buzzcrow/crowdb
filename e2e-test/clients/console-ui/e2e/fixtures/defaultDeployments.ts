// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Response } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { expect, test } from './realBackend';
import { deploymentState, observeDeploymentDiagnostics } from './deploymentDiagnostics';

/** Observe accepted mutations and target-owned asynchronous deployment completion. */
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
  const expectedWait = (response: Response, kind: string) => response.status() === 409 && ['chunkdb', 'chunk-kv'].includes(kind);
  async function completed(nodes: number[], kind: string) {
    const timeout = process.platform === 'darwin' ? 30_000 : 10_000;
    let found: { node: number; response: Response } | undefined;
    try {
      await expect.poll(async () => {
        for (const node of nodes) {
          const unexpected = replies.get(`${node}:${kind}`)?.find(response => response.status() !== 201 && !expectedWait(response, kind));
          if (unexpected) { found = { node, response: unexpected }; return true; }
          const response = replies.get(`${node}:${kind}`)?.find(response => response.status() === 201);
          if (response) { found = { node, response }; return true; }
        }
        const waiting = nodes.filter(node => replies.get(`${node}:${kind}`)?.some(response => expectedWait(response, kind)));
        if (!waiting.length) return false;
        const response = await page.request.get('/api/service-plans');
        expect(response.ok(), await response.text()).toBe(true);
        const plans = await response.json();
        const node = waiting.find(node => plans[node]?.steps[kind]?.state === 'deployed');
        if (node === undefined) return false;
        found = { node, response: replies.get(`${node}:${kind}`)!.find(response => expectedWait(response, kind))! };
        return true;
      }, { timeout, intervals: [100] }).toBe(true);
      return found!;
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
      if (error instanceof Error) error.message = `Deployment completion missing: nodes=${nodes.join(',')} service=${kind}\n${error.message}`;
      throw error;
    }
  }
  async function verifyResponse(response: Response, kind: string) {
    if (expectedWait(response, kind)) {
      expect((await response.json()).error).toMatch(/^Waiting: /);
    } else {
      expect(response.status(), await response.text()).toBe(201);
    }
  }
  return {
    async nextCompleted(nodes: number[], kind: string) {
      const result = await completed(nodes, kind);
      await verifyResponse(result.response, kind);
      return result.node;
    },
    async verify(node: number, kind: string) {
      const result = await completed([node], kind);
      for (const response of replies.get(`${node}:${kind}`) ?? []) await verifyResponse(response, kind);
      await verifyResponse(result.response, kind);
    },
  };
}
