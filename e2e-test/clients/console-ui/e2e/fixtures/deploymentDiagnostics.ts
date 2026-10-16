// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Page, Request, Response } from '@playwright/test';

const kinds = ['paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server'];
const bounded = (value: unknown) => String(value).replace(/(password|secret|token|access_key)(["\s:=]+)[^\s,"}]+/gi, '$1$2<redacted>').slice(0, 1024);

type Attempt = {
  node: number; service: string; path: string; started_at: string; started_ms: number;
  elapsed_ms?: number; status?: number; failure?: string; error?: string;
};

export function deploymentTarget(request: Pick<Request, 'method' | 'url' | 'postDataJSON'>) {
  if (request.method() !== 'POST') return undefined;
  const path = new URL(request.url()).pathname;
  const match = path.match(/^\/api\/nodes\/(\d+)\/(server|diskdb|services)\/deploy$/);
  if (!match) return undefined;
  const service = match[2] === 'server' ? 'paxos-kv' : match[2] === 'diskdb' ? 'diskdb' : request.postDataJSON()?.kind;
  if (!kinds.includes(service)) return undefined;
  return { node: Number(match[1]), service: String(service), path };
}

/** Retain bounded deployment evidence without credentials or request bodies. */
export function observeDeploymentDiagnostics(page: Pick<Page, 'on'>, now = () => performance.now()) {
  const started = now();
  const attempts: Attempt[] = [];
  const requests = new WeakMap<Request, Attempt>();
  page.on('request', request => {
    const target = deploymentTarget(request);
    if (!target) return;
    const attempt = { ...target, started_at: new Date().toISOString(), started_ms: Math.round(now() - started) };
    requests.set(request, attempt);
    attempts.push(attempt);
    if (attempts.length > 80) attempts.shift();
  });
  page.on('response', response => {
    const attempt = requests.get(response.request());
    if (!attempt) return;
    attempt.status = response.status();
    attempt.elapsed_ms = Math.round(now() - started - attempt.started_ms);
    if (response.status() !== 201) void readError(response, attempt);
  });
  page.on('requestfailed', request => {
    const attempt = requests.get(request);
    if (!attempt) return;
    attempt.elapsed_ms = Math.round(now() - started - attempt.started_ms);
    attempt.failure = bounded(request.failure()?.errorText ?? 'request failed');
  });
  return () => attempts.map(attempt => ({ ...attempt,
    elapsed_ms: attempt.elapsed_ms ?? Math.round(now() - started - attempt.started_ms),
    state: attempt.failure ? 'failed' : attempt.status === undefined ? 'pending' : 'responded',
  }));
}

async function readError(response: Response, attempt: Attempt) {
  try {
    const body = await response.json();
    if (typeof body?.error === 'string') attempt.error = bounded(body.error);
  } catch (error) {
    attempt.error = `response body unavailable: ${bounded(error)}`;
  }
}

/** Observation errors remain visible and cannot replace the original failure. */
export async function deploymentState(page: Pick<Page, 'request'>) {
  const observe = async (path: string, select: (body: any) => unknown) => {
    try {
      const response = await page.request.get(path, { timeout: 1000 });
      if (!response.ok()) return { path, status: response.status() };
      return { path, status: response.status(), value: select(await response.json()) };
    } catch (error) {
      return { path, error: bounded(error) };
    }
  };
  return Promise.all([
    observe('/api/service-plans', body => Object.fromEntries(Object.entries(body).slice(0, 16).map(([node, raw]) => {
      const plan = raw as { revision?: number; steps?: Record<string, { state?: string; detail?: string }> };
      return [node, { revision: plan.revision, steps: Object.fromEntries(kinds.map(kind => [kind, {
        state: plan.steps?.[kind]?.state, detail: plan.steps?.[kind]?.detail && bounded(plan.steps[kind].detail),
      }])) }];
    }))),
    observe('/api/servers', body => body.slice(0, 96).map((server: any) => ({
      node: server.node_id, service: server.service_type, id: server.id,
      pid: server.pid, health: server.health,
    }))),
  ]);
}
