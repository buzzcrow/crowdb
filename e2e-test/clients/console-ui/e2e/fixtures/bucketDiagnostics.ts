// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { APIRequestContext, APIResponse } from '@playwright/test';
import { test } from './realBackend';

export function bucketReply(response: APIResponse, started: number, body: string) {
  const headers = response.headers();
  return { observed_at: new Date().toISOString(), elapsed_ms: Date.now() - started,
    status: response.status(), path: new URL(response.url()).pathname,
    request_id: headers['x-amz-request-id'], content_type: headers['content-type'],
    body: body.slice(0, 8192), body_truncated: body.length > 8192 };
}

/** Run only after the immediate visibility assertion fails, before cleanup. */
export async function bucketFailure(request: APIRequestContext, bucketPath: string, initial: unknown) {
  const observe = async (path: string, method: 'GET' | 'HEAD', select?: (body: any) => unknown) => {
    const started = Date.now();
    try {
      const response = await request.fetch(path, { method, timeout: 1000 });
      const body = await response.text();
      const reply = bucketReply(response, started, select ? '' : body);
      return { method, ...reply, ...(select && response.ok() ? { value: select(JSON.parse(body)) } : {}) };
    } catch (error) {
      return { path, method, elapsed_ms: Date.now() - started, error: String(error).slice(0, 1024) };
    }
  };
  const observations = await Promise.all([
    observe(bucketPath, 'HEAD'),
    observe('/api/access/s3/', 'GET'),
    observe('/api/access/connections', 'GET', body => ({ s3: body.s3 })),
    observe('/api/servers', 'GET', body => body.slice(0, 32).map((server: any) => ({
      id: server.id, node_id: server.node_id, service_type: server.service_type,
      pid: server.pid, health: server.health,
    }))),
  ]);
  const text = JSON.stringify({ initial, observations }, null, 2);
  console.log(`[S3 BUCKET FAILURE] ${text}`);
  try {
    await test.info().attach('s3-bucket-diagnostics', { body: text, contentType: 'application/json' });
  } catch (error) {
    console.log(`[S3 BUCKET DIAGNOSTIC ATTACH FAILED] ${String(error)}`);
  }
}
