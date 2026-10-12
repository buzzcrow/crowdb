// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext } from '@playwright/test';
import { expect } from './realBackend';

export async function queueFullPlans(request: APIRequestContext, nodes: number[]) {
  for (const node of nodes) {
    const saved = await request.put(`/api/nodes/${node}/service-plan`, { data: {
      revision: 0,
      overrides: {
        diskdb: { rpc_port: 7100 }, diskio: { rpc_port: 7200 },
        chunkdb: { http_port: 7300, rpc_port: 7301, dynamic_ownership: true },
        'chunk-kv': { http_port: 7400, rpc_port: 7401 },
        'access-server': { http_port: 9000, s3_port: 9001, health_port: 9002 },
      },
      steps: Object.fromEntries(['paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server']
        .map(kind => [kind, { state: kind === 'paxos-kv' ? 'deployed' : 'pending' }])),
    } });
    expect(saved.ok(), await saved.text()).toBe(true);
  }
}
