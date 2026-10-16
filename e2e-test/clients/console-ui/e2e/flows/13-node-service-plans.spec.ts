// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext } from '@playwright/test';
import { test, expect } from '../fixtures/realBackend';
import { diskMenu } from '../fixtures/capacity';
import { nativeProcessAlive } from '../fixtures/nativeProcess';

const kinds = ['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'];
async function json(request: APIRequestContext, path: string) {
  const response = await request.get(path);
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

async function occupyDiskdbPort(request: APIRequestContext) {
  const group = await request.post('/api/stores/0/groups', { data: { group_id: 1, replica_id: 1, nodes: [1, 2, 3] } });
  expect(group.status(), await group.text()).toBe(201);
  const added = await request.post('/api/nodes/1/disk-groups', { data: { id: 1, store_id: 0, group_id: 1 } });
  expect(added.status(), await added.text()).toBe(201);
  // Port ownership needs the actual DiskIO listener, without admitting media.
  const defaults = await json(request, '/api/deployment-defaults?node_id=1');
  const deployed = await request.post('/api/nodes/1/services/deploy', { data: {
    ...defaults.diskio, kind: 'diskio', instance_id: '99', rpc_port: 7102, disk_group_id: 1, test_single_node: false,
  } });
  expect(deployed.status(), await deployed.text()).toBe(201);
  const guard = await deployed.json();
  expect(await nativeProcessAlive(0, guard.pid)).toBe(true);
  return guard;
}

async function queuePlans(request: APIRequestContext) {
  for (const node of [1, 2, 3]) {
    const saved = await request.put(`/api/nodes/${node}/service-plan`, { data: {
      revision: 0, overrides: { diskdb: { rpc_port: 7100 } },
      steps: Object.fromEntries(kinds.map(kind => [kind, { state: kind === 'paxos-kv' ? 'deployed' : kind === 'diskdb' ? 'pending' : 'disabled' }])),
    } });
    expect(saved.ok(), await saved.text()).toBe(true);
  }
}

// Baseline: 7.9s (2026-10-12).
test('native plans: fixed ports isolate three nodes and failed DiskDB retries preserve identity', async ({ page, request }) => {
  test.setTimeout(120_000);
  const guard = await occupyDiskdbPort(request);
  await queuePlans(request);
  const registrations: string[] = [];
  page.on('request', response => {
    if (response.method() === 'POST' && ['/api/nodes', '/api/node/admit'].includes(new URL(response.url()).pathname)) registrations.push(response.url());
  });
  await page.goto('/?domain=Cluster');
  await expect.poll(async () => {
    const plans = await json(request, '/api/service-plans');
    return Object.fromEntries([1, 2, 3].map(node => [node, plans[node]?.steps.diskdb.state]));
  }, { timeout: 10_000, intervals: [100] }).toEqual({ 1: 'failed', 2: 'deployed', 3: 'deployed' });
  const before = await json(request, '/api/service-plans');
  expect(before[1].overrides.diskdb.rpc_port).toBe(7100);
  expect(before[1].steps.diskdb.detail).toContain('crowdb-disk-db');
  expect(before[1].steps.diskdb.detail).toContain('7100');
  for (const node of [1, 2, 3]) {
    expect(before[node].steps['paxos-kv'].state).toBe('deployed');
    for (const kind of ['chunkdb', 'diskio', 'chunk-kv', 'access-server']) expect(before[node].steps[kind].state).toBe('disabled');
  }
  await page.reload();
  const retained = await json(request, '/api/service-plans');
  expect(retained[1].overrides).toEqual(before[1].overrides);
  expect(retained[1].steps.diskdb).toEqual(before[1].steps.diskdb);
  const removed = await request.delete(`/api/services/${guard.id}`);
  expect(removed.ok(), await removed.text()).toBe(true);
  await expect.poll(() => nativeProcessAlive(0, guard.pid), { intervals: [100] }).toBe(false);
  const node = page.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  await diskMenu(page, node, 'Deploy default services');
  const progress = page.getByRole('dialog', { name: 'Node 1 services', exact: true });
  await progress.getByRole('button', { name: 'Retry failed services', exact: true }).click();
  await expect.poll(async () => (await json(request, '/api/service-plans'))[1].steps.diskdb.state,
    { timeout: 10_000, intervals: [100] }).toBe('deployed');
  expect(registrations).toEqual([]);
  expect((await json(request, '/api/nodes')).map((row: { id: number }) => row.id).sort()).toEqual([1, 2, 3]);
  const after = await json(request, '/api/service-plans');
  expect(after[1].overrides).toEqual(before[1].overrides);
  const servers = (await json(request, '/api/servers')).filter((row: { service_type: string }) => row.service_type === 'diskdb');
  expect(servers).toHaveLength(3);
  expect(new Set(servers.map((row: { endpoint: string; rpc_url: string }) => row.endpoint || row.rpc_url)).size).toBe(3);
  for (const server of servers) {
    expect(server.endpoint || server.rpc_url).toMatch(/:7102$/);
    expect(server.pid_node_id).toBe(server.node_id);
    expect(await nativeProcessAlive(server.node_id - 1, server.pid)).toBe(true);
  }
});
