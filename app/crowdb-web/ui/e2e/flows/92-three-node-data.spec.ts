// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 49.6s (2026-10-06), three storage nodes, three diskless nodes and real data round trips.
import { test, expect } from '../fixtures/realBackend';
import { resetAll, waitForLeader } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';
import { observeDefaultDeployments } from '../fixtures/defaultDeployments';
import { disklessExpansion, verifyDisklessSlotBalance } from '../fixtures/disklessExpansion';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';

import type { APIRequestContext, Page } from '@playwright/test';

type UIContext = { page: Page; request: APIRequestContext; baseURL: string; deployments: ReturnType<typeof observeDefaultDeployments> };

test('UI creates three storage nodes, adds three diskless nodes, then KV, S3 and Iceberg persist real data', async ({ page, request, baseURL }) => {
  test.setTimeout(300_000);
  const context = { page, request, baseURL: baseURL!, deployments: observeDefaultDeployments(page) };
  await step('three-node: empty owned backend', () => resetAll(baseURL!));
  let primaryFailure: unknown;
  try {
    await page.goto('/');
    await createCluster(context);
    await addDisks(context);
    await verifyServices(context);
    await disklessExpansion(context);
    await s3RoundTrip(context);
    await icebergRoundTrip(context);
    await verifyDisklessSlotBalance(context);
  } catch (error) {
    primaryFailure = error;
    throw error;
  } finally {
    await page.close();
    try { await teardown(context); }
    catch (cleanupFailure) {
      if (primaryFailure) throw new AggregateError([primaryFailure, cleanupFailure], `Cluster flow failed: ${String(primaryFailure)}; teardown failed: ${String(cleanupFailure)}`);
      throw cleanupFailure;
    }
  }
});

async function createCluster({ page, request, baseURL }: UIContext) {
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await step('three-node: rack and default node plans through UI', async () => {
    await page.getByRole('button', { name: 'Add Rack', exact: true }).click();
    const rack = page.getByRole('dialog', { name: 'Add Rack', exact: true });
    await rack.getByLabel('Rack ID').fill('1');
    await rack.getByRole('button', { name: 'Create Rack', exact: true }).click();
    for (const id of [1, 2, 3]) {
      await sidebar.getByRole('button', { name: 'R-1', exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: 'Add Node', exact: true }).click();
      const dialog = page.getByRole('dialog', { name: 'Add Node', exact: true });
      await dialog.getByLabel('Node ID', { exact: true }).fill(String(id));
      await dialog.getByLabel('Host', { exact: true }).fill('127.0.0.1');
      await expect(dialog.getByRole('checkbox', { checked: true })).toHaveCount(7);
      await dialog.getByLabel('Automatically redistribute ChunkDB slots').check();
      await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
      await expect(dialog).toHaveCount(0);
    }
    await expect.poll(async () => (await (await request.get('/api/servers')).json()).filter((s: { service_type: string; pid: number }) => s.service_type === 'paxos-kv' && s.pid).length, { intervals: [100] }).toBe(3);
    // Initialization selects the live UI snapshot when its dialog opens.
    // A registered PID alone does not prove that the UI has observed readiness.
    const servers = await (await request.get('/api/servers')).json();
    for (const server of servers.filter((server: { service_type: string }) => server.service_type === 'paxos-kv')) {
      await expect(sidebar.getByTestId(`tree-node-SERVICE-${server.id}`).getByTitle('Healthy', { exact: true })).toBeVisible();
    }
  });
  await step('three-node: initialize both KV groups through UI', async () => {
    await page.getByTestId('domain-kv').click();
    await page.getByLabel('Initialize Cluster', { exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Initialize Cluster', exact: true });
    await expect(dialog.getByRole('checkbox', { checked: true })).toHaveCount(3);
    const initialized = page.waitForResponse(response => response.url().endsWith('/api/cluster/init') && response.request().method() === 'POST', { timeout: 10_000 });
    await dialog.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
    const response = await initialized;
    expect(response.status(), await response.text()).toBe(201);
    await waitForLeader(baseURL!, 0, 1, 10_000);
    await expect(dialog).toHaveCount(0);
  });
  await step('three-node: missing disks keep storage services queued', async () => {
    for (const id of [1, 2, 3]) {
      await expect.poll(async () => (await (await request.get('/api/service-plans')).json())[id].steps.diskio.state, { intervals: [100] }).toBe('deployed');
    }
    await expect.poll(async () => {
      const plans = await (await request.get('/api/service-plans')).json();
      return Object.values(plans).map((plan: any) => plan.steps['chunk-kv'].state);
    }, { intervals: [100] }).toEqual(['waiting', 'waiting', 'waiting']);
  });
}

async function addDisks({ page }: UIContext) {
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await step('three-node: disk groups and disks through UI', async () => {
    await page.getByTestId('domain-capacity').click();
    for (const id of [1, 2, 3]) {
      const node = sidebar.getByRole('button', { name: `N-${id}`, exact: true });
      await node.click({ button: 'right' });
      await page.getByRole('menuitem', { name: 'Add Disk Group', exact: true }).click();
      const group = page.getByRole('dialog', { name: 'Add Disk Group', exact: true });
      await group.getByLabel('Disk Group ID (auto-assigned)').fill(String(id));
      await group.getByLabel('KV group', { exact: true }).selectOption('0/1');
      await group.getByRole('button', { name: 'Create Disk Group', exact: true }).click();
      await expect(group).toHaveCount(0);
      const branch = sidebar.getByTestId(`tree-node-N-${id}`);
      const expand = branch.getByRole('button', { name: 'Expand', exact: true });
      if (await expand.count()) await expand.click();
      await sidebar.getByRole('button', { name: `DG-${id}`, exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: 'Add Disk', exact: true }).click();
      const disks = page.getByRole('dialog', { name: 'Add Disks', exact: true });
      await disks.getByRole('button', { name: 'Add Disks', exact: true }).click();
      await expect(disks).toHaveCount(0);
    }
  });
}

async function verifyServices({ page, request, deployments }: UIContext) {
  await step('three-node: all six services resume without Retry', async () => {
    await expect.poll(async () => (await (await request.get('/api/chunk-storage-readiness')).json()), { intervals: [100] }).toMatchObject({ ready: true });
    for (const kind of ['paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server']) {
      for (const id of [1, 2, 3]) {
        await deployments.verify(id, kind);
        await expect.poll(async () => {
          const plans = await (await request.get('/api/service-plans')).json();
          return plans[id].steps[kind];
        }, { intervals: [100], message: `Node ${id}: ${kind} automatically deploys` }).toEqual({ state: 'deployed' });
      }
    }
    const servers = await (await request.get('/api/servers')).json();
    expect(servers.filter((server: { pid: number }) => server.pid)).toHaveLength(18);
    const bindings = await (await request.get('/api/hardware/disk-group-bindings')).json();
    expect(bindings).toHaveLength(3);
    for (const binding of bindings) { expect(binding.store_id).toBe(0); expect(binding.group_id).toBe(1); }
    const owners = await (await request.get('/api/diskdb/instances')).json();
    expect(owners).toHaveLength(3);
    expect(owners.flatMap((owner: { owned_dg_ids: number[] }) => owner.owned_dg_ids).sort()).toEqual([1, 2, 3]);
    expect((await (await request.get('/api/chunk-storage-readiness')).json()).ready).toBe(true);
    await page.getByTestId('domain-chunk').click();
    await expect(page.getByRole('table', { name: 'Chunks', exact: true })).toBeVisible();
    await expect(page.getByText('No live ChunkDB service is registered.', { exact: false })).toHaveCount(0);
    await page.getByTestId('domain-chunk-kv').click();
    await expect(page.getByLabel('Partition range map')).toBeVisible();
    await expect(page.getByText('Catalog unavailable.', { exact: false })).toHaveCount(0);
  });
}

async function s3RoundTrip({ page, request }: UIContext) {
  await step('three-node: S3 upload and exact read through UI', async () => {
    const content = 'CROWDB three-node UI round trip\n雪\n';
    await page.getByTestId('domain-s3').click();
    await page.getByText('S3 actions', { exact: true }).click();
    await page.getByLabel('New bucket', { exact: true }).fill('ui-three-node');
    await page.getByRole('button', { name: 'Create bucket', exact: true }).click();
    await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'ui-three-node', exact: true }).click();
    await page.getByText('Bucket actions', { exact: true }).click();
    await page.getByLabel('Object key', { exact: true }).fill('round-trip.txt');
    await page.getByLabel('Object file').setInputFiles({ name: 'round-trip.txt', mimeType: 'text/plain', buffer: Buffer.from(content) });
    await page.getByRole('button', { name: 'Upload', exact: true }).click();
    await page.getByRole('button', { name: 'round-trip.txt', exact: true }).click();
    await page.getByText('Object actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Preview first 4 KiB', exact: true }).click();
    await expect(page.getByLabel('Object preview')).toHaveText(content);
    const response = await request.get('/api/access/s3/ui-three-node/round-trip.txt');
    expect(response.ok(), await response.text()).toBe(true);
    expect(await response.body()).toEqual(Buffer.from(content));
  });
}

async function icebergRoundTrip({ page, baseURL }: UIContext) {
  await step('three-node: Iceberg table through UI, actual SDK append and read', async () => {
    await page.getByTestId('domain-iceberg').click();
    await page.getByText('Catalog actions', { exact: true }).click();
    await page.getByLabel('Namespace name', { exact: true }).fill('ui_three_node');
    await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
    const tree = page.getByRole('navigation', { name: 'Iceberg tree' });
    await tree.getByRole('button', { name: 'ui_three_node', exact: true }).click();
    await page.getByText('Namespace actions', { exact: true }).click();
    await page.getByLabel('Table name', { exact: true }).fill('events');
    await page.getByLabel('Schema fields (JSON)', { exact: true }).fill(JSON.stringify([{ id: 1, name: 'id', required: false, type: 'long' }, { id: 2, name: 'message', required: false, type: 'string' }]));
    const createdTable = page.waitForResponse(response => new URL(response.url()).pathname === '/api/access/iceberg/v1/namespaces/ui_three_node/tables' && response.request().method() === 'POST');
    await page.getByRole('button', { name: 'Create table', exact: true }).click();
    const created = await createdTable;
    expect(created.status(), await created.text()).toBe(200);
    const writerConfig = (await created.json()).config;
    await tree.getByRole('button', { name: 'events', exact: true }).click();
    const execution = promisify(execFile)('pixi', ['run', '-e', 'iceberg-e2e', 'python', resolve('e2e/fixtures/icebergRoundTrip.py'), `${baseURL}/api/access/iceberg`, 'ui_three_node'], { env: { ...process.env, PYICEBERG_MAX_WORKERS: '4' }, timeout: 300_000 });
    execution.child.stdin!.end(JSON.stringify(writerConfig));
    const result = await execution;
    console.log(result.stdout); if (result.stderr) console.log(result.stderr);
    await page.getByRole('button', { name: 'Refresh table', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Inspect current snapshot', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Inspect current snapshot', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Manifest list records' })).toContainText('avro');
  });
}

async function teardown({ request, baseURL }: UIContext) {
  await step('three-node: owned teardown', async () => {
    const servers = await (await request.get('/api/servers')).json();
    for (const kind of ['access-server', 'chunk-kv', 'chunkdb', 'diskio']) {
      const results = await Promise.all(servers.filter((server: { service_type: string }) => server.service_type === kind)
        .map((server: { id: string }) => request.delete(`/api/services/${server.id}`)));
      for (const response of results) expect(response.ok(), await response.text()).toBe(true);
    }
    await resetAll(baseURL!);
  });
}
