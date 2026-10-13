// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';
import { observeDefaultDeployments } from '../fixtures/defaultDeployments';
import { nativeProcessAlive } from '../fixtures/nativeProcess';

test('native diagnostics: auxiliary menus stop and restart the actual typed process', async ({ page, request }) => {
  const alive = (pid: number) => nativeProcessAlive(0, pid);
  const list = async () => {
    const response = await request.get('/api/servers');
    expect(response.ok(), await response.text()).toBe(true);
    return response.json();
  };
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const [kind, label, prefix] of [['chunkdb', 'crowdb-chunk-db', 'CDB'], ['diskio', 'crowdb-disk-io', 'DIO'], ['chunk-kv', 'crowdb-chunk-kv', 'CKV'], ['access-server', 'crowdb-access-server', 'AS']]) {
    const original = (await list()).find((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind);
    expect(original.pid_node_id).toBe(original.node_id);
    expect(original.pid).toBeGreaterThan(0); expect(await alive(original.pid)).toBe(true);
    const node = sidebar.getByRole('button', { name: 'N-1', exact: true });
    const item = sidebar.getByText(`${prefix}-1`, { exact: true });
    await expect(item).toBeVisible();
    await node.click({ button: 'right' });
    await page.getByRole('menuitem', { name: `${prefix}-1 Running`, exact: true }).hover();
    const stoppedObservation = page.waitForResponse(async response => {
      if (new URL(response.url()).pathname !== '/api/servers' || !response.ok()) return false;
      const rows = await response.json();
      return rows.some((row: { id: string; pid?: number }) => row.id === original.id && !row.pid);
    });
    await page.getByRole('menuitem', { name: `Stop ${label}`, exact: true }).click();
    await stoppedObservation;
    await expect(page.getByRole('button', { name: 'Refresh', exact: true }).locator('svg')).not.toHaveClass(/tw-animate-spin/);
    await expect.poll(() => alive(original.pid), { intervals: [100] }).toBe(false);
    await expect.poll(async () => {
      const stopped = (await list()).find((row: { id: string }) => row.id === original.id);
      return { id: stopped?.id, pid: stopped?.pid ?? null };
    }, { intervals: [100] }).toEqual({ id: original.id, pid: null });
    let startSubmitted = false;
    try {
      await node.click({ button: 'right' });
      await page.getByRole('menuitem', { name: `${prefix}-1 Stopped`, exact: true }).hover();
      // Native recovery previously reached its listener after 3.15s; keep a
      // modest response budget while process and DOM assertions remain 3s.
      const started = page.waitForResponse(response =>
        new URL(response.url()).pathname === `/api/services/${original.id}/restart`
        && response.request().method() === 'POST', { timeout: 5_000 });
      const startAt = performance.now();
      startSubmitted = true;
      const response = await step(`native lifecycle: ${original.id} Start response`, async () => {
        await page.getByRole('menuitem', { name: `Start ${label}`, exact: true }).click();
        return started;
      });
      console.log(`[START] ${original.id}: ${(performance.now() - startAt).toFixed(0)}ms`);
      expect(response.status(), await response.text()).toBe(200);
      await step(`native lifecycle: ${original.id} new process`, () => expect.poll(async () => {
        const current = (await list()).find((row: { id: string }) => row.id === original.id);
        return current?.pid && current.pid !== original.pid && await alive(current.pid);
      }, { intervals: [100] }).toBe(true));
      expect((await list()).filter((row: { id: string }) => row.id === original.id)).toHaveLength(1);
    } finally {
      const current = (await list()).find((row: { id: string }) => row.id === original.id);
      if (!startSubmitted && !current?.pid) expect((await request.post(`/api/services/${original.id}/restart`, { data: {} })).ok()).toBe(true);
    }
  }
  expect(await list()).toHaveLength(18);
});

// Baseline: new native defaults/reopen acceptance (2026-10-04).
test('native diagnostics: auxiliary dialogs reopen with unused IDs ports and valid dependencies', async ({ page, request }) => {
  const response = await request.get('/api/deployment-defaults?node_id=1');
  expect(response.ok(), await response.text()).toBe(true);
  const defaults = await response.json();
  const serversResponse = await request.get('/api/servers');
  expect(serversResponse.ok()).toBe(true);
  const servers = await serversResponse.json();
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const [kind, label] of [['chunkdb', 'crowdb-chunk-db'], ['diskio', 'crowdb-disk-io'], ['chunk-kv', 'crowdb-chunk-kv'], ['access-server', 'crowdb-access-server']]) {
    for (let reopen = 0; reopen < 2; reopen++) {
      await sidebar.getByRole('button', { name: 'N-1', exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: `Deploy ${label}`, exact: true }).click();
      const dialog = page.getByRole('dialog', { name: `Deploy ${label}`, exact: true });
      await expect(dialog.getByRole('button', { name: 'Deploy service', exact: true })).toBeEnabled();
      await expect(dialog.getByLabel('Instance ID', { exact: true })).toHaveValue(defaults[kind].instance_id);
      expect(servers.some((server: { id: string }) => server.id === `${kind}-${defaults[kind].instance_id}`)).toBe(false);
      const fields = kind === 'diskio' ? [['RPC port', 'rpc_port']] : kind === 'access-server'
        ? [['Iceberg HTTP port', 'http_port'], ['S3 HTTP port', 's3_port']]
        : [['Management HTTP port', 'http_port'], ['RPC port', 'rpc_port']];
      for (const [field, key] of fields) await expect(dialog.getByLabel(field, { exact: true })).toHaveValue(String(defaults[kind][key]));
      if (kind === 'diskio') await expect(dialog.getByLabel('Disk group', { exact: true })).toHaveValue('1');
      if (kind === 'chunk-kv') {
        await expect(dialog.getByLabel('Metadata store', { exact: true })).toHaveValue('1');
        await expect(dialog.getByLabel('Journal metadata group', { exact: true })).toHaveValue('1');
      }
      await expect(dialog.getByLabel('Single-node test deployment (reduced redundancy)')).not.toBeChecked();
      await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
      await expect(dialog).toHaveCount(0);
    }
  }
  expect(await (await request.get('/api/servers')).json()).toEqual(servers);
});

// Baseline: 1.3s (2026-10-04); existing instances come from the owned fixture.
test('native diagnostics: interrupted six-service plan reconciles without duplicate deployment', async ({ page, request }) => {
  test.skip(!!process.env.CROWDB_NATIVE_PLAN_PREREQUISITES, 'Requires the complete six-service fixture phase');
  const kinds = ['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'];
  const servers = await request.get('/api/servers');
  expect(servers.ok()).toBe(true);
  const original = await servers.json();
  const identity = (rows: { id: string; pid: number; node_id: number; service_type: string }[]) => rows
    .map(row => `${row.node_id}/${row.service_type}/${row.id}/${row.pid}`).sort();
  expect(original).toHaveLength(18);
  for (const kind of kinds) expect(original.filter((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind)).toHaveLength(1);
  const saved = await request.put('/api/nodes/1/service-plan', { data: {
    revision: 0,
    steps: Object.fromEntries(kinds.map(kind => [kind, { state: kind === 'paxos-kv' ? 'deployed' : kind === 'chunkdb' ? 'deploying' : 'pending' }])),
  } });
  expect(saved.ok(), await saved.text()).toBe(true);
  let deploymentRequests = 0;
  page.on('request', current => {
    if (current.method() === 'POST' && /\/api\/nodes\/\d+\/(server|diskdb|services)\/deploy$/.test(new URL(current.url()).pathname)) deploymentRequests++;
  });
  const dialog = page.getByRole('dialog', { name: 'Node 1 services', exact: true });
  const open = async () => {
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expand = aside.getByRole('treeitem').filter({ hasText: 'R-1' }).locator('button[aria-label="Expand"]');
    if (await expand.count() > 0) await expand.click();
    await aside.getByText('N-1', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  };
  await step('native plan recovery DOM', async () => {
    await page.goto('/?domain=Cluster');
    await page.reload();
    await open();
    await expect(dialog).toContainText('Deployment was interrupted');
    await expect(dialog.getByRole('listitem')).toHaveCount(6);
    await dialog.getByRole('button', { name: 'Retry failed services', exact: true }).click();
    await expect(dialog.getByRole('button', { name: 'Done', exact: true })).toBeEnabled();
  });
  await step('native plan authoritative reconciliation', async () => {
    await expect.poll(async () => {
      const response = await request.get('/api/service-plans');
      expect(response.ok()).toBe(true);
      return Object.values((await response.json())['1'].steps).map((entry: any) => entry.state);
    }, { timeout: 3000, intervals: [100] }).toEqual(kinds.map(() => 'deployed'));
    const current = await request.get('/api/servers');
    expect(current.ok()).toBe(true);
    expect(identity(await current.json())).toEqual(identity(original));
    expect(deploymentRequests).toBe(0);
  });
  await step('native completed plan reload', async () => {
    await page.reload();
    await open();
    await expect(dialog.getByRole('button', { name: 'Done', exact: true })).toBeEnabled();
    await expect(dialog).not.toContainText('Deployment was interrupted');
    expect(deploymentRequests).toBe(0);
  });
});

// Baseline: 7.6s (2026-10-04); prerequisite services are controlled through real lifecycle APIs.
test('native diagnostics: waiting plan resumes when DiskIO prerequisites arrive', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NATIVE_PLAN_PREREQUISITES, 'Requires the partial deployment fixture phase');
  const kinds = ['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'];
  const deployments = observeDefaultDeployments(page);
  const list = async () => {
    const response = await request.get('/api/servers');
    expect(response.ok()).toBe(true);
    return response.json();
  };
  const original = await list();
  expect(original).toHaveLength(12);
  expect(original.filter((row: { service_type: string }) => ['chunk-kv', 'access-server'].includes(row.service_type))).toHaveLength(0);
  const diskioIds = original.filter((row: { service_type: string; node_id: number }) => row.service_type === 'diskio' && row.node_id !== 1)
    .map((row: { id: string }) => row.id);
  expect(diskioIds).toHaveLength(2);
  const deploys: string[] = [];
  page.on('request', current => {
    if (current.method() === 'POST' && /\/api\/nodes\/\d+\/(server|diskdb|services)\/deploy$/.test(new URL(current.url()).pathname)) deploys.push(current.postDataJSON().kind);
  });
  const dialog = page.getByRole('dialog', { name: 'Node 1 services', exact: true });
  const open = async () => {
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expand = aside.getByRole('treeitem').filter({ hasText: 'R-1' }).locator('button[aria-label="Expand"]');
    if (await expand.count() > 0) await expand.click();
    await aside.getByText('N-1', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  };
  try {
    await step('native plan missing prerequisite', async () => {
      for (const id of diskioIds) expect((await request.post(`/api/services/${id}/stop`, { data: {} })).ok()).toBe(true);
      const saved = await request.put('/api/nodes/1/service-plan', { data: {
        revision: 0,
        steps: Object.fromEntries(kinds.map(kind => [kind, { state: ['chunk-kv', 'access-server'].includes(kind) ? 'pending' : 'deployed' }])),
      } });
      expect(saved.ok(), await saved.text()).toBe(true);
      await page.goto('/?domain=Cluster');
      await open();
      await expect(dialog).toContainText('restart registered DiskIO services');
      await expect(dialog).toContainText('deploy Chunk-KV and initialize its catalog');
      expect(deploys).toEqual([]);
      await page.reload();
      await open();
      await expect(dialog).toContainText('restart registered DiskIO services');
      expect(deploys).toEqual([]);
    });
    await step('native plan prerequisite arrival', async () => {
      expect((await request.post(`/api/services/${diskioIds[0]}/restart`, { data: {} })).ok()).toBe(true);
      await expect.poll(async () => {
        const response = await request.get('/api/service-plans');
        expect(response.ok()).toBe(true);
        return (await response.json())['1'].steps['chunk-kv'].state;
      }, { timeout: 3000, intervals: [100] }).toBe('waiting');
      expect(deploys).toEqual([]);
      expect((await request.post(`/api/services/${diskioIds[1]}/restart`, { data: {} })).ok()).toBe(true);
      await expect(dialog.getByRole('listitem').filter({ hasText: 'deploying' })).toHaveCount(1);
      await deployments.verify(1, 'chunk-kv');
      await deployments.verify(1, 'access-server');
      await expect.poll(async () => {
        const response = await request.get('/api/service-plans');
        expect(response.ok()).toBe(true);
        return Object.values((await response.json())['1'].steps).map((entry: any) => entry.state);
      }, { timeout: 3000, intervals: [100] }).toEqual(kinds.map(() => 'deployed'));
      await expect(dialog.getByRole('listitem').filter({ hasText: 'deployed' })).toHaveCount(6);
      expect(deploys).toEqual(['chunk-kv', 'access-server']);
      const current = await list();
      expect(current).toHaveLength(14);
      for (const kind of kinds) expect(current.filter((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind)).toHaveLength(1);
      for (const previous of original.filter((row: { service_type: string }) => row.service_type !== 'diskio')) {
        expect(current.find((row: { id: string }) => row.id === previous.id)?.pid).toBe(previous.pid);
      }
      const buckets = await request.get('/api/access/s3/');
      expect(buckets.ok()).toBe(true);
      expect(await buckets.text()).toContain('ListAllMyBucketsResult');
    });
  } finally {
    for (const id of diskioIds) {
      const current = (await list()).find((row: { id: string }) => row.id === id);
      if (!current.pid) expect((await request.post(`/api/services/${id}/restart`, { data: {} })).ok()).toBe(true);
    }
  }
});
