// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: empty 0.92s, datacenter 1.3s, ownership 1.3s, Access plan 2.1s (2026-10-12)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createNode } from '../fixtures/registrySetup';
import { nativeProcessAlive } from '../fixtures/nativeProcess';
import { step } from '../fixtures/stepTimer';

test.describe('cluster · registry browser', () => {
  test('renders the SPA shell against a real empty backend', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') {
        consoleErrors.push(message.text());
      }
    });

    await step('shell: goto', () => page.goto('/'));

    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible({ timeout: 3_000 });

    const healthText = page.locator('header').getByText(/healthy|degraded|failed|unknown/i);
    await expect(healthText).toHaveText(/Unknown/);

    // Ignore transient network 404s; fail only on real JS/runtime errors.
    const jsErrors = consoleErrors.filter((e) => !/Failed to load resource/i.test(e));
    expect(jsErrors, jsErrors.join('\n')).toEqual([]);
  });

  test('datacenter root wraps racks; Add Rack from datacenter context menu', async ({ page, baseURL }) => {
    await step('dc-root: createRack 60', () => createRack(baseURL!, { id: 60, name: 'Rack Sixty' }));

    await step('dc-root: goto', () => page.goto('/'));
    await step('dc-root: add rack from DC UI', async () => {
      await page.getByTestId('domain-cluster').click();
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

      // The datacenter root is the top treeitem, above the rack.
      const dcItem = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
      await expect(dcItem).toBeVisible({ timeout: 3_000 });
      await expect(aside.getByRole('treeitem').first()).toHaveText(/datacenter/);

      // The rack appears as the datacenter's child.
      await expect(aside.getByText('R-60 (Rack Sixty)')).toBeVisible({ timeout: 3_000 });

      await page.getByRole('button', { name: 'Show diagram', exact: true }).click();
      // The topology canvas also renders the datacenter node at layer 0.
      await expect(page.locator('.react-flow__node').filter({ hasText: 'datacenter' })).toBeVisible({ timeout: 5_000 });

      // Right-click the datacenter → Add Rack (the only menu item).
      await aside.getByText('datacenter', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add rack/i }).click();

      await expect(page.getByRole('dialog', { name: 'Add Rack' })).toBeVisible();
      await page.getByLabel('Rack ID').fill('61');
      await page.getByLabel('Name (optional)').fill('Rack Sixty-One');
      await page.getByRole('button', { name: /create rack/i }).click();

      // The new rack appears as another child of the datacenter.
      await expect(aside.getByText('R-61 (Rack Sixty-One)')).toBeVisible({ timeout: 3_000 });
    });

    const api = await apiContext(baseURL!);
    try {
      await step('dc-root: verify rack API', async () => {
        const response = await api.get('/api/racks');
        expect(response.ok(), await response.text()).toBeTruthy();
        const racks = await response.json();
        expect(racks).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: 61, name: 'Rack Sixty-One' }),
          ]),
        );
      });
    } finally {
      await api.dispose();
    }
  });

  test('keeps Cluster and unprovisioned Chunk selections free of ownership probes', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 381, name: 'Ownership scope' });
    await createNode(baseURL!, { id: 381, rack_id: 381 });
    const probes: string[] = [];
    page.on('request', request => { if (request.url().includes('/api/chunk-slots')) probes.push(request.url()); });
    await page.goto('/');
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    for (const name of ['datacenter', 'R-381 (Ownership scope)', 'N-381']) {
      await aside.getByText(name, { exact: true }).click();
      await expect(page.getByRole('region', { name: 'Chunk ownership', exact: true })).toHaveCount(0);
    }
    await page.getByTestId('domain-chunk').click();
    const tree = page.getByRole('navigation', { name: 'Chunk hierarchy' });
    await tree.getByText('N-381', { exact: true }).click();
    await expect(page.getByRole('region', { name: 'Chunk ownership', exact: true })).toHaveCount(0);
    expect(probes).toEqual([]);
  });

  // Baseline: 2.2s (2026-10-16), before adding the fourth listener.
  test('persists an Access-only plan with four independent ports across reload', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 382, name: 'Access plan' });
    await page.goto('/');
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await aside.getByText('R-382 (Access plan)', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Node' });
    await dialog.getByLabel('Node ID', { exact: true }).fill('382');
    for (const kind of ['paxos-kv', 'chunk-kv', 'chunk-db', 'disk-db', 'disk-io']) await dialog.getByRole('checkbox', { name: `crowdb-${kind}`, exact: true }).uncheck();
    const base = 9000;
    const ports = [base, base + 1, base + 2, base + 3];
    for (const [index, label] of ['Iceberg', 'S3', 'Health', 'Dataset'].entries()) await dialog.getByLabel(`crowdb-access-server ${label} port`, { exact: true }).fill(String(ports[index]));
    await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const api = await apiContext(baseURL!);
    try {
      const read = async () => { const response = await api.get('/api/service-plans'); expect(response.ok()).toBeTruthy(); return (await response.json())['382']; };
      const plan = await read();
      expect(plan.overrides['access-server']).toMatchObject({ http_port: ports[0], s3_port: ports[1], health_port: ports[2], dataset_port: ports[3] });
      for (const kind of ['paxos-kv', 'chunk-kv', 'chunkdb', 'diskdb', 'diskio']) expect(plan.steps[kind].state).toBe('disabled');
      await page.reload();
      await expect(aside.getByText('N-382', { exact: true })).toBeVisible();
      expect((await read()).overrides).toEqual(plan.overrides);
      await expect.poll(async () => (await read()).steps['access-server'].state, { intervals: [100], timeout: 3000 }).toBe('waiting');
    } finally { await api.dispose(); }
  });

  // Baseline: 2.036s (2026-10-12)
  test('rejects duplicate rack and node IDs from the add dialogs', async ({ page, baseURL }) => {

    // --- Adding a rack with an existing ID is rejected ---
    {
      await step('dup-id: createRack 37', () => createRack(baseURL!, { id: 37, name: 'r37' }));

      await step('dup-id: dup rack UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        // Click Add Rack
        await aside.getByRole('button', { name: 'Add Rack' }).click();
        const dialog = page.getByRole('dialog', { name: 'Add Rack' });
        await expect(dialog).toBeVisible();
        await dialog.getByLabel('Rack ID').fill('37');
        await dialog.getByLabel('Name (optional)').fill('duplicate');

        const responsePromise = page.waitForResponse(r => r.request().method() === 'POST' && new URL(r.url()).pathname === '/api/racks');
        await dialog.getByRole('button', { name: /create rack/i }).click();
        const response = await responsePromise;
        expect(response.status()).toBe(409);
      });
    }

    // --- Adding a node with an existing ID retains its inline error ---
    {
      await step('dup-id: setup dup node', async () => {
        await createRack(baseURL!, { id: 372, name: 'r37b' });
        await createNode(baseURL!, { id: 372, rack_id: 372 });
      });

      await step('dup-id: dup node UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        // Right-click rack to get Add Node
        const rackItem = page.getByRole('treeitem').filter({ hasText: 'R-372' });
        await expect(rackItem).toBeVisible({ timeout: 3_000 });
        await rackItem.click({ button: 'right' });
        await page.getByRole('menuitem', { name: /add node/i }).click();

        const dialog = page.getByRole('dialog', { name: 'Add Node' });
        await expect(dialog).toBeVisible();
        await dialog.getByLabel('Node ID').fill('372');
        await dialog.getByLabel('Host').fill('127.0.0.1');

        const responsePromise = page.waitForResponse(r => r.request().method() === 'POST' && new URL(r.url()).pathname === '/api/nodes');
        await dialog.getByRole('button', { name: /create node/i }).click();
        const response = await responsePromise;
        expect(response.status()).toBe(409);
        await expect(dialog.getByRole('alert')).toContainText('node 372 already exists');
      });
    }
  });
  // Baseline: 2.0s (2026-10-12).
  test('persists all six selected services but starts only PKV before Group 0', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 383, name: 'Queued services' });
    const deployments: string[] = [];
    page.on('request', request => {
      if (request.method() === 'POST' && /\/api\/nodes\/383\/.*\/deploy$/.test(new URL(request.url()).pathname)) deployments.push(new URL(request.url()).pathname);
    });
    await page.goto('/');
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await aside.getByText('R-383 (Queued services)', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Node' });
    await dialog.getByLabel('Node ID', { exact: true }).fill('383');
    for (const kind of ['access-server', 'chunk-kv', 'chunk-db', 'disk-db', 'disk-io', 'paxos-kv']) await expect(dialog.getByRole('checkbox', { name: `crowdb-${kind}`, exact: true })).toBeChecked();
    await expect(dialog.getByRole('button', { name: 'Create Node', exact: true })).toBeEnabled();
    await dialog.getByLabel('crowdb-paxos-kv RPC port', { exact: true }).fill('10100');
    await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const api = await apiContext(baseURL!);
    try {
      await expect.poll(async () => {
        const response = await api.get('/api/service-plans');
        expect(response.ok()).toBeTruthy();
        const plan = (await response.json())['383'];
        return plan && Object.fromEntries(Object.entries(plan.steps).map(([kind, step]: [string, any]) => [kind, step.state]));
      }, { timeout: 10_000, intervals: [100] }).toEqual({
        'paxos-kv': 'deployed', 'access-server': 'waiting', 'chunk-kv': 'waiting',
        chunkdb: 'waiting', diskdb: 'waiting', diskio: 'waiting',
      });
      const response = await api.get('/api/servers');
      expect(response.ok()).toBeTruthy();
      const servers = await response.json();
      expect(servers.map((server: any) => server.service_type)).toEqual(['paxos-kv']);
      expect(servers[0].pid).toBeGreaterThan(1);
      expect(await nativeProcessAlive(0, servers[0].pid)).toBe(true);
      expect(deployments).toEqual(['/api/nodes/383/server/deploy']);
      await page.reload();
      await expect(aside.getByText('N-383', { exact: true })).toBeVisible();
      const persisted = await api.get('/api/service-plans');
      expect(persisted.ok()).toBeTruthy();
      const plan = (await persisted.json())['383'];
      for (const kind of ['access-server', 'chunk-kv', 'chunkdb', 'diskdb', 'diskio']) expect(plan.steps[kind].state).toBe('waiting');
    } finally { await api.dispose(); }
  });

});
