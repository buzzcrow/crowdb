// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: empty 0.92s, datacenter 1.3s, ownership 1.3s, Access plan 2.1s (2026-10-12)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, createRack, createNode } from '../fixtures/registrySetup';
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
    await expect(healthText).toBeVisible({ timeout: 3_000 });

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

  test('persists an Access-only plan with three independent ports across reload', async ({ page, baseURL }) => {
    await createRack(baseURL!, { id: 382, name: 'Access plan' });
    await page.goto('/');
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    await aside.getByText('R-382 (Access plan)', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add node/i }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Node' });
    await dialog.getByLabel('Node ID', { exact: true }).fill('382');
    for (const kind of ['paxos-kv', 'chunk-kv', 'chunk-db', 'disk-db', 'disk-io']) await dialog.getByRole('checkbox', { name: `crowdb-${kind}`, exact: true }).uncheck();
    const base = 9000;
    const ports = [base, base + 1, base + 2];
    for (const [index, label] of ['Iceberg', 'S3', 'Health'].entries()) await dialog.getByLabel(`crowdb-access-server ${label} port`, { exact: true }).fill(String(ports[index]));
    await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const api = await apiContext(baseURL!);
    try {
      const read = async () => { const response = await api.get('/api/service-plans'); expect(response.ok()).toBeTruthy(); return (await response.json())['382']; };
      const plan = await read();
      expect(plan.overrides['access-server']).toMatchObject({ http_port: ports[0], s3_port: ports[1], health_port: ports[2] });
      for (const kind of ['paxos-kv', 'chunk-kv', 'chunkdb', 'diskdb', 'diskio']) expect(plan.steps[kind].state).toBe('disabled');
      await page.reload();
      await expect(aside.getByText('N-382', { exact: true })).toBeVisible();
      expect((await read()).overrides).toEqual(plan.overrides);
      await expect.poll(async () => (await read()).steps['access-server'].state, { intervals: [100], timeout: 3000 }).toBe('waiting');
    } finally { await api.dispose(); }
  });
});
