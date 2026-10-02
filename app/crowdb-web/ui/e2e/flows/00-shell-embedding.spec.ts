// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: four tests passed; embedding 2.4s, domain toggle 0.7s (2026-09-27)

import { test, expect } from '../fixtures/realBackend';
import {
  addGroup,
  createStore,
  deployNodeServer,
  freePort,
  resetAll,
  seedRackAndNode,
  stopNodeServer,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Shell-level surfaces that need no shared cluster: backend-unreachable
 * alert and the embedding contract read from the URL query string.
 *
 * Each test needs its own page state (aborted routes / proxied routes /
 * a different mount URL), so they stay separate `test()`s.
 */
test.describe('shell · embedding', () => {
  test('shows an alert when backend API requests fail', async ({ page }) => {
    await step('shell: route abort', () => page.route('**/api/**', route => route.abort('failed')));

    await step('shell: goto', () => page.goto('/'));

    await expect(page.getByRole('alert').filter({ hasText: 'Console mode unavailable.' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
  });

  test('Docker mode shows Group 0 and monitor state without hardware controls', async ({ page }) => {
    await page.route('**/api/mode', route => route.fulfill({ json: { mode: 'docker' } }));
    await page.route('**/api/preview', route => route.fulfill({ json: {
      source: 'group0',
      racks: [{ id: 1, status: 1, node_ids: [1] }],
      nodes: [{ id: 1, rack_id: 1, status: 1 }],
      disk_groups: [],
      disks: [],
      stores: [{ store_id: 0, node_ids: [1] }, { store_id: 7, node_ids: [1] }],
      groups: [{ store_id: 0, group_id: 0 }, { store_id: 7, group_id: 70 }],
      replicas: [],
      services: [{ kind: 'kv-server', node_id: 1, endpoint: 'http://127.0.0.1:19001', monitor: { healthy: true, pid: 100 } }],
      monitor: { phase: 'Ready', revision: 1, updated_at_ms: 1, services: {} },
    } }));
    await page.goto('/');
    await expect(page.getByTestId('managed-preview')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('managed-source')).toHaveText('Source: Group 0');
    await expect(page.getByTestId('managed-readonly')).toHaveText('Hardware topology is read-only');
    await expect(page.getByTestId('managed-monitor-phase')).toContainText('Ready');
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
    await page.route('**/api/management/check', route => route.fulfill({ status: 204 }));
    await page.route(/\/api\/stores(\?.*)?$/, route => route.request().method() === 'GET'
      ? route.fulfill({ json: [{ store_id: 0, nodes: [1], groups: [{ group_id: 0, replicas: [] }] }, { store_id: 7, nodes: [1], groups: [{ group_id: 70, replicas: [] }] }] })
      : route.fallback());
    await page.route(/\/api\/stores\/([07])\/groups\/(0|70)(\?.*)?$/, route => route.fulfill({ json: { store_id: Number(new URL(route.request().url()).pathname.split('/')[3]), group_id: Number(new URL(route.request().url()).pathname.split('/')[5]), state: 'Running', replicas: [] } }));
    await page.getByTestId('domain-kv').click();
    await expect(page.getByRole('button', { name: 'Add Store' })).toHaveCount(0);
    const writes: Array<{ path: string; token: string | undefined; body: unknown }> = [];
    await page.route('**/api/stores**', async (route) => {
      const request = route.request();
      if (request.method() === 'GET') return route.fallback();
      writes.push({
        path: new URL(request.url()).pathname,
        token: request.headers().authorization,
        body: request.postDataJSON(),
      });
      await route.fulfill({ status: 201, json: {} });
    });
    await page.getByLabel('Management token').fill('m'.repeat(64));
    await page.getByRole('button', { name: 'Authorize', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Add Store', exact: true })).toBeVisible({ timeout: 3000 });
    await page.getByRole('button', { name: 'Add Store', exact: true }).click();
    await page.getByLabel('KV Store ID (numeric)').fill('8');
    await page.getByLabel(/^1\b/).check();
    await page.getByRole('button', { name: /create kv store/i }).click();
    await expect.poll(() => writes.length, { intervals: [100] }).toBe(1);
    expect(writes[0]).toEqual({ path: '/api/stores', token: `Bearer ${'m'.repeat(64)}`, body: { store_id: 8, nodes: [1] } });
    await page.getByRole('button', { name: 'S-7', exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add group/i }).click();
    await page.getByLabel('Group ID (numeric)').fill('71');
    await page.getByLabel('Starting Replica ID (numeric)').fill('710');
    await page.getByLabel(/^1\b/).check();
    await page.getByRole('button', { name: /create group/i }).click();
    await expect.poll(() => writes.length, { intervals: [100] }).toBe(2);
    expect(writes[1].path).toBe('/api/stores/7/groups');
    await page.getByRole('button', { name: 'G-70', exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: /add replica/i }).click();
    await page.getByLabel('Node', { exact: true }).selectOption('1');
    await page.getByRole('button', { name: 'Add replica' }).click();
    await expect.poll(() => writes.length, { intervals: [100] }).toBe(3);
    expect(writes[2].path).toBe('/api/stores/7/groups/70/replicas');
    await page.getByTestId('domain-cluster').click();
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
  });

  test('Docker mode separates unavailable topology from current monitor recovery', async ({ page }) => {
    await page.clock.install();
    await page.route('**/api/mode', route => route.fulfill({ json: { mode: 'docker' } }));
    let available = true;
    let monitor: object | null = {
      phase: 'ready', revision: 1, updated_at_ms: 1,
      services: { kv: { pid: 100, generation: 1, restart_attempts: 0, healthy: true } },
    };
    await page.route('**/api/preview', route => route.fulfill({
      status: available ? 200 : 503,
      json: available ? {
        source: 'group0', racks: [], nodes: [], disks: [], disk_groups: [],
        stores: [{ store_id: 7, node_ids: [1] }], groups: [], replicas: [], services: [], monitor,
      } : { source: 'group0', available: false,
        reason: monitor ? 'group0_unavailable' : 'monitor_unavailable', monitor },
    }));
    await page.goto('/');
    await expect(page.getByTestId('managed-process-kv')).toContainText('PID 100');
    await expect(page.getByTestId('managed-monitor-phase')).toContainText('ready');
    available = false;
    monitor = { phase: 'restarting', revision: 2, updated_at_ms: 2,
      services: { kv: { pid: null, generation: 1, restart_attempts: 1, healthy: false } } };
    await page.clock.runFor(3001);
    await expect(page.getByTestId('managed-unavailable')).toContainText('Group 0 is unavailable');
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'S-7', exact: true })).toHaveCount(0);
    await expect(page.getByTestId('managed-process-kv')).toContainText('unhealthy');
    await expect(page.getByTestId('managed-monitor-phase')).toContainText('restarting');
    monitor = null;
    await page.clock.runFor(3001);
    await expect(page.getByTestId('managed-unavailable')).toContainText('missing or stale');
    await expect(page.getByTestId('managed-process-kv')).toHaveCount(0);
    available = true;
    monitor = { phase: 'ready', revision: 3, updated_at_ms: 3,
      services: { kv: { pid: 200, generation: 2, restart_attempts: 1, healthy: true } } };
    await page.clock.runFor(3001);
    await expect(page.getByTestId('managed-unavailable')).toHaveCount(0);
    await expect(page.getByTestId('managed-process-kv')).toContainText('PID 200');
    await expect(page.getByTestId('managed-process-kv')).toContainText('generation 2');
  });

  test('embedding honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL }) => {
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: seed rack/node', () => seedRackAndNode(baseURL!, 23, 23));
    await step('shell: deploy server', () => deployNodeServer(baseURL!, 23, freePort(), freePort()));
    await step('shell: create store', () => createStore(baseURL!, 233, [23]));
    await step('shell: add group', () => addGroup(baseURL!, 233, 2330, 23300, [23]));

    // Reverse-proxy emulation: the SPA issues /proxy/api/* which we rewrite
    // back onto the real /api/* surface served by crowdb-web.
    await step('shell: route proxy', () => page.route('**/proxy/api/**', (route) => {
      const u = new URL(route.request().url());
      u.pathname = u.pathname.replace('/proxy/api', '/api');
      route.continue({ url: u.toString() });
    }));

    const seen: string[] = [];
    page.on('request', (req) => seen.push(req.url()));

    try {
      const apiPrefix = encodeURIComponent('/proxy/api');
      const proxyRequest = page.waitForRequest('**/proxy/api/**', { timeout: 3_000 });
      await step('shell: goto embed', () => page.goto(`/?domain=KV&readonly=1&disableModules=${encodeURIComponent('kv')}&apiPrefix=${apiPrefix}`));

      // apiPrefix: the SPA re-roots every data-plane call under /proxy/api.
      await step('shell: wait proxy request', () => proxyRequest);
      expect(seen.some((u) => u.includes('/proxy/api/'))).toBeTruthy();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // Data still loads (rewritten back onto /api by the route above).
      await expect(aside.getByText('S-233', { exact: true })).toBeVisible({ timeout: 3_000 });

      // readonly: no Add control in the sidebar.
      await expect(aside.getByRole('button', { name: 'Add Store' })).toHaveCount(0);

      // modules: selecting the group exposes Details/Activity but no KV tab.
      const group233 = page.getByRole('treeitem').filter({ hasText: 'G-2330' });
      const expandStore = page.getByRole('treeitem').filter({ hasText: 'S-233' }).getByRole('button', { name: 'Expand' });
      if (await expandStore.count()) await expandStore.click();
      await group233.getByRole('button', { name: 'G-2330' }).click();
      const inspector = page.locator('aside[aria-label="Entity inspector"]');
      await expect(inspector.getByRole('tab', { name: 'Details' })).toBeVisible({ timeout: 3_000 });
      await expect(inspector.getByRole('tab', { name: 'KV' })).toHaveCount(0);
    } finally {
      await step('shell: stop server', () => stopNodeServer(baseURL!, 23));
    }
  });

  test('domain toggle switches between Cluster, KV, and Chunk', async ({ page }) => {
    await step('shell: goto', () => page.goto('/'));

    // Domain toggle buttons are visible.
    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible();
    await expect(page.getByTestId('domain-chunk')).toBeVisible();

    // Default domain is Cluster.
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');

    // Switch to KV.
    await page.getByTestId('domain-kv').click();
    await expect(page.getByTestId('domain-kv')).toHaveAttribute('aria-pressed', 'true');

    // Switch to Chunk.
    await page.getByTestId('domain-chunk').click();
    await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');

    // Switch back to Cluster.
    await page.getByTestId('domain-cluster').click();
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
  });
});
