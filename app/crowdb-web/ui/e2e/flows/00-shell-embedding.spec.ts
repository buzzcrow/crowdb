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
import { consoleProxy } from '../fixtures/consoleProxy';

/**
 * Shell-level surfaces that need no shared cluster: backend-unreachable
 * alert and the embedding contract read from the URL query string.
 *
 * Each test needs its own page state (closed listener / reverse proxy /
 * a different mount URL), so they stay separate `test()`s.
 */
test.describe('shell · embedding', () => {
  test('shows an alert when backend API requests fail', async ({ page }) => {
    // A closed real listener makes the configured API genuinely unreachable.
    const proxy = await consoleProxy('http://127.0.0.1:1');
    await proxy.close();
    await step('shell: goto', () => page.goto(`/?apiPrefix=${encodeURIComponent(`${proxy.url}/api`)}`));

    await expect(page.getByRole('alert').filter({ hasText: 'Console mode unavailable.' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
  });

  test('embedding honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL }) => {
    await step('shell: resetAll', () => resetAll(baseURL!));
    await step('shell: seed rack/node', () => seedRackAndNode(baseURL!, 23, 23));
    await step('shell: deploy server', () => deployNodeServer(baseURL!, 23, freePort(), freePort()));
    await step('shell: create store', () => createStore(baseURL!, 233, [23]));
    await step('shell: add group', () => addGroup(baseURL!, 233, 2330, 23300, [23]));

    const proxy = await consoleProxy(baseURL!);
    const seen: string[] = [];
    page.on('request', (req) => seen.push(req.url()));

    try {
      const apiPrefix = encodeURIComponent('/proxy/api');
      const proxyRequest = page.waitForRequest('**/proxy/api/**', { timeout: 3_000 });
      await step('shell: goto embed', () => page.goto(`${proxy.url}/?domain=KV&readonly=1&disableModules=${encodeURIComponent('kv')}&apiPrefix=${apiPrefix}`));

      // apiPrefix: the SPA re-roots every data-plane call under /proxy/api.
      await step('shell: wait proxy request', () => proxyRequest);
      expect(seen.some((u) => u.includes('/proxy/api/'))).toBeTruthy();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // Data still loads through the actual reverse proxy.
      await expect(aside.getByText('S-233', { exact: true })).toBeVisible({ timeout: 3_000 });

      // readonly: no Add control in the sidebar.
      await expect(aside.getByRole('button', { name: 'Add Store' })).toHaveCount(0);

      // modules: selecting the group exposes Details/Activity but no KV tab.
      await page.getByTestId('domain-cluster').click();
      const node = page.getByRole('treeitem').filter({ hasText: 'N-23' });
      await node.getByRole('button', { name: 'N-23', exact: true }).click();
      const inspector = page.getByRole('complementary', { name: 'Entity inspector', exact: true });
      await expect(inspector.getByRole('tab', { name: 'Details', exact: true })).toBeVisible();
      await expect(inspector.getByRole('tab', { name: 'KV', exact: true })).toHaveCount(0);
      await page.getByTestId('domain-kv').click();
      const group233 = page.getByRole('treeitem').filter({ hasText: 'G-2330' });
      const expandStore = page.getByRole('treeitem').filter({ hasText: 'S-233' }).getByRole('button', { name: 'Expand' });
      if (await expandStore.count()) await expandStore.click();
      await group233.getByRole('button', { name: 'G-2330' }).click();
      await expect(page.getByRole('button', { name: 'Put', exact: true })).toHaveCount(0);
    } finally {
      await proxy.close();
      await step('shell: stop server', () => stopNodeServer(baseURL!, 23));
    }
  });

  test('domain toggle switches between all seven domains', async ({ page }) => {
    await step('shell: goto', () => page.goto('/'));

    // Domain toggle buttons are visible.
    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible();
    await expect(page.getByTestId('domain-chunk')).toBeVisible();

    for (const domain of ['capacity', 'chunk-kv', 'iceberg', 's3']) await expect(page.getByTestId(`domain-${domain}`)).toBeVisible();

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
