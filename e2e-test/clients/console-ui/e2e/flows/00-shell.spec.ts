// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: embedding 2.8s, unavailable 0.453s, navigation 1.1s (2026-10-15)

import { test, expect } from "../fixtures/realBackend";
import { step } from "../fixtures/stepTimer";
import { consoleProxy } from "../fixtures/consoleProxy";

test.describe("shell · embedding", () => {
  test('shows an alert when backend API requests fail', async ({ page, baseURL }) => {
    // A closed fixed listener on the owned node preserves the real network fault.
    const unavailable = new URL(baseURL!);
    unavailable.port = '9091';
    await step('shell: goto', () => page.goto(`/?apiPrefix=${encodeURIComponent(`${unavailable.origin}/api`)}`));

    await expect(page.getByRole('alert').filter({ hasText: 'Console mode unavailable.' })).toBeVisible({ timeout: 3_000 });
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
  });

  test('domain toggle switches between six domains and shared Cluster views', async ({ page }) => {
    await step('shell: goto', () => page.goto('/'));

    await expect(page).toHaveTitle('CrowDB Console');
    await expect(page.getByLabel('CrowDB Console', { exact: true })).toBeVisible();

    // Domain toggle buttons are visible.
    await expect(page.getByTestId('domain-cluster')).toBeVisible({ timeout: 3_000 });
    await expect(page.getByTestId('domain-kv')).toBeVisible();
    await expect(page.getByTestId('domain-chunk')).toBeVisible();

    for (const domain of ['capacity', 'chunk-kv', 'iceberg', 's3']) await expect(page.getByTestId(`domain-${domain}`)).toBeVisible();

    // Default domain is Cluster.
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');

    const domains = page.getByRole('navigation', { name: 'Console domains' });
    await expect(domains.getByRole('button')).toHaveCount(6);
    await expect(domains.getByRole('button', { name: 'Capacity', exact: true })).toHaveCount(0);
    await page.getByTestId('domain-capacity').click();
    await expect(page.getByTestId('domain-capacity')).toHaveAttribute('aria-pressed', 'true');
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
    await expect(page).toHaveURL(/domain=Cluster&view=capacity/);

    // Switch to KV.
    await page.getByTestId('domain-kv').click();
    await expect(page.getByTestId('domain-kv')).toHaveAttribute('aria-pressed', 'true');

    // Switch to Chunk.
    await page.getByTestId('domain-chunk').click();
    await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');

    // Switch back to Cluster.
    await page.getByTestId('domain-cluster').click();
    await expect(page.getByTestId('domain-cluster')).toHaveAttribute('aria-pressed', 'true');
    await expect(page.getByTestId('domain-capacity')).toHaveAttribute('aria-pressed', 'true');
    await page.getByTestId('cluster-topology').click();
    await expect(page.getByTestId('cluster-topology')).toHaveAttribute('aria-pressed', 'true');
  });
  test('embedding honors apiPrefix, readonly, and module opt-out', async ({ page, baseURL, request }) => {
    const post = async (path: string, data: Record<string, unknown>, status = 201) => {
      const response = await request.post(path, { data, timeout: 20_000 });
      expect(response.status(), await response.text()).toBe(status);
    };
    await step('shell: seed rack/node', async () => {
      await post('/api/racks', { id: 23, name: 'rack-23' });
      await post('/api/nodes', { id: 23, rack_id: 23, host: '127.0.0.1', ssh_user: '' });
    });
    await step('shell: deploy server', () => post('/api/nodes/23/server/deploy', { rest_port: 7000, rpc_port: 7001 }));
    await step('shell: initialize authority', () => post('/api/cluster/init', { nodes: [23] }));
    await step('shell: create store', () => post('/api/stores', { store_id: 233, nodes: [23] }));
    await step('shell: add group', () => post('/api/stores/233/groups', { group_id: 2330, replica_id: 23300, nodes: [23] }));

    const proxy = await consoleProxy(baseURL!);
    const seen: string[] = [];
    page.on('request', (req) => seen.push(req.url()));

    try {
      await step('shell: proxy readiness', () => page.goto(proxy.url));
      await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible({ timeout: 3_000 });
      const apiPrefix = encodeURIComponent('/proxy/api');
      await Promise.all([
        page.waitForRequest('**/proxy/api/**', { timeout: 3_000 }),
        step('shell: goto embed', () => page.goto(`${proxy.url}/?domain=KV&readonly=1&disableModules=${encodeURIComponent('kv')}&apiPrefix=${apiPrefix}`)),
      ]);

      // apiPrefix: the SPA re-roots every data-plane call under /proxy/api.
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
      await step('shell: stop server', () => post('/api/nodes/23/server/stop', {}, 200));
    }
  });

});
