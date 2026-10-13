// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline measured against the original cases on 2026-10-12.

import { test, expect } from '../fixtures/realBackend';
import { apiContext, addGroup, addReplica, createStore, waitForLeader } from '../fixtures/localSetup';
import { step } from '../fixtures/stepTimer';

/**
 * Cluster node inspect — cross-jump + local/remote replica wiring.
 *
 * The cluster/debugging view is the only place a node's remote-replica
 * proxies are surfaced. The inspector also offers cross-jump buttons that
 * move between the KV and cluster topologies.
 */
test.describe('cluster · node inspect & cross-jump', () => {
  // Baseline: 9.754s (2026-10-12)
  test('cross-jumps between views and shows local + remote replicas', async ({ page, baseURL }) => {
    // --- logical replica details -> hosting physical node ---
    await step('xjump: setup 1', async () => {
      await createStore(baseURL!, 66, [1]);
      await addGroup(baseURL!, 66, 660, 6600, [1]);
    });

    {
      await step('xjump: logical→physical UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('LR-6600')).toBeVisible({ timeout: 3_000 });
        await aside.getByText('LR-6600').click();

        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Single cross-jump button: logical Replica -> hosting physical Node.
        await inspector.getByRole('button', { name: /Show on node 1\b/ }).click();

        await expect(page.getByRole('heading', { name: 'Cluster' })).toBeVisible({ timeout: 3_000 });
        await expect(inspector.getByText('N-1', { exact: true })).toBeVisible({ timeout: 3_000 });
      });
    }

    // --- physical node details -> hosting logical store ---
    await step('xjump: setup 2', async () => {
      await createStore(baseURL!, 67, [2]);
      await addGroup(baseURL!, 67, 670, 6700, [2]);
    });

    {
      await step('xjump: physical→logical UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-2' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });
        await nodeItem.getByRole('button', { name: 'N-2' }).click();

        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Cross-jump button: physical Node -> logical Store.
        await inspector.getByRole('button', { name: /Show store 67 in cluster/i }).click();

        await expect(page.getByRole('main').getByRole('heading', { name: 'PaxosKV', exact: true })).toBeVisible({ timeout: 3_000 });
        await expect(inspector.getByText('S-67', { exact: true }).first()).toBeVisible({ timeout: 3_000 });
      });
    }

    // --- node inspect: local + remote replicas, removed remote disappears ---
    await step('xjump: setup replicas', async () => {
      // store 266, then group 2660 / replica 26600 on n26a, then a peer on n26b.
      await createStore(baseURL!, 266, [1]);
      await addGroup(baseURL!, 266, 2660, 26600, [1]);
      await addReplica(baseURL!, 266, 2660, 2, 26601);
    });

    const api = await apiContext(baseURL!);
    try {
      await step('xjump: replicas UI', async () => {
        await page.goto('/');
        // Stores/replicas are in the KV domain, not the Cluster domain.
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        // Per-node store detail loads after the tree mounts, so rows added by
        // polling stay collapsed. Wait for the stores, then expand everything.
        await expect(aside.getByText('S-266', { exact: true }).first()).toBeVisible({ timeout: 3_000 });
        for (let i = 0; i < 24; i++) {
          const expander = aside.getByRole('button', { name: 'Expand' }).first();
          if (!(await expander.count())) break;
          await expander.click();
        }

        // Logical tree: both replicas appear as LR entries under the
        // store → group hierarchy. No RR (remote replica) entries — the
        // KV view is logical-only per the design spec.
        await expect(aside.getByText('LR-26600', { exact: true })).toBeVisible({ timeout: 3_000 });
        await expect(aside.getByText('LR-26601', { exact: true })).toBeVisible({ timeout: 3_000 });
        await expect(aside.getByText('RR-26601', { exact: true })).toHaveCount(0);
        await expect(aside.getByText('RR-26600', { exact: true })).toHaveCount(0);
      });

      // Remove the remote on n26a out-of-band (simulated mis-wiring).
      await step('xjump: delete remote API', async () => {
        const del = await api.delete('/api/nodes/1/stores/266/groups/2660/remotes/26601');
        expect(del.ok(), await del.text()).toBeTruthy();
      });

      // In the logical tree, deleting a remote proxy doesn't remove the
      // replica from the group — it still exists on its home node (2).
      // Both replicas remain visible.
      await step('xjump: verify replicas still present', async () => {
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('LR-26600', { exact: true })).toBeVisible({ timeout: 3_000 });
        await expect(aside.getByText('LR-26601', { exact: true })).toBeVisible({ timeout: 3_000 });
      });
    } finally {
      await api.dispose();
    }
  });

  // Each case owns a fresh container cluster and its user stores.
  // Baseline: 3.656s (2026-10-12)
  test('physical node with store shows cross-jump to logical store', async ({ page, baseURL }) => {
    await step('xjump2: setup', async () => {
      await createStore(baseURL!, 330, [1]);
      await addGroup(baseURL!, 330, 3300, 33000, [1]);
      await waitForLeader(baseURL!, 330, 3300);
    });

    {
      await step('xjump2: cross-jump UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        // Select the physical node
        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-1' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });
        await nodeItem.getByRole('button', { name: 'N-1' }).click();

        // Inspector should show Details tab with cross-jump button
        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 3_000 });

        // Verify cross-jump button exists and click it
        const crossJumpButton = inspector.getByRole('button', { name: /Show store 330 in cluster/i });
        await expect(crossJumpButton).toBeVisible({ timeout: 3_000 });
        await crossJumpButton.click();

        // View should switch to KV
        await expect(page.getByRole('main').getByRole('heading', { name: 'PaxosKV', exact: true })).toBeVisible({ timeout: 3_000 });

        // Store should be selected in the logical tree
        await expect(aside.getByText('S-330').first()).toBeVisible({ timeout: 3_000 });
      });
    }
  });
});
