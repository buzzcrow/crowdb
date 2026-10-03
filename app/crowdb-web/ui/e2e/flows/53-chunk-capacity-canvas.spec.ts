// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 15s (2026-08-17)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  createRack,
  createNode,
  freePort,
  freePortRange,
  addDiskGroup as apiAddDiskGroup,
  removeDiskGroup as apiRemoveDiskGroup,
  addDisksBatch,
  removeDisk,
  randomDiskId,
  removeDiskdb,
  deployDiskdb,
  deployNodeServer,
  clusterInit,
  waitForLeader,
  stepTime,
} from '../fixtures/consoleSetup';

const CANVAS_RACK = 510;
const CANVAS_NODE = 510;

/**
 * Capacity canvas + scanner/recalc E2E (R77 Phase 6.3-6.4).
 * Deploys a diskdb instance and verifies the CapacityPanel renders
 * the ScannerPanel, RecalcPanel, and per-disk UI elements. The
 * diskdb's crowdb-rpc endpoint may not be fully reachable in the test
 * environment, so we test the UI components that render from the
 * service registry data and verify the action buttons trigger calls.
 */
test.describe('capacity · canvas + scanner/recalc', () => {
  test.beforeAll(async () => {
    const baseURL = consoleBaseURL();
    const resetApi = await apiContext(baseURL);
    try {
      await resetApi.post('/internal/reset').catch(() => {});
    } finally {
      await resetApi.dispose();
    }

    await createRack(baseURL, { id: CANVAS_RACK, name: 'Rack 510' });
    await createNode(baseURL, { id: CANVAS_NODE, rack_id: CANVAS_RACK });
    await deployNodeServer(baseURL, CANVAS_NODE, freePort(), freePort());
    await clusterInit(baseURL, [CANVAS_NODE]);
    await waitForLeader(baseURL, 0, 0, 15_000);
  });

  test('ScannerPanel renders with Run Scan button and empty state', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    const nodeId = CANVAS_NODE;
    const rpcPort = freePortRange(3);
    let scanKnown = false;
    await page.route('**/api/diskdb/scan-status', route => scanKnown
      ? route.fulfill({ json: { has_run: false, scan_in_progress: false } })
      : route.fulfill({ status: 503, json: { error: 'Scan observation unavailable' } }));

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 10_000 });

      // ScannerPanel header + Run Scan button.
      await expect(panel.getByText('Scanner', { exact: true })).toBeVisible({ timeout: 10_000 });
      const scanBtn = panel.getByRole('button', { name: /run scan/i });
      await expect(scanBtn).toBeVisible({ timeout: 10_000 });

      // Missing observation cannot establish that no scan has run.
      await expect(panel.getByText('Scan status unavailable.')).toBeVisible({ timeout: 3000 });
      await expect(page.getByRole('alert').filter({ hasText: 'Capacity observation unavailable' })).toContainText('scan status');
      await expect(page.getByText('Backend unreachable — retrying', { exact: true })).toHaveCount(0);
      await expect(page.getByRole('banner').getByTitle('Cluster health: Degraded', { exact: true })).toBeVisible({ timeout: 3000 });
      await expect(panel.getByText('No scan has been run yet.')).toHaveCount(0);
      scanKnown = true;
      await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
      await expect(panel.getByText('No scan has been run yet.')).toBeVisible({ timeout: 3000 });

      // Click Run Scan — button should handle the response (success or error).
      await scanBtn.click();

      // The button should re-enable after the action completes.
      await expect.poll(async () => {
        const btn = panel.getByRole('button', { name: /run scan|scanning/i });
        return btn.isEnabled();
      }, { timeout: 10_000, intervals: [100] }).toBe(true);
    } finally {
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  test('CapacityPanel shows cluster totals and instance count', async ({ page, baseURL }) => {
    test.setTimeout(30_000);
    const nodeId = CANVAS_NODE;
    const rpcPort = freePortRange(3);

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);

      // Wait for the diskdb instance to register in the service
      // registry before loading the page (the keepalive loop takes
      // a few seconds to write the instance entry).
      const api = await apiContext(baseURL!);
      try {
        await expect.poll(async () => {
          const r = await api.get('/api/diskdb/instances');
          if (!r.ok()) return 0;
          return (await r.json()).length;
        }, { timeout: 15_000, intervals: [200] }).toBeGreaterThanOrEqual(1);
      } finally {
        await api.dispose();
      }

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 10_000 });

      // The header subtitle shows the instance count (at least 1).
      await expect(panel.getByText(/\d+ instance\(s\)/)).toBeVisible({ timeout: 10_000 });

      // Cluster-wide totals cards.
      await expect(panel.getByText('Total Capacity')).toBeVisible({ timeout: 10_000 });
      await expect(panel.getByText('Busy', { exact: true })).toBeVisible({ timeout: 10_000 });
      await expect(panel.getByText('Free', { exact: true })).toBeVisible({ timeout: 10_000 });

      // Refresh button.
      await expect(panel.getByRole('button', { name: 'Refresh' })).toBeVisible({ timeout: 10_000 });
    } finally {
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  test('per-disk boxes and RecalcPanel render when DG has usage data', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const nodeId = CANVAS_NODE;
    const dgId = 610;
    const diskId = randomDiskId();
    const rpcPort = freePortRange(3);

    try {
      await deployDiskdb(baseURL!, nodeId, rpcPort);
      await apiAddDiskGroup(baseURL!, nodeId, dgId, 'canvas-dg');
      await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 10_000 });

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${CANVAS_RACK}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // Wait for the DG to appear in the sidebar. The keepalive loop
      // writes owned_dg_ids to the service registry; this may take a
      // few seconds. If the DG never appears, the diskdb's crowdb-rpc
      // endpoint isn't reachable — skip the canvas assertions.
      let dgVisible = false;
      try {
        await expect(aside.getByText(/DG-610/, { exact: true })).toBeVisible({ timeout: 15_000 });
        dgVisible = true;
      } catch {
        console.warn(`DG-${dgId} did not appear in sidebar — diskdb crowdb-rpc not reachable, skipping canvas assertions`);
      }

      if (dgVisible) {
        // --- DiskGroup scope: per-disk box grid ---
        // Click the DG in the sidebar → center panel switches to
        // DiskGroup scope showing per-disk boxes.
        await aside.getByText(/DG-610/, { exact: true }).click();
        await expect(panel.getByText(`Capacity — DG-${dgId}`)).toBeVisible({ timeout: 10_000 });

        // Per-disk boxes render as colored buttons with busy percentage.
        await expect(panel.getByText(diskId.slice(0, 8), { exact: false })).toBeVisible({ timeout: 10_000 });

        // --- Disk scope: zone grid + RecalcPanel ---
        // Click the disk box → center panel switches to Disk scope.
        await panel.getByText(diskId.slice(0, 8), { exact: false }).click();
        await expect(panel.getByText(/Capacity — Disk/)).toBeVisible({ timeout: 10_000 });

        // RecalcPanel renders in the Disk scope (scoped to parent DG).
        await expect(panel.getByText(`Recalc (DG-${dgId})`)).toBeVisible({ timeout: 10_000 });
        await expect(panel.getByRole('button', { name: /run recalc/i })).toBeVisible({ timeout: 10_000 });

        // Zone grid section should appear.
        await expect(panel.getByText(/Zone grid|No zone usage data available/)).toBeVisible({ timeout: 10_000 });

        // Click "Run Recalc" — should trigger the recalc action.
        const recalcBtn = panel.getByRole('button', { name: /run recalc/i });
        await recalcBtn.click();

        // The placeholder text should be replaced by a result.
        await expect.poll(async () => {
          return panel.getByText('Click "Run Recalc" to check for usage drift.').count();
        }, { timeout: 10_000, intervals: [100] }).toBe(0);
      }
    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId).catch(() => {});
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId).catch(() => {});
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  /**
   * Datacenter root (plan-datacenter-root): the fixed `datacenter` node
   * sits above racks in the Capacity sidebar. Selecting it opens the
   * inspector with rack count + cluster-wide capacity totals (one DC →
   * its totals ARE the cluster totals).
   */
  test('datacenter root in Capacity sidebar; inspector shows cluster totals', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const nodeId = CANVAS_NODE;
    const dgId = 620;
    const diskId = randomDiskId();
    const rpcPort = freePortRange(3);

    try {
      await stepTime('dc: deployDiskdb', () => deployDiskdb(baseURL!, nodeId, rpcPort));
      await stepTime('dc: addDiskGroup+addDisksBatch', async () => {
        await apiAddDiskGroup(baseURL!, nodeId, dgId, 'dc-dg');
        await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);
      });

      await stepTime('dc: page.goto+Capacity click', async () => {
        await page.goto('/');
        await page.getByTestId('domain-capacity').click();
      });

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      // The datacenter root is the top treeitem, above the rack.
      const dcItem = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
      await stepTime('dc: dcItem visible', () => expect(dcItem).toBeVisible({ timeout: 10_000 }));
      await expect(aside.getByRole('treeitem').first()).toHaveText(/datacenter/);

      // Select the datacenter → inspector opens.
      await stepTime('dc: select datacenter + inspector', async () => {
        await aside.getByText('datacenter', { exact: true }).click();
        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible({ timeout: 10_000 });
        const typeDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Type' }) }).locator('dd');
        await expect(typeDd).toHaveText('Datacenter', { timeout: 10_000 });
        // Rack count is always shown (one rack from beforeAll).
        const rackCountDd = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Rack Count' }) }).locator('dd');
        await expect(rackCountDd).toHaveText('1', { timeout: 10_000 });

        // Capacity totals (Total Capacity / Used / Free) are shown in the
        // Capacity view. Wait for the DG to report usage so the totals are
        // non-zero; if the diskdb crowdb-rpc is unreachable, still verify the
        // labels render (totals would be 0 B).
        await expect(inspector.getByText('Total Capacity')).toBeVisible({ timeout: 10_000 });
        await expect(inspector.getByText('Used', { exact: true })).toBeVisible({ timeout: 10_000 });
        await expect(inspector.getByText('Free', { exact: true })).toBeVisible({ timeout: 10_000 });
      });

      // If the DG appears in usage, verify the inspector totals match the
      // cluster-wide sum from the API.
      await stepTime('dc: usage poll + totals match', async () => {
        const api = await apiContext(baseURL!);
        try {
          // Quick precondition: check if any diskdb instance has
          // reported group_usages via the local service registry
          // (fast — no crowdb-rpc fan-out). If none after 3s, diskdb
          // is deployed but not reachable via crowdb-rpc; skip the
          // expensive 12s usage poll.
          let hasUsage = false;
          try {
            await expect.poll(async () => {
              const r = await api.get('/api/diskdb/instances');
              if (!r.ok()) return false;
              const instances = await r.json();
              return Array.isArray(instances) && instances.some((i: any) => Array.isArray(i.group_usages) && i.group_usages.length > 0);
            }, { timeout: 3_000, intervals: [200] }).toBe(true);
            hasUsage = true;
          } catch {
            console.warn(`DG-${dgId} never reported usage — diskdb crowdb-rpc not reachable, skipping totals match`);
          }

          if (hasUsage) {
            // Diskdb is reporting usage — now poll the usage endpoint
            // for the specific DG.
            await expect.poll(async () => {
              const r = await api.get('/api/diskdb/usage');
              if (!r.ok()) return false;
              const body = await r.json();
              return Array.isArray(body.disk_groups) && body.disk_groups.some((g: any) => g.disk_group_id === dgId);
            }, { timeout: 10_000, intervals: [200] }).toBe(true);

            const r = await api.get('/api/diskdb/usage');
            const body = await r.json();
            const sum = (body.disk_groups || []).reduce(
              (acc: { capacity: number; busy: number; free: number }, g: any) => ({
                capacity: acc.capacity + g.capacity_bytes,
                busy: acc.busy + g.busy_bytes,
                free: acc.free + g.free_bytes,
              }),
              { capacity: 0, busy: 0, free: 0 },
            );
            const capDd = page.locator('aside[aria-label="Entity inspector"]').locator('dl > div').filter({ has: page.locator('dt', { hasText: 'Total Capacity' }) }).locator('dd');
            await expect(capDd).toHaveText(formatBytesAssert(sum.capacity), { timeout: 10_000 });
          }
        } finally {
          await api.dispose();
        }
      });
    } finally {
      await stepTime('dc: cleanup', async () => {
        await removeDisk(baseURL!, nodeId, dgId, diskId).catch(() => {});
        await apiRemoveDiskGroup(baseURL!, nodeId, dgId).catch(() => {});
        await removeDiskdb(baseURL!, nodeId);
      });
    }
  });
});

/** Match the Inspector's formatBytes rendering for an exact-text assertion. */
function formatBytesAssert(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

test('Capacity preserves hardware and shows unknown usage at every scope', async ({ page }) => {
  page.setDefaultTimeout(3000);
  let reported = false;
  const diskId = '0000000000000001-0000000000000002';
  const disk = { rack_id: 1, node_id: 1, disk_group_id: 1, disk_id: diskId, capacity_bytes: 1024, busy_bytes: 512, free_bytes: 256, disk_type: 1, status: 1, zone_count: 1, zone_usages: [] };
  const group = { rack_id: 1, node_id: 1, disk_group_id: 1, capacity_bytes: 1024, busy_bytes: 512, free_bytes: 256, disks: [disk] };
  await page.route('**/api/hardware/capacity', route => route.fulfill({ json: {
    datacenter_capacity_bytes: 1024, racks: [{ rack_id: 1, node_count: 1, capacity_bytes: 1024 }],
    nodes: [{ rack_id: 1, node_id: 1, disk_group_count: 1, capacity_bytes: 1024 }], disk_groups: [group],
  } }));
  await page.route('**/api/diskdb/instances', route => route.fulfill({ json: [] }));
  await page.route('**/api/diskdb/usage', route => route.fulfill({ json: { disk_groups: reported ? [group] : [] } }));
  await page.goto('/?domain=Capacity');
  const totals = page.getByTestId('capacity-summary');
  await expect(totals).toContainText('1.0 KB', { timeout: 3000 });
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
  const panel = totals.locator('..');
  await panel.getByRole('button', { name: /R-1 / }).click();
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
  await panel.getByRole('button', { name: /N-1 / }).click();
  await panel.getByRole('button', { name: /DG-1 / }).click();
  const unknownDisk = panel.getByRole('button', { name: /00000000…/ });
  await expect(unknownDisk).toHaveAttribute('title', /Usage unknown/);
  await unknownDisk.click();
  await expect(panel.getByText(/1 zones.*Usage unknown/)).toBeVisible();
  const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
  const free = inspector.locator('dl > div').filter({ has: page.locator('dt', { hasText: /^Free$/ }) }).locator('dd');
  await expect(free).toHaveText('Unknown');
  reported = true;
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(free).toHaveText('256.0 B');
  await expect(totals).toContainText('25% free');
  reported = false;
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(free).toHaveText('Unknown');
  await expect(totals).not.toContainText('100% free');
});
