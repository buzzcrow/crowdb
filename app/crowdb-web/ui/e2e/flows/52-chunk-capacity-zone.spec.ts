// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: zone detail 0.656s (2026-10-03)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  createRack,
  createNode,
  freePort,
  addDiskGroup as apiAddDiskGroup,
  removeDiskGroup as apiRemoveDiskGroup,
  addDisksBatch,
  removeDisk,
  randomDiskId,
  deployDiskdb as apiDeployDiskdb,
  deployNodeServer,
  clusterInit,
  waitForLeader,
} from '../fixtures/consoleSetup';

const DISKDB_RACK = 501;
const DISKDB_NODE = 501;

/**
 * All Capacity / DiskDB flows share ONE rack + node (and, for the final
 * lifecycle test, ONE diskdb deploy). diskdb deploy is the dominant setup
 * cost, so the deploy → restart → stop lifecycle runs last.
 *
 * A kv-server is deployed on the same node and the cluster is initialized
 * so that group-0 sysdata operations (set_disk_group_status, etc.) work
 * against the real backend instead of mocks.
 */
test.describe('chunk · capacity · zone', () => {
  test.beforeAll(async () => {
    const baseURL = consoleBaseURL();
    // Full cluster reset to clear any stale group-0 sysdata (e.g.
    // service registry entries from a previous diskdb deploy). This
    // stops all servers, cleans workspace dirs, and wipes config.
    const resetApi = await apiContext(baseURL);
    try {
      await resetApi.post('/internal/reset').catch(() => {});
    } finally {
      await resetApi.dispose();
    }

    await createRack(baseURL, { id: DISKDB_RACK, name: 'Rack 501' });
    await createNode(baseURL, { id: DISKDB_NODE, rack_id: DISKDB_RACK });
    // Deploy a kv-server on the node and init the cluster so group-0
    // sysdata operations (set_disk_group_status, set_disk_status) work
    // against the real backend.
    await deployNodeServer(baseURL, DISKDB_NODE, freePort(), freePort());
    await clusterInit(baseURL, [DISKDB_NODE]);
    // Wait for group-0 to be visible in the monitor cache (store 0,
    // group 0 with an elected leader). clusterInit refreshes the cache,
    // but in the full suite the refresh may lag behind the server's
    // readiness — poll until build_hardware_client can resolve an endpoint.
    await waitForLeader(baseURL, 0, 0, 15_000);
    // Deploy a diskdb instance so addDiskGroup auto-assigns ownership
    // (required since diskdb ownership enforcement — a3d39f0e).
    await apiDeployDiskdb(baseURL, DISKDB_NODE, freePort());
  });

  // No afterAll — the beforeAll reset of the next test file (or the
  // next run's beforeAll) cleans up all state. An afterAll here would
  // stop the kv-server between tests, breaking group-0 ops for later
  // tests in this file.

  test('zone bitmap on-demand fetch when clicking a zone in Disk view', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dgId = 582;
    const diskId = randomDiskId();

    await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-bitmap');
    await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);

    try {
      const dashed = (s: string) => s.length === 32 ? `${s.slice(0, 16)}-${s.slice(16)}` : s;
      const diskIdDashed = dashed(diskId);

      // Disk-level usage: zone_usages WITHOUT usage_bitmap (the
      // backend omits usage_bitmap at disk level; it's fetched
      // on-demand via the zone-level query).
      const diskLevelUsage = {
        disk_groups: [{
          rack_id: rackId,
          node_id: nodeId,
          disk_group_id: dgId,
          status: 1,
          disk_ids: [diskIdDashed],
          disks: [{
            rack_id: rackId,
            node_id: nodeId,
            disk_group_id: dgId,
            disk_id: diskIdDashed,
            disk_type: 1,
            capacity_units: 1000,
            zone_size_units: 100,
            unit_size_bytes: 4096,
            zone_count: 130,
            status: 1,
            busy_units: 100,
            free_units: 900,
            capacity_bytes: 4096000,
            busy_bytes: 409600,
            free_bytes: 3686400,
            active_zone_count: 5,
            zone_usages: Array.from({ length: 10 }, (_, i) => ({
              zone_index: i,
              capacity_bytes: 409600,
              busy_bytes: 40960,
              free_bytes: 368640,
              busy_block_count: 10,
              free_block_count: 90,
              alloc_state: 0,
              usage_bitmap: null, // omitted at disk level
            })),
          }],
          capacity_bytes: 4096000,
          busy_bytes: 409600,
          free_bytes: 3686400,
          allocatable_disk_count: 1,
        }],
      };

      // Zone-level usage: returns the zone WITH usage_bitmap.
      // The useZoneBitmap hook calls getDiskdbUsage(dg, disk, zone)
      // which hits /api/diskdb/usage?dg=<id>&disk=<id>&zone=<idx>.
      const zoneLevelUsage = (zoneIdx: number) => ({
        disk_groups: [{
          rack_id: rackId,
          node_id: nodeId,
          disk_group_id: dgId,
          status: 1,
          disk_ids: [diskIdDashed],
          disks: [{
            rack_id: rackId,
            node_id: nodeId,
            disk_group_id: dgId,
            disk_id: diskIdDashed,
            disk_type: 1,
            capacity_units: 1000,
            zone_size_units: 100,
            unit_size_bytes: 4096,
            zone_count: 130,
            status: 1,
            busy_units: 100,
            free_units: 900,
            capacity_bytes: 4096000,
            busy_bytes: 409600,
            free_bytes: 3686400,
            active_zone_count: 5,
            zone_usages: [{
              zone_index: zoneIdx,
              capacity_bytes: 409600,
              busy_bytes: 40960,
              free_bytes: 368640,
              busy_block_count: zoneIdx === 1 ? 1 : 10,
              free_block_count: zoneIdx === 1 ? 8192 : 90,
              alloc_state: 0,
              usage_bitmap: zoneIdx === 1 ? '01' + '00'.repeat(1024) : '0100',
            }],
          }],
          capacity_bytes: 4096000,
          busy_bytes: 409600,
          free_bytes: 3686400,
          allocatable_disk_count: 1,
        }],
      });

      // Route handler: return zone-level response when zone param is
      // present, otherwise return disk-level response. Use a function
      // matcher (not glob) because the zone-level query has URL params
      // (e.g. /api/diskdb/usage?dg=582&disk=...&zone=0) which glob
      // patterns like `**/api/diskdb/usage` don't match.
      await page.route((url) => url.pathname === '/api/diskdb/usage', (route) => {
        const reqUrl = route.request().url();
        const zoneMatch = reqUrl.match(/[?&]zone=(\d+)/);
        if (zoneMatch) {
          route.fulfill({
            status: 200,
            contentType: 'application/json',
            body: JSON.stringify(zoneLevelUsage(Number(zoneMatch[1]))),
          });
        } else {
          route.fulfill({
            status: 200,
            contentType: 'application/json',
            body: JSON.stringify(diskLevelUsage),
          });
        }
      });

      await page.route('**/api/hardware/capacity', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            datacenter_capacity_bytes: 0,
            racks: [],
            nodes: [],
            disk_groups: [],
          }),
        });
      });

      await page.route('**/api/diskdb/instances', (route) => {
        route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify([
            {
              instance_id: `diskdb-bitmap`,
              node_id: nodeId,
              rpc_endpoint: `http://127.0.0.1:30099`,
              owned_dg_ids: [dgId],
              status: 'up',
            },
          ]),
        });
      });

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();
      await expect(aside.getByText(/DG-582/, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Expand DG and click the disk to enter Disk view.
      const expandDg = aside.getByRole('treeitem').filter({ hasText: /DG-582/ }).locator('button[aria-label="Expand"]');
      if (await expandDg.count() > 0) await expandDg.click();
      const diskLabel = aside.getByText(diskId.slice(0, 12), { exact: false });
      await expect(diskLabel).toBeVisible({ timeout: 5_000 });
      await diskLabel.first().click();

      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity — Disk/)).toBeVisible({ timeout: 3_000 });

      // Zone grid should be visible with 10 zones.
      await expect(panel.getByText(/Zone grid.*130 zones/)).toBeVisible({ timeout: 5_000 });

      // Before clicking a zone, no bitmap section should be visible.
      await expect(panel.getByText(/Zone \d+ bitmap/)).toHaveCount(0);

      const zones = panel.getByRole('group', { name: 'Disk zones' });
      await expect(zones.getByRole('button', { name: /^Zone \d+$/ })).toHaveCount(64);
      await panel.getByRole('button', { name: 'Zone 0', exact: true }).click();

      // The zone bitmap section should appear with the zone index.
      await expect(panel.getByText(/Zone 0 bitmap/)).toBeVisible({ timeout: 5_000 });

      // The bitmap section should show busy/free block counts.
      await expect(panel.getByText(/10 busy.*90 free blocks/)).toBeVisible({ timeout: 5_000 });
      const bitmap = panel.getByLabel('Zone block usage');
      const pixel = (block: number) => bitmap.evaluate((canvas: HTMLCanvasElement, index) =>
        Array.from(canvas.getContext('2d')!.getImageData((index % 64) * 6 + 2, Math.floor(index / 64) * 6 + 2, 1, 1).data), block);
      await expect.poll(() => pixel(0), { timeout: 3000, intervals: [100] }).toEqual([59, 130, 246, 255]);
      expect(await pixel(1)).toEqual([34, 197, 94, 255]);
      expect(await pixel(16)).toEqual([107, 114, 128, 255]);
      await expect(panel.getByText(/Capacity — Disk/)).toBeVisible();
      await panel.getByRole('button', { name: 'Zone 1', exact: true }).click();
      await expect(panel.getByText('Blocks 0–4095 of 8193', { exact: false })).toBeVisible();
      await panel.getByRole('button', { name: 'Next blocks' }).click();
      await expect(panel.getByText('Blocks 4096–8191 of 8193', { exact: false })).toBeVisible();
      await panel.getByRole('button', { name: 'Next blocks' }).click();
      await expect(panel.getByText('Blocks 8192–8192 of 8193', { exact: false })).toBeVisible();
      await panel.getByRole('button', { name: 'Back to parent Disk' }).click();
      await expect(panel.getByTestId('zone-bitmap')).toHaveCount(0);
      await expect(panel.getByText(/Capacity — Disk/)).toBeVisible();
      await panel.getByRole('button', { name: 'Next zones' }).click();
      await panel.getByRole('button', { name: 'Zone 64', exact: true }).click();
      await expect(panel.getByRole('region', { name: 'Zone 64 detail' })).toBeVisible();

    } finally {
      await removeDisk(baseURL!, nodeId, dgId, diskId).catch(() => {});
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId).catch(() => {});
    }
  });
});
