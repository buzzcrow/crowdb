// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: split from 50-capacity-diskdb.spec.ts (2026-09-01)

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
  deployDiskdb as apiDeployDiskdb,
  deployNodeServer,
  clusterInit,
  addGroup,
  waitForLeader,
  assignDiskGroup,
} from '../fixtures/consoleSetup';

const DISKDB_RACK = 501;
const DISKDB_NODE = 501;

async function ownDisks(baseURL: string, groups: Array<[number, string]>) {
  const ownerApi = await apiContext(baseURL);
  try {
    const response = await ownerApi.get('/api/diskdb/instances');
    expect(response.ok(), await response.text()).toBe(true);
    const instances = await response.json();
    expect(instances).toHaveLength(1);
    for (const [dgId, diskId] of groups) {
      await assignDiskGroup(baseURL, DISKDB_RACK, DISKDB_NODE, dgId, instances[0].instance_id, 0, 1);
      await expect.poll(async () => {
        const usage = await ownerApi.get(`/api/diskdb/usage?dg=${dgId}`);
        if (!usage.ok()) return [];
        const body = await usage.json();
        return body.disk_groups.flatMap((group: { disks: { disk_id: string }[] }) => group.disks)
          .map((disk: { disk_id: string }) => disk.disk_id.replace(/-/g, ''));
      }, { intervals: [100] }).toContain(diskId);
    }
  } finally { await ownerApi.dispose(); }
}

/**
 * All Capacity / DiskDB flows share ONE rack + node (and, for the final
 * lifecycle test, ONE diskdb deploy). diskdb deploy is the dominant setup
 * cost, so the deploy → restart → stop lifecycle runs last.
 *
 * A kv-server is deployed on the same node and the cluster is initialized
 * so that group-0 sysdata operations (set_disk_group_status, etc.) work
 * against the real backend instead of mocks.
 */
test.describe('chunk · capacity · disk', () => {
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
    await addGroup(baseURL, 0, 1, 1, [DISKDB_NODE]);
    // Wait for group-0 to be visible in the monitor cache (store 0,
    // group 0 with an elected leader). clusterInit refreshes the cache,
    // but in the full suite the refresh may lag behind the server's
    // readiness — poll until build_hardware_client can resolve an endpoint.
    await waitForLeader(baseURL, 0, 0, 15_000);
    // Deploy a diskdb instance so addDiskGroup auto-assigns ownership
    // (required since diskdb ownership enforcement — a3d39f0e).
    await apiDeployDiskdb(baseURL, DISKDB_NODE, freePortRange(3));
  });

  test.afterAll(async () => {
    const api = await apiContext(consoleBaseURL());
    try {
      for (const service of ['diskdb', 'server']) {
        const response = await api.post(`/api/nodes/${DISKDB_NODE}/${service}/stop`, { data: {} });
        expect(response.ok(), await response.text()).toBe(true);
      }
      const response = await api.post('/internal/reset'); expect(response.ok(), await response.text()).toBe(true);
    }
    finally { await api.dispose(); }
  });

  test('disk Inspector fields and CapacityPanel selection behavior', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dg580 = 580;
    const dg581 = 581;
    const disk580 = randomDiskId();
    const disk581 = randomDiskId();

    await apiAddDiskGroup(baseURL!, nodeId, dg580, 'test-dg-580');
    await addDisksBatch(baseURL!, nodeId, dg580, [{ disk_id: disk580 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg581, 'test-dg-581');
    await addDisksBatch(baseURL!, nodeId, dg581, [{ disk_id: disk581 }]);

    await ownDisks(baseURL!, [[dg580, disk580], [dg581, disk581]]);

    try {
      const disk580Dashed = `${disk580.slice(0, 16)}-${disk580.slice(16)}`;
      const api = await apiContext(baseURL!);
      let hardware;
      try {
        const response = await api.get('/api/hardware/capacity');
        expect(response.ok(), await response.text()).toBe(true);
        hardware = await response.json();
      } finally { await api.dispose(); }
      const actualDisk = hardware.disk_groups.find((group: { disk_group_id: number }) => group.disk_group_id === dg580).disks[0];

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // --- CapacityPanel shows cluster-wide totals by default ---
      const panel = page.locator('.tw-h-full.tw-overflow-auto');
      await expect(panel.getByText(/Capacity —/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByText('Capacity — Cluster')).toBeVisible();

      // Both real fixture disks use the default 4-TiB geometry.
      await expect(page.getByTestId('capacity-summary').locator(':scope > div').filter({ hasText: 'Total Capacity' }).getByText('8.0 TB', { exact: true })).toBeVisible({ timeout: 3_000 });

      // --- ClusterView: per-rack breakdown ---
      // Should show "Racks (1)" section with R-501 button.
      await expect(panel.getByText(/Racks \(1\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /R-501/ })).toBeVisible({ timeout: 3_000 });
      // R-501 should show DG count (2) and node count.
      await expect(panel.getByText(/2 DG\(s\)/)).toBeVisible();
      // Scanner panel is cluster-scope only.
      await expect(panel.getByText('Scanner', { exact: true })).toBeVisible({ timeout: 3_000 });

      // --- Click Rack in sidebar → header shows "Rack 501", totals filtered ---
      await aside.getByText(`R-${rackId}`, { exact: false }).first().click();
      await expect(panel.getByText(`Capacity — Rack ${rackId}`)).toBeVisible({ timeout: 3_000 });

      // --- RackView: per-node breakdown ---
      // Should show "Nodes in R-501 (1)" with N-501 button.
      await expect(panel.getByText(/Nodes in R-501 \(1\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /N-501/ })).toBeVisible({ timeout: 3_000 });
      // N-501 should show DG count (2).
      await expect(panel.getByText(/2 DG\(s\)/)).toBeVisible();
      // Scanner panel should NOT be visible in Rack scope.
      await expect(panel.getByText('Scanner', { exact: true })).toHaveCount(0);

      // --- Click Node in sidebar → header shows "Node 501" ---
      await aside.getByText(`N-${nodeId}`, { exact: true }).click();
      await expect(panel.getByText(`Capacity — Node ${nodeId}`)).toBeVisible({ timeout: 3_000 });

      // --- NodeView: per-DG breakdown ---
      // Should show "Disk-groups on N-501 (2)" with DG-580 and DG-581 buttons.
      await expect(panel.getByText(/Disk-groups on N-501 \(2\)/)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /DG-580/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /DG-581/ })).toBeVisible({ timeout: 3_000 });
      // Each DG should show disk count (1 disk each).
      await expect(panel.getByText(/1 disk\(s\)/)).toHaveCount(2);

      // --- Click DiskGroup in sidebar → header shows "DG-580" ---
      await expect(aside.getByText(/DG-580/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await aside.getByText(/DG-580/, { exact: true }).click();
      await expect(panel.getByText('Capacity — DG-580')).toBeVisible({ timeout: 3_000 });

      // --- DiskGroupView: per-disk box grid ---
      // Should show "Disks in DG-580 (1)" with a disk box showing busy%.
      await expect(panel.getByText(/Disks in DG-580 \(1\)/)).toBeVisible({ timeout: 3_000 });
      // The disk box shows the disk ID prefix (first 8 chars + …).
      await expect(panel.getByText(disk580Dashed.slice(0, 8) + '…')).toBeVisible({ timeout: 3_000 });
      // The actual owned DiskDB supplies this disk usage.
      await expect(panel.getByRole('button').filter({ hasText: disk580Dashed.slice(0, 8) + '…' })).not.toHaveAttribute('title', /Usage unknown/);

      // --- Click Disk in sidebar → header shows "Disk …", zone grid appears ---
      // Expand DG-580 to see the disk.
      const expandDg580 = aside.getByRole('treeitem').filter({ hasText: /DG-580/ }).locator('button[aria-label="Expand"]');
      if (await expandDg580.count() > 0) await expandDg580.click();

      const disk580Label = aside.getByText(disk580.slice(0, 12), { exact: false });
      await expect(disk580Label).toBeVisible({ timeout: 5_000 });
      await disk580Label.first().click();

      // The panel header should show the disk scope.
      await expect(panel.getByText(/Capacity — Disk/)).toBeVisible({ timeout: 3_000 });

      // --- DiskView: disk header + action buttons + RecalcPanel + zone grid ---
      // Disk header shows the full disk ID, type, status, zone count, capacity.
      await expect(panel.getByText(actualDisk.disk_id, { exact: true })).toBeVisible({ timeout: 3_000 });
      await expect(page.getByTestId('disk-geometry')).toContainText(`${['BlockHdd', 'BlockSsd', 'ZoneSsd', 'SmrHdd'][actualDisk.disk_type]} · Up · ${actualDisk.zone_count} zones`);
      // Action buttons: Scan, Recalc, Compact, Rebuild, Down, Up.
      await expect(panel.getByRole('button', { name: /^Scan$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Recalc$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Compact$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Rebuild$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Down$/ })).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /^Up$/ })).toBeVisible({ timeout: 3_000 });
      // RecalcPanel scoped to parent DG.
      await expect(panel.getByText(`Recalc (DG-${dg580})`)).toBeVisible({ timeout: 3_000 });
      await expect(panel.getByRole('button', { name: /run recalc/i })).toBeVisible({ timeout: 3_000 });

      // The disk row should auto-expand and show the zone grid.
      await expect(panel.getByText('Zone grid', { exact: false })).toBeVisible({ timeout: 5_000 });

      // --- Disk Inspector: no Name, no Parent: disk_id ---
      const inspector = page.getByRole('complementary', { name: 'Entity inspector' });
      await expect(inspector).toBeVisible();
      await expect(inspector.getByText('Type', { exact: true })).toBeVisible();
      await expect(inspector.getByText('ID', { exact: true })).toBeVisible();
      // Regression: Disk entities must NOT show a Name field.
      await expect(inspector.getByText('Name', { exact: true })).toHaveCount(0);
      // Regression: Disk entities must NOT show "Parent: disk_id"
      // (disk_id is the entity's own ID, not a parent).
      await expect(inspector.getByText('Parent: disk_id')).toHaveCount(0);
      // Should show the correct parent fields.
      await expect(inspector.getByText('Parent: rack_id')).toBeVisible();
      await expect(inspector.getByText('Parent: node_id')).toBeVisible();
      await expect(inspector.getByText('Parent: disk_group_id')).toBeVisible();
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg580, disk580);
      await removeDisk(baseURL!, nodeId, dg581, disk581);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg580);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg581);
    }
  });


});

// Rendering/request-budget fixture; production storage semantics are covered above.
// Baseline: new native lazy inventory/scoped usage acceptance (2026-10-04).
