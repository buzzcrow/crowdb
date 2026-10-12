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

// Assert the menu once; UI refresh must not require silent click retries.
async function clickMenuItem(
  page: import('@playwright/test').Page,
  treeItem: import('@playwright/test').Locator,
  menuItemName: string | RegExp,
) {
  await treeItem.click({ button: 'right' });
  const item = page.getByRole('menuitem', { name: menuItemName });
  await expect(item).toBeVisible();
  await item.dispatchEvent('click');
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

  test('disk maintenance operations, set-status, and health badges', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dg545 = 545;
    const dg546 = 546;
    const dg547 = 547;
    const dg548 = 548;
    const dg549 = 549;
    const dg551 = 551;
    const dg552 = 552;
    const dg553 = 553;
    // Extra DGs for all-status badge verification (one per HwStatus).
    const dg554 = 554; // Init (0)
    const dg555 = 555; // Maintenance (2)
    const dg556 = 556; // Suspect (3)
    const dg557 = 557; // Missing (4)
    const dg558 = 558; // Bad (5)
    const dg559 = 559; // Offline (6)
    const disk545 = randomDiskId();
    const disk546 = randomDiskId();
    const disk547 = randomDiskId();
    const disk549 = randomDiskId();
    const disk551 = randomDiskId();
    const disk552 = randomDiskId();
    const disk553 = randomDiskId();
    const disk554 = randomDiskId();
    const disk555 = randomDiskId();
    const disk556 = randomDiskId();
    const disk557 = randomDiskId();
    const disk558 = randomDiskId();
    const disk559 = randomDiskId();

    // The kv-server deployed in beforeAll + clusterInit makes group-0
    // available for set-status operations (real backend). Recalc/scan/
    // usage use the actual registered DiskDB owner.

    await apiAddDiskGroup(baseURL!, nodeId, dg545, 'test-dg-545');
    await addDisksBatch(baseURL!, nodeId, dg545, [{ disk_id: disk545 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg546, 'test-dg-546');
    await addDisksBatch(baseURL!, nodeId, dg546, [{ disk_id: disk546 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg547, 'test-dg-547');
    await addDisksBatch(baseURL!, nodeId, dg547, [{ disk_id: disk547 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg548, 'test-dg-548');
    await apiAddDiskGroup(baseURL!, nodeId, dg549, 'test-dg-549');
    await addDisksBatch(baseURL!, nodeId, dg549, [{ disk_id: disk549 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg551, 'test-dg-551');
    await addDisksBatch(baseURL!, nodeId, dg551, [{ disk_id: disk551 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg552, 'test-dg-552');
    await addDisksBatch(baseURL!, nodeId, dg552, [{ disk_id: disk552 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg553, 'test-dg-553');
    await addDisksBatch(baseURL!, nodeId, dg553, [{ disk_id: disk553 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg554, 'test-dg-554');
    await addDisksBatch(baseURL!, nodeId, dg554, [{ disk_id: disk554 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg555, 'test-dg-555');
    await addDisksBatch(baseURL!, nodeId, dg555, [{ disk_id: disk555 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg556, 'test-dg-556');
    await addDisksBatch(baseURL!, nodeId, dg556, [{ disk_id: disk556 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg557, 'test-dg-557');
    await addDisksBatch(baseURL!, nodeId, dg557, [{ disk_id: disk557 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg558, 'test-dg-558');
    await addDisksBatch(baseURL!, nodeId, dg558, [{ disk_id: disk558 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg559, 'test-dg-559');
    await addDisksBatch(baseURL!, nodeId, dg559, [{ disk_id: disk559 }]);
    await ownDisks(baseURL!, [[dg551, disk551], [dg552, disk552]]);

    try {
      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // --- Compact Zones opens zone select dialog with range input ---

      // Wait for DG-545 to appear — fetchNodeDiskGroups fetches all
      // 15 disk groups + their disks (16 API calls) and is not polled,
      // so the tree may not have them yet.
      await expect(aside.getByText(/DG-545/, { exact: true })).toBeVisible({ timeout: 10_000 });
      const expandDg545 = aside.getByRole('treeitem').filter({ hasText: /DG-545/ }).locator('button[aria-label="Expand"]');
      if (await expandDg545.count() > 0) await expandDg545.click();

      // Right-click the disk → Compact Zones.
      const disk545Label = aside.getByText(disk545.slice(0, 12), { exact: false });
      await expect(disk545Label).toBeVisible({ timeout: 10_000 });
      await clickMenuItem(page, disk545Label.first(), /compact zones/i);

      const compactDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(compactDialog).toBeVisible();
      // Should have a Zones input field.
      await expect(compactDialog.getByLabel(/zones/i)).toBeVisible();
      // Default value should be "all".
      await expect(compactDialog.getByLabel(/zones/i)).toHaveValue('all');
      // Should have a Compact button.
      await expect(compactDialog.getByRole('button', { name: /compact/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Rebuild Bitmap opens zone select dialog ---

      const expandDg546 = aside.getByRole('treeitem').filter({ hasText: /DG-546/ }).locator('button[aria-label="Expand"]');
      if (await expandDg546.count() > 0) await expandDg546.click();

      // Right-click the disk → Rebuild Bitmap.
      const disk546Label = aside.getByText(disk546.slice(0, 12), { exact: false });
      await expect(disk546Label).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, disk546Label.first(), /rebuild bitmap/i);

      const rebuildDialog = page.getByRole('dialog', { name: /rebuild bitmap/i });
      await expect(rebuildDialog).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toBeVisible();
      await expect(rebuildDialog.getByLabel(/zones/i)).toHaveValue('all');
      await expect(rebuildDialog.getByRole('button', { name: /rebuild/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Compact Zones dialog validates zone input (invalid disables button) ---

      const expandDg547 = aside.getByRole('treeitem').filter({ hasText: /DG-547/ }).locator('button[aria-label="Expand"]');
      if (await expandDg547.count() > 0) await expandDg547.click();

      // Right-click the disk → Compact Zones.
      const disk547Label = aside.getByText(disk547.slice(0, 12), { exact: false });
      await expect(disk547Label).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, disk547Label.first(), /compact zones/i);

      const validateDialog = page.getByRole('dialog', { name: /compact zones/i });
      await expect(validateDialog).toBeVisible();
      const zoneInput = validateDialog.getByLabel(/zones/i);
      const compactBtn = validateDialog.getByRole('button', { name: /compact/i });

      // Default "all" → button enabled.
      await expect(zoneInput).toHaveValue('all');
      await expect(compactBtn).toBeEnabled();

      // Valid range "1-5,10" → button enabled.
      await zoneInput.fill('1-5,10');
      await expect(compactBtn).toBeEnabled();

      // Single zone "3" → button enabled.
      await zoneInput.fill('3');
      await expect(compactBtn).toBeEnabled();

      // Invalid "abc" → button disabled.
      await zoneInput.fill('abc');
      await expect(compactBtn).toBeDisabled();

      // Invalid "1-5,abc" → button disabled.
      await zoneInput.fill('1-5,abc');
      await expect(compactBtn).toBeDisabled();

      // Empty → button enabled (means all zones).
      await zoneInput.fill('');
      await expect(compactBtn).toBeEnabled();

      await page.keyboard.press('Escape');

      // --- Change DG status to Offline via context menu submenu ---

      // Refresh the monitor cache by polling group-0 — in the full
      // suite the kv-server may have been temporarily unreachable from
      // earlier sysdata sync retries, and the cache entry may be stale.
      await waitForLeader(baseURL!, 0, 0, 15_000);

      // Right-click disk-group → Change Status → Offline. The real API
      // writes to group-0 via HardwareClient and returns 204.
      await expect(aside.getByText(/DG-548/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await clickMenuItem(page, aside.getByText(/DG-548/, { exact: true }), /change status/i);
      const dgStatusResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'PUT' && r.url().includes(`/api/disk-groups/${rackId}/${nodeId}/${dg548}/status`),
        { timeout: 10_000 },
      );
      await page.getByRole('menuitem', { name: /^Offline$/ }).click();
      const dgResp = await dgStatusResponse;
      expect(dgResp.status()).toBe(204);

      // --- Change Disk status to Offline via context menu submenu ---

      const expandDg549 = aside.getByRole('treeitem').filter({ hasText: /DG-549/ }).locator('button[aria-label="Expand"]');
      if (await expandDg549.count() > 0) await expandDg549.click();

      // Right-click disk → Change Status → Offline.
      const disk549Label = aside.getByText(disk549.slice(0, 12), { exact: false });
      await expect(disk549Label).toBeVisible({ timeout: 5_000 });
      // The UI sends the dashed disk_id format (from the tree node's
      // rawId, which comes from the API). The waitForResponse filter
      // must use the dashed format to match.
      const disk549Dashed = disk549.length === 32 ? `${disk549.slice(0, 16)}-${disk549.slice(16)}` : disk549;
      const diskStatusResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'PUT' && r.url().includes(`/api/disks/${encodeURIComponent(disk549Dashed)}/status`),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk549Label.first(), /change status/i);
      await page.getByRole('menuitem', { name: /^Offline$/ }).click();
      const diskResp = await diskStatusResponse;
      expect(diskResp.status()).toBe(204);

      // Recalc through the actual registered DiskDB owner.
      const expandDg551 = aside.getByRole('treeitem').filter({ hasText: /DG-551/ }).locator('button[aria-label="Expand"]');
      if (await expandDg551.count() > 0) await expandDg551.click();

      // Right-click disk → Recalc Usage.
      const disk551Label = aside.getByText(disk551.slice(0, 12), { exact: false });
      await expect(disk551Label).toBeVisible({ timeout: 5_000 });
      const recalcResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'POST' && r.url().includes('/api/diskdb/recalc'),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk551Label.first(), /recalc usage/i);
      const recalcResp = await recalcResponse;
      expect(recalcResp.ok(), await recalcResp.text()).toBeTruthy();

      // Trigger the real consistency scanner through the disk menu.

      const expandDg552 = aside.getByRole('treeitem').filter({ hasText: /DG-552/ }).locator('button[aria-label="Expand"]');
      if (await expandDg552.count() > 0) await expandDg552.click();

      // Right-click disk → Trigger Consistency Scan.
      const disk552Label = aside.getByText(disk552.slice(0, 12), { exact: false });
      await expect(disk552Label).toBeVisible({ timeout: 5_000 });
      const scanResponse = page.waitForResponse(
        (r: any) => r.request().method() === 'POST' && r.url().includes('/api/diskdb/scan'),
        { timeout: 10_000 },
      );
      await clickMenuItem(page, disk552Label.first(), /trigger consistency scan/i);
      const scanResp = await scanResponse;
      expect(scanResp.ok(), await scanResp.text()).toBeTruthy();

      // Persist all hardware states and inspect their actual sidebar badges.
      const statusCases: Array<[number, number, string, string]> = [
        [dg553, 1, 'Up', disk553],
        [dg554, 0, 'Init', disk554],
        [dg555, 2, 'Maintenance', disk555],
        [dg556, 3, 'Suspect', disk556],
        [dg557, 4, 'Missing', disk557],
        [dg558, 5, 'Bad', disk558],
        [dg559, 6, 'Offline', disk559],
      ];
      const api = await apiContext(baseURL!);
      try {
        for (const [dgId, , label, diskId] of statusCases) {
          const group = await api.put(`/api/disk-groups/${rackId}/${nodeId}/${dgId}/status`, { data: { status: label } });
          expect(group.status(), await group.text()).toBe(204);
          const disk = await api.put(`/api/disks/${diskId}/status`, { data: { status: label } });
          expect(disk.status(), await disk.text()).toBe(204);
        }
      } finally { await api.dispose(); }

      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const expandRackAgain = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRackAgain.count() > 0) await expandRackAgain.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      const expandNodeAgain = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNodeAgain.count() > 0) await expandNodeAgain.click();

      // Each DG and its disk should show the HwStatusBadge with the
      // correct title (compact mode: icon only, title attribute set).
      for (const [dgId, , label, diskId] of statusCases) {
        const dgTreeitem = aside.getByRole('treeitem').filter({ hasText: new RegExp(`DG-${dgId}`) });
        await expect(dgTreeitem).toBeVisible({ timeout: 5_000 });
        await expect(dgTreeitem.getByTitle(label)).toBeVisible();

        // Expand disk-group to see the disk.
        const expandDg = dgTreeitem.locator('button[aria-label="Expand"]');
        if (await expandDg.count() > 0) await expandDg.click();

        const diskTreeitem = aside.getByRole('treeitem').filter({ hasText: diskId.slice(0, 12) });
        await expect(diskTreeitem).toBeVisible({ timeout: 5_000 });
        await expect(diskTreeitem.getByTitle(label)).toBeVisible();
      }
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg545, disk545);
      await removeDisk(baseURL!, nodeId, dg546, disk546);
      await removeDisk(baseURL!, nodeId, dg547, disk547);
      await removeDisk(baseURL!, nodeId, dg549, disk549);
      await removeDisk(baseURL!, nodeId, dg551, disk551);
      await removeDisk(baseURL!, nodeId, dg552, disk552);
      await removeDisk(baseURL!, nodeId, dg553, disk553);
      await removeDisk(baseURL!, nodeId, dg554, disk554);
      await removeDisk(baseURL!, nodeId, dg555, disk555);
      await removeDisk(baseURL!, nodeId, dg556, disk556);
      await removeDisk(baseURL!, nodeId, dg557, disk557);
      await removeDisk(baseURL!, nodeId, dg558, disk558);
      await removeDisk(baseURL!, nodeId, dg559, disk559);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg545);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg546);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg547);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg548);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg549);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg551);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg552);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg553);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg554);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg555);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg556);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg557);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg558);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg559);
    }
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
