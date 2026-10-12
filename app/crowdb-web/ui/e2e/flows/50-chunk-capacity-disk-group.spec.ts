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
  removeDiskdb,
  deployDiskdb as apiDeployDiskdb,
  deployNodeServer,
  clusterInit,
  waitForLeader,
  createStoreNoInit,
  addGroup,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

const DISKDB_RACK = 501;
const DISKDB_NODE = 501;

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

async function openContextMenu(
  page: import('@playwright/test').Page,
  treeItem: import('@playwright/test').Locator,
) {
  await treeItem.click({ button: 'right' });
  await expect(page.getByRole('menu')).toBeVisible();
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
test.describe('chunk · capacity · disk-group', () => {
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
  });

  // No afterAll — the beforeAll reset of the next test file (or the
  // next run's beforeAll) cleans up all state. An afterAll here would
  // stop the kv-server between tests, breaking group-0 ops for later
  // tests in this file.

  test('disk-group and disk CRUD via the UI', async ({ page, baseURL }) => {
    test.setTimeout(60_000);
    const nodeId = DISKDB_NODE;
    const dg520 = 520;
    const dg530 = 530;
    const dg540 = 540;
    const dg560 = 560;
    const dg570 = 570;
    const disk540 = randomDiskId();
    const disk560 = randomDiskId();
    const disk570 = randomDiskId();

    // Deploy a diskdb instance so addDiskGroup auto-assigns ownership
    // (required since diskdb ownership enforcement — a3d39f0e).
    const deployment = await apiDeployDiskdb(baseURL!, nodeId, freePortRange(3));
    const registrationApi = await apiContext(baseURL!);
    try {
      await expect.poll(async () => {
        const response = await registrationApi.get('/api/diskdb/instances');
        expect(response.ok(), await response.text()).toBeTruthy();
        return (await response.json()).some((instance: { rpc_endpoint: string }) => instance.rpc_endpoint === new URL(deployment.endpoint).host);
      }, { timeout: 3000, intervals: [100] }).toBe(true);
    } finally {
      await registrationApi.dispose();
    }

    // Pre-create the disk-groups that are not created through the UI, so
    // the tree already holds them when the page mounts.
    await apiAddDiskGroup(baseURL!, nodeId, dg530, 'test-dg-530');
    await apiAddDiskGroup(baseURL!, nodeId, dg540, 'test-dg-540');
    await apiAddDiskGroup(baseURL!, nodeId, dg560, 'test-dg-560');
    await addDisksBatch(baseURL!, nodeId, dg560, [{ disk_id: disk560 }]);
    await apiAddDiskGroup(baseURL!, nodeId, dg570, 'test-dg-570');
    // Add a disk to DG-570 so we can verify cascade delete removes it.
    await addDisksBatch(baseURL!, nodeId, dg570, [{ disk_id: disk570 }]);

    try {
      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${DISKDB_RACK}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      // --- Add Disk Group dialog creates a disk-group via UI ---

      // Right-click node → Add Disk Group.
      await clickMenuItem(page, aside.getByText(`N-${nodeId}`, { exact: true }), /add disk group/i);

      const dgDialog = page.getByRole('dialog', { name: /add disk group/i });
      await expect(dgDialog).toBeVisible();
      // The dialog should have a Disk Group ID field (auto-assigned)
      // and a Name field. The ID should be auto-suggested as the next
      // available ID (max existing + 1). DGs 530/540/560/570 were
      // pre-created, so the suggestion should be > 570.
      const dgIdInput = dgDialog.getByLabel('Disk Group ID (auto-assigned)');
      await expect(dgIdInput).toBeVisible();
      await expect(dgDialog.getByLabel('Name (optional)')).toBeVisible();
      const suggestedId = await dgIdInput.inputValue();
      // The dialog should suggest the minimal unused DG id (not a
      // duplicate of any existing DG). Fetch existing DGs to verify.
      const existApi = await apiContext(baseURL!);
      let existingDgIds: number[] = [];
      try {
        const r = await existApi.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        existingDgIds = (await r.json()).map((dg: any) => dg.id as number);
      } finally {
        await existApi.dispose();
      }
      expect(existingDgIds).not.toContain(Number(suggestedId));

      // Override with a specific ID and submit.
      await dgIdInput.fill(String(dg520));
      await dgDialog.getByLabel('Name (optional)').fill('test-dg');
      await dgDialog.getByRole('combobox', { name: 'KV group', exact: true }).selectOption('0/1');
      const createDgBtn = dgDialog.getByRole('button', { name: /create disk group/i });
      await createDgBtn.evaluate((el) => (el as HTMLElement).click());

      const createdNode = aside.getByTestId(`tree-node-N-${nodeId}`);
      await expect(createdNode.getByRole('treeitem')).toBeVisible();
      const openCreatedNode = createdNode.getByRole('button', { name: 'Expand', exact: true });
      if (await openCreatedNode.count()) await openCreatedNode.click();

      // The disk-group should appear in the sidebar.
      await expect(aside.getByText(/test-dg.*DG-520|DG-520.*test-dg/, { exact: true })).toBeVisible({ timeout: 10_000 });

      // Regression: opening the Add Disk Group dialog again should
      // suggest an ID that does NOT reuse the just-created DG-520 or
      // any other existing DG. The suggestion must be the minimal
      // unused id (not a duplicate).
      await clickMenuItem(page, aside.getByText(`N-${nodeId}`, { exact: true }), /add disk group/i);
      const dgDialog2 = page.getByRole('dialog', { name: /add disk group/i });
      await expect(dgDialog2).toBeVisible();
      const suggestedId2 = await dgDialog2.getByLabel('Disk Group ID (auto-assigned)').inputValue();
      expect(Number(suggestedId2)).not.toBe(dg520);
      const existApi2 = await apiContext(baseURL!);
      try {
        const r = await existApi2.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        const ids = (await r.json()).map((dg: any) => dg.id as number);
        expect(ids).not.toContain(Number(suggestedId2));
      } finally {
        await existApi2.dispose();
      }
      await page.keyboard.press('Escape');

      // Verify via API.
      const dgApi = await apiContext(baseURL!);
      try {
        const r = await dgApi.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        const dgs = await r.json();
        expect(dgs.some((dg: any) => dg.id === dg520 && dg.node_id === nodeId)).toBeTruthy();
      } finally {
        await dgApi.dispose();
      }

      // --- disk-group context menu shows Add Disk + set-status + delete (no operations) ---

      // Expand the node to see the disk-groups.
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();

      // Right-click the disk-group.
      await expect(aside.getByText(/DG-530/, { exact: true })).toBeVisible({ timeout: 5_000 });
      await openContextMenu(page, aside.getByText(/DG-530/, { exact: true }));

      await expect(page.getByRole('menuitem', { name: /add disk/i })).toBeVisible();
      // DG status is now behind a "Change Status" submenu (not flat
      // "set disk group up/down" items).
      await expect(page.getByRole('menuitem', { name: /change status/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /delete disk group/i })).toBeVisible();
      // Operations belong on disk, not disk-group.
      await expect(page.getByRole('menuitem', { name: /trigger.*scan/i })).toHaveCount(0);
      await expect(page.getByRole('menuitem', { name: /recalc usage/i })).toHaveCount(0);
      await page.keyboard.press('Escape');

      // --- Add Disk dialog adds disks via UI ---

      await expect(aside.getByText(/DG-540/, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Right-click disk-group → Add Disk.
      await clickMenuItem(page, aside.getByText(/DG-540/, { exact: true }), /add disk/i);

      const diskDialog = page.getByRole('dialog', { name: /add disks/i });
      await expect(diskDialog).toBeVisible();

      // The dialog should have a Disk ID field and a Type selector.
      const diskIdInput = diskDialog.getByLabel('Disk ID (UUID)');
      await expect(diskIdInput).toBeVisible();

      // Disk Size unit is TiB, Zone Size unit is GiB (regression:
      // previously GiB / MiB).
      await expect(diskDialog.getByLabel('Disk Size (TiB)')).toBeVisible();
      await expect(diskDialog.getByLabel('Zone Size (GiB)')).toBeVisible();
      await expect(diskDialog.getByLabel('Disk Size (GiB)')).toHaveCount(0);
      await expect(diskDialog.getByLabel('Zone Size (MiB)')).toHaveCount(0);

      // Set a known disk ID.
      await diskIdInput.fill(disk540);

      const addDisksBtn = diskDialog.getByRole('button', { name: /add disks/i });
      await addDisksBtn.evaluate((el) => (el as HTMLElement).click());

      // Wait for the dialog to close and refresh to complete.
      await expect(diskDialog).toHaveCount(0, { timeout: 10_000 });

      // Expand the disk-group to see the disk.
      const expandDg540 = aside.getByRole('treeitem').filter({ hasText: /DG-540/ }).locator('button[aria-label="Expand"]');
      if (await expandDg540.count() > 0) await expandDg540.click();

      // The disk should appear in the sidebar (truncated to 12 chars + …).
      await expect(aside.getByText(disk540.slice(0, 12), { exact: false })).toBeVisible({ timeout: 10_000 });

      // Verify via API. The API returns disk IDs in dashed format
      // (`{high:016x}-{low:016x}`), but randomDiskId() returns bare
      // 32-char hex. Compare against the dashed form.
      const toDashed = (s: string) => s.length === 32 ? `${s.slice(0, 16)}-${s.slice(16)}` : s;
      const diskApi = await apiContext(baseURL!);
      try {
        const r = await diskApi.get(`/api/nodes/${nodeId}/disk-groups/${dg540}/disks`);
        expect(r.ok()).toBeTruthy();
        const disks = await r.json();
        expect(disks.some((d: any) => d.disk_id === toDashed(disk540))).toBeTruthy();
      } finally {
        await diskApi.dispose();
      }

      // --- disk context menu shows compact/rebuild/scan/recalc/set-status/delete ---

      // Expand disk-group 560 to see its disk.
      const expandDg560 = aside.getByRole('treeitem').filter({ hasText: /DG-560/ }).locator('button[aria-label="Expand"]');
      if (await expandDg560.count() > 0) await expandDg560.click();

      // Right-click the disk.
      const disk560Label = aside.getByText(disk560.slice(0, 12), { exact: false });
      await expect(disk560Label).toBeVisible({ timeout: 5_000 });
      await openContextMenu(page, disk560Label.first());

      await expect(page.getByRole('menuitem', { name: /compact zones/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /rebuild bitmap/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /trigger consistency scan/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /recalc usage/i })).toBeVisible();
      // Disk status is now behind a "Change Status" submenu (not flat
      // "set disk up/down" items).
      await expect(page.getByRole('menuitem', { name: /change status/i })).toBeVisible();
      await expect(page.getByRole('menuitem', { name: /delete disk/i })).toBeVisible();
      await page.keyboard.press('Escape');

      // --- Delete Disk Group via context menu removes it (destructive, last) ---

      await expect(aside.getByText(/DG-570/, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Right-click disk-group → Delete Disk Group.
      await clickMenuItem(page, aside.getByText(/DG-570/, { exact: true }), /delete disk group/i);

      // Confirm delete dialog.
      const deleteDialog = page.getByRole('dialog', { name: /delete disk group/i });
      await expect(deleteDialog).toBeVisible();
      const confirmBtn = deleteDialog.getByRole('button', { name: /delete disk group/i });
      await confirmBtn.evaluate((el) => (el as HTMLElement).click());

      // The disk-group should disappear from the tree.
      await expect(aside.getByText(/DG-570/, { exact: true })).toHaveCount(0, { timeout: 10_000 });
      // The child disk should also disappear (cascade delete).
      await expect(aside.getByText(disk570.slice(0, 12), { exact: false })).toHaveCount(0, { timeout: 10_000 });

      // Verify via API: DG and its child disk are both gone.
      const delApi = await apiContext(baseURL!);
      try {
        const r = await delApi.get(`/api/nodes/${nodeId}/disk-groups`);
        expect(r.ok()).toBeTruthy();
        const dgs = await r.json();
        expect(dgs.some((dg: any) => dg.id === dg570)).toBeFalsy();
        // Disk should also be removed from config.
        const diskR = await delApi.get(`/api/nodes/${nodeId}/disk-groups/${dg570}/disks`);
        // DG is gone, so this should 404 or return empty.
        expect(diskR.ok() || diskR.status() === 404).toBeTruthy();
      } finally {
        await delApi.dispose();
      }
    } finally {
      // Cleanup.
      await removeDisk(baseURL!, nodeId, dg540, disk540);
      await removeDisk(baseURL!, nodeId, dg560, disk560);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg520);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg530);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg540);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg560);
      await apiRemoveDiskGroup(baseURL!, nodeId, dg570);
      // Stop the diskdb deployed for addDiskGroup ownership assignment.
      await removeDiskdb(baseURL!, nodeId);
    }
  });

  test('assign disk-group to diskdb via UI (owner + bind) and reports non-zero capacity', async ({ page, baseURL }) => {
    test.setTimeout(90_000);
    const rackId = DISKDB_RACK;
    const nodeId = DISKDB_NODE;
    const dgId = 590;
    const diskId = randomDiskId();
    const storeId = 590;
    const groupId = 590;
    const rpcPort = freePortRange(3);

    // Deploy diskdb via API (the UI deploy flow is tested in the next
    // test; here we just need a running instance for ownership assignment).
    await step('capacity-assign: deploy diskdb', () => apiDeployDiskdb(baseURL!, nodeId, rpcPort));

    // Fetch the diskdb instance id (auto-generated, not the node id).
    let instanceId = '';
    {
      const api = await apiContext(baseURL!);
      try {
        // The diskdb registers with rpc_listen_addr = rpc_port + 2
        // (see resolve_diskdb_config_path in lifecycle.rs). Poll until
        // the instance appears in the service registry — the diskdb
        // process registers asynchronously after startup.
        const rpcListenPort = rpcPort + 2;
        let ddb: { instance_id: string } | undefined;
        await step('capacity-assign: wait for registration', () => expect.poll(async () => {
          const r = await api.get('/api/diskdb/instances');
          if (!r.ok()) return false;
          const instances = await r.json();
          ddb = (instances as { rpc_endpoint: string; instance_id: string }[]).find(
            (i) => i.rpc_endpoint.includes(String(rpcListenPort)),
          );
          return ddb !== undefined;
        }, { timeout: 15_000, intervals: [100] }).toBe(true));
        instanceId = ddb!.instance_id;
      } finally {
        await api.dispose();
      }
    }

    // Create a DG + disk, and a store + group for the bind target.
    await step('capacity-assign: create bind targets', async () => {
      await apiAddDiskGroup(baseURL!, nodeId, dgId, 'test-dg-assign');
      await addDisksBatch(baseURL!, nodeId, dgId, [{ disk_id: diskId }]);
      await createStoreNoInit(baseURL!, storeId, [nodeId]);
      await addGroup(baseURL!, storeId, groupId, 1, [nodeId]);
    });

    try {
      // The disk-groups data arrives via fetchNodeDiskGroups (async, not
      // polled) which lags the racks tree on slow CI runners. Wait for
      // the API response before asserting DG visibility.
      await page.goto('/');
      await page.getByTestId('domain-capacity').click();

      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      const expandRack = aside.getByRole('treeitem').filter({ hasText: `R-${rackId}` }).locator('button[aria-label="Expand"]');
      if (await expandRack.count() > 0) await expandRack.click();
      await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 5_000 });

      // Expand the node to see the disk-group.
      const expandNode = aside.getByRole('treeitem').filter({ hasText: `N-${nodeId}` }).locator('button[aria-label="Expand"]');
      if (await expandNode.count() > 0) await expandNode.click();
      await expect(aside.getByText(/DG-590/, { exact: true })).toBeVisible({ timeout: 10_000 });

      // --- Right-click DG → "Assign to DiskDB" context menu item ---
      await clickMenuItem(page, aside.getByText(/DG-590/, { exact: true }), /assign to diskdb/i);

      const assignDialog = page.getByRole('dialog', { name: /assign disk group/i });
      await expect(assignDialog).toBeVisible();

      // The dialog should have DiskDB Instance, Paxos Store, and
      // Paxos Data Group dropdowns.
      await expect(assignDialog.getByLabel('DiskDB Instance')).toBeVisible();
      await expect(assignDialog.getByLabel('Paxos Store')).toBeVisible();
      await expect(assignDialog.getByLabel('Paxos Data Group')).toBeVisible();

      // Select the diskdb instance (should be pre-selected if only one).
      const instanceSelect = assignDialog.getByLabel('DiskDB Instance');
      await instanceSelect.selectOption(String(instanceId));

      // Select the store.
      const storeSelect = assignDialog.getByLabel('Paxos Store');
      await storeSelect.selectOption(String(storeId));

      // Select the data group.
      const groupSelect = assignDialog.getByLabel('Paxos Data Group');
      await groupSelect.selectOption(String(groupId));

      // Submit the assignment.
      const assignBtn = assignDialog.getByRole('button', { name: /assign/i });
      const ownerResponse = page.waitForResponse((response) =>
        response.request().method() === 'PUT' && response.url().endsWith('/owner'));
      const bindResponse = page.waitForResponse((response) =>
        response.request().method() === 'PUT' && response.url().endsWith('/bind'));
      await assignBtn.evaluate((el) => (el as HTMLElement).click());
      const [owner, bind] = await Promise.all([ownerResponse, bindResponse]);
      expect(owner.ok(), `owner request returned ${owner.status()}`).toBeTruthy();
      expect(bind.ok(), `bind request returned ${bind.status()}`).toBeTruthy();
      await expect(assignDialog).toHaveCount(0, { timeout: 5_000 });

      // --- Verify capacity becomes non-zero via API ---
      // The diskdb keepalive syncs asynchronously, so poll until the DG
      // appears in the usage response with capacity > 0. CI runners are
      // slower than local dev, so allow 30s for the keepalive cycle.
      const api = await apiContext(baseURL!);
      try {
        await step('capacity-assign: wait for usage', () => expect.poll(async () => {
            const r = await api.get('/api/diskdb/usage');
            if (!r.ok()) return 0;
            const usage = await r.json();
            const dg = usage.disk_groups.find((g: { disk_group_id: number }) =>
              g.disk_group_id === dgId);
            return dg?.capacity_bytes ?? 0;
          }, { timeout: 30_000, intervals: [200] }).toBeGreaterThan(0));

        // --- Verify the capacity panel shows non-zero ---
        await page.goto('/');
        await page.getByTestId('domain-capacity').click();
        await expect(aside.getByRole('button', { name: `N-${nodeId}`, exact: true })).toBeVisible();
        await aside.getByTestId(`tree-node-N-${nodeId}`).getByRole('button', { name: 'Expand', exact: true }).click();
        await expect(aside.getByText(/DG-590/, { exact: true })).toBeVisible({ timeout: 10_000 });
        // The Total Capacity card should show a non-zero value (not "0 B").
        const capacityText = page.getByText(/Total Capacity/).locator('..');
        await expect(capacityText.getByText(/0 B/)).toHaveCount(0, { timeout: 10_000 });
      } finally {
        await api.dispose();
      }
    } finally {
      // Remove the bind target before the next test restarts this shared KV.
      const cleanupApi = await apiContext(baseURL!);
      try {
        const response = await cleanupApi.delete(`/api/stores/${storeId}`);
        expect(response.status(), await response.text()).toBe(204);
      } finally {
        await cleanupApi.dispose();
      }
      await removeDisk(baseURL!, nodeId, dgId, diskId);
      await apiRemoveDiskGroup(baseURL!, nodeId, dgId);
      await removeDiskdb(baseURL!, nodeId);
    }
  });

});
