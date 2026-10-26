// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { diskMenu, diskBranch } from '../fixtures/capacity';
import { step } from '../fixtures/stepTimer';
import type { APIRequestContext } from '@playwright/test';

async function json(request: APIRequestContext, path: string) {
  const response = await request.get(path);
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

// Baseline: 12.4s (2026-10-12).
test('native capacity: disk-group CRUD and explicit owner binding use mounted storage', async ({ page, request }) => {
  test.setTimeout(90_000);
  for (const group_id of [1, 2]) {
    const added = await request.post('/api/stores/0/groups', { data: { group_id, replica_id: 1, nodes: [1, 2, 3] } });
    expect(added.status(), await added.text()).toBe(201);
    await expect.poll(async () => (await json(request, `/api/stores/0/groups/${group_id}`))
      .replicas.filter((replica: { role: string }) => replica.role === 'leader').length,
    { timeout: 10_000, intervals: [100] }).toBe(1);
  }
  const deployed = await request.post('/api/nodes/1/diskdb/deploy', { data: { rpc_port: 7100, keepalive_interval_secs: 1 } });
  expect(deployed.ok(), await deployed.text()).toBe(true);
  let instance = '';
  await expect.poll(async () => {
    const endpoint = new URL((await deployed.json()).endpoint).host;
    instance = (await json(request, '/api/diskdb/instances')).find(
      (row: { rpc_endpoint: string }) => row.rpc_endpoint === endpoint)?.instance_id ?? '';
    return instance;
  }, { intervals: [100] }).not.toBe('');
  await page.goto('/?domain=Capacity');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const node = sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  await expect(sidebar.getByRole('button', { name: 'Add Rack', exact: true })).toBeEnabled();
  await diskMenu(page, node, 'Add Disk Group');
  const create = page.getByRole('dialog', { name: 'Add Disk Group', exact: true });
  const id = create.getByLabel('Disk Group ID (auto-assigned)');
  await expect(create.getByRole('button', { name: 'Create Disk Group', exact: true })).toBeEnabled();
  expect((await json(request, '/api/nodes/1/disk-groups')).map((row: { id: number }) => row.id))
    .not.toContain(Number(await id.inputValue()));
  await id.fill('520');
  await create.getByLabel('Name (optional)').fill('mounted CRUD');
  await create.getByRole('combobox', { name: 'KV group', exact: true }).selectOption('0/1');
  await create.getByRole('button', { name: 'Create Disk Group', exact: true }).click();
  await expect(create).toHaveCount(0);
  await sidebar.getByTestId('tree-node-STORAGE-1').getByRole('button', { name: 'Expand', exact: true }).click();
  const group = sidebar.getByTestId('tree-node-CH-DG-1-520').locator(':scope > [role="treeitem"]');
  await expect(group).toContainText('mounted CRUD');
  expect(await json(request, '/api/nodes/1/disk-groups'))
    .toEqual(expect.arrayContaining([expect.objectContaining({ id: 520, node_id: 1 })]));
  await diskMenu(page, node, 'Add Disk Group');
  await expect(create.getByRole('button', { name: 'Create Disk Group', exact: true })).toBeEnabled();
  expect(Number(await id.inputValue())).not.toBe(520);
  expect((await json(request, '/api/nodes/1/disk-groups')).map((row: { id: number }) => row.id))
    .not.toContain(Number(await id.inputValue()));
  await page.keyboard.press('Escape');
  await group.click({ button: 'right' });
  for (const name of ['Create File Disk', 'Manage node storage', 'Delete Disk Group']) {
    await expect(page.getByRole('menuitem', { name, exact: true })).toBeVisible();
  }
  for (const name of ['Trigger Consistency Scan', 'Recalc Usage']) {
    await expect(page.getByRole('menuitem', { name, exact: true })).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  await diskMenu(page, group, 'Assign to DiskDB');
  const assign = page.getByRole('dialog', { name: /Assign Disk Group/ });
  await assign.getByLabel('DiskDB Instance').selectOption(instance);
  await assign.getByLabel('Paxos Store').selectOption('0');
  await assign.getByLabel('Paxos Data Group').selectOption('2');
  const owner = page.waitForResponse(response => response.request().method() === 'PUT' && response.url().endsWith('/owner'));
  const bind = page.waitForResponse(response => response.request().method() === 'PUT' && response.url().endsWith('/bind'));
  await assign.getByRole('button', { name: 'Assign', exact: true }).click();
  for (const response of await Promise.all([owner, bind])) expect(response.ok(), `assignment HTTP ${response.status()}`).toBe(true);
  await expect(assign).toHaveCount(0, { timeout: 5_000 });
  expect(await json(request, '/api/hardware/disk-group-bindings')).toEqual(expect.arrayContaining([
    expect.objectContaining({ node_id: 1, dg_id: 520, store_id: 0, group_id: 2 }),
  ]));
  await step('deploy actual DiskIO for the bound group', async () => {
    const defaults = (await json(request, '/api/deployment-defaults?node_id=1')).diskio;
    const response = await request.post('/api/nodes/1/services/deploy', { data: {
      ...defaults, kind: 'diskio', instance_id: '1', disk_group_id: 520,
      rpc_port: 7200,
    } });
    expect(response.status(), await response.text()).toBe(201);
  });
  const accepted = await step('create a managed file through the group menu', async () => {
    await diskMenu(page, group, 'Create File Disk');
    const add = page.getByRole('dialog', { name: 'Create File Disk', exact: true });
    await expect(add.getByLabel('Capacity (GiB)')).toHaveValue('16');
    await expect(add.getByLabel('Disk group')).toHaveValue('520');
    await expect(add.getByLabel('Sparse file')).not.toBeChecked();
    await add.getByLabel('Capacity (GiB)').fill('0.0625');
    const pending = page.waitForResponse(response => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/nodes/1/disk-files');
    await add.getByRole('button', { name: 'Create and Join', exact: true }).click();
    const response = await pending;
    expect(response.status(), await response.text()).toBe(202);
    await expect(add).toHaveCount(0);
    return response.json();
  });
  await step('observe durable registration and actual capacity', async () => {
    await expect.poll(async () => (await json(request,
      `/api/nodes/1/disk-files/${accepted.request.operation_id}`)).phase,
    { intervals: [100] }).toBe('registered');
    await expect.poll(async () => (await json(request, '/api/diskdb/usage')).disk_groups
      .find((row: { disk_group_id: number }) => row.disk_group_id === 520)?.capacity_bytes ?? 0,
    { intervals: [100] }).toBe(64 * 1024 ** 2);
    const disks = await json(request, '/api/nodes/1/disk-groups/520/disks');
    expect(disks).toHaveLength(1);
    expect(disks[0]).toMatchObject({ device_path: accepted.path, capacity_bytes: 64 * 1024 ** 2 });
    expect(accepted.path).toBe(`/opt/crowdb/data/disks/managed-${accepted.request.operation_id}.img`);
  });
  await page.goto('/?domain=Capacity');
  const branch = await diskBranch(page, 1, 520);
  await branch.disk.click({ button: 'right' });
  for (const name of ['Compact Zones', 'Rebuild Bitmap', 'Trigger Consistency Scan', 'Recalc Usage', 'Manage node storage']) {
    await expect(page.getByRole('menuitem', { name, exact: true })).toBeVisible();
  }
  for (const name of ['Change Status', 'Delete Disk']) {
    await expect(page.getByRole('menuitem', { name, exact: true })).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  await diskMenu(page, branch.group, 'Delete Disk Group');
  const remove = page.getByRole('dialog', { name: /Delete Disk Group/ });
  await expect(remove).toContainText('Removed disks still retain their group');
  const rejected = page.waitForResponse(response => response.request().method() === 'DELETE'
    && new URL(response.url()).pathname === '/api/nodes/1/disk-groups/520');
  await remove.getByRole('button', { name: 'Delete Disk Group', exact: true })
    .evaluate(element => (element as HTMLElement).click());
  expect((await rejected).status()).toBe(409);
  expect(await json(request, '/api/nodes/1/disk-groups'))
    .toEqual(expect.arrayContaining([expect.objectContaining({ id: 520 })]));
  expect(await json(request, '/api/nodes/1/disk-groups/520/disks'))
    .toEqual(expect.arrayContaining([expect.objectContaining({ disk_id: branch.id, device_path: accepted.path })]));

});

// Baseline: 5.8s (2026-10-12).
test('KV binding recommends the least loaded group and initialization creates Group 1', async ({ page, request }) => {
  test.setTimeout(60_000);
  await page.goto('/?domain=KV');
  await page.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  const init = page.getByRole('dialog', { name: 'Initialize Cluster', exact: true });
  await expect(init).toContainText('data Group 1');
  await expect(init.getByRole('checkbox', { checked: true })).toHaveCount(3);
  await init.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  await expect(init).toHaveCount(0, { timeout: 10_000 });
  await page.getByTestId('domain-cluster').click();
  await step('observe accepted initialization completion', async () => {
    await expect(page.getByRole('region', { name: 'Cluster initialization' }).getByRole('status'))
      .toHaveText('Initialization complete', { timeout: 10_000 });
  });
  const group = await request.get('/api/stores/0/groups/1');
  expect(group.ok(), await group.text()).toBe(true);
  await page.getByTestId('domain-capacity').click();
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const node = sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  await expect(sidebar.getByRole('button', { name: 'Add Rack', exact: true })).toBeEnabled();
  await diskMenu(page, node, 'Add Disk Group');
  const dialog = page.getByRole('dialog', { name: 'Add Disk Group', exact: true });
  const binding = dialog.getByRole('combobox', { name: 'KV group', exact: true });
  const submit = dialog.getByRole('button', { name: 'Create Disk Group', exact: true });
  await expect(binding).toHaveValue('0/1');
  await expect(submit).toBeEnabled();
  await dialog.getByLabel('Disk Group ID (auto-assigned)').fill('591');
  const missing = await request.post('/api/nodes/1/disk-groups', { data: { id: 591 } });
  expect(missing.status(), await missing.text()).toBe(422);
  expect(await (await request.get('/api/nodes/1/disk-groups')).json()).toEqual([]);
  await submit.click();
  await expect(dialog).toHaveCount(0);
  expect(await (await request.get('/api/nodes/1/disk-groups')).json())
    .toEqual(expect.arrayContaining([expect.objectContaining({ id: 591 })]));
  await sidebar.getByTestId('tree-node-STORAGE-1').getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(sidebar.getByRole('button', { name: 'DG-591', exact: true })).toBeVisible();
  const added = await request.post('/api/stores/0/groups', {
    data: { group_id: 2, replica_id: 1, nodes: [1, 2, 3] },
  });
  expect(added.status(), await added.text()).toBe(201);
  await step('observe Group 2 leader election before binding', async () => {
    await expect.poll(async () => {
      const response = await request.get('/api/stores/0/groups/2');
      expect(response.ok(), await response.text()).toBe(true);
      return (await response.json()).replicas.filter((row: { role: string }) => row.role === 'leader').length;
    }, {
      message: 'Created Group 2 must elect exactly one leader before binding recommendations',
      timeout: 10_000, intervals: [100],
    }).toBe(1);
  });
  const remote = await request.post('/api/nodes/2/disk-groups', {
    data: { id: 592, store_id: 0, group_id: 1 },
  });
  expect(remote.status(), await remote.text()).toBe(201);
  await diskMenu(page, node, 'Add Disk Group');
  await expect(binding).toHaveValue('0/2');
  await expect(binding.locator('option[value="0/1"]')).toHaveText('Store 0 / Group 1 · 2 disk groups');
  await expect(binding.locator('option[value="0/2"]')).toHaveText('Store 0 / Group 2 · 0 disk groups · Recommended');
  await binding.selectOption('0/1');
  await expect(binding).toHaveValue('0/1');
  await binding.selectOption('0/2');
  await dialog.getByLabel('Disk Group ID (auto-assigned)').fill('593');
  await submit.click();
  await expect(dialog).toHaveCount(0);
  const distribution = await request.get('/api/hardware/disk-group-bindings');
  expect(distribution.ok(), await distribution.text()).toBe(true);
  expect(await distribution.json()).toEqual(expect.arrayContaining([
    expect.objectContaining({ node_id: 1, dg_id: 593, store_id: 0, group_id: 2 }),
  ]));
  await diskMenu(page, node, 'Add Disk Group');
  await expect(binding).toHaveValue('0/2');
  await expect(binding.locator('option[value="0/2"]')).toHaveText('Store 0 / Group 2 · 1 disk groups · Recommended');
  await page.keyboard.press('Escape');
  const servers = await request.get('/api/servers');
  expect(servers.ok(), await servers.text()).toBe(true);
  expect((await servers.json()).filter((row: { service_type: string }) => row.service_type === 'diskdb')).toHaveLength(0);
  await page.goto('/?domain=Cluster');
  await expect(sidebar.getByRole('button', { name: 'DG-593', exact: true })).toHaveCount(0);
  await page.getByTestId('domain-cluster').click();
  await page.getByTestId('domain-capacity').click();
  const physical = sidebar.getByTestId('tree-node-STORAGE-1').locator(':scope > [role="treeitem"]');
  if (await physical.getAttribute('aria-expanded') !== 'true') {
    await physical.getByRole('button', { name: 'Expand', exact: true }).click();
  }
  await expect(sidebar.getByRole('button', { name: 'DG-593', exact: true })).toBeVisible();
});
