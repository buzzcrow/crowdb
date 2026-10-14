// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { diskMenu } from '../fixtures/capacity';

// Baseline: 6.0s (2026-10-15).
test('shell defaults and tree interactions distinguish live and stopped release nodes', async ({ page, request }) => {
  test.setTimeout(90_000);
  const stopped = await request.post('/api/nodes/4/server/stop');
  expect(stopped.ok(), await stopped.text()).toBe(true);
  await expect.poll(async () => {
    const response = await request.get('/api/node/status');
    expect(response.ok(), await response.text()).toBe(true);
    return (await response.json()).phase;
  }, { timeout: 10_000, intervals: [100] }).toBe('active');
  await expect.poll(async () => {
    const response = await request.get('/api/racks?recursive=1');
    expect(response.ok(), await response.text()).toBe(true);
    const nodes = (await response.json()).items.flatMap((rack: { nodes: unknown[] }) => rack.nodes);
    return nodes.filter((node: { id: number; kv_server?: { health: string } }) =>
      [1, 2, 3].includes(node.id) && node.kv_server?.health === 'up').length;
  }, { timeout: 10_000, intervals: [100] }).toBe(3);
  const store = await request.post('/api/stores', { data: { store_id: 207, nodes: [1, 2] } });
  expect(store.status(), await store.text()).toBe(201);
  await page.goto('/?domain=KV');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await sidebar.getByRole('button', { name: 'Add Store', exact: true }).click();
  const addStore = page.getByRole('dialog', { name: 'Add KV Store', exact: true });
  await expect(addStore.getByLabel('KV Store ID (numeric)')).toHaveValue('208');
  for (const node of [1, 2, 3]) await expect(addStore.getByLabel(new RegExp(`^${node}\\b`))).toBeVisible();
  await expect(addStore.getByLabel(/^4\b/)).toHaveCount(0);
  await addStore.getByRole('button', { name: 'Cancel', exact: true }).click();
  const storeRow = sidebar.getByTestId('tree-node-S-207').locator(':scope > [role="treeitem"]');
  await diskMenu(page, storeRow, 'Add Group');
  const addGroup = page.getByRole('dialog', { name: 'Add Group', exact: true });
  await expect(addGroup.getByLabel('KV Store')).toHaveValue('207');
  await expect(addGroup.getByLabel('Group ID (numeric)')).toHaveValue('1');
  await expect(addGroup.getByLabel('Starting Replica ID (numeric)')).toHaveValue('1');
  for (const node of [1, 2, 3]) await expect(addGroup.getByLabel(new RegExp(`^${node}\\b`))).toBeVisible();
  await expect(addGroup.getByLabel(/^4\b/)).toHaveCount(0);
  await addGroup.getByLabel(/^1\b/).check();
  await addGroup.getByLabel(/^2\b/).check();
  await addGroup.getByLabel(/^3\b/).uncheck();
  await addGroup.getByRole('button', { name: 'Create Group', exact: true }).click();
  await expect(addGroup).toHaveCount(0);
  const storeBranch = sidebar.getByTestId('tree-node-S-207');
  await expect(storeRow).toHaveAttribute('aria-expanded', /^(true|false)$/);
  if (await storeRow.getAttribute('aria-expanded') !== 'true') await storeRow.getByRole('button', { name: 'Expand', exact: true }).click();
  const group = storeBranch.getByRole('button', { name: 'G-1', exact: true });
  await group.click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Add Replica', exact: true }).click();
  const replica = page.getByRole('dialog', { name: 'Add Replica', exact: true });
  await expect(replica.getByLabel('Replica ID (optional)')).toHaveValue('3');
  const eligible = async () => replica.getByLabel('Node', { exact: true }).locator('option').evaluateAll(options =>
    options.filter(option => !(option as HTMLOptionElement).disabled).map(option => (option as HTMLOptionElement).value));
  expect(await eligible()).toEqual(['3', '4']);
  await replica.getByLabel('Node', { exact: true }).selectOption('3');
  await replica.getByRole('button', { name: 'Add Replica', exact: true }).click();
  await expect(replica).toHaveCount(0);
  await group.click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Add Replica', exact: true }).click();
  await expect(replica.getByLabel('Replica ID (optional)')).toHaveValue('4');
  expect(await eligible()).toEqual(['4']);
  await replica.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByTestId('domain-cluster').click();
  await sidebar.getByRole('button', { name: 'Add Rack', exact: true }).click();
  const rackDialog = page.getByRole('dialog', { name: 'Add Rack', exact: true });
  await rackDialog.getByLabel('Rack ID').fill('20');
  await rackDialog.getByLabel('Name (optional)').fill('cancelled');
  await rackDialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(rackDialog).toHaveCount(0);
  const racks = await request.get('/api/racks?recursive=1');
  expect(racks.ok(), await racks.text()).toBe(true);
  expect((await racks.json()).items.map((row: { id: number }) => row.id)).not.toContain(20);
  for (const id of [2, 3]) {
    const added = await request.post('/api/racks', { data: { id, name: `rack ${id}` } });
    expect(added.status(), await added.text()).toBe(201);
  }
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  for (const id of [1, 2, 3]) await expect(sidebar.getByTestId(`tree-node-R-${id}`)).toBeVisible();
  const rack = sidebar.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]');
  await rack.getByRole('button', { name: 'Collapse', exact: true }).click();
  await expect(rack).toHaveAttribute('aria-expanded', 'false');
  await expect(rack).not.toHaveAttribute('aria-selected', 'true');
  await rack.getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(rack).toHaveAttribute('aria-expanded', 'true');
  await expect(rack).not.toHaveAttribute('aria-selected', 'true');
  const node = sidebar.getByTestId('tree-node-N-3').locator(':scope > [role="treeitem"]');
  await node.getByRole('button', { name: 'N-3', exact: true }).click();
  await expect(node).toHaveAttribute('aria-selected', 'true');
  const storage = sidebar.getByTestId('tree-node-STORAGE-3').locator(':scope > [role="treeitem"]');
  await expect(storage).toHaveAttribute('aria-expanded', 'false');
  await page.getByTestId('domain-capacity').click();
  await expect(node).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('heading', { name: 'Capacity — Node 3', exact: true })).toBeVisible();
  await expect(sidebar.getByTestId('tree-node-SERVICES-3')).toBeVisible();
  await storage.getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(storage).toHaveAttribute('aria-expanded', 'true');
  await page.getByTestId('cluster-topology').click();
  await expect(node).toHaveAttribute('aria-selected', 'true');
  await expect(storage).toHaveAttribute('aria-expanded', 'true');
});
