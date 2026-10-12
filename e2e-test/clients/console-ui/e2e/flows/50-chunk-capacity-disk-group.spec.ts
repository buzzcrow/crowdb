// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { diskMenu } from '../fixtures/capacity';

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
  const group = await request.get('/api/stores/0/groups/1');
  expect(group.ok(), await group.text()).toBe(true);
  await page.getByTestId('domain-capacity').click();
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const node = sidebar.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
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
  await node.getByRole('button', { name: 'Expand', exact: true }).click();
  await expect(sidebar.getByRole('button', { name: 'DG-591', exact: true })).toBeVisible();
  const added = await request.post('/api/stores/0/groups', {
    data: { group_id: 2, replica_id: 1, nodes: [1, 2, 3] },
  });
  expect(added.status(), await added.text()).toBe(201);
  await expect.poll(async () => {
    const response = await request.get('/api/stores/0/groups/2');
    expect(response.ok(), await response.text()).toBe(true);
    return (await response.json()).replicas.filter((row: { role: string }) => row.role === 'leader').length;
  }, { intervals: [100] }).toBe(1);
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
});
