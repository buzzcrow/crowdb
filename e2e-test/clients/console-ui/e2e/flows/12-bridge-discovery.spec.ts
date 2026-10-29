// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { reviewDisklessCandidate } from '../fixtures/candidateSetup';
import { step } from '../fixtures/stepTimer';

test('Docker default setup: submit storage and services then close the initiating browser', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_IDENTITIES || !process.env.CROWDB_NODE_UI_ORIGIN, 'Owned default node fixture required');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const identities: string[] = JSON.parse(process.env.CROWDB_NODE_UI_IDENTITIES!);
  await page.goto(`${origin}/?domain=Cluster`);
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Add Rack', exact: true }).click();
  const rack = page.getByRole('dialog', { name: 'Add Rack' });
  await rack.getByLabel('Rack ID', { exact: true }).fill('1');
  await rack.getByRole('button', { name: 'Create Rack', exact: true }).click();
  await expect(rack).toHaveCount(0);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  for (const id of identities) {
    await candidates.getByTestId(`tree-node-candidate-${id}`).getByRole('button', { name: /^node-/ }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Join node…', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Move candidate to cluster' });
    await dialog.getByRole('button', { name: 'Verify and review', exact: true }).click();
    await expect(dialog.getByText(/Access verified/)).toBeVisible();
    await expect(dialog.getByLabel('Initial SSH password', { exact: true })).toHaveValue('');
    for (const checkbox of await dialog.getByRole('region', { name: 'Node service choices' }).getByRole('checkbox').all()) await expect(checkbox).toBeChecked();
    await expect(dialog.getByLabel('Managed file capacity (GiB)', { exact: true })).toHaveValue('16');
    await expect(dialog.getByLabel('Managed file capacity (GiB)', { exact: true })).toBeDisabled();
    await expect(dialog.getByLabel('crowdb-disk-db RPC port', { exact: true })).toHaveValue('7100');
    await expect(dialog.getByLabel('crowdb-disk-db RPC port', { exact: true })).toBeDisabled();
    await dialog.getByRole('button', { name: 'Join and start', exact: true }).click();
    await expect(dialog).toHaveCount(0, { timeout: 10_000 });
  }
  await page.getByTestId('domain-kv').click();
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Initialize Cluster', exact: true }).click();
  const initialize = page.getByRole('dialog', { name: 'Initialize Cluster' });
  const accepted = page.waitForResponse(response => response.url().endsWith('/api/cluster/init') && response.request().method() === 'POST');
  await initialize.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  const response = await accepted;
  expect(response.status(), await response.text()).toBe(202);
  expect(await response.json()).toMatchObject({ phase: 'preparing' });
  await page.close();
});

test('Docker default setup: progress is visible in the node properties', async ({ page }) => {
  test.skip(!process.env.CROWDB_NODE_UI_ORIGIN, 'Owned default node fixture required');
  await step('default progress: initial page', () => page.goto(`${process.env.CROWDB_NODE_UI_ORIGIN}/?domain=Cluster`));
  await step('default progress: open node properties', () => page.getByTestId('tree-node-N-1').getByRole('button', { name: 'N-1', exact: true }).click());
  const setup = page.getByRole('complementary', { name: 'Entity inspector' }).getByRole('region', { name: 'Node 1 setup', exact: true });
  await step('default progress: observe completed setup', () => expect(setup.getByRole('status', { exact: true })).toHaveText('Completed'));
  await expect(setup).toContainText('Managed file');
  await expect(setup).toContainText('crowdb-access-server · ready');
  // A fresh node console must resolve DiskDB from its persisted binding,
  // without a legacy local-process cache populated by another page.
  const usage = await page.request.get(`${process.env.CROWDB_NODE_UI_ORIGIN}/api/diskdb/usage?dg=1`);
  expect(usage.ok(), await usage.text()).toBe(true);
  const groups = (await usage.json()).disk_groups;
  expect(groups.map((group: { disk_group_id: number }) => group.disk_group_id)).toContain(1);
  const disk = groups.find((group: { disk_group_id: number }) => group.disk_group_id === 1).disks[0];
  expect(disk.zone_count).toBe(64);
  expect(disk.zone_size_units * disk.unit_size_bytes).toBe(256 * 1024 * 1024);
  const bitmap = await page.request.get(`${process.env.CROWDB_NODE_UI_ORIGIN}/api/diskdb/usage?dg=1&disk=${disk.disk_id}&zone=0`);
  expect(bitmap.ok(), await bitmap.text()).toBe(true);
  expect((await bitmap.json()).disk_groups[0].disks[0].zone_usages[0].usage_bitmap).toMatch(/^[0-9a-f]+$/);

  await page.getByTestId('tree-node-STORAGE-1').getByRole('button', { name: 'Storage', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Node 1 storage', exact: true })).toContainText('Preallocated');
});

// Docker supplies the real node roots, SSH endpoints and candidate identities.
// Baseline: 7.5s (2026-10-10; preparation phase).
test('Docker candidates: prepare nodes in UI and initialize their shared cluster', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_ORIGIN, 'Run through the Docker node acceptance harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const identities: string[] = JSON.parse(process.env.CROWDB_NODE_UI_IDENTITIES!);
  await page.goto(`${origin}/?domain=Cluster`);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  expect((await (await request.get(`${origin}/api/node/status`)).json()).phase).toBe('unbound_draft');
  await expect(page.getByTestId('tree-node-NODE-CANDIDATES')).toBeVisible();
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Add Rack', exact: true }).click();
  const rack = page.getByRole('dialog', { name: 'Add Rack' });
  await rack.getByLabel('Rack ID', { exact: true }).fill('1');
  await rack.getByLabel('Name (optional)', { exact: true }).fill('rack-one');
  await rack.getByRole('button', { name: 'Create Rack', exact: true }).click();
  await expect(rack).toHaveCount(0);
  for (const id of identities) {
    const candidate = candidates.getByTestId(`tree-node-candidate-${id}`);
    await candidate.getByRole('button', { name: /^node-/ }).click({ button: 'right' });
    const join = page.getByRole('menuitem', { name: 'Join node…', exact: true });
    await expect.poll(() => join.evaluate(element => {
      const rect = element.getBoundingClientRect();
      return document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2)?.closest('[role="menuitem"]') === element;
    }), { intervals: [100] }).toBe(true);
    await join.click();
    const dialog = page.getByRole('dialog', { name: 'Move candidate to cluster' });
    await expect(dialog.getByLabel('SSH user', { exact: true })).toHaveValue('crowdb');
    await expect(dialog.getByLabel('Initial SSH password', { exact: true })).toHaveValue('crowdb');
    await reviewDisklessCandidate(dialog, request, origin, id);
    // Admission performs authenticated key exchange and proves every peer pair.
    await expect(dialog).toHaveCount(0, { timeout: 10_000 });
    await candidate.getByRole('button', { name: /^node-/ }).click({ button: 'right' });
    await expect(page.getByRole('menuitem', { name: 'Cancel admission', exact: true })).toBeVisible();
    await page.keyboard.press('Escape');
  }
  await page.getByTestId('domain-kv').click();
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Initialize Cluster', exact: true }).click();
  const initialize = page.getByRole('dialog', { name: 'Initialize Cluster' });
  await expect(initialize.getByRole('checkbox')).toHaveCount(identities.length);
  for (const checkbox of await initialize.getByRole('checkbox').all()) {
    await expect(checkbox).toBeChecked();
  }
  if (process.env.CROWDB_NODE_UI_PREPARE_ONLY) return;
  await initialize.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  // Group 0 must elect a leader and publish topology before becoming active.
  await expect.poll(async () => {
    const response = await request.get(`${origin}/api/node/status`);
    return (await response.json()).phase;
  }, { timeout: 10_000, intervals: [100] }).toBe('active');
  await expect(initialize).toHaveCount(0);
  await page.getByTestId('domain-cluster').click();
  await expect(page.getByTestId('tree-node-N-1')).toBeVisible();
  for (const id of identities) {
    await expect(candidates.getByTestId(`tree-node-candidate-${id}`)).toHaveCount(0);
  }
});

// Baseline: 4.7s (2026-10-10).
test('Docker recovery: another UI resumes the fixed bootstrap', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_RECOVERY_ORIGIN, 'Run through the interrupted Docker bootstrap harness');
  const origin = process.env.CROWDB_NODE_UI_RECOVERY_ORIGIN!;
  const before = await (await request.get(`${origin}/api/node/status`)).json();
  expect(before.phase).toBe('bootstrap_in_progress');
  await page.goto(`${origin}/?domain=Cluster`);
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'datacenter', exact: true }).click();
  const initialization = page.getByRole('region', { name: 'Cluster initialization' });
  const edit = await request.post(`${origin}/api/racks`, { data: { id: 99, name: 'sealed-edit' } });
  expect(edit.status()).toBe(409);
  await initialization.getByRole('button', { name: 'Resume bootstrap', exact: true }).click();
  const resume = page.getByRole('dialog', { name: 'Resume fixed bootstrap' });
  await resume.getByRole('button', { name: 'Resume', exact: true }).click();
  await expect.poll(async () => {
    const status = await (await request.get(`${origin}/api/node/status`)).json();
    expect(status.cluster_id).toBe(before.cluster_id);
    return status.phase;
  }, { timeout: 10_000, intervals: [100] }).toBe('active');
  await expect(resume).toHaveCount(0);
  await expect(page.getByTestId('tree-node-N-1')).toBeVisible();
});


// Baseline: 2.4s (2026-10-10).
test('Docker independent cluster: select and explicitly clean the other cluster', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_FOREIGN_ORIGIN, 'Run through the disjoint cluster Docker harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const foreign = process.env.CROWDB_NODE_UI_FOREIGN_ORIGIN!;
  const id = process.env.CROWDB_NODE_UI_FOREIGN_UUID!;
  const other = await (await request.get(`${foreign}/api/node/status`)).json();
  const retained = await (await request.get(`${origin}/api/node/status`)).json();
  expect(other.cluster_id).not.toBe(retained.cluster_id);
  await page.goto(`${origin}/?domain=Cluster`);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  await expect(candidates.getByTestId(`tree-node-candidate-${id}`)).toHaveCount(0);
  await expect(candidates.getByRole('combobox', { name: 'Discovered cluster' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Clean up Group 0', exact: true })).toHaveCount(0);
  // Cleanup remains an explicit backend operation, outside normal candidate navigation.
  const cleanup = await request.post(`${foreign}/api/node/cleanup`, { data: {
    operation_id: other.operation_id, confirm_delete_system_store: true,
  } });
  expect(cleanup.ok(), await cleanup.text()).toBe(true);
  await expect.poll(async () => (await (await request.get(`${foreign}/api/node/status`)).json()).phase,
    { intervals: [100] }).toBe('unbound_draft');
  expect((await (await request.get(`${origin}/api/node/status`)).json()).cluster_id).toBe(retained.cluster_id);
});

// Baseline: 4.9s (2026-10-10).
test('Docker node update: authenticated rack and endpoint changes preserve allocation', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_UPDATE_UUID, 'Run through the persistent IP change Docker harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const id = process.env.CROWDB_NODE_UI_UPDATE_UUID!;
  const before = (await (await request.get(`${origin}/api/node/admissions`)).json()).find((node: { discovery_id: string }) => node.discovery_id === id);
  await page.goto(`${origin}/?domain=Cluster`);
  await expect(page.getByTestId(`tree-node-candidate-${id}`)).toHaveCount(0);
  await page.getByTestId(`tree-node-N-${before.node_id}`).getByRole('button', { name: `N-${before.node_id}`, exact: true }).click();
  await page.getByRole('complementary', { name: 'Entity inspector' }).getByRole('button', { name: 'Edit node…', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Update cluster node' });
  await expect(dialog.getByLabel('Initial SSH password', { exact: true })).toHaveValue('');
  await dialog.getByLabel('Rack', { exact: true }).selectOption('2');
  await dialog.getByRole('button', { name: 'Verify and update', exact: true }).click();
  await expect(dialog).toHaveCount(0, { timeout: 10_000 });
  const after = (await (await request.get(`${origin}/api/node/admissions`)).json()).find((node: { discovery_id: string }) => node.discovery_id === id);
  expect(after.node_id).toBe(before.node_id);
  expect(after.physical_host_id).toBe(before.physical_host_id);
  expect(after.rack_id).toBe(2);
  // Independent monitor recovery may publish the new address before the dialog.
  expect(after.host).toBe(process.env.CROWDB_NODE_UI_NEW_HOST);
  expect(after.host).not.toBe(process.env.CROWDB_NODE_UI_OLD_HOST);
});
