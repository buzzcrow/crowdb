// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { containerControl } from '../fixtures/containerControl';

// Baseline: 2.8s (2026-10-12).
test('discovery: release monitor observations remain visible and stale after provider loss', async ({ page, request }) => {
  const response = await request.get('/api/node/candidates');
  expect(response.ok(), await response.text()).toBe(true);
  const snapshot = await response.json();
  await page.goto('/?domain=Cluster');
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  const local = candidates.getByTestId(`tree-node-candidate-${snapshot.local.advertisement.discovery_id}`);
  await local.getByRole('button', { name: /^node-/ }).click();
  const properties = page.getByRole('region', { name: 'Candidate properties' });
  await expect(properties).toContainText('unbound');
  await expect(properties).toContainText(snapshot.local.advertisement.discovery_id);
  await expect(local).not.toContainText('http://');
  await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
  const branch = page.getByTestId('tree-node-NODE-CANDIDATES');
  await expect(branch).toBeVisible();
  await expect(page.getByTestId('tree-node-DC-datacenter').locator(':scope > [role="group"] > [data-testid="tree-node-NODE-CANDIDATES"]')).toBeVisible();
  await expect(local.getByRole('checkbox')).toHaveCount(0);
  await local.getByRole('button', { name: /^node-/ }).focus();
  await page.keyboard.press('Shift+F10');
  await expect(page.getByRole('menuitem', { name: /Auto join/ })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(candidates.getByText(/Announce\(|Respond\(/)).toHaveCount(0);
  await expect(candidates.getByRole('button', { name: /Clean up Group 0|Update node/ })).toHaveCount(0);
  await branch.getByRole('button', { name: 'Candidate nodes', exact: true }).click();
  await expect(local).toHaveCount(0);
  await branch.getByRole('button', { name: 'Candidate nodes', exact: true }).click();
  await expect(local).toBeVisible();
  await expect(candidates.getByRole('status').filter({ hasText: 'last observations shown' })).toHaveCount(0);
  expect(await containerControl(process.env.CROWDB_E2E_CONTROL, { action: 'stop-discovery-provider' }))
    .toEqual({ signalled: true });
  await expect(candidates.getByRole('status').filter({ hasText: 'last observations shown' })).toBeVisible();
  await expect(local).toBeVisible();
});
