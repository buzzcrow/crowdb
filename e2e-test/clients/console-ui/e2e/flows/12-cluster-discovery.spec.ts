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
  const local = candidates.getByTestId(`candidate-${snapshot.local.advertisement.discovery_id}`);
  await expect(local).toContainText('Available · This node');
  for (const endpoint of snapshot.local.advertisement.monitor_endpoints) await expect(local).toContainText(endpoint);
  await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
  await expect(candidates.getByRole('combobox', { name: 'Discovered cluster' })).toBeVisible();
  await candidates.getByRole('combobox', { name: 'Discovered cluster' }).selectOption('unbound');
  await expect(local).toBeVisible();
  expect(await containerControl(process.env.CROWDB_E2E_CONTROL, { action: 'stop-discovery-provider' }))
    .toEqual({ signalled: true });
  await expect(candidates.getByRole('status')).toContainText('last observations shown');
  await expect(local).toBeVisible();
});
