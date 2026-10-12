// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Capacity retains hardware and marks interrupted DDB usage unknown', async ({ page, request }) => {
  await page.goto('/?domain=Capacity');
  const totals = page.getByTestId('capacity-summary');
  const panel = totals.locator('..');
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  expect((await request.post('/api/nodes/1/diskdb/stop', { data: {} })).ok()).toBe(true);
  try {
    await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(page.getByRole('alert').filter({ hasText: 'Capacity observation unavailable' })).toBeVisible();
    await panel.getByRole('button', { name: /R-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await panel.getByRole('button', { name: /N-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await panel.getByRole('button', { name: /DG-1 / }).click();
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    const disk = panel.getByRole('button', { name: /00000000…/ });
    await expect(disk).toHaveAttribute('title', /Usage unknown/);
    await disk.click();
    await expect(page.getByTestId('disk-geometry')).toContainText(/80 zones.*Usage unknown/);
    await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(2);
    await expect(totals).not.toContainText('100% free');
  } finally {
    expect((await request.post('/api/nodes/1/diskdb/restart', { data: {} })).ok()).toBe(true);
  }
  await panel.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(totals.getByText('Unknown', { exact: true })).toHaveCount(0);
  await expect(page.getByTestId('disk-geometry')).not.toContainText('Usage unknown');
  await expect(page.getByRole('heading', { name: /^Capacity — Disk/ })).toBeVisible();
});