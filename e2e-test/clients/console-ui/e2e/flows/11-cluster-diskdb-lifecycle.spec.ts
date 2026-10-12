// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Page, APIRequestContext } from '@playwright/test';
import { test, expect } from '../fixtures/realBackend';
import { diskMenu } from '../fixtures/capacity';
import { nativeProcessAlive } from '../fixtures/nativeProcess';
import { liveService, service, serviceRow, stoppedService, unchangedService, type LiveService } from '../fixtures/serviceLifecycle';

// Baseline: 15.9s (2026-10-12).
test('full deploy flow: deploy diskdb via UI, restart, stop, delete via context menu', async ({ page, request }) => {
  test.setTimeout(90_000);
  const kv = await liveService(request, 'paxos-kv');
  const ddb = await deployDiskDb(page, request, kv);
  const restarted = await diskDbLifecycle(page, request, kv, ddb);
  const resumedKv = await kvLifecycle(page, request, kv, restarted);
  await deleteDiskDb(page, request, resumedKv, restarted);
});

async function deployDiskDb(page: Page, request: APIRequestContext, kv: LiveService) {
  expect(await service(request, 'diskdb')).toBeUndefined();
  await page.goto('/?domain=Capacity');
  const node = page.getByRole('complementary', { name: 'Cluster tree sidebar' })
    .getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(sidebar.getByRole('button', { name: 'Add Rack', exact: true })).toHaveCount(0);
  await node.click({ button: 'right' });
  for (const label of ['Add Disk Group', 'Deploy DiskDB']) {
    await expect(page.getByRole('menuitem', { name: label, exact: true })).toBeVisible();
  }
  for (const label of ['Add Node', 'Delete Node', 'Restart DiskDB', 'Stop DiskDB']) {
    await expect(page.getByRole('menuitem', { name: label, exact: true })).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  const rack = sidebar.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]');
  await rack.click({ button: 'right' });
  for (const label of ['Add Node', 'Delete Rack']) {
    await expect(page.getByRole('menuitem', { name: label, exact: true })).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  await diskMenu(page, node, 'Deploy DiskDB');
  const dialog = page.getByRole('dialog', { name: 'Deploy DiskDB', exact: true });
  await expect(dialog).toBeVisible();
  for (const label of ['REST Port', 'Binary Path', 'Listen Address', 'HTTP Address', 'Config Path']) {
    await expect(dialog.getByLabel(label, { exact: true })).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await diskMenu(page, node, 'Deploy DiskDB');
  await expect(dialog.getByRole('button', { name: 'Deploy', exact: true })).toBeEnabled();
  await dialog.getByLabel('RPC Port (crowdb-rpc)').fill('7100');
  await dialog.getByRole('button', { name: 'Deploy', exact: true }).click();
  await expect(dialog).toHaveCount(0, { timeout: 5_000 });
  const ddb = await liveService(request, 'diskdb');
  await unchangedService(request, kv);
  await page.goto('/?domain=Cluster');
  await expect(page.locator('header').getByTitle('Cluster health: Healthy', { exact: true })).toBeVisible();
  const row = serviceRow(page, ddb);
  await row.click({ button: 'right' });
  for (const label of ['Restart DiskDB', 'Stop DiskDB', 'Delete DiskDB']) {
    await expect(page.getByRole('menuitem', { name: label, exact: true })).toBeVisible();
  }
  await expect(page.getByRole('menuitem', { name: 'Deploy DiskDB', exact: true })).toHaveCount(0);
  await page.keyboard.press('Escape');
  return ddb;
}

async function act(page: Page, row: LiveService, label: string, path: string) {
  const response = page.waitForResponse(response => new URL(response.url()).pathname === path
    && response.request().method() === 'POST');
  await diskMenu(page, serviceRow(page, row), label);
  const completed = await response;
  expect(completed.ok(), await completed.text()).toBe(true);
}

async function diskDbLifecycle(page: Page, request: APIRequestContext, kv: LiveService, original: LiveService) {
  await act(page, original, 'Restart DiskDB', '/api/nodes/1/diskdb/restart');
  const restarted = await liveService(request, 'diskdb', original.pid);
  expect(await nativeProcessAlive(0, original.pid)).toBe(false);
  await unchangedService(request, kv);
  await act(page, restarted, 'Stop DiskDB', '/api/nodes/1/diskdb/stop');
  await stoppedService(request, restarted);
  await unchangedService(request, kv);
  await expect(serviceRow(page, restarted).getByTitle('Healthy', { exact: true })).toHaveCount(0, { timeout: 10_000 });
  await expect(serviceRow(page, kv).getByTitle('Healthy', { exact: true })).toBeVisible({ timeout: 10_000 });
  await act(page, restarted, 'Restart DiskDB', '/api/nodes/1/diskdb/restart');
  const resumed = await liveService(request, 'diskdb', restarted.pid);
  await unchangedService(request, kv);
  return resumed;
}

async function kvLifecycle(page: Page, request: APIRequestContext, original: LiveService, ddb: LiveService) {
  await act(page, original, 'Stop CrowDB Storage', '/api/nodes/1/server/stop');
  await stoppedService(request, original);
  await unchangedService(request, ddb);
  await expect(serviceRow(page, original).getByTitle('Healthy', { exact: true })).toHaveCount(0, { timeout: 10_000 });
  await expect(serviceRow(page, ddb).getByTitle('Healthy', { exact: true })).toBeVisible({ timeout: 10_000 });
  await act(page, original, 'Restart CrowDB Storage', '/api/nodes/1/server/restart');
  const resumed = await liveService(request, 'paxos-kv', original.pid);
  await unchangedService(request, ddb);
  await expect(serviceRow(page, resumed).getByTitle('Healthy', { exact: true })).toBeVisible({ timeout: 20_000 });
  return resumed;
}

async function deleteDiskDb(page: Page, request: APIRequestContext, kv: LiveService, original: LiveService) {
  await diskMenu(page, serviceRow(page, original), 'Delete DiskDB');
  const dialog = page.getByRole('dialog', { name: 'Delete DiskDB', exact: true });
  await expect(dialog).toBeVisible();
  await dialog.getByRole('button', { name: 'Delete DiskDB', exact: true }).evaluate(element => (element as HTMLElement).click());
  await expect(serviceRow(page, original)).toHaveCount(0, { timeout: 10_000 });
  await expect.poll(() => service(request, 'diskdb'), { timeout: 10_000, intervals: [100] }).toBeUndefined();
  await expect(serviceRow(page, kv)).toBeVisible();
  await unchangedService(request, kv);
  await expect.poll(() => nativeProcessAlive(0, original.pid), { intervals: [100] }).toBe(false);
}
