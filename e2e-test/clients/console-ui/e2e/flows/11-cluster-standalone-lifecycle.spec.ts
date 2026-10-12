// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { test, expect } from '../fixtures/realBackend';
import { javascriptErrors } from '../fixtures/javascriptErrors';
import { diskMenu } from '../fixtures/capacity';
import { service, serviceRow, type LiveService } from '../fixtures/serviceLifecycle';
import { nativeProcessAlive } from '../fixtures/nativeProcess';

async function live(request: APIRequestContext, previous?: number): Promise<LiveService> {
  let actual: Awaited<ReturnType<typeof service>>;
  await expect.poll(async () => {
    actual = await service(request, 'paxos-kv');
    return actual?.pid && actual.pid !== previous ? actual.pid : 0;
  }, { timeout: 5_000, intervals: [100] }).toBeGreaterThan(1);
  expect(await nativeProcessAlive(0, actual!.pid!)).toBe(true);
  return { ...actual!, pid: actual!.pid! };
}

async function deploy(page: Page, request: APIRequestContext) {
  const node = page.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  await diskMenu(page, node, 'Deploy CrowDB Storage');
  const dialog = page.getByRole('dialog', { name: /Deploy CrowDB Storage on 1/ });
  await expect(dialog.getByRole('button', { name: 'Deploy', exact: true })).toBeEnabled();
  await dialog.getByLabel('REST Port', { exact: true }).fill('10000');
  await dialog.getByLabel('RPC Port', { exact: true }).fill('10100');
  await dialog.getByRole('button', { name: 'Deploy', exact: true }).click();
  await expect(dialog).toHaveCount(0);
  const row = await live(request);
  const response = await request.get('/api/nodes/1/server');
  expect(response.ok(), await response.text()).toBe(true);
  expect(await response.json()).toMatchObject({ node_id: 1, url: 'http://127.0.0.1:10000', rpc_url: 'http://127.0.0.1:10100', pid: row.pid });
  return row;
}

async function mutation(page: Page, row: LiveService, name: string, path: string) {
  const response = page.waitForResponse(response => response.request().method() === 'POST'
    && new URL(response.url()).pathname === path);
  await diskMenu(page, serviceRow(page, row), name);
  const completed = await response;
  expect(completed.ok(), `lifecycle HTTP ${completed.status()}`).toBe(true);
}

async function createEmptyNode(page: Page, request: APIRequestContext) {
  await page.goto('/?domain=Cluster');
  await page.getByRole('button', { name: 'Add Rack', exact: true }).click();
  let dialog = page.getByRole('dialog', { name: 'Add Rack', exact: true });
  await dialog.getByLabel('Rack ID', { exact: true }).fill('1');
  await dialog.getByLabel('Name (optional)', { exact: true }).fill('lifecycle');
  await dialog.getByRole('button', { name: 'Create Rack', exact: true }).click();
  await expect(dialog).toHaveCount(0);
  const rack = page.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]');
  await expect(rack).toContainText('lifecycle');
  await diskMenu(page, rack, 'Add Node');
  dialog = page.getByRole('dialog', { name: 'Add Node', exact: true });
  await dialog.getByLabel('Node ID', { exact: true }).fill('1');
  await dialog.getByLabel('Host', { exact: true }).fill('127.0.0.1');
  await dialog.getByLabel('Configure services on this node', { exact: true }).uncheck();
  await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
  await expect(dialog).toHaveCount(0);
  const response = await request.get('/api/nodes/1');
  expect(response.ok(), await response.text()).toBe(true);
  expect(await response.json()).toMatchObject({ id: 1, host: '127.0.0.1' });
  expect(await service(request, 'paxos-kv')).toBeUndefined();
}

// Baseline: 10.8s (2026-10-12).
test('standalone release KV lifecycle preserves node records and cascades owned processes', async ({ page, request }) => {
  const noErrors = javascriptErrors(page);
  await createEmptyNode(page, request);
  const node = page.getByTestId('tree-node-N-1').locator(':scope > [role="treeitem"]');
  await node.click({ button: 'right' });
  for (const name of ['Deploy CrowDB Storage', 'Delete Node']) await expect(page.getByRole('menuitem', { name, exact: true })).toBeVisible();
  for (const name of ['Restart CrowDB Storage', 'Stop CrowDB Storage']) await expect(page.getByRole('menuitem', { name, exact: true })).toHaveCount(0);
  await page.keyboard.press('Escape');
  const original = await deploy(page, request);
  noErrors();
  await node.click({ button: 'right' });
  for (const name of ['Deploy DiskDB', 'Ping', 'Delete Node']) await expect(page.getByRole('menuitem', { name, exact: true })).toBeVisible();
  for (const name of ['Deploy CrowDB Storage', 'Restart CrowDB Storage', 'Stop CrowDB Storage']) await expect(page.getByRole('menuitem', { name, exact: true })).toHaveCount(0);
  await page.keyboard.press('Escape');
  const ping = page.waitForResponse(response => new URL(response.url()).pathname === '/api/nodes/1/ping');
  await diskMenu(page, node, 'Ping');
  const pinged = await ping;
  expect(pinged.ok(), await pinged.text()).toBe(true);
  expect((await pinged.json()).ok).toBe(true);
  await serviceRow(page, original).click({ button: 'right' });
  for (const name of ['Restart CrowDB Storage', 'Stop CrowDB Storage', 'Delete CrowDB Storage']) await expect(page.getByRole('menuitem', { name, exact: true })).toBeVisible();
  for (const name of ['Deploy CrowDB Storage', 'Delete Node']) await expect(page.getByRole('menuitem', { name, exact: true })).toHaveCount(0);
  await page.keyboard.press('Escape');
  await mutation(page, original, 'Restart CrowDB Storage', '/api/nodes/1/server/restart');
  const restarted = await live(request, original.pid);
  expect(await nativeProcessAlive(0, original.pid)).toBe(false);
  await mutation(page, restarted, 'Stop CrowDB Storage', '/api/nodes/1/server/stop');
  await expect.poll(async () => (await service(request, 'paxos-kv'))?.pid ?? null,
    { timeout: 10_000, intervals: [100] }).toBeNull();
  await expect.poll(() => nativeProcessAlive(0, restarted.pid), { intervals: [100] }).toBe(false);
  await expect(serviceRow(page, restarted).getByTitle('Healthy', { exact: true })).toHaveCount(0, { timeout: 10_000 });
  await mutation(page, restarted, 'Restart CrowDB Storage', '/api/nodes/1/server/restart');
  const resumed = await live(request, restarted.pid);
  await diskMenu(page, serviceRow(page, resumed), 'Delete CrowDB Storage');
  let remove = page.getByRole('dialog', { name: /Delete CrowDB Storage/ });
  await remove.getByRole('button', { name: 'Cancel', exact: true }).click();
  expect((await service(request, 'paxos-kv'))?.pid).toBe(resumed.pid);
  await diskMenu(page, serviceRow(page, resumed), 'Delete CrowDB Storage');
  await remove.getByRole('button', { name: 'Delete CrowDB Storage', exact: true }).evaluate(element => (element as HTMLElement).click());
  await expect(serviceRow(page, resumed)).toHaveCount(0, { timeout: 10_000 });
  expect((await request.get('/api/nodes/1')).status()).toBe(200);
  expect((await request.get('/api/nodes/1/server')).status()).toBe(404);
  await expect.poll(() => nativeProcessAlive(0, resumed.pid), { intervals: [100] }).toBe(false);
  const cascade = await deploy(page, request);
  await diskMenu(page, node, 'Delete Node');
  remove = page.getByRole('dialog', { name: /Delete Node/ });
  await remove.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(node).toBeVisible();
  expect((await service(request, 'paxos-kv'))?.pid).toBe(cascade.pid);
  await diskMenu(page, node, 'Delete Node');
  await remove.getByRole('button', { name: 'Delete Node', exact: true }).evaluate(element => (element as HTMLElement).click());
  await expect(node).toHaveCount(0, { timeout: 10_000 });
  expect((await request.get('/api/nodes/1')).status()).toBe(404);
  expect((await request.get('/api/nodes/1/server')).status()).toBe(404);
  await expect.poll(() => nativeProcessAlive(0, cascade.pid), { intervals: [100] }).toBe(false);
  const rack = page.getByTestId('tree-node-R-1').locator(':scope > [role="treeitem"]');
  await diskMenu(page, rack, 'Delete Rack');
  remove = page.getByRole('dialog', { name: /Delete Rack/ });
  await remove.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(rack).toBeVisible();
  await diskMenu(page, rack, 'Delete Rack');
  await remove.getByRole('button', { name: 'Delete Rack', exact: true }).evaluate(element => (element as HTMLElement).click());
  await expect(rack).toHaveCount(0);
  noErrors();
});
