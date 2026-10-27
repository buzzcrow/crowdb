// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { expect } from '../realBackend';
import { resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';
import { parquetWorkflow } from './parquet';

export async function icebergWorkflow(page: Page, request: APIRequestContext) {
  const namespace = `console_crud_${Date.now()}.literal`;
  const path = `/api/access/iceberg/v1/namespaces/${namespace}`;
  const tree = page.getByRole('navigation', { name: 'Iceberg tree', exact: true });
  await step('Iceberg: create namespace and table through the tree', async () => {
    await page.getByTestId('domain-iceberg').click();
    await expect(page.getByLabel('Catalog write token')).toHaveCount(0);
    await resourceAction(page, tree.getByRole('button', { name: 'Catalog', exact: true }), 'Create namespace…');
    await page.getByLabel('Namespace name', { exact: true }).fill(namespace);
    await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
    await page.getByRole('table', { name: 'Namespaces', exact: true }).getByRole('button', { name: namespace, exact: true }).click();
    const namespaceResponse = await request.get(path);
    expect(namespaceResponse.status(), await namespaceResponse.text()).toBe(200);
    expect((await namespaceResponse.json()).namespace).toEqual([namespace]);
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Create table…');
    await page.getByLabel('Table name', { exact: true }).fill('example');
    await page.getByRole('button', { name: 'Create table', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Refresh table', exact: true })).toBeVisible();
    await page.getByRole('button', { name: 'Schema', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Table schema', exact: true })).toContainText('long');
  });
  await namespaceTree(page, request, namespace);
  await step('Iceberg: a nonempty namespace cannot be deleted', async () => {
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Drop namespace…');
    const dialog = page.getByRole('dialog', { name: 'Drop namespace', exact: true });
    const [response] = await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname === path && response.request().method() === 'DELETE'),
      dialog.getByRole('button', { name: 'Drop namespace', exact: true }).click(),
    ]);
    expect(response.status(), await response.text()).toBe(409);
    await expect(dialog.getByRole('alert')).toContainText('Namespace has children');
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  });
  await updateAndRename(page, request, namespace, path);
  await parquetWorkflow(page, request, namespace);
  await step('Iceberg: delete table and empty namespace, then reload the list', async () => {
    await resourceAction(page, tree.getByRole('button', { name: 'renamed', exact: true }), 'Drop table…');
    await page.getByRole('dialog', { name: 'Drop table', exact: true }).getByRole('button', { name: 'Drop table', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Namespace resources', exact: true }).getByRole('button', { name: 'renamed', exact: true })).toHaveCount(0);
    expect((await request.get(`${path}/tables/renamed`)).status()).toBe(404);
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Drop namespace…');
    await page.getByRole('dialog', { name: 'Drop namespace', exact: true }).getByRole('button', { name: 'Drop namespace', exact: true }).click();
    await expect(tree.getByRole('button', { name: namespace, exact: true })).toHaveCount(0);
    expect((await request.get(path)).status()).toBe(404);
    await page.reload();
    await expect(page.getByRole('table', { name: 'Namespaces', exact: true })).toBeVisible();
    await expect(page.getByRole('table', { name: 'Namespaces', exact: true }).getByRole('button', { name: namespace, exact: true })).toHaveCount(0);
  });
}

async function namespaceTree(page: Page, request: APIRequestContext, parent: string) {
  await step('Iceberg: literal components and nested namespaces remain distinct', async () => {
    const tree = page.getByRole('navigation', { name: 'Iceberg tree', exact: true });
    for (const [target, name] of [[parent, 'child.literal'], ['child.literal', 'grandchild']]) {
      await resourceAction(page, tree.getByRole('button', { name: target, exact: true }), 'Create namespace…');
      await page.getByLabel('Namespace name', { exact: true }).fill(name);
      await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
      await expect(page.getByRole('dialog', { name: 'Create namespace', exact: true })).toHaveCount(0);
      const full = target === parent ? [parent, name] : [parent, 'child.literal', name];
      const response = await request.get(`/api/access/iceberg/v1/namespaces/${encodeURIComponent(full.join('\x1f'))}`);
      expect(response.ok(), await response.text()).toBe(true); expect((await response.json()).namespace).toEqual(full);
      await page.getByRole('table', { name: 'Namespace resources', exact: true }).getByRole('button', { name: full.join('.'), exact: true }).click();
    }
    const children = await request.get(`/api/access/iceberg/v1/namespaces?parent=${encodeURIComponent(parent)}`);
    expect(children.ok(), await children.text()).toBe(true);
    expect((await children.json()).namespaces).toEqual([[parent, 'child.literal']]);
    for (const name of ['grandchild', 'child.literal']) {
      await resourceAction(page, tree.getByRole('button', { name, exact: true }), 'Drop namespace…');
      await page.getByRole('dialog', { name: 'Drop namespace', exact: true }).getByRole('button', { name: 'Drop namespace', exact: true }).click();
      await expect(tree.getByRole('button', { name, exact: true })).toHaveCount(0);
      await tree.getByRole('button', { name: parent, exact: true }).click();
    }
    await tree.getByRole('button', { name: parent, exact: true }).click();
    await page.getByRole('table', { name: 'Namespace resources', exact: true }).getByRole('button', { name: 'example', exact: true }).click();
  });
}

async function updateAndRename(page: Page, request: APIRequestContext, namespace: string, path: string) {
  await step('Iceberg: metadata-only update preserves the empty data history', async () => {
    const before = await table(request, `${path}/tables/example`);
    expect(before.metadata.snapshots).toEqual([]);
    const tree = page.getByRole('navigation', { name: 'Iceberg tree', exact: true });
    await resourceAction(page, tree.getByRole('button', { name: 'example', exact: true }), 'Commit metadata…');
    const editor = page.getByRole('region', { name: 'Commit metadata', exact: true });
    await expect(editor.getByRole('button', { name: 'Add property', exact: true })).toBeVisible();
    await editor.getByRole('button', { name: 'Add property', exact: true }).click();
    const count = await editor.getByRole('table', { name: 'Table property edits', exact: true }).getByRole('row').count() - 1;
    await editor.getByLabel(`Property ${count} name`, { exact: true }).fill('console_crud');
    await editor.getByLabel(`Property ${count} value`, { exact: true }).fill('verified');
    await editor.getByRole('button', { name: 'Submit metadata', exact: true }).click();
    await expect(editor).toHaveCount(0);
    await expandProperties(page);
    await expect(page.getByLabel('Iceberg properties', { exact: true })).toContainText('verified');
    const after = await table(request, `${path}/tables/example`);
    expect(after['metadata-location']).not.toBe(before['metadata-location']);
    expect(after.metadata['table-uuid']).toBe(before.metadata['table-uuid']);
    expect(after.metadata.snapshots).toEqual([]);
    expect(after.metadata['current-snapshot-id']).toBe(before.metadata['current-snapshot-id']);
    expect(after.metadata.properties.console_crud).toBe('verified');
    await resourceAction(page, tree.getByRole('button', { name: 'example', exact: true }), 'Rename table…');
    await page.getByLabel('New table name', { exact: true }).fill('renamed');
    await page.getByRole('dialog', { name: 'Rename table', exact: true }).getByRole('button', { name: 'Rename table', exact: true }).click();
    const resources = page.getByRole('table', { name: 'Namespace resources', exact: true });
    await expect(resources.getByRole('button', { name: 'example', exact: true })).toHaveCount(0);
    await resources.getByRole('button', { name: 'renamed', exact: true }).click();
    expect((await request.get(`${path}/tables/example`)).status()).toBe(404);
    expect((await table(request, `${path}/tables/renamed`)).metadata['table-uuid']).toBe(before.metadata['table-uuid']);
    await page.reload();
    await tree.getByRole('button', { name: namespace, exact: true }).click();
    await tree.getByRole('button', { name: 'renamed', exact: true }).click();
    await expandProperties(page);
    await expect(page.getByLabel('Iceberg properties', { exact: true })).toContainText('verified');
    await page.getByRole('button', { name: 'Schema', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Table schema', exact: true })).toContainText('long');
  });
}

async function expandProperties(page: Page) {
  await page.getByLabel('Iceberg properties', { exact: true }).locator('dt').filter({ hasText: /^properties$/ }).locator('..').locator('summary').click();
}

async function table(request: APIRequestContext, path: string) {
  const response = await request.get(path);
  expect(response.status(), await response.text()).toBe(200);
  return response.json();
}
