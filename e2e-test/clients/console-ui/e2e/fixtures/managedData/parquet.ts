// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';
import type { APIRequestContext, Page } from '@playwright/test';
import { parseIcebergJson } from '../../../src/iceberg/json';
import { expect } from '../realBackend';
import { resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';
import { maintenanceWorkflow } from './maintenance';

export async function parquetWorkflow(page: Page, request: APIRequestContext, namespace: string) {
  const result = await step('Iceberg: generate local Parquet upload inputs', () => promisify(execFile)('pixi', ['run', '-e', 'iceberg-e2e', 'python', resolve('e2e/fixtures/parquetFiles.py')]));
  if (result.stderr) console.log(result.stderr);
  const inputs = JSON.parse(result.stdout) as Record<string, string>;
  const file = (name: string, key: string) => ({ name, mimeType: 'application/octet-stream', buffer: Buffer.from(inputs[key], 'base64') });
  const tree = page.getByRole('navigation', { name: 'Iceberg tree', exact: true });
  const path = `/api/access/iceberg/v1/namespaces/${encodeURIComponent(namespace)}/tables/imported`;
  await step('Iceberg: create one Table and Snapshot from two Parquet files', async () => {
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Create table…');
    await page.getByLabel('Table name', { exact: true }).fill('imported');
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles(file('a.parquet', 'a'));
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('row')).toHaveCount(2);
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('cell', { name: 'Uploaded', exact: true })).toHaveCount(1);
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles(file('b.parquet', 'b'));
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('row')).toHaveCount(3);
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('cell', { name: 'Uploaded', exact: true })).toHaveCount(2);
    await page.getByRole('button', { name: 'Remove b.parquet', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('row')).toHaveCount(2);
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles(file('b.parquet', 'b'));
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('row')).toHaveCount(3);
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('cell', { name: 'Uploaded', exact: true })).toHaveCount(2);
    await page.getByRole('button', { name: 'Save Draft', exact: true }).click();
    await expect(page.getByText('Draft saved in this browser.', { exact: true })).toBeVisible();
    expect((await request.get(path)).status()).toBe(404);
    await page.reload();
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Create table…');
    await expect(page.getByLabel('Table name', { exact: true })).toHaveValue('imported');
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true })).not.toContainText('Reselect');
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('cell', { name: 'Uploaded', exact: true })).toHaveCount(2);
    await page.getByRole('button', { name: 'Commit', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Create Table', exact: true })).toHaveCount(0);
    await expect(page.getByRole('table', { name: 'Table snapshots', exact: true }).getByRole('row')).toHaveCount(2);
    await inspect(request, path, 1, 2, '4');
    await page.getByRole('button', { name: 'Inspect current snapshot', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Manifest list records', exact: true })).toBeVisible();
  });
  await step('Iceberg: reject an incompatible file before upload or commit', async () => {
    await resourceAction(page, tree.getByRole('button', { name: 'imported', exact: true }), 'Edit as Draft…');
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles(file('bad.parquet', 'bad'));
    await expect(page.getByRole('region', { name: 'Edit Table Draft', exact: true }).getByRole('alert')).toContainText('does not match');
    await page.getByRole('button', { name: 'Close editor', exact: true }).click();
    await inspect(request, path, 1, 2, '4');
  });
  await step('Iceberg: append a batch and preserve the previous data Snapshot', async () => {
    await resourceAction(page, tree.getByRole('button', { name: 'imported', exact: true }), 'Edit as Draft…');
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles(file('more.parquet', 'a'));
    await expect(page.getByRole('table', { name: 'Parquet editor table', exact: true }).getByRole('row')).toHaveCount(2);
    await page.getByRole('button', { name: 'Commit', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Edit Table Draft', exact: true })).toHaveCount(0);
    await inspect(request, path, 2, 3, '6');
    await page.reload();
    await tree.getByRole('button', { name: namespace, exact: true }).click();
    await tree.getByRole('button', { name: 'imported', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Table snapshots', exact: true }).getByRole('row')).toHaveCount(3);
  });
  await step('Iceberg: edit a historical Snapshot and atomically publish its changed file set', async () => {
    const before = parseIcebergJson(await (await request.get(path)).text()) as any;
    const first = String(before.metadata.snapshots[0]['snapshot-id']);
    const priorMain = String(before.metadata['current-snapshot-id']);
    const snapshots = page.getByRole('table', { name: 'Table snapshots', exact: true });
    await resourceAction(page, snapshots.getByRole('button', { name: first, exact: true }), 'Edit as Draft…');
    const grid = page.getByRole('table', { name: 'Parquet editor table', exact: true });
    await expect(grid.getByRole('row')).toHaveCount(3);
    const remove = grid.getByRole('button', { name: /^Remove / });
    const names = await remove.allTextContents(); expect(names).toHaveLength(2);
    // Use the first API-confirmed file identity, not an ambiguous page locator.
    const query = new URLSearchParams({ metadata: before['metadata-location'], snapshot: first });
    const list = await (await request.get(`${path}/inspect?${query}`)).json();
    const manifest = list.rows[0];
    const entries = await (await request.get(`${path}/inspect?${query}&manifest=${encodeURIComponent(manifest.location)}`)).json();
    const removedName = entries.rows[0].location.split('/').pop();
    await grid.getByRole('button', { name: `Remove ${removedName}`, exact: true }).click();
    await expect(grid.getByRole('cell', { name: 'Removed', exact: true })).toHaveCount(1);
    await page.getByLabel('Parquet files', { exact: true }).setInputFiles([file('historical-a.parquet', 'a'), file('historical-b.parquet', 'b')]);
    await expect(grid.getByRole('cell', { name: 'Uploaded', exact: true })).toHaveCount(2);
    await page.getByRole('button', { name: 'Commit', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Edit Table Draft', exact: true })).toHaveCount(0);
    const after = parseIcebergJson(await (await request.get(path)).text()) as any;
    const current = after.metadata.snapshots.find((value: any) => String(value['snapshot-id']) === String(after.metadata['current-snapshot-id']));
    expect(String(current['parent-snapshot-id'])).toBe(priorMain);
    expect(current.summary['crowdb.source-snapshot-id']).toBe(first);
    await inspect(request, path, 3, 3, '6');
  });
  await maintenanceWorkflow(page, request, path);
  await step('Iceberg: drop the maintained Table', async () => {
    await resourceAction(page, tree.getByRole('button', { name: 'imported', exact: true }), 'Drop table…');
    await page.getByRole('dialog', { name: 'Drop table', exact: true }).getByRole('button', { name: 'Drop table', exact: true }).click();
    await expect(page.getByRole('dialog', { name: 'Drop table', exact: true })).toHaveCount(0);
    expect((await request.get(path)).status()).toBe(404);
  });
}

async function inspect(request: APIRequestContext, path: string, snapshots: number, files: number, records: string) {
  const response = await request.get(path); expect(response.ok(), await response.text()).toBe(true);
  const table = parseIcebergJson(await response.text()) as any;
  expect(table.metadata.snapshots).toHaveLength(snapshots);
  const snapshot = table.metadata.snapshots.find((value: any) => String(value['snapshot-id']) === String(table.metadata['current-snapshot-id']));
  const query = new URLSearchParams({ metadata: table['metadata-location'], snapshot: String(snapshot['snapshot-id']) });
  const list = await request.get(`${path}/inspect?${query}`); expect(list.ok(), await list.text()).toBe(true);
  const manifests = (await list.json()).rows; let count = 0; let rows = 0n;
  for (const manifest of manifests) {
    const entries = await request.get(`${path}/inspect?${query}&manifest=${encodeURIComponent(manifest.location)}`);
    expect(entries.ok(), await entries.text()).toBe(true);
    for (const file of (await entries.json()).rows) {
      if (file.status === 'Deleted') continue;
      count++; rows += BigInt(file.records);
      const parquet = await request.get(`${path}/inspect?${query}&manifest=${encodeURIComponent(manifest.location)}&file=${encodeURIComponent(file.location)}`);
      expect(parquet.ok(), await parquet.text()).toBe(true);
      expect((await parquet.json()).kind).toBe('parquet');
    }
  }
  expect(count).toBe(files); expect(rows.toString()).toBe(records);
}
