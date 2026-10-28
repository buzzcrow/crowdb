// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { parseIcebergJson } from '../../../src/iceberg/json';
import { expect, test } from '../realBackend';
import { resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';

async function json(request: APIRequestContext, path: string): Promise<any> {
  const response = await request.get(path); expect(response.ok(), await response.text()).toBe(true);
  return parseIcebergJson(await response.text());
}
async function files(request: APIRequestContext, path: string, table: any, snapshot: string) {
  const query = new URLSearchParams({ metadata: table['metadata-location'], snapshot });
  const list = await json(request, `${path}/inspect?${query}`);
  const manifests = await Promise.all(list.rows.map(async (manifest: any) => ({ manifest,
    entries: (await json(request, `${path}/inspect?${query}&manifest=${encodeURIComponent(manifest.location)}`)).rows })));
  return manifests.flatMap(({ manifest, entries }: any) => entries.filter((file: any) => file.status !== 'Deleted').map((file: any) => ({ manifest, file })));
}
export async function maintenanceWorkflow(page: Page, request: APIRequestContext, path: string) {
  await step('Iceberg: remove a file, preserve history, then expire an unreferenced Snapshot', async () => {
    const before = await json(request, path); const old = String(before.metadata['current-snapshot-id']);
    const original = await files(request, path, before, old); expect(original).toHaveLength(3);
    const chosen = original[0];
    await page.getByRole('button', { name: 'Inspect current snapshot', exact: true }).click();
    await page.getByRole('button', { name: `Expand manifest ${chosen.manifest.location.split('/').pop()}`, exact: true }).click();
    const region = page.getByRole('region', { name: `Files in ${chosen.manifest.location.split('/').pop()}`, exact: true });
    await resourceAction(page, region.getByRole('button', { name: chosen.file.location.split('/').pop(), exact: true }), 'Remove from table…');
    const editor = page.getByRole('region', { name: 'Edit Table Draft', exact: true });
    await expect(editor.getByRole('cell', { name: 'Removed', exact: true })).toHaveCount(1);
    await page.screenshot({ path: test.info().outputPath('iceberg-remove-file.png') });
    await editor.getByRole('button', { name: 'Commit', exact: true }).click();
    await expect(editor).toHaveCount(0);
    const after = await json(request, path); const current = String(after.metadata['current-snapshot-id']);
    expect(current).not.toBe(old); expect(after.metadata.snapshots).toHaveLength(before.metadata.snapshots.length + 1);
    const remaining = await files(request, path, after, current);
    expect(remaining.map((row: any) => row.file.location).sort()).toEqual(original.slice(1).map((row: any) => row.file.location).sort());
    for (const row of remaining) {
      const source = original.find((value: any) => value.file.location === row.file.location)!;
      expect([row.file.data_sequence, row.file.file_sequence]).toEqual([source.file.data_sequence, source.file.file_sequence]);
    }
    expect((await files(request, path, after, old)).map((row: any) => row.file.location)).toEqual(original.map((row: any) => row.file.location));
    const snapshots = page.getByRole('table', { name: 'Table snapshots', exact: true });
    await snapshots.getByRole('button', { name: current, exact: true }).click({ button: 'right' });
    await expect(page.getByRole('menuitem', { name: 'Expire snapshot… Snapshot is current or referenced by main.', exact: true })).toBeDisabled();
    await page.keyboard.press('Escape');
    await resourceAction(page, snapshots.getByRole('button', { name: old, exact: true }), 'Expire snapshot…');
    const expiry = page.getByRole('region', { name: 'Expire snapshot', exact: true });
    await expiry.getByRole('button', { name: 'Expire snapshot', exact: true }).click();
    await expect(expiry).toHaveCount(0);
    const expired = await json(request, path);
    expect(expired.metadata.snapshots).toHaveLength(before.metadata.snapshots.length);
    expect(expired.metadata.snapshots.some((snapshot: any) => String(snapshot['snapshot-id']) === old)).toBe(false);
    expect(String(expired.metadata['current-snapshot-id'])).toBe(current);
    await expect(page.getByRole('table', { name: 'Table snapshots', exact: true }).getByRole('button', { name: old, exact: true })).toHaveCount(0);
  });
}
