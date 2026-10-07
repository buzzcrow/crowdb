// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';
import { parseIcebergJson } from '../../src/iceberg/json';
import type { TableLoad } from '../../src/iceberg/types';

test('native diagnostics: Iceberg empty Catalog omits the first-success guide', async ({ page }) => {
  await page.goto('/?domain=Iceberg');
  const guide = page.getByRole('region', { name: 'First success guide', exact: true });
  await expect(guide).toHaveCount(0);
});

// Baseline: 12.3s (2026-10-04), including 101 real files and owned teardown.
test('native diagnostics: Iceberg committed references and Parquet footer', async ({ page, request, baseURL }) => {
  const namespace = `console_files_${process.pid}_${Date.now()}`;
  const tablePath = `/api/access/iceberg/v1/namespaces/${namespace}/tables/events`;
  try {
    await step('native Iceberg commit two snapshots', async () => {
      const result = await promisify(execFile)('pixi', ['run', '-e', 'iceberg-e2e', 'python', resolve('e2e/fixtures/icebergInspection.py'), `${baseURL}/api/access/iceberg`, namespace], { env: { ...process.env, PYICEBERG_MAX_WORKERS: '4' } });
      console.log(result.stdout); if (result.stderr) console.log(result.stderr);
    });
    const response = await request.get(tablePath);
    expect(response.ok(), await response.text()).toBeTruthy();
    const table = parseIcebergJson(await response.text()) as TableLoad;
    const snapshots = table.metadata.snapshots ?? [];
    expect(snapshots).toHaveLength(2);
    const snapshot = snapshots.find(s => String(s['snapshot-id']) === String(table.metadata['current-snapshot-id']))!;
    const query = new URLSearchParams({ metadata: table['metadata-location'], snapshot: String(snapshot['snapshot-id']) });
    const manifestsResponse = await request.get(`${tablePath}/inspect?${query}`);
    expect(manifestsResponse.ok(), await manifestsResponse.text()).toBeTruthy();
    const manifests = await manifestsResponse.json();
    expect(manifests.rows.length).toBeGreaterThanOrEqual(2);
    const manifest = manifests.rows[0];
    query.set('manifest', manifest.location);
    const filesResponse = await request.get(`${tablePath}/inspect?${query}`);
    expect(filesResponse.ok(), await filesResponse.text()).toBeTruthy();
    const files = await filesResponse.json();
    const file = files.rows[0];
    query.set('file', file.location);
    const footerResponse = await request.get(`${tablePath}/inspect?${query}`);
    expect(footerResponse.ok(), await footerResponse.text()).toBeTruthy();
    const footer = await footerResponse.json();
    expect(footer.kind).toBe('parquet'); expect(footer.data_page_bytes).toBe('0');
    expect(footer.row_group_count).toBe('22'); expect(footer.groups).toHaveLength(20);
    const invalid = new URLSearchParams(query); invalid.set('offset', '1');
    expect((await request.get(`${tablePath}/inspect?${invalid}`)).status()).toBe(400);
    invalid.set('offset', 'c.unrelated.cursor');
    expect((await request.get(`${tablePath}/inspect?${invalid}`)).status()).toBe(400);
    await page.goto('/?domain=Iceberg');
    const tree = page.getByRole('navigation', { name: 'Iceberg tree' });
    await tree.getByRole('button', { name: namespace, exact: true }).click();
    await tree.getByRole('button', { name: 'events', exact: true }).click();
    await page.getByRole('button', { name: 'Inspect current snapshot', exact: true }).click();
    const manifestName = manifest.location.split('/').pop();
    await page.getByRole('table', { name: 'Manifest list records' }).getByRole('button', { name: manifestName, exact: true }).click();
    await page.getByRole('table', { name: 'Manifest file entries' }).getByRole('button', { name: file.location.split('/').pop(), exact: true }).click();
    const inspection = page.getByLabel('Iceberg file inspection');
    await expect(inspection).toContainText('Data pages read: 0 B');
    const span = inspection.getByRole('button', { name: 'Byte span row group 0 column id', exact: true });
    await expect(span).not.toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
    await expect(inspection.getByRole('button', { name: 'Footer byte span', exact: true })).not.toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
    expect((await span.boundingBox())!.width).toBeGreaterThan(0);
    await inspection.getByRole('button', { name: 'Row group 0 column id', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Column chunk details' })).toContainText(footer.groups[0].columns[0].compressed);
    await inspection.getByRole('button', { name: 'Next row groups', exact: true }).click();
    await expect(inspection.getByRole('heading', { name: /^Row group 20 ·/ })).toBeVisible();
    await expect(inspection.getByRole('heading', { name: /^Row group 0 ·/ })).toHaveCount(0);
    await inspection.getByRole('button', { name: 'Previous row groups', exact: true }).click();
    await expect(inspection.getByRole('heading', { name: /^Row group 0 ·/ })).toBeVisible();
    await inspection.getByRole('button', { name: 'Footer', exact: true }).click();
    await expect(inspection).toContainText(footer.footer.writer);
    await page.getByTestId('domain-s3').click();
    await page.goBack();
    await expect(inspection.getByRole('button', { name: 'Footer', exact: true })).toHaveAttribute('aria-pressed', 'true');
    await inspection.getByRole('button', { name: 'Row group layout', exact: true }).click();
    await expect(page.getByRole('region', { name: 'Column chunk details' })).toContainText(footer.groups[0].columns[0].compressed);
    await page.screenshot({ path: '/tmp/crowdb-iceberg-inspector.png', fullPage: true });
    const changed = await request.post(tablePath, { data: { requirements: [], updates: [{ action: 'set-properties', updates: { inspection: 'new-generation' } }] } });
    expect(changed.ok(), await changed.text()).toBeTruthy();
    const stale = await request.get(`${tablePath}/inspect?${query}`); expect(stale.status()).toBe(409);
    await page.getByText('Parquet actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Refresh parquet', exact: true }).click();
    await expect(inspection.getByRole('alert')).toContainText('Table metadata changed');
    await expect(inspection.getByRole('button', { name: 'Next row groups', exact: true })).toHaveCount(0);
    await tree.getByRole('button', { name: 'many_files', exact: true }).click();
    await page.getByRole('button', { name: 'Inspect current snapshot', exact: true }).click();
    const manifestTable = page.getByRole('table', { name: 'Manifest list records', exact: true });
    await manifestTable.getByRole('button', { name: /^Expand manifest / }).click();
    const fileTable = page.getByRole('table', { name: 'Manifest files', exact: true });
    await expect(fileTable.getByRole('row')).toHaveCount(101);
    const firstFiles = await fileTable.getByRole('button').allTextContents();
    await page.getByRole('button', { name: 'Next files', exact: true }).click();
    await expect(fileTable.getByRole('row')).toHaveCount(2);
    expect(firstFiles).not.toContain(await fileTable.getByRole('button').textContent());
    await page.getByRole('button', { name: 'Previous files', exact: true }).click();
    await expect(fileTable.getByRole('row')).toHaveCount(101);
    expect(await fileTable.getByRole('button').allTextContents()).toEqual(firstFiles);
  } finally {
    const removed = await Promise.all([request.delete(tablePath), request.delete(tablePath.replace(/events$/, 'many_files'))]);
    for (const response of removed) expect([204, 404]).toContain(response.status());
    const namespaceRemoved = await request.delete(`/api/access/iceberg/v1/namespaces/${namespace}`); expect([204, 404]).toContain(namespaceRemoved.status());
  }
});

// Baseline: 4.7s (2026-10-04); native metadata, no intercepted responses.
test('native diagnostics: Iceberg actual catalog tables and nested schema preserve scope', async ({ page, request }) => {
  const namespace = `console_schema_${process.pid}_${Date.now()}`;
  const path = `/api/access/iceberg/v1/namespaces/${namespace}`;
  const names = ['region', 'nation', 'supplier', 'customer', 'part', 'partsupp', 'orders', 'lineitem'];
  const created: string[] = [];
  const response = await request.post('/api/access/iceberg/v1/namespaces', { data: { namespace: [namespace] } });
  expect(response.ok(), await response.text()).toBeTruthy();
  try {
    await step('native Iceberg table setup', async () => {
      for (const name of names) {
        const response = await step(`native Iceberg create ${name}`, () => request.post(`${path}/tables`, { data: {
          name,
          schema: { type: 'struct', 'schema-id': 0, fields: [
            { id: 1, name: 'id', required: true, type: 'long' },
            { id: 2, name: 'details', required: false, type: { type: 'struct', fields: [
              { id: 3, name: 'tags', required: false, type: { type: 'list', 'element-id': 4, 'element-required': false, element: 'string' } },
            ] } },
          ] },
        } }));
        expect(response.ok(), await response.text()).toBeTruthy();
        created.push(name);
      }
      const response = await request.get(`${path}/tables`);
      expect(response.ok(), await response.text()).toBeTruthy();
      const catalog = await response.json();
      expect(catalog.identifiers.map((entry: { name: string }) => entry.name).sort()).toEqual([...names].sort());
    });
    await step('native Iceberg tree and schema', async () => {
      await page.goto('/?domain=Iceberg');
      const tree = page.getByRole('navigation', { name: 'Iceberg tree', exact: true });
      await tree.getByRole('button', { name: namespace, exact: true }).click();
      for (const name of names) await expect(tree.getByRole('button', { name, exact: true })).toBeVisible();
      await tree.getByRole('button', { name: 'lineitem', exact: true }).click();
      await expect(page.getByRole('heading', { level: 1 })).toHaveText('lineitem');
      await expect(tree.getByRole('button', { name: 'Metadata', exact: true })).toHaveCount(0);
      await page.getByRole('button', { name: 'Schema', exact: true }).click();
      const schema = page.getByRole('table', { name: 'Table schema', exact: true });
      await expect(schema.getByRole('row')).toHaveCount(5);
      await expect(schema).toContainText('element');
      await schema.getByRole('button', { name: 'Collapse field details', exact: true }).click();
      await expect(schema.getByRole('row')).toHaveCount(3);
      await schema.getByRole('button', { name: 'Expand field details', exact: true }).click();
      await expect(schema.getByRole('row')).toHaveCount(5);
      const actions = page.getByLabel('Table actions', { exact: true });
      await expect(actions).not.toHaveAttribute('open', '');
      await actions.getByText('Table actions', { exact: true }).click();
      await expect(actions).toContainText(`Target: ${namespace} / lineitem`);
      await expect(page.getByText('Catalog actions', { exact: true })).toHaveCount(0);
      await page.getByRole('separator', { name: 'Sidebar width' }).press('ArrowRight');
      await expect(page.getByRole('separator', { name: 'Sidebar width' })).toHaveAttribute('aria-valuenow', '300');
    });
    await step('native Iceberg domain return', async () => {
      await page.getByTestId('domain-chunk').click();
      await page.getByRole('button', { name: 'Back', exact: true }).click();
      await expect(page.getByRole('heading', { level: 1 })).toHaveText('lineitem');
      await expect(page.getByRole('button', { name: 'Schema', exact: true })).toHaveAttribute('aria-pressed', 'true');
      await expect(page.getByRole('table', { name: 'Table schema', exact: true }).getByRole('row')).toHaveCount(5);
      await page.goForward();
      await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
      await page.goBack();
      await expect(page.getByRole('table', { name: 'Table schema', exact: true }).getByRole('row')).toHaveCount(5);
    });
  } finally {
    await step('native Iceberg owned metadata teardown', async () => {
      for (const name of created) {
        const response = await step(`native Iceberg delete ${name}`, () => request.delete(`${path}/tables/${name}`));
        expect(response.ok(), await response.text()).toBeTruthy();
      }
      const response = await request.delete(path);
      expect(response.ok(), await response.text()).toBeTruthy();
    });
  }
});

test('native diagnostics: unavailable catalog retries the actual Access services', async ({ page, request }) => {
  const stopped: string[] = [];
  try {
    for (const id of ['access-server-1', 'access-server-2', 'access-server-3']) {
      expect((await request.post(`/api/services/${id}/stop`, { data: {} })).ok()).toBe(true);
      stopped.push(id);
    }
    await page.goto('/?domain=Iceberg');
    await expect(page.getByRole('button', { name: 'Retry catalog', exact: true })).toBeVisible();
    await expect(page.getByLabel('Catalog write token')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Create metadata demo', exact: true })).toHaveCount(0);
  } finally {
    for (const id of stopped) expect((await request.post(`/api/services/${id}/restart`, { data: {} })).ok()).toBe(true);
  }
  await page.getByRole('button', { name: 'Retry catalog', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Retry catalog', exact: true })).toHaveCount(0);
  await page.getByText('Catalog actions', { exact: true }).click();
  await expect(page.getByRole('button', { name: 'Create metadata demo', exact: true })).toBeEnabled();
});
