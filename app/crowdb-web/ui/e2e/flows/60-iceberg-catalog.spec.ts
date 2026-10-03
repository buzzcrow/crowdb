// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

// Baseline: 0.711s (2026-10-03)
test('Iceberg cluster catalog loads automatically and retains scope across domains', async ({ page }) => {
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', s3: null, configurable: false, iceberg_ready: true } }));
  let loads = 0;
  await page.route('**/api/access/iceberg/**', route => {
    const request = route.request();
    expect(request.headers().authorization).toBeUndefined();
    const path = new URL(request.url()).pathname;
    if (path.endsWith('/tables/events') && ++loads > 1) {
      return route.fulfill({ status: 409, json: { error: { message: 'Stale metadata' } } });
    }
    return route.fulfill({ json: path.endsWith('/config') ? { defaults: {}, overrides: {} }
      : path.endsWith('/namespaces') ? { namespaces: [['demo']] }
      : path.endsWith('/tables') ? { identifiers: [{ namespace: ['demo'], name: 'events' }] }
      : path.endsWith('/tables/events') ? { 'metadata-location': 's3://warehouse/demo/metadata.json', metadata: { 'table-uuid': 'native-uuid', 'format-version': 2, 'current-schema-id': 0, schemas: [{ 'schema-id': 0, fields: [{ id: 1, name: 'id', type: 'long' }] }], properties: {}, snapshots: [{ 'snapshot-id': 7, 'manifest-list': 's3://warehouse/manifest.avro' }] } }
      : { namespace: ['demo'], properties: { owner: 'console' } } });
  });
  await page.goto('/?domain=Iceberg');
  await expect(page.getByLabel('Catalog bearer token')).toHaveCount(0);
  await expect(page.getByLabel('iceberg endpoint')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Load catalog', exact: true })).toHaveCount(0);
  await page.getByRole('navigation', { name: 'Iceberg namespaces' }).getByRole('button', { name: 'demo', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg tables' }).getByRole('button', { name: 'events', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
  await expect(page.locator('main aside:visible')).toHaveCount(1);
  await expect(page.locator('main section').getByRole('button', { name: 'Refresh table' })).toBeVisible();
  await page.getByRole('button', { name: 'Schema', exact: true }).click();
  await expect(page.getByRole('navigation', { name: 'Table sections' }).locator('..')).toContainText('schema-id');
  await expect(page.locator('main section pre')).toHaveCount(0);
  await page.getByRole('button', { name: 'Refresh table', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'Stale metadata' })).toBeVisible({ timeout: 3000 });
  await page.getByTestId('domain-s3').click();
  await page.getByTestId('domain-iceberg').click();
  await expect(page.getByRole('button', { name: 'Schema', exact: true })).toHaveAttribute('aria-pressed', 'true');
});

// Baseline: new metadata-inspection flow (2026-10-03).
test('Iceberg reference pages preserve exact identities and selected footer without a property panel', async ({ page }) => {
  const snapshot = '9223372036854775700';
  const metadata = 's3://warehouse/metadata.json';
  const offsets: string[] = [];
  const column = { path: ['id'], field_id: '1', physical_type: 'INT64', logical_type: null, offset: '4', compressed: '100', uncompressed: '180', data_offset: '20', values: '8', codec: 'SNAPPY', encodings: ['PLAIN', 'RLE'], statistics: { nulls: '0', distinct: null, lower: '1', upper: '8', lower_exact: true, upper_exact: true } };
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', s3: null, configurable: false, iceberg_ready: true } }));
  await page.route('**/api/access/iceberg/**', route => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith('/inspect')) {
      expect(url.searchParams.get('snapshot')).toBe(snapshot);
      expect(url.searchParams.get('metadata')).toBe(metadata);
      const offset = url.searchParams.get('offset') ?? '0';
      const base = { metadata_location: metadata, snapshot_id: snapshot, offset: '0', next: null };
      if (url.searchParams.has('file')) return route.fulfill({ json: { ...base, kind: 'parquet', location: 's3://warehouse/file-0.parquet', size: '200', physical_rows: '8', row_group_count: '1', groups: [{ index: '0', rows: '8', columns: [column] }], footer: { offset: '104', length: '88', version: '2', writer: 'test writer', properties: [] }, logical_metadata_bytes: '100', data_page_bytes: '0' } });
      if (url.searchParams.has('manifest')) {
        offsets.push(offset);
        expect(['0', 'c.next-files']).toContain(offset);
        const start = offset === '0' ? 0 : 100;
        return route.fulfill({ json: { ...base, kind: 'manifest', location: 's3://warehouse/manifest.avro', offset: String(start), next: start ? null : 'c.next-files', codec: 'deflate', rows: Array.from({ length: 100 }, (_, i) => ({ location: `s3://warehouse/file-${start + i}.parquet`, size: '200', content: 'Data', format: 'Parquet', status: 'Added', records: '8' })) } });
      }
      return route.fulfill({ json: { ...base, kind: 'manifest-list', location: 's3://warehouse/list.avro', size: '500', rows: [{ location: 's3://warehouse/manifest.avro', size: '20000', content: 'Data', partition_spec_id: '0', sequence: '1', file_counts: ['200', '0', '0'] }] } });
    }
    if (url.pathname.endsWith('/tables/events')) return route.fulfill({ contentType: 'application/json', body: `{"metadata-location":"${metadata}","metadata":{"snapshots":[{"snapshot-id":${snapshot},"manifest-list":"s3://warehouse/list.avro"}]}}` });
    return route.fulfill({ json: url.pathname.endsWith('/config') ? {} : url.pathname.endsWith('/namespaces') ? { namespaces: [['demo']] } : url.pathname.endsWith('/tables') ? { identifiers: [{ namespace: ['demo'], name: 'events' }] } : {} });
  });
  await page.goto('/?domain=Iceberg');
  await expect(page.getByLabel('Catalog bearer token')).toHaveCount(0);
  await expect(page.getByLabel('iceberg endpoint')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Load catalog', exact: true })).toHaveCount(0);
  await page.getByRole('navigation', { name: 'Iceberg namespaces' }).getByRole('button', { name: 'demo', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg tables' }).getByRole('button', { name: 'events', exact: true }).click();
  const tree = page.getByRole('navigation', { name: 'Iceberg references' });
  await tree.getByRole('button', { name: `Snapshot ${snapshot}` }).click();
  await page.getByRole('button', { name: 'Open manifest list' }).click();
  await page.getByRole('table', { name: 'Manifest list records' }).getByRole('button', { name: 'manifest.avro', exact: true }).click();
  await expect(page.getByRole('table', { name: 'Manifest file entries' }).getByRole('row')).toHaveCount(101);
  await tree.getByRole('button', { name: 'Added · file-0.parquet', exact: true }).click();
  await page.getByRole('button', { name: 'Row group 0 column id', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Column chunk details' })).toContainText('SNAPPY');
  await expect(page.getByText('Logical metadata ranges:', { exact: false })).toContainText('Data pages read: 0 B');
  await tree.getByRole('button', { name: 'Next page', exact: true }).click();
  await expect(tree.getByRole('button', { name: 'Added · file-100.parquet', exact: true })).toBeVisible();
  await expect(tree.getByRole('button', { name: 'Added · file-0.parquet', exact: true })).toHaveCount(0);
  await expect(page.getByRole('region', { name: 'Column chunk details' })).toContainText('SNAPPY');
  await tree.getByRole('button', { name: 'Previous page', exact: true }).click();
  await expect(tree.getByRole('button', { name: 'Added · file-0.parquet', exact: true })).toBeVisible();
  expect(offsets).toEqual(['0', 'c.next-files', '0']);
  await expect(page.locator('main aside:visible')).toHaveCount(1);
  await page.getByRole('button', { name: 'Catalog', exact: true }).click();
  await expect(tree).toHaveCount(0);
  await expect(page.getByRole('region', { name: 'Column chunk details' })).toHaveCount(0);
});

test('Unavailable cluster catalog offers retry without a connection wizard', async ({ page }) => {
  let ready = false;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', iceberg_ready: ready, s3: null, configurable: false } }));
  await page.route('**/api/access/iceberg/**', route => route.fulfill({ json: route.request().url().endsWith('/config') ? {} : { namespaces: [] } }));
  await page.goto('/?domain=Iceberg');
  await expect(page.getByRole('alert').filter({ hasText: 'cluster Catalog is not ready' })).toBeVisible();
  await expect(page.getByLabel('Catalog bearer token')).toHaveCount(0);
  await expect(page.getByLabel('iceberg endpoint')).toHaveCount(0);
  ready = true;
  await page.getByRole('button', { name: 'Retry catalog' }).click();
  await expect(page.getByText('No namespaces in this catalog.')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Retry catalog' })).toHaveCount(0);
});

test('Catalog writes use a separate session token and clear back to automatic reading', async ({ page }) => {
  const writes: string[] = [];
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', iceberg_ready: true, s3: null, configurable: false } }));
  await page.route('**/api/access/iceberg/**', route => {
    const request = route.request();
    if (request.method() === 'POST') {
      writes.push(request.headers().authorization);
      return route.fulfill({ json: {} });
    }
    return route.fulfill({ json: request.url().endsWith('/config') ? {} : { namespaces: [] } });
  });
  await page.goto('/?domain=Iceberg');
  await expect(page.getByText('No namespaces in this catalog.')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Create namespace', exact: true })).toHaveCount(0);
  await page.getByText('Catalog write authorization', { exact: true }).click();
  await page.getByLabel('Catalog write token', { exact: true }).fill('native-write-credential');
  await page.getByRole('button', { name: 'Use catalog write token', exact: true }).click();
  await page.getByLabel('Namespace name', { exact: true }).fill('demo');
  await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Namespace created' })).toBeVisible();
  expect(writes).toEqual(['Bearer native-write-credential']);
  await page.getByRole('button', { name: 'Clear catalog write token', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Create namespace', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Catalog write token', { exact: true })).toHaveValue('');
  await expect(page.getByText('No namespaces in this catalog.')).toBeVisible();
});
