// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

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
      : path.endsWith('/tables/events') ? { 'metadata-location': 's3://warehouse/demo/metadata.json', metadata: { 'table-uuid': 'native-uuid', 'format-version': 2, 'current-schema-id': 0, schemas: [{ 'schema-id': 0, fields: [{ id: 1, name: 'id', type: 'long' }, { id: 2, name: 'details', type: { type: 'struct', fields: [{ id: 3, name: 'tags', type: { type: 'list', 'element-id': 4, element: 'string' } }] } }] }], properties: {}, snapshots: [{ 'snapshot-id': 7, 'manifest-list': 's3://warehouse/manifest.avro' }] } }
      : { namespace: ['demo'], properties: { owner: 'console' } } });
  });
  await page.goto('/?domain=Iceberg');
  await expect(page.getByLabel('Catalog bearer token')).toHaveCount(0);
  await expect(page.getByLabel('iceberg endpoint')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Load catalog', exact: true })).toHaveCount(0);
  await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: 'demo', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: 'events', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
  await expect(page.locator('main aside:visible')).toHaveCount(2);
  await expect(page.locator('main section').getByRole('button', { name: 'Refresh table' })).toBeVisible();
  await page.getByRole('button', { name: 'Schema', exact: true }).click();
  await expect(page.getByRole('table', { name: 'Table schema' })).toContainText('long');
  const schema = page.getByRole('table', { name: 'Table schema' });
  await expect(schema).toContainText('element');
  await schema.getByRole('button', { name: 'Collapse field details' }).click();
  await expect(schema).not.toContainText('element');
  await schema.getByRole('button', { name: 'Expand field details' }).click();
  await expect(schema).toContainText('element');
  const actions = page.getByLabel('Table actions', { exact: true });
  await expect(actions.getByText('Table actions', { exact: true })).toBeVisible();
  await expect(actions).not.toHaveAttribute('open', '');
  await actions.getByText('Table actions', { exact: true }).click();
  await expect(actions.getByLabel('New table name')).toBeVisible();
  await actions.getByText('Table actions', { exact: true }).click();
  await expect(page.getByText('Catalog actions', { exact: true })).toHaveCount(0);
  const schemaBox = await schema.boundingBox();
  const actionsBox = await actions.boundingBox();
  expect(actionsBox!.y).toBeLessThan(schemaBox!.y);
  const separator = page.getByRole('separator', { name: 'Sidebar width' });
  await expect(separator).toHaveAttribute('aria-valuenow', '280');
  await expect(page.locator('main')).toHaveCSS('transition-property', 'all');
  await expect(page.locator('main')).toHaveCSS('transition-duration', '0s');
  await separator.press('ArrowRight');
  await expect(separator).toHaveAttribute('aria-valuenow', '300');
  const grip = await separator.boundingBox();
  await page.mouse.move(grip!.x + 3, grip!.y + 50);
  await page.mouse.down();
  await page.mouse.move(grip!.x + 83, grip!.y + 50);
  await page.mouse.up();
  await expect(separator).toHaveAttribute('aria-valuenow', '383');
  const propertiesWidth = page.getByRole('separator', { name: 'Properties width' });
  await expect(propertiesWidth).toHaveAttribute('aria-valuenow', '320');
  await propertiesWidth.press('ArrowLeft');
  await expect(propertiesWidth).toHaveAttribute('aria-valuenow', '340');

  await expect(page.locator('main section pre')).toHaveCount(0);
  await page.getByRole('button', { name: 'Refresh table', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'Stale metadata' })).toBeVisible({ timeout: 3000 });
  await page.getByTestId('domain-s3').click();
  await page.getByTestId('domain-iceberg').click();
  await expect(page.getByRole('button', { name: 'Schema', exact: true })).toHaveAttribute('aria-pressed', 'true');
});

// Baseline: new metadata-inspection flow (2026-10-03).
test('Iceberg reference pages preserve exact identities and selected footer in the property panel', async ({ page }) => {
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
  await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: 'demo', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: 'events', exact: true }).click();
  const tree = page.getByRole('navigation', { name: 'Iceberg tree' });
  const sections = page.getByRole('navigation', { name: 'Table sections' });
  await expect(sections).toBeVisible();
  await tree.getByRole('button', { name: `Snapshot ${snapshot}` }).click();
  await expect(sections).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Refresh table' })).toHaveCount(0);
  await expect(page.getByRole('heading', { level: 1 })).toHaveText(`Snapshot ${snapshot}`);
  await expect(page.getByText('Table actions', { exact: true })).toHaveCount(0);
  await expect(sections).toHaveCount(0);
  const manifests = page.getByRole('table', { name: 'Manifest list records' });
  await manifests.getByRole('button', { name: 'Expand manifest manifest.avro', exact: true }).click();
  const files = page.getByRole('region', { name: 'Files in manifest.avro' });
  await expect(files.getByRole('table', { name: 'Manifest files', exact: true }).getByRole('row')).toHaveCount(101);
  await files.getByRole('button', { name: 'Next files', exact: true }).click();
  await expect(files).toContainText('file-100.parquet');
  await files.getByRole('button', { name: 'Previous files', exact: true }).click();
  await expect(files).toContainText('file-0.parquet');
  await manifests.getByRole('button', { name: 'Collapse manifest manifest.avro', exact: true }).click();
  await expect(files).toHaveCount(0);
  await page.getByRole('table', { name: 'Manifest list records' }).getByRole('button', { name: 'manifest.avro', exact: true }).click();
  await expect(sections).toHaveCount(0);
  await expect(page.getByRole('table', { name: 'Manifest file entries' }).getByRole('row')).toHaveCount(101);
  await tree.getByRole('button', { name: 'Data · file-0.parquet', exact: true }).click();
  await page.getByRole('button', { name: 'Row group 0 column id', exact: true }).click();
  await expect(sections).toHaveCount(0);
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('file-0.parquet');
  await expect(page.getByLabel('Iceberg properties').getByRole('region', { name: 'Column chunk details' })).toContainText('SNAPPY');
  await expect(page.getByText('Logical metadata ranges:', { exact: false })).toContainText('Data pages read: 0 B');
  await tree.getByRole('button', { name: 'Next page', exact: true }).click();
  await expect(tree.getByRole('button', { name: 'Data · file-100.parquet', exact: true })).toBeVisible();
  await expect(tree.getByRole('button', { name: 'Data · file-0.parquet', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Iceberg properties').getByRole('region', { name: 'Column chunk details' })).toContainText('SNAPPY');
  await tree.getByRole('button', { name: 'Previous page', exact: true }).click();
  await expect(tree.getByRole('button', { name: 'Data · file-0.parquet', exact: true })).toBeVisible();
  expect(offsets).toEqual(['0', 'c.next-files', '0', 'c.next-files', '0']);
  await expect(page.locator('main aside:visible')).toHaveCount(2);
  await page.getByRole('navigation', { name: 'Iceberg breadcrumbs' }).getByRole('button', { name: 'events', exact: true }).click();
  await expect(sections).toBeVisible();
  await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible();
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('file-0.parquet');
  await expect(page.getByText('Logical metadata ranges:', { exact: false })).toContainText('Data pages read: 0 B');
  await page.getByRole('button', { name: 'Forward', exact: true }).click();
  await expect(sections).toBeVisible();
  await tree.getByRole('button', { name: 'Catalog', exact: true }).click();
  await expect(sections).toHaveCount(0);
  await expect(tree.getByRole('button', { name: `Snapshot ${snapshot}` })).toHaveCount(0);
  await expect(page.getByRole('region', { name: 'Column chunk details' })).toHaveCount(0);
});

test('Unavailable cluster catalog offers retry without a connection wizard', async ({ page }) => {
  let ready = false;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', iceberg_ready: ready, s3: null, configurable: false } }));
  await page.route('**/api/access/iceberg/**', route => route.fulfill({ json: new URL(route.request().url()).pathname.endsWith('/config') ? {} : { namespaces: [] } }));
  await page.goto('/?domain=Iceberg');
  await expect(page.getByRole('alert').filter({ hasText: 'cluster Catalog is not ready' })).toBeVisible();
  await expect(page.getByLabel('Catalog bearer token')).toHaveCount(0);
  await expect(page.getByLabel('iceberg endpoint')).toHaveCount(0);
  ready = true;
  await page.getByRole('button', { name: 'Retry catalog' }).click();
  await expect(page.getByText('No namespaces in this catalog.')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Retry catalog' })).toHaveCount(0);
});

test('Root catalog writes require no browser credential', async ({ page }) => {
  const writes: Array<string | undefined> = [];
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', iceberg_ready: true, s3: null, configurable: false } }));
  await page.route('**/api/access/iceberg/**', route => {
    const request = route.request();
    if (request.method() === 'POST') {
      writes.push(request.headers().authorization);
      return route.fulfill({ json: {} });
    }
    return route.fulfill({ json: new URL(request.url()).pathname.endsWith('/config') ? {} : { namespaces: [] } });
  });
  await page.goto('/?domain=Iceberg');
  await expect(page.getByText('No namespaces in this catalog.')).toBeVisible();
  await expect(page.getByLabel('Catalog write token', { exact: true })).toHaveCount(0);
  await page.getByText('Catalog actions', { exact: true }).click();
  await page.getByLabel('Namespace name', { exact: true }).fill('demo');
  await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Namespace created' })).toBeVisible();
  expect(writes).toEqual([undefined]);
  await expect(page.getByRole('button', { name: 'Create namespace', exact: true })).toBeVisible();
});

// Baseline: new shared-tree flow (2026-10-04).
test('Iceberg shared tree distinguishes all eight tables and lazily expands file references', async ({ page }) => {
  const names = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier'];
  let inspected = 0;
  let tableLoads = 0;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', iceberg_ready: true } }));
  await page.route('**/api/access/iceberg/**', route => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith('/inspect')) {
      inspected++;
      return route.fulfill({ json: { metadata_location: 's3://warehouse/meta.json', snapshot_id: '7', kind: 'manifest-list', location: 's3://warehouse/list.avro', size: '200', rows: [], next: null } });
    }
    if (url.pathname.endsWith('/tables/lineitem')) {
      tableLoads++;
      return route.fulfill({ json: { 'metadata-location': 's3://warehouse/meta.json', metadata: { 'table-uuid': 'lineitem-uuid', 'current-snapshot-id': '7', snapshots: [{ 'snapshot-id': '7', 'manifest-list': 's3://warehouse/list.avro' }] } } });
    }
    if (url.pathname.endsWith('/tables')) {
      expect(url.searchParams.get('pageSize')).toBe('30');
      return route.fulfill({ json: { identifiers: names.map(name => ({ namespace: ['ui_tpch_sf1'], name })) } });
    }
    return route.fulfill({ json: url.pathname.endsWith('/config') ? {} : url.pathname.endsWith('/namespaces') ? { namespaces: url.searchParams.has('parent') ? [] : [['ui_tpch_sf1']] } : { properties: {} } });
  });
  await page.goto('/?domain=Iceberg');
  const nav = page.getByRole('navigation', { name: 'Iceberg tree' });
  await expect(nav.getByRole('tree')).toBeVisible();
  const namespace = nav.getByTestId('tree-node-ice-ns-["ui_tpch_sf1"]');
  await expect(namespace.getByRole('button', { name: 'ui_tpch_sf1', exact: true })).toHaveAttribute('title', 'Namespace · ui_tpch_sf1');
  await namespace.getByRole('button', { name: 'Expand', exact: true }).click();
  for (const name of names) await expect(nav.getByRole('button', { name, exact: true })).toHaveAttribute('title', `Table · ui_tpch_sf1.${name}`);
  expect(tableLoads).toBe(0);
  await nav.getByRole('button', { name: 'lineitem', exact: true }).click();
  await expect(nav.getByRole('button', { name: 'Snapshot 7 · Current', exact: true })).toBeVisible();
  expect(inspected).toBe(0);
  await expect(nav.getByRole('button', { name: 'Metadata', exact: true })).toHaveCount(0);
  await expect(page.getByRole('table', { name: 'Table snapshots' })).toContainText('7');
  await nav.getByRole('button', { name: 'Snapshot 7 · Current', exact: true }).click();
  await expect(page.getByLabel('Iceberg properties')).toContainText('Snapshot');
  await expect(nav.getByRole('button', { name: 'Manifest List', exact: true })).toHaveCount(0);
  await expect(nav.getByRole('button', { name: 'Snapshots (1)', exact: true })).toHaveCount(0);
  const list = nav.getByTestId('tree-node-ice-snapshot-7||');
  await expect(list).toContainText('0 loaded');
  expect(inspected).toBe(1);
  await list.getByRole('button', { name: 'Collapse', exact: true }).click();
  await expect(list.getByText('0 loaded')).toHaveCount(0);
  await page.getByRole('navigation', { name: 'Iceberg breadcrumbs' }).getByRole('button', { name: 'ui_tpch_sf1', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Tables', exact: true })).toBeVisible();
  await expect(page.getByText('Session activity', { exact: true })).toBeHidden();
});
