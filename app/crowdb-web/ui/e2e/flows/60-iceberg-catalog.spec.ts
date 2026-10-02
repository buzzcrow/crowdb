// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('Iceberg native catalog browsing, commit conflict and scope retention', async ({ page }) => {
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: 'http://127.0.0.1:17000', s3: null, configurable: false } }));
  let commit: unknown;
  await page.route('**/api/access/iceberg/**', route => {
    const request = route.request();
    expect(request.headers().authorization).toBe('Bearer catalog-session');
    const path = new URL(request.url()).pathname;
    if (request.method() === 'POST' && path.endsWith('/tables/events')) {
      commit = request.postDataJSON();
      return route.fulfill({ status: 409, json: { error: { message: 'Concurrent commit' } } });
    }
    return route.fulfill({ json: path.endsWith('/config') ? { defaults: {}, overrides: {} }
      : path.endsWith('/namespaces') ? { namespaces: [['demo']] }
      : path.endsWith('/tables') ? { identifiers: [{ namespace: ['demo'], name: 'events' }] }
      : path.endsWith('/tables/events') ? { 'metadata-location': 's3://warehouse/demo/metadata.json', metadata: { 'table-uuid': 'native-uuid', 'format-version': 2, 'current-schema-id': 0, schemas: [{ 'schema-id': 0, fields: [{ id: 1, name: 'id', type: 'long' }] }], properties: {}, snapshots: [{ 'snapshot-id': 7, 'manifest-list': 's3://warehouse/manifest.avro' }] } }
      : { namespace: ['demo'], properties: { owner: 'console' } } });
  });
  await page.goto('/?domain=Iceberg');
  await page.getByLabel('Catalog bearer token').fill('catalog-session');
  await page.getByRole('button', { name: 'Load catalog', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg namespaces' }).getByRole('button', { name: 'demo', exact: true }).click();
  await page.getByRole('navigation', { name: 'Iceberg tables' }).getByRole('button', { name: 'events', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
  await page.getByRole('button', { name: 'Schema', exact: true }).click();
  await expect(page.locator('main section pre')).toContainText('schema-id');
  await page.getByRole('button', { name: 'Commit metadata', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'Concurrent commit' })).toBeVisible({ timeout: 3000 });
  expect(commit).toEqual({ requirements: [{ type: 'assert-table-uuid', uuid: 'native-uuid' }], updates: [{ action: 'set-properties', updates: { demo: 'true' } }] });
  await page.getByTestId('domain-s3').click();
  await page.getByTestId('domain-iceberg').click();
  await expect(page.getByLabel('Catalog bearer token')).toHaveValue('catalog-session');
  await expect(page.getByRole('button', { name: 'Schema', exact: true })).toHaveAttribute('aria-pressed', 'true');
});
