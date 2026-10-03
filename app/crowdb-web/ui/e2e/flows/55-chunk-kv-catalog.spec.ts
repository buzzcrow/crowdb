// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: partition/overlay 0.676s, unavailable 0.238s (2026-10-03).
import { test, expect } from '../fixtures/realBackend';

test('Chunk-KV preserves exact partition identity and separates inherited journal tracks', async ({ page }) => {
  const id = 'ffffffffffffffff0000000000000002';
  const stream = { high: '18446744073709551615', low: '2' };
  const entry = {
    id, start: '80', end: null, owner_id: '42', endpoint: '127.0.0.1:45200', epoch: '9007199254740993', state: 'Serving', transition_id: 'transition-1',
    artifact: { tree_id: '9007199254740995', stream_name: stream, tail_overlay: {
      source_partition_id: { ...stream, low: '1' }, source_epoch: '9007199254740991',
      source_stream_name: { ...stream, low: '3' }, source_stream_manifest_generation: '15',
      replay_offset: '4096', cutover_offset: '8192', cutover_seq: '50',
      base_root_manifest_generation: '7', base_tree_manifest: '6', base_applied_seq: '40', target_stream_start_seq: '51',
    } },
  };
  const requests: string[] = [];
  await page.route('**/api/chunk-kv/catalog**', route => {
    requests.push(route.request().url());
    const next = new URL(route.request().url()).searchParams.has('generation');
    return route.fulfill({ status: next ? 409 : 200, json: next ? { error: 'Chunk-KV catalog changed; refresh the range map' } : {
      generation: '9007199254740997', page: 0, offset: 0, catalog_pages: 2, entries: [entry], next: { page: 1, offset: 0 }, source: 'group0',
    } });
  });
  await page.route('**/api/servers', route => route.fulfill({ json: [{ node_id: 1, endpoint: entry.endpoint, health: 'unknown', service_type: 'chunk-kv' }] }));
  await page.goto('/?domain=Chunk-KV');
  await expect(page.getByTestId('domain-chunk-kv')).toHaveAttribute('aria-pressed', 'true');
  await page.getByLabel('Partition range map').getByRole('button', { name: `Partition ${id}`, exact: true }).click();
  await expect(page.getByRole('tabpanel', { name: 'Overview' })).toContainText('9007199254740993');
  await page.getByRole('tab', { name: 'Journal', exact: true }).click();
  const journal = page.getByRole('tabpanel', { name: 'Journal' });
  await expect(journal.getByRole('heading', { name: 'Inherited parent stream' })).toBeVisible();
  await expect(journal.getByRole('heading', { name: 'Partition journal stream' })).toBeVisible();
  await expect(journal).toContainText('ffffffffffffffff0000000000000003');
  await expect(journal).toContainText('ffffffffffffffff0000000000000002');
  await page.getByTestId('domain-capacity').click();
  await page.getByTestId('domain-chunk-kv').click();
  await expect(journal).toBeVisible();
  await page.getByRole('button', { name: 'Next partitions', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'catalog changed' })).toBeVisible();
  expect(new URL(requests[requests.length - 1]).searchParams.get('generation')).toBe('9007199254740997');
  await expect(page.getByRole('button', { name: 'Next partitions', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Refresh catalog', exact: true }).click();
  await expect(page.getByRole('alert').filter({ hasText: 'catalog changed' })).toHaveCount(0);
  await page.getByRole('tab', { name: 'Dependencies', exact: true }).click();
  await expect(page.getByRole('tabpanel', { name: 'Dependencies' })).toContainText('Parent recovery dependency');
});

test('Chunk-KV unavailable catalog is explicit and can be retried', async ({ page }) => {
  await page.route('**/api/chunk-kv/catalog**', route => route.fulfill({ status: 502, json: { error: 'Group 0 is unavailable' } }));
  await page.goto('/?domain=Chunk-KV');
  await expect(page.getByRole('alert').filter({ hasText: 'Group 0 is unavailable' })).toBeVisible();
  await expect(page.getByLabel('Partition range map')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Refresh catalog', exact: true })).toBeEnabled();
});
