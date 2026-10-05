// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 2.4s (2026-10-04), real three-node fixture.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: Chunk ownership matches both durable maps and node scopes', async ({ page, request }) => {
  const metricsRequests: string[] = [];
  page.on('request', request => { if (/\/metrics(?:\?|$)/.test(request.url())) metricsRequests.push(request.url()); });
  const observations = await Promise.all(['service', 'storage'].map(async layer => {
    const response = await request.get(`/api/chunk-slots?layer=${layer}&view=bitmap`);
    expect(response.status()).toBe(200);
    return response.json();
  }));
  await page.goto('/?domain=Chunk');
  const tree = page.getByRole('navigation', { name: 'Chunk hierarchy' });
  await tree.getByTestId('tree-node-chunk-node-1').getByText('N-1', { exact: true }).click();
  const ownership = page.getByRole('region', { name: 'Chunk ownership', exact: true });
  await expect(ownership).toBeVisible();
  for (let index = 0; index < observations.length; index++) {
    const title = index === 0 ? 'Chunk Serving Ownership' : 'Chunk Storage Ownership';
    const bitmap = page.getByRole('group', { name: `${title} bitmap` });
  await expect(bitmap.locator('[data-slot]')).toHaveCount(1024);
  await expect.poll(() => bitmap.evaluate(element => {
    const style = getComputedStyle(element);
    return [style.gridTemplateColumns.split(' ').length, style.gridTemplateRows.split(' ').length];
  })).toEqual([128, 8]);
    expect(await bitmap.locator('[data-slot]').evaluateAll(elements => elements.map(element => element.getAttribute('data-owner')))).toEqual(observations[index].owners);
    await expect(bitmap).toHaveAttribute('data-generation', observations[index].generation);
  }
  const serving = page.getByRole('group', { name: 'Chunk Serving Ownership bitmap' });
  expect(await serving.locator('[data-scope="inside"]').count()).toBe(observations[0].owners.filter((owner: string) => owner === '1').length);
  await expect(serving.locator('[data-scope="unknown"]')).toHaveCount(0);
  const storage = page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' });
  await expect(storage.locator('[data-scope="inside"]')).toHaveCount(1024);
  await serving.locator('[data-slot="1023"]').click();
  await expect(page.getByRole('complementary', { name: 'Chunk Serving Ownership slot properties' })).toContainText('Slot 1023');
  await page.getByRole('region', { name: 'Chunk Serving Ownership', exact: true }).getByRole('button', { name: /^CDB-1 ·/ }).click();
  await expect(page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' })).toHaveCount(0);
  await expect(serving.locator('[data-scope="inside"]')).toHaveCount(observations[0].owners.filter((owner: string) => owner === '1').length);
  await page.goBack();
  await expect(page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' })).toBeVisible();
  await expect(serving.locator('[data-slot="1023"]')).toHaveAttribute('aria-pressed', 'true');
  await page.goForward();
  await expect(page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' })).toHaveCount(0);
  await page.goBack();
  await tree.getByRole('button', { name: 'R-1', exact: true }).click();
  await expect(serving.locator('[data-scope="inside"]')).toHaveCount(1024);
  await expect(page.getByRole('region', { name: 'Chunk Serving Ownership', exact: true }).getByRole('button', { name: /^CDB-/ })).toHaveCount(3);
  const nodeRow = tree.getByTestId('tree-node-chunk-node-1').getByRole('treeitem').filter({ has: page.getByRole('button', { name: 'N-1', exact: true }) });
  await nodeRow.getByRole('button', { name: 'Expand', exact: true }).click();
  const serviceResponse = await request.get('/api/servers');
  expect(serviceResponse.status()).toBe(200);
  const kvId = (await serviceResponse.json()).find((service: { service_type: string; node_id: number }) => service.service_type === 'kv' && service.node_id === 1).id;
  const kv = tree.getByTestId(`tree-node-chunk-server-${kvId}`);
  await kv.getByRole('treeitem').filter({ has: page.getByRole('button', { name: 'KV-1', exact: true }) }).getByRole('button', { name: 'Expand', exact: true }).click();
  const store = tree.getByTestId('tree-node-chunk-store-1-1');
  await store.getByRole('treeitem').filter({ has: page.getByRole('button', { name: 'S-1', exact: true }) }).getByRole('button', { name: 'Expand', exact: true }).click();
  await tree.getByTestId('tree-node-chunk-group-1-1-1').getByRole('button', { name: 'G-1', exact: true }).click();
  await expect(page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' })).toBeVisible();
  await expect(page.getByRole('group', { name: 'Chunk Serving Ownership bitmap' })).toHaveCount(0);
  await expect(page.getByRole('group', { name: 'Chunk Storage Ownership bitmap' }).locator('[data-scope="inside"]')).toHaveCount(observations[1].owners.filter((owner: string) => owner === '1/1').length);
  await page.goBack();
  await expect(serving.locator('[data-scope="inside"]')).toHaveCount(1024);
  await expect(tree.getByRole('treeitem', { selected: true })).toHaveCount(1);
  await expect(tree.getByRole('treeitem', { selected: true })).toContainText('R-1');
  expect(metricsRequests).toEqual([]);
  await expect(page.getByRole('button', { name: 'Metrics', exact: true })).toHaveCount(0);
  await page.screenshot({ path: 'test-results/chunk-ownership.png', fullPage: true });
});
