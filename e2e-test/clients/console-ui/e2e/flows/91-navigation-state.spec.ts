// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 3.3s (2026-10-04), real three-node fixture.
import { test, expect } from '../fixtures/realBackend';

test('native diagnostics: browser history preserves S3 windows and rejects replaced or deleted resources', async ({ page, request }) => {
  const bucket = `navigation-${process.pid}-${Date.now()}`;
  const path = `/api/access/s3/${bucket}`;
  const keys = Array.from({ length: 21 }, (_, index) => `item-${String(index).padStart(2, '0')}`);
  expect((await request.put(path)).status()).toBe(200);
  try {
    for (let start = 0; start < keys.length; start += 4) {
      const results = await Promise.all(keys.slice(start, start + 4).map(key => request.put(`${path}/${key}`, { data: `payload-${key}` })));
      for (const response of results) expect(response.status(), await response.text()).toBe(200);
    }
    await page.goto('/?domain=S3');
    await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: bucket, exact: true }).click();
    const objects = page.getByRole('table', { name: 'S3 objects' });
    await expect(objects.getByRole('row')).toHaveCount(21);
    const center = page.locator('[data-navigation-scroll="workbench-center"]').filter({ has: objects });
    const savedScroll = await center.evaluate(element => { element.scrollTop = element.scrollHeight; return element.scrollTop; });
    expect(savedScroll).toBeGreaterThan(0);
    await page.getByRole('navigation', { name: 'Object pages' }).getByRole('button', { name: 'Next', exact: true }).click();
    await expect(objects.getByRole('row')).toHaveCount(2);
    await page.goBack();
    await expect(objects.getByRole('row')).toHaveCount(21);
    await expect.poll(() => center.evaluate(element => element.scrollTop), { intervals: [100] }).toBe(savedScroll);
    await page.goForward();
    await expect(objects.getByRole('row')).toHaveCount(2);
    await objects.getByRole('button', { name: 'item-20', exact: true }).click();
    const locations = page.getByRole('region', { name: 'Storage locations', exact: true });
    await expect(locations.getByRole('table')).toBeVisible();
    await page.getByTestId('domain-kv').click();
    await page.goBack();
    await expect(page.getByRole('heading', { level: 1 })).toHaveText('item-20');
    await expect(locations.getByRole('table')).toBeVisible();
    await page.goBack();
    await expect(objects.getByRole('row')).toHaveCount(2);
    await expect(objects).toContainText('item-20');
    await page.goForward();
    await expect(page.getByRole('heading', { level: 1 })).toHaveText('item-20');
    await page.getByTestId('domain-kv').click();
    expect((await request.put(`${path}/item-20`, { data: 'replacement with a different generation' })).status()).toBe(200);
    await page.goBack();
    await expect(locations.getByRole('alert')).toContainText('Stale locations');
    await expect(locations.getByRole('button', { name: /^Open Chunk/ })).toBeDisabled();
    await locations.getByRole('button', { name: 'Refresh locations', exact: true }).click();
    await expect(locations.getByRole('alert')).toHaveCount(0);
    const services = await (await request.get('/api/servers')).json();
    const access = services.filter((service: { service_type: string }) => service.service_type === 'access-server');
    expect(access).toHaveLength(3);
    const stopped: string[] = [];
    try {
      for (const service of access) {
        expect((await request.post(`/api/services/${service.id}/stop`, { data: {} })).ok()).toBe(true);
        stopped.push(service.id);
      }
      await locations.getByRole('button', { name: 'Refresh locations', exact: true }).click();
      await expect(locations.getByRole('alert')).toBeVisible();
      await expect(page.getByRole('heading', { level: 1 })).toHaveText('item-20');
      await expect(locations.getByRole('button', { name: /^Open Chunk/ })).toBeDisabled();
    } finally {
      for (const id of stopped) expect((await request.post(`/api/services/${id}/restart`, { data: {} })).ok()).toBe(true);
    }
    await locations.getByRole('button', { name: 'Refresh locations', exact: true }).click();
    await expect(locations.getByRole('alert')).toHaveCount(0);

    await page.getByTestId('domain-kv').click();
    expect((await request.delete(`${path}/item-20`)).status()).toBe(204);
    await page.goBack();
    await expect(page.getByRole('heading', { level: 1 })).toHaveText('item-20');
    await expect(locations.getByRole('alert')).toContainText('404');
    await expect(page.getByLabel('Object metadata')).not.toContainText('content-length');
    await page.getByRole('navigation', { name: 'S3 breadcrumbs' }).getByRole('button', { name: bucket, exact: true }).click();
    await page.getByRole('button', { name: 'List objects', exact: true }).click();
    await expect(objects.getByRole('button', { name: 'item-20', exact: true })).toHaveCount(0);
  } finally {
    for (let start = 0; start < keys.length; start += 4) {
      const results = await Promise.all(keys.slice(start, start + 4).map(key => request.delete(`${path}/${key}`)));
      for (const response of results) expect(response.status()).toBe(204);
    }
    expect((await request.delete(path)).status()).toBe(204);
  }
});
