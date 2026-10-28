// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext, Page } from '@playwright/test';
import { expect } from '../realBackend';
import { resourceAction } from '../resourceMenus';
import { step } from '../stepTimer';

export async function s3Workflow(page: Page, request: APIRequestContext) {
  await step('managed S3 preview and actual block placement', async () => {
    await page.getByTestId('domain-s3').click();
    await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
    await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);

    const tree = page.getByRole('navigation', { name: 'S3 buckets' });
    await resourceAction(page, tree.getByRole('button', { name: 'S3', exact: true }), 'Create bucket…');
    const demoBucket = await page.getByLabel('New bucket', { exact: true }).inputValue();
    expect(demoBucket).toMatch(/^bucket-[a-f0-9]{8}$/);
    await page.getByRole('button', { name: 'Create bucket', exact: true }).click();
    await listBucketsByPrefix(page, demoBucket);
    await tree.getByRole('button', { name: demoBucket, exact: true }).click();
    await expect(page.getByLabel('Object prefix', { exact: true })).toHaveValue('');
    await resourceAction(page, tree.getByRole('button', { name: demoBucket, exact: true }), 'Upload object…');
    await page.getByLabel('Object files', { exact: true }).setInputFiles({ name: 'example.txt', mimeType: 'text/plain', buffer: Buffer.from('CROWDB console demo\n') });
    await page.getByRole('button', { name: 'Upload object', exact: true }).click();
    await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'example.txt', exact: true }).click();
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Preview first 4 KiB');
    await expect(page.getByLabel('Object preview', { exact: true })).toContainText('CROWDB console demo', { timeout: 3000 });
    const locationsResponse = await request.get(`/api/access/s3-inspect/locations?bucket=${demoBucket}&key=example.txt&limit=20`);
    expect(locationsResponse.ok(), await locationsResponse.text()).toBeTruthy();
    const locations = await locationsResponse.json();
    expect(locations.locations).toHaveLength(1);
    const chunkId = locations.locations[0].chunk_id;
    const response = await request.get(`/api/chunks/${chunkId}`);
    expect(response.ok(), await response.text()).toBeTruthy();
    const detail = await response.json();
    const strip = [...detail.chunk.strips].sort((left, right) => left.strip_sequence - right.strip_sequence)[0];
    expect(strip).toBeDefined();
    const segment = (strip.strip.MirrorStrip?.segments ?? strip.strip.EcStrip?.segments)[0];
    const diskId = BigInt(segment.disk_id.high).toString(16).padStart(16, '0') + BigInt(segment.disk_id.low).toString(16).padStart(16, '0');
    const placement = detail.placements.find((value: { disk_id: string }) => value.disk_id.replace(/-/g, '').toLowerCase() === diskId);
    expect(placement).toBeDefined();

    await page.getByTestId('domain-chunk').click();
    await page.getByLabel('Chunk type').selectOption('5');
    await page.getByLabel('Exact Chunk ID', { exact: true }).fill(chunkId);
    await page.getByRole('button', { name: 'Lookup ID', exact: true }).click();
    const card = page.getByLabel('Chunk strips').getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
    await card.getByRole('button', { name: /^(Mirror 1|Data 0)( · unavailable)?$/, exact: true }).click();
    const properties = page.getByLabel('Chunk properties');
    for (const [field, expected] of [['Rack', placement.rack_id], ['Node', placement.node_id], ['Diskgroup', placement.disk_group_id], ['Disk', diskId]]) {
      await expect(properties.locator('dt').filter({ hasText: new RegExp(`^${field}$`) }).locator('..').locator('dd')).toHaveText(String(expected));
    }
    const [restored] = await Promise.all([
      page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === '/api/access/s3/' && url.searchParams.get('prefix') === demoBucket;
      }),
      page.getByTestId('domain-s3').click(),
    ]);
    expect(restored.status(), await restored.text()).toBe(200);
    expect(await restored.text()).toContain(`<Prefix>${demoBucket}</Prefix>`);
    await exerciseObjectChanges(page, request, demoBucket);
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Delete object…');
    await page.getByRole('button', { name: 'Delete object', exact: true }).click();
    await expect(page.getByRole('dialog', { name: 'Delete object', exact: true })).toHaveCount(0);
    expect((await request.get(`/api/access/s3/${demoBucket}/example.txt`)).status()).toBe(404);
    const objects = page.getByRole('table', { name: 'S3 objects', exact: true });
    await expect(objects.getByRole('button', { name: 'other.txt', exact: true })).toBeVisible();
    await resourceAction(page, objects.getByRole('button', { name: 'other.txt', exact: true }), 'Delete object…');
    await page.getByRole('dialog', { name: 'Delete object', exact: true }).getByRole('button', { name: 'Delete object', exact: true }).click();
    await expect(objects.getByRole('row')).toHaveCount(1);
    expect((await request.get(`/api/access/s3/${demoBucket}/other.txt`)).status()).toBe(404);
    await resourceAction(page, tree.getByRole('button', { name: demoBucket, exact: true }), 'Delete empty bucket…');
    await page.getByRole('button', { name: 'Delete empty bucket', exact: true }).click();
    await expect(tree).not.toContainText(demoBucket);
    const missing = await request.get(`/api/access/s3/${demoBucket}?list-type=2`);
    expect(missing.status(), await missing.text()).toBe(404);
    expect(await missing.text()).toContain('NoSuchBucket');
    await page.reload();
    await expect(page.getByRole('table', { name: 'S3 bucket list', exact: true })).toBeVisible();
    await expect(tree.getByRole('button', { name: demoBucket, exact: true })).toHaveCount(0);
  });
}

async function listBucketsByPrefix(page: Page, bucket: string) {
  await step('S3: the Bucket list uses the common Prefix query', async () => {
    await expect(page.getByRole('heading', { name: 'Buckets', exact: true })).toHaveCount(0);
    await expect(page.getByText('Buckets refreshed', { exact: true })).toHaveCount(0);
    const listing = page.getByRole('table', { name: 'S3 bucket list', exact: true });
    await expect(listing.getByRole('button', { name: bucket, exact: true })).toBeVisible();
    const form = page.getByRole('form', { name: 'Bucket list query', exact: true });
    await page.getByLabel('Bucket prefix', { exact: true }).fill('unmatched-prefix');
    await form.getByRole('button', { name: 'List', exact: true }).click();
    await expect(listing.getByRole('row')).toHaveCount(1);
    await page.getByLabel('Bucket prefix', { exact: true }).fill(bucket);
    const [listed] = await Promise.all([
      page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === '/api/access/s3/' && url.searchParams.get('prefix') === bucket;
      }),
      form.getByRole('button', { name: 'List', exact: true }).click(),
    ]);
    expect(listed.status(), await listed.text()).toBe(200);
    expect(await listed.text()).toContain(`<Prefix>${bucket}</Prefix>`);
    await expect(listing.getByRole('button')).toHaveText([bucket]);
  });
}

async function exerciseObjectChanges(page: Page, request: APIRequestContext, bucket: string) {
  const tree = page.getByRole('navigation', { name: 'S3 buckets', exact: true });
  await step('S3: additional upload and server-side prefix listing', async () => {
    await tree.getByRole('button', { name: bucket, exact: true }).click();
    await expect(page.getByLabel('Object prefix', { exact: true })).toBeVisible();
    await resourceAction(page, tree.getByRole('button', { name: bucket, exact: true }), 'Upload object…');
    await page.getByLabel('Object files', { exact: true }).setInputFiles([
      { name: 'other.txt', mimeType: 'text/plain', buffer: Buffer.from('keep other object') },
      { name: 'example.txt', mimeType: 'text/plain', buffer: Buffer.from('batch overwrite') },
    ]);
    await page.getByRole('dialog', { name: 'Upload object', exact: true }).getByRole('button', { name: 'Upload object', exact: true }).click();
    const objects = page.getByRole('table', { name: 'S3 objects', exact: true });
    await expect(objects.getByRole('row')).toHaveCount(3);
    const overwritten = await request.get(`/api/access/s3/${bucket}/example.txt`);
    expect(overwritten.ok(), await overwritten.text()).toBe(true);
    expect(await overwritten.text()).toBe('batch overwrite');
    await expect(page.getByText(`Loaded ${bucket}`, { exact: true })).toHaveCount(0);
    await page.getByLabel('Object prefix', { exact: true }).fill('example');
    await page.getByRole('form', { name: 'Object list query', exact: true }).getByRole('button', { name: 'List', exact: true }).click();
    await expect(objects.getByRole('button')).toHaveText(['example.txt']);
    await page.getByLabel('Object prefix', { exact: true }).fill('missing');
    await page.getByRole('form', { name: 'Object list query', exact: true }).getByRole('button', { name: 'List', exact: true }).click();
    await expect(objects.getByRole('row')).toHaveCount(1);
    await page.getByLabel('Object prefix', { exact: true }).fill('');
    await page.getByRole('form', { name: 'Object list query', exact: true }).getByRole('button', { name: 'List', exact: true }).click();
    await expect(objects.getByRole('row')).toHaveCount(3);
  });
  await step('S3: reject nonempty Bucket deletion and replace an Object', async () => {
    await resourceAction(page, tree.getByRole('button', { name: bucket, exact: true }), 'Delete empty bucket…');
    const dialog = page.getByRole('dialog', { name: 'Delete empty bucket', exact: true });
    const [response] = await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname === `/api/access/s3/${bucket}` && response.request().method() === 'DELETE'),
      dialog.getByRole('button', { name: 'Delete empty bucket', exact: true }).click(),
    ]);
    expect(response.status(), await response.text()).toBe(409);
    await expect(dialog.getByRole('alert')).toContainText(/not empty/i);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await page.getByRole('table', { name: 'S3 objects', exact: true }).getByRole('button', { name: 'example.txt', exact: true }).click();
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Replace object…');
    const replacement = 'CROWDB console replacement\n';
    await page.getByLabel('Object file', { exact: true }).setInputFiles({ name: 'replacement.txt', mimeType: 'text/plain', buffer: Buffer.from(replacement) });
    await page.getByRole('dialog', { name: 'Replace object', exact: true }).getByRole('button', { name: 'Replace object', exact: true }).click();
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Preview first 4 KiB');
    await expect(page.getByLabel('Object preview', { exact: true })).toHaveText(replacement);
    const actual = await request.get(`/api/access/s3/${bucket}/example.txt`);
    expect(actual.status(), await actual.text()).toBe(200);
    expect(await actual.text()).toBe(replacement);
    const other = await request.get(`/api/access/s3/${bucket}/other.txt`);
    expect(other.status(), await other.text()).toBe(200);
    expect(await other.text()).toBe('keep other object');
  });
}
