// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

// Large transfer acceptance is separate from routine page behavior tests.
// Baseline: 5.3s (2026-10-04); normal three-node fixture, actual multipart acceptance.
test('S3 native multipart upload, HEAD, bounded preview and full round trip', async ({ page, request }) => {
  const bucket = `console-multipart-${process.pid}-${Date.now()}`;
  const key = 'multipart a/中文.txt';
  const bucketPath = `/api/access/s3/${bucket}`;
  const objectPath = `${bucketPath}/multipart%20a/%E4%B8%AD%E6%96%87.txt`;
  const bytes = Buffer.alloc(9 * 1024 * 1024, 'x');
  await step('native bucket setup', async () => {
    const response = await request.put(bucketPath);
    expect(response.status(), await response.text()).toBe(200);
  });
  try {
    await step('native post-create bucket discovery', async () => {
      const response = await request.get('/api/access/s3/');
      expect(response.status(), await response.text()).toBe(200);
      expect(await response.text()).toContain(`<Name>${bucket}</Name>`);
    });
    await step('native S3 DOM setup', async () => {
      await page.goto('/?domain=S3');
      await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
      await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: bucket, exact: true }).click();
      await page.getByText('Bucket actions', { exact: true }).click();
      await page.getByLabel('Object key', { exact: true }).fill(key);
      await page.getByLabel('Object file').setInputFiles({ name: 'multipart.txt', mimeType: 'text/plain', buffer: bytes });
    });
    await step('native 9 MiB multipart mutation and DOM refresh', async () => {
      await page.getByRole('button', { name: 'Upload', exact: true }).click();
      await expect(page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: key, exact: true })).toBeVisible();
    });
    await step('native object HEAD and bounded preview', async () => {
      await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: key, exact: true }).click();
      await expect(page.getByLabel('Object metadata')).toContainText(String(bytes.length));
      await page.getByText('Object actions', { exact: true }).click();
      await page.getByRole('button', { name: 'Preview first 4 KiB' }).click();
      await expect(page.getByLabel('Object preview', { exact: true })).toHaveText('x'.repeat(4096));
    });
    await step('native storage mapping and Chunk return', async () => {
      const response = await request.get(`/api/access/s3-inspect/locations?bucket=${bucket}&key=${encodeURIComponent(key)}&limit=20`);
      expect(response.status(), await response.text()).toBe(200);
      const metadata = await response.json();
      expect(metadata.logical_length).toBe(String(bytes.length));
      expect(metadata.locations.length).toBeGreaterThan(1);
      expect(metadata.next_cursor).toBeNull();
      const locations = page.getByRole('region', { name: 'Storage locations', exact: true });
      await expect(locations.getByRole('table').getByRole('row')).toHaveCount(metadata.locations.length + 1);
      let logicalEnd = 0n;
      for (const extent of metadata.locations) {
        expect(BigInt(extent.logical_offset)).toBe(logicalEnd);
        logicalEnd += BigInt(extent.logical_length);
        await locations.getByRole('button', { name: `Select extent ${extent.index}`, exact: true }).click();
        const properties = page.getByLabel('Storage extent properties');
        for (const value of [extent.chunk_id, extent.logical_offset, extent.logical_length, extent.offset, extent.length, metadata.generation]) {
          await expect(properties).toContainText(value);
        }
      }
      expect(logicalEnd).toBe(BigInt(bytes.length));
      const extent = metadata.locations[metadata.locations.length - 1];
      const row = locations.getByRole('row').filter({ has: page.getByRole('button', { name: `Select extent ${extent.index}`, exact: true }) });
      await row.getByRole('button', { name: `Open Chunk ${extent.chunk_id}`, exact: true }).click();
      await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
      await expect(page.getByLabel('Exact Chunk ID')).toHaveValue(extent.chunk_id);
      await expect(page.getByRole('region', { name: 'Chunk layout', exact: true })).toBeVisible();
      const chunkResponse = await request.get(`/api/chunks/${extent.chunk_id}`);
      expect(chunkResponse.status(), await chunkResponse.text()).toBe(200);
      const detail = await chunkResponse.json();
      expect(detail.chunk.id_hex).toBe(extent.chunk_id);
      const ordered = [...detail.chunk.strips].sort((left, right) => left.strip_sequence - right.strip_sequence).slice(0, 16);
      expect(ordered.length).toBeGreaterThan(0);
      const stripLayout = page.getByLabel('Chunk strips', { exact: true });
      await expect(stripLayout.getByTestId('chunk-strip')).toHaveCount(ordered.length);
      for (const strip of ordered) {
        const card = stripLayout.getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
        const segments = strip.strip.MirrorStrip?.segments ?? strip.strip.EcStrip?.segments;
        expect(segments.length).toBeGreaterThan(0);
        await expect(card.getByTestId('chunk-disk-block')).toHaveCount(segments.length);
      }
      const strip = ordered[0];
      const segment = (strip.strip.MirrorStrip?.segments ?? strip.strip.EcStrip?.segments)[0];
      const diskId = BigInt(segment.disk_id.high).toString(16).padStart(16, '0') + BigInt(segment.disk_id.low).toString(16).padStart(16, '0');
      const placement = detail.placements.find((entry: { disk_id: string }) => entry.disk_id.replace(/-/g, '').toLowerCase() === diskId);
      expect(placement).toBeDefined();
      const card = stripLayout.getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
      await card.getByRole('button', { name: /^(Mirror 1|Data 0)( · unavailable)?$/, exact: true }).click();
      const chunkProperties = page.getByLabel('Chunk properties', { exact: true });
      for (const [label, value] of [['Disk', diskId], ['Node', placement.node_id], ['Diskgroup', placement.disk_group_id], ['Zone', segment.zone_index], ['Zone offset (units)', segment.unit_offset]]) {
        await expect(chunkProperties.locator('dt').filter({ hasText: new RegExp(`^${label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) }).locator('..').locator('dd')).toHaveText(String(value));
      }
      await page.getByRole('button', { name: 'Back', exact: true }).click();
      await expect(page.getByRole('heading', { level: 1 })).toHaveText(key);
      await expect(locations.getByRole('button', { name: `Select extent ${extent.index}`, exact: true })).toHaveAttribute('aria-pressed', 'true');
      await expect(page.getByLabel('Storage extent properties')).toContainText(extent.chunk_id);
    });
    await step('native full object byte verification', async () => {
      const response = await request.get(objectPath);
      expect(response.status()).toBe(200);
      expect((await response.body()).equals(bytes)).toBe(true);
    });
    await step('native object delete and DOM refresh', async () => {
      page.once('dialog', dialog => dialog.accept());
      await page.getByRole('button', { name: 'Delete object', exact: true }).click();
      await expect(page.getByRole('table', { name: 'S3 objects' })).not.toContainText(key);
    });
  } finally {
    await step('native owned-resource teardown', async () => {
      const object = await request.delete(objectPath);
      expect(object.status(), await object.text()).toBe(204);
      const response = await request.delete(bucketPath);
      expect(response.status(), await response.text()).toBe(204);
    });
  }
});
