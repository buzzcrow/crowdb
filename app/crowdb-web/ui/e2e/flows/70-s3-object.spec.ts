// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test('S3 signs native requests, preserves keys and bounds object previews', async ({ page }) => {
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: null, s3: 'http://127.0.0.1:18000', configurable: false } }));
  const requests: Array<{ method: string; path: string; range?: string }> = [];
  await page.route('**/api/access/s3/**', route => {
    const request = route.request();
    const url = new URL(request.url());
    expect(request.headers().authorization).toMatch(/^AWS4-HMAC-SHA256 Credential=demo\//);
    expect(request.headers()['x-amz-content-sha256']).toMatch(/^[a-f0-9]{64}$/);
    requests.push({ method: request.method(), path: url.pathname, range: request.headers().range });
    if (request.method() === 'HEAD') return route.fulfill({ status: 200, headers: { 'content-length': '99999', etag: 'native-etag' } });
    if (!url.search) return route.fulfill({ contentType: 'application/xml', body: url.pathname.endsWith('/s3/')
      ? '<ListAllMyBucketsResult><Buckets><Bucket><Name>demo-bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>'
      : 'x'.repeat(10000) });
    return route.fulfill({ contentType: 'application/xml', body: '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a b/中文.txt</Key><Size>99999</Size><ETag>native-etag</ETag></Contents></ListBucketResult>' });
  });
  await page.goto('/?domain=S3');
  await page.getByLabel('Access key', { exact: true }).fill('demo');
  await page.getByLabel('Secret key', { exact: true }).fill('secret');
  await page.getByRole('button', { name: 'List buckets', exact: true }).click();
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'demo-bucket', exact: true }).click();
  await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'a b/中文.txt', exact: true }).click();
  await page.getByRole('button', { name: 'Preview first 4 KiB', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Loaded first 4 KiB' })).toBeVisible({ timeout: 3000 });
  expect(requests).toContainEqual({ method: 'GET', path: '/api/access/s3/demo-bucket/a%20b/%E4%B8%AD%E6%96%87.txt', range: 'bytes=0-4095' });
  const preview = await page.locator('main aside pre').textContent();
  expect(JSON.parse(preview!).preview).toHaveLength(4096);
  await page.getByTestId('domain-iceberg').click();
  await page.getByTestId('domain-s3').click();
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveValue('secret');
  await expect(page.getByRole('table', { name: 'S3 objects' })).toContainText('a b/中文.txt');
});


test('S3 multipart parts use native markers and credential changes clear resource scope', async ({ page }) => {
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: null, s3: 'http://127.0.0.1:18000', configurable: false } }));
  await page.route('**/api/access/s3/**', route => {
    const url = new URL(route.request().url());
    let body = '<ListAllMyBucketsResult><Buckets><Bucket><Name>parts-bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>';
    if (url.searchParams.has('list-type')) body = '<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>';
    if (url.searchParams.has('uploads')) body = '<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>pending.bin</Key><UploadId>native-upload</UploadId></Upload></ListMultipartUploadsResult>';
    if (url.searchParams.has('uploadId')) {
      expect(url.searchParams.get('max-parts')).toBe('100');
      const next = url.searchParams.get('part-number-marker') === '100';
      body = `<ListPartsResult><IsTruncated>${!next}</IsTruncated><NextPartNumberMarker>100</NextPartNumberMarker><Part><PartNumber>${next ? 101 : 1}</PartNumber><ETag>part-etag</ETag><Size>8388608</Size></Part></ListPartsResult>`;
    }
    return route.fulfill({ contentType: 'application/xml', body });
  });
  await page.goto('/?domain=S3');
  await page.getByLabel('Access key', { exact: true }).fill('demo');
  await page.getByLabel('Secret key', { exact: true }).fill('secret');
  await page.getByRole('button', { name: 'List buckets', exact: true }).click();
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'parts-bucket', exact: true }).click();
  await page.getByRole('button', { name: 'List multipart uploads', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect parts', exact: true }).click();
  await page.getByRole('button', { name: 'Next parts page', exact: true }).click();
  await expect(page.locator('main aside pre')).toContainText('101');
  await expect(page.getByRole('button', { name: 'Next parts page', exact: true })).toHaveCount(0);
  await page.getByLabel('Secret key', { exact: true }).fill('different-secret');
  await expect(page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button')).toHaveCount(0);
  await expect(page.getByRole('heading', { name: 'S3 object browser', exact: true })).toBeVisible();
});
