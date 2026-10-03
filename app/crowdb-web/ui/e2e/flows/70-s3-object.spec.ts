// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test.use({ actionTimeout: 3000 });

// Baseline: 0.683s (2026-10-03).
test('Root S3 sends credential-free Console requests, preserves keys and bounds object previews', async ({ page }) => {
  await page.route('**/api/access/connections', route => route.fulfill({ json: { iceberg: null, s3: 'http://127.0.0.1:18000', configurable: false } }));
  const requests: Array<{ method: string; path: string; range?: string }> = [];
  await page.route('**/api/access/s3/**', route => {
    const request = route.request();
    const url = new URL(request.url());
    expect(request.headers().authorization).toBeUndefined();
    expect(request.headers()['x-amz-content-sha256']).toBeUndefined();
    requests.push({ method: request.method(), path: url.pathname, range: request.headers().range });
    if (request.method() === 'HEAD') return route.fulfill({ status: 200, headers: { 'content-length': '99999', etag: 'native-etag' } });
    if (!url.search) return route.fulfill({ contentType: 'application/xml', body: url.pathname.endsWith('/s3/')
      ? '<ListAllMyBucketsResult><Buckets><Bucket><Name>demo-bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>'
      : 'x'.repeat(10000) });
    return route.fulfill({ contentType: 'application/xml', body: '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>a b/中文.txt</Key><Size>99999</Size><ETag>native-etag</ETag></Contents></ListBucketResult>' });
  });
  await page.goto('/?domain=S3');
  await expect(page.getByLabel('s3 endpoint', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Save endpoint', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
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
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('table', { name: 'S3 objects' })).toContainText('a b/中文.txt');
});


// Baseline: 0.609s (2026-10-03).
test('S3 multipart parts use native markers without credential forms', async ({ page }) => {
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
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'List buckets', exact: true }).click();
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'parts-bucket', exact: true }).click();
  await page.getByRole('button', { name: 'List multipart uploads', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect parts', exact: true }).click();
  await page.getByRole('button', { name: 'Next parts page', exact: true }).click();
  await expect(page.locator('main aside pre')).toContainText('101');
  await expect(page.getByRole('button', { name: 'Next parts page', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);

});

test('S3 cluster deployment retry never offers an endpoint editor', async ({ page }) => {
  let ready = false;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { s3: ready ? 'http://127.0.0.1:18000' : null, configurable: true } }));
  await page.goto('/?domain=S3');
  await expect(page.getByRole('alert').filter({ hasText: 'Deploy Access Server in Cluster' })).toBeVisible();
  await expect(page.getByLabel('s3 endpoint', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'List buckets' })).toBeDisabled();
  ready = true;
  await page.getByRole('button', { name: 'Retry cluster S3' }).click();
  await expect(page.getByRole('button', { name: 'Retry cluster S3' })).toHaveCount(0);
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'List buckets' })).toBeEnabled();
});

test('S3 bounds bucket rendering, accumulated objects and XML responses', async ({ page }) => {
  let oversized = false;
  let objectRequests = 0;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { s3: 'http://127.0.0.1:18000', configurable: true } }));
  await page.route('**/api/access/s3/**', route => {
    const url = new URL(route.request().url());
    if (oversized) return route.fulfill({ contentType: 'application/xml', body: `<Buckets>${' '.repeat(4 * 1024 * 1024)}</Buckets>` });
    if (!url.search) return route.fulfill({ contentType: 'application/xml', body: `<ListAllMyBucketsResult><Buckets>${Array.from({ length: 150 }, (_, i) => `<Bucket><Name>bucket-${i}</Name></Bucket>`).join('')}</Buckets></ListAllMyBucketsResult>` });
    const start = Number(url.searchParams.get('continuation-token') ?? 0);
    objectRequests++;
    return route.fulfill({ contentType: 'application/xml', body: `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>${start + 100}</NextContinuationToken>${Array.from({ length: 100 }, (_, i) => `<Contents><Key>object-${start + i}</Key><Size>1</Size></Contents>`).join('')}</ListBucketResult>` });
  });
  await page.goto('/?domain=S3');
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'List buckets' }).click();
  const buckets = page.getByRole('navigation', { name: 'S3 buckets' });
  await expect(buckets.getByRole('button')).toHaveCount(100);
  await page.getByRole('button', { name: 'Next buckets' }).click();
  await expect(buckets.getByRole('button')).toHaveCount(50);
  await buckets.getByRole('button', { name: 'bucket-100', exact: true }).click();
  const rows = page.getByRole('table', { name: 'S3 objects' }).getByRole('row');
  await expect(rows).toHaveCount(101);
  for (let i = 2; i <= 10; i++) {
    await page.getByRole('button', { name: 'Load more', exact: true }).click();
    await expect(rows).toHaveCount(i * 100 + 1);
  }
  await expect(page.getByRole('button', { name: 'Load more', exact: true })).toHaveCount(0);
  await expect(page.getByRole('status').filter({ hasText: 'Showing 1,000 objects' })).toBeVisible();
  expect(objectRequests).toBe(10);
  oversized = true;
  await page.getByRole('button', { name: 'List buckets' }).click();
  await expect(page.getByRole('alert').filter({ hasText: '4 MiB budget' })).toBeVisible();
});
