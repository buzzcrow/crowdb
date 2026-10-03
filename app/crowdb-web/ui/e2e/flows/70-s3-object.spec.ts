// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test.use({ actionTimeout: 3000 });

test.beforeEach(async ({ page }) => {
  await page.route('**/api/access/s3-inspect/locations?**', route => {
    const query = new URL(route.request().url()).searchParams;
    return route.fulfill({ json: { bucket: query.get('bucket'), key: query.get('key'), generation: 'a'.repeat(64), etag: 'etag', logical_length: '0', locations: [], next_cursor: null } });
  });
});

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
  await expect(page.getByRole('heading', { name: 'S3', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('s3 endpoint', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Save endpoint', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'List buckets', exact: true }).click();
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'demo-bucket', exact: true }).click();
  await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'a b/中文.txt', exact: true }).click();
  await page.getByText('Object actions', { exact: true }).click();
  await page.getByRole('button', { name: 'Preview first 4 KiB', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Loaded first 4 KiB' })).toBeVisible({ timeout: 3000 });
  expect(requests).toContainEqual({ method: 'GET', path: '/api/access/s3/demo-bucket/a%20b/%E4%B8%AD%E6%96%87.txt', range: 'bytes=0-4095' });
  expect(await page.getByLabel('Object preview', { exact: true }).textContent()).toHaveLength(4096);
  await page.getByTestId('domain-iceberg').click();
  await page.getByTestId('domain-s3').click();
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('a b/中文.txt');
  await page.getByRole('navigation', { name: 'S3 breadcrumbs' }).getByRole('button', { name: 'demo-bucket', exact: true }).click();
  await expect(page.getByRole('table', { name: 'S3 objects' })).toContainText('a b/中文.txt');
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('a b/中文.txt');
  await expect(page.getByLabel('Object metadata')).toContainText('native-etag');
  await page.getByRole('button', { name: 'Forward', exact: true }).click();
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
  await page.getByText('Bucket actions', { exact: true }).click();
  await page.getByRole('button', { name: 'List multipart uploads', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect parts', exact: true }).click();
  await page.getByRole('button', { name: 'Next parts page', exact: true }).click();
  await expect(page.getByRole('region', { name: 'Object metadata' })).toContainText('101');
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
    return route.fulfill({ contentType: 'application/xml', body: `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>${start + 20}</NextContinuationToken>${Array.from({ length: 20 }, (_, i) => `<Contents><Key>object-${start + i}</Key><Size>1</Size></Contents>`).join('')}</ListBucketResult>` });
  });
  await page.goto('/?domain=S3');
  await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
  await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
  await page.getByRole('button', { name: 'List buckets' }).click();
  const buckets = page.getByRole('navigation', { name: 'S3 buckets' });
  await expect(page.getByRole('table', { name: 'S3 bucket list' }).getByRole('row')).toHaveCount(21);
  await page.getByRole('button', { name: 'Next buckets' }).click();
  await buckets.getByRole('button', { name: 'bucket-20', exact: true }).click();
  const rows = page.getByRole('table', { name: 'S3 objects' }).getByRole('row');
  await expect(rows).toHaveCount(21);
  await page.getByRole('navigation', { name: 'Object pages' }).getByRole('button', { name: 'Next', exact: true }).click();
  await expect(rows).toHaveCount(21);
  await expect(page.getByRole('table', { name: 'S3 objects' })).toContainText('object-20');
  await page.getByRole('navigation', { name: 'Object pages' }).getByRole('button', { name: 'Previous', exact: true }).click();
  await expect(page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'object-0', exact: true })).toBeVisible();
  expect(objectRequests).toBe(3);
  await page.getByRole('navigation', { name: 'S3 breadcrumbs' }).getByRole('button', { name: 'S3', exact: true }).click();
  oversized = true;
  await page.getByRole('button', { name: 'List buckets' }).click();
  await expect(page.getByRole('alert').filter({ hasText: '4 MiB budget' })).toBeVisible();
});

// Metadata inspection never requests object payload or eagerly resolves disk placement.
test('S3 storage extents preserve exact integers, page windows and source selection on return', async ({ page }) => {
  const chunk = '0123456789abcdef0123456789abcdef';
  let stale = false;
  let payloadReads = 0;
  await page.route('**/api/access/connections', route => route.fulfill({ json: { s3: 'http://127.0.0.1:18000', configurable: false } }));
  await page.route('**/api/access/s3/**', route => {
    const request = route.request();
    const url = new URL(request.url());
    if (request.method() === 'HEAD') return route.fulfill({ headers: { etag: 'native-etag', 'content-length': '184' } });
    if (url.searchParams.has('list-type')) return route.fulfill({ contentType: 'application/xml', body: '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>exact.bin</Key><Size>184</Size></Contents></ListBucketResult>' });
    if (url.pathname.endsWith('/s3/')) return route.fulfill({ contentType: 'application/xml', body: '<ListAllMyBucketsResult><Buckets><Bucket><Name>extent-bucket</Name></Bucket></Buckets></ListAllMyBucketsResult>' });
    payloadReads++;
    return route.fulfill({ body: 'unexpected payload' });
  });
  await page.route('**/api/access/s3-inspect/locations?**', route => {
    expect(route.request().headers().authorization).toBeUndefined();
    const query = new URL(route.request().url()).searchParams;
    expect(query.get('limit')).toBe('20');
    if (stale && query.has('cursor')) return route.fulfill({ status: 409, json: { error: 'Object generation changed' } });
    const start = query.has('cursor') ? 20 : 0;
    return route.fulfill({ json: { bucket: 'extent-bucket', key: 'exact.bin', generation: 'a'.repeat(64), etag: 'etag', logical_length: '184',
      locations: Array.from({ length: start ? 3 : 20 }, (_, index) => ({ index: String(start + index), chunk_id: start + index === 22 ? null : chunk, offset: '9007199254740993', length: '8', logical_offset: String((start + index) * 8), logical_length: '8' })), next_cursor: start ? null : 'opaque-page-2' } });
  });
  await page.goto('/?domain=S3');
  await page.getByRole('button', { name: 'List buckets', exact: true }).click();
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: 'extent-bucket', exact: true }).click();
  await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'exact.bin', exact: true }).click();
  const locations = page.getByRole('region', { name: 'Storage locations', exact: true });
  await expect(locations.getByRole('table').getByRole('row')).toHaveCount(21);
  await locations.getByRole('button', { name: 'Next locations', exact: true }).click();
  await expect(locations.getByRole('table').getByRole('row')).toHaveCount(4);
  await expect(locations).toContainText('Location unavailable');
  await locations.getByRole('button', { name: 'Select extent 20', exact: true }).click();
  await expect(page.getByLabel('Storage extent properties')).toContainText('9007199254740993');
  // Choose a single row because several extents can reference the same Chunk.
  await locations.getByRole('row').filter({ has: page.getByRole('button', { name: 'Select extent 20', exact: true }) }).getByRole('button', { name: `Open Chunk ${chunk}`, exact: true }).click();
  await expect(page.getByTestId('domain-chunk')).toHaveAttribute('aria-pressed', 'true');
  await page.getByRole('button', { name: 'Back', exact: true }).click();
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('exact.bin');
  await expect(locations.getByRole('button', { name: 'Select extent 20', exact: true })).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByLabel('Storage extent properties')).toContainText('9007199254740993');
  await locations.getByRole('button', { name: 'Previous locations', exact: true }).click();
  await expect(locations.getByRole('table').getByRole('row')).toHaveCount(21);
  stale = true;
  await locations.getByRole('button', { name: 'Next locations', exact: true }).click();
  await expect(locations.getByRole('alert')).toContainText('Stale locations');
  await expect(locations.getByRole('button', { name: 'Next locations', exact: true })).toBeDisabled();
  await expect(page.getByLabel('Object metadata')).toContainText('native-etag');
  await locations.getByRole('button', { name: 'Refresh locations', exact: true }).click();
  await expect(locations.getByRole('alert')).toHaveCount(0);
  expect(payloadReads).toBe(0);
});
