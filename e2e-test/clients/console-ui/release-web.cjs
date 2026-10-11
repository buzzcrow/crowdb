// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

const { existsSync, mkdirSync } = require('node:fs');
const { writeFileSync, renameSync, readFileSync } = require('node:fs');
const { randomUUID } = require('node:crypto');
const { resolve } = require('node:path');
const { chromium, expect } = require('@playwright/test');

async function main() {
  const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || [
    '/snap/bin/chromium', '/usr/bin/chromium', '/usr/bin/chromium-browser',
    '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable', '/usr/bin/microsoft-edge',
  ].find(existsSync);
  if (!executablePath) throw new Error('Container acceptance requires an installed system browser');
  const browser = await chromium.launch({ executablePath, headless: true });
  try {
    const page = await browser.newPage();
    const pageErrors = [];
    page.on('pageerror', error => pageErrors.push(error.message));
    // Exercise the HTTP-origin path even when acceptance uses localhost.
    await page.addInitScript(() => {
      Object.defineProperty(crypto, 'randomUUID', { value: undefined });
    });
    await page.goto(process.argv[2]);
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible({ timeout: 3000 });
    for (const domain of ['cluster', 'kv', 'capacity', 'chunk', 'chunk-kv', 'iceberg', 's3']) {
      await expect(page.getByTestId(`domain-${domain}`)).toBeVisible({ timeout: 3000 });
    }
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
    if (process.argv[3]) {
      await verifyAuthorityOutage(page, process.argv[3]);
    }
    if (process.env.CROWDB_PREVIEW_TEST_ARTIFACTS) {
      mkdirSync(process.env.CROWDB_PREVIEW_TEST_ARTIFACTS, { recursive: true });
      await page.screenshot({ path: resolve(process.env.CROWDB_PREVIEW_TEST_ARTIFACTS, 'managed-web.png'), fullPage: true });
    }
    expect(pageErrors).toEqual([]);
    console.log('Container managed Web browser acceptance passed');
  } finally {
    await browser.close();
  }
}

async function verifyAuthorityOutage(page, container) {
  const pid = Number(process.env.CROWDB_E2E_KV_PID);
  await page.clock.install();
  await signalService('STOP', pid);
  try {
    const failedSnapshot = page.waitForResponse(response =>
      response.url().endsWith('/api/preview') && response.status() === 503,
    { timeout: 10000 });
    await page.clock.runFor(3001);
    const response = await failedSnapshot;
    const body = await response.json();
    expect(body.reason).toBe('group0_unavailable');
    expect(body.monitor.services.kv.pid).toBe(Number(pid));
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'N-1', exact: true })).toHaveCount(0, { timeout: 3000 });
  } finally {
    await signalService('CONT', pid);
  }
  await page.clock.runFor(3001);
  await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible({ timeout: 3000 });
}

async function signalService(signal, pid) {
  const id = randomUUID();
  const root = process.env.CROWDB_E2E_CONTROL;
  const request = resolve(root, `${id}.request.json`);
  const response = resolve(root, `${id}.response.json`);
  writeFileSync(request + '.tmp', JSON.stringify({ action: 'signal-kv', node: 0, signal, pid }));
  renameSync(request + '.tmp', request);
  const deadline = Date.now() + 10000;
  while (!existsSync(response)) {
    if (Date.now() >= deadline) throw new Error('Owned browser signal timed out');
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  const result = JSON.parse(readFileSync(response, 'utf8'));
  if (!result.ok) throw new Error(result.error);
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
