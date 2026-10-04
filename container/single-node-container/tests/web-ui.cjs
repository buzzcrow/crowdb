// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

const { existsSync, mkdirSync } = require('node:fs');
const { execFileSync } = require('node:child_process');
const { resolve } = require('node:path');
const { chromium, expect } = require('../../../app/crowdb-web/ui/node_modules/@playwright/test');

async function main() {
  const executablePath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || [
    '/snap/bin/chromium', '/usr/bin/chromium', '/usr/bin/chromium-browser',
    '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable', '/usr/bin/microsoft-edge',
  ].find(existsSync);
  if (!executablePath) throw new Error('Container acceptance requires an installed system browser');
  const browser = await chromium.launch({ executablePath, headless: true });
  try {
    const page = await browser.newPage();
    await page.goto(process.argv[2]);
    await expect(page.getByTestId('managed-source')).toHaveText('Source: Group 0', { timeout: 3000 });
    await expect(page.getByTestId('managed-readonly')).toHaveText('Hardware topology is read-only', { timeout: 3000 });
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible({ timeout: 3000 });
    for (const domain of ['cluster', 'kv', 'capacity', 'chunk', 'chunk-kv', 'iceberg', 's3']) {
      await expect(page.getByTestId(`domain-${domain}`)).toBeVisible({ timeout: 3000 });
    }
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
    await expect(page.getByTestId('managed-monitor-phase')).toContainText('Phase: ready', { timeout: 3000 });
    for (const service of ['kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access', 'web']) {
      await expect(page.getByTestId(`managed-process-${service}`)).toContainText(/PID \d+ · generation \d+/, { timeout: 3000 });
    }
    await expect(page.getByTestId('managed-unavailable')).toHaveCount(0, { timeout: 3000 });
    if (process.argv[3]) {
      await verifyAuthorityOutage(page, process.argv[3]);
    }
    if (process.env.CROWDB_PREVIEW_TEST_ARTIFACTS) {
      mkdirSync(process.env.CROWDB_PREVIEW_TEST_ARTIFACTS, { recursive: true });
      await page.screenshot({ path: resolve(process.env.CROWDB_PREVIEW_TEST_ARTIFACTS, 'managed-web.png'), fullPage: true });
    }
    console.log('Container managed Web browser acceptance passed');
  } finally {
    await browser.close();
  }
}

async function verifyAuthorityOutage(page, container) {
  const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8', timeout: 10000 });
  const status = JSON.parse(docker('exec', container, 'cat', '/opt/crowdb/run/status/monitor.json'));
  const pid = String(status.services.kv.pid);
  await page.clock.install();
  docker('exec', container, 'kill', '-STOP', pid);
  try {
    const failedSnapshot = page.waitForResponse(response =>
      response.url().endsWith('/api/preview') && response.status() === 503,
    { timeout: 10000 });
    await page.clock.runFor(3001);
    const response = await failedSnapshot;
    const body = await response.json();
    expect(body.reason).toBe('group0_unavailable');
    expect(body.monitor.services.kv.pid).toBe(Number(pid));
    await expect(page.getByTestId('managed-unavailable')).toContainText('Group 0 is unavailable', { timeout: 3000 });
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'N-1', exact: true })).toHaveCount(0, { timeout: 3000 });
    await expect(page.getByRole('region', { name: 'Monitor status' })).toBeVisible({ timeout: 3000 });
  } finally {
    docker('exec', container, 'kill', '-CONT', pid);
  }
  await page.clock.runFor(3001);
  await expect(page.getByTestId('managed-unavailable')).toHaveCount(0, { timeout: 3000 });
  await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible({ timeout: 3000 });
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
