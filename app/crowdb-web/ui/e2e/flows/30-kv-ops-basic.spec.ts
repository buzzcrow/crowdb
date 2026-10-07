// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: CRUD 1.2s, delete 0.742s, auto-scan 0.560s (2026-10-03).
// Baseline: binary byte runs 1.6s (2026-10-06).

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import { addGroup, createStore, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer, waitForLeader } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';
import type { APIRequestContext } from '@playwright/test';

const dataPath = '/api/stores/99/groups/990/kv';
async function seedRows(request: APIRequestContext, rows: Array<{ key_hex: string; value_hex: string }>) {
  await step('kv: seed native byte records', async () => {
    for (const row of rows) {
      const response = await request.post(`${dataPath}/put`, { data: row });
      expect(response.ok(), await response.text()).toBe(true);
    }
  });
}
async function removeRows(request: APIRequestContext, rows: Array<{ key_hex: string }>) {
  await step('kv: remove native byte records', async () => {
    for (const row of rows) {
      const response = await request.post(`${dataPath}/delete`, { data: { key_hex: row.key_hex } });
      expect(response.ok(), await response.text()).toBe(true);
    }
  });
}

// One rack/node/server/store/group shared by every test in this file
// (IDs reused from the former 09-kv-put-get spec so they stay unique).
const apiBase = consoleBaseURL();

async function openKvPanel(page: any) {
  await step('kv: goto', () => page.goto('/'));
  await page.getByTestId('domain-kv').click();
  await page.getByTestId('kv-store-select').selectOption('99');
  await page.getByTestId('kv-group-select').selectOption('990');
}

test.describe('kv ops · put/get/scan/delete', () => {
  test.beforeAll(async () => {
    // Reset the cluster so a re-run (e.g. worker restart) deploys fresh:
    // stopNodeServer alone keeps the config entry, which makes the next
    // deployNodeServer fail with 409 "already hosts a deployed server".
    await step('kv: resetAll', () => resetAll(apiBase));
    try {
      await step('kv: seed rack/node', () => seedRackAndNode(apiBase, 9, 9));
    } catch (err) {
      if (!String(err).includes('already exists')) throw err;
    }
    try {
      await step('kv: deploy server', () => deployNodeServer(apiBase, 9, freePort(), freePort()));
      await step('kv: create store', () => createStore(apiBase, 99, [9]));
      await step('kv: add group', () => addGroup(apiBase, 99, 990, 9900, [9]));
      await step('kv: wait for leader', () => waitForLeader(apiBase, 99, 990));
    } catch (err) {
      await stopNodeServer(apiBase, 9);
      throw err;
    }
  });

  test.afterAll(async () => {
    await step('kv: stop server', () => stopNodeServer(apiBase, 9));
  });

  test('Group selection opens data directly; pages replace rows and expose complete values', async ({ page, request }) => {
    const records = Array.from({ length: 60 }, (_, i) => ({
      key_hex: Buffer.from(`key-${String(i).padStart(3, '0')}`).toString('hex'),
      value_hex: Buffer.from(`complete value ${i}`).toString('hex'),
    }));
    await seedRows(request, records);
    try {
      await openKvPanel(page);
      await expect(page.getByRole('navigation', { name: 'KV views' })).toHaveCount(0);
      const rows = page.getByTestId('kv-scan-table').locator('tbody tr');
      await expect(rows).toHaveCount(20);
      await page.getByTestId('kv-scan-table').getByText('key-000', { exact: true }).click();
      await expect(page.getByRole('region', { name: 'Selected key' })).toContainText('complete value 0');
      const pages = page.getByRole('navigation', { name: 'Key pages' });
      await pages.getByRole('button', { name: 'Next', exact: true }).click();
      await expect(rows).toHaveCount(20);
      await expect(rows).toContainText(Array.from({ length: 20 }, (_, i) => `key-${String(20 + i).padStart(3, '0')}`));
      await page.getByTestId('kv-scan-table').getByText('key-020', { exact: true }).click();
      await page.getByTestId('domain-capacity').click();
      await page.getByRole('button', { name: 'Back', exact: true }).click();
      await expect(pages).toContainText('Page 2');
      await expect(page.getByRole('region', { name: 'Selected key' })).toContainText('complete value 20');
      await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'S-99', exact: true }).click();
      await expect(pages).toContainText('Page 1');
      await page.getByRole('button', { name: 'Back', exact: true }).click();
      await expect(pages).toContainText('Page 2');
      await expect(page.getByRole('region', { name: 'Selected key' })).toContainText('complete value 20');
      await pages.getByRole('button', { name: 'Previous', exact: true }).click();
      await expect(rows).toContainText(Array.from({ length: 20 }, (_, i) => `key-${String(i).padStart(3, '0')}`));
    } finally { await removeRows(request, records); }
  });

  test('binary byte runs retain printable fields and use distinct uppercase hex without a prefix', async ({ page, request }) => {
    const records = [
      { key_hex: '6669656c6400ff656e64', value_hex: '68656c6c6ffe8001e4b8ade69687' },
      { key_hex: 'e4b8ade69687', value_hex: '68656c6c6f' },
    ];
    await seedRows(request, records);
    try {
      await openKvPanel(page);
      const table = page.getByTestId('kv-scan-table');
      await expect(table).toContainText('field00FFend');
      await expect(table).toContainText('helloFE8001中文');
      await expect(table).not.toContainText('0x');
      await expect(table.locator('[data-byte-format="hex"]')).toHaveText(['00FF', 'FE8001']);
      for (const run of await table.locator('[data-byte-format="hex"]').all()) {
        await expect(run).toHaveCSS('color', 'rgb(133, 133, 133)');
        await expect(run).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
        await expect(run).toHaveCSS('border-width', '0px');
        await expect(run).toHaveCSS('padding', '0px');
        await expect(run).toHaveCSS('margin', '0px');
      }
      await table.getByText('field', { exact: true }).click();
      const detail = page.getByRole('region', { name: 'Selected key' });
      await expect(detail).toContainText('helloFE8001中文');
      await expect(detail.locator('[data-byte-format="hex"]')).toHaveText(['00FF', 'FE8001']);
      for (const run of await detail.locator('[data-byte-format="hex"]').all()) {
        await expect(run).toHaveCSS('color', 'rgb(133, 133, 133)');
        await expect(run).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
        await expect(run).toHaveCSS('border-width', '0px');
      }
    } finally { await removeRows(request, records); }
  });




  test('auto-scan fires on initial group selection (G-0 shows data without manual Scan)', async ({ page, request }) => {
    // Regression: selecting a group used to not auto-scan on the
    // first selection because a `userSelectedRef` guard suppressed
    // the scan until the user manually changed the group. This left
    // G-0 (the system store) showing no data until the user clicked
    // Scan. The fix removed the guard so auto-scan fires on every
    // group selection, including the initial one.
    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(String(err)));

    // --- Part 1: G-0 (system store) auto-scans on initial selection ---
    // G-0 is read-only (no Put/Delete), but Scan + Get work. The
    // auto-scan must fire when the user selects store 0 / group 0
    // without clicking Scan.
    await step('kv: goto + select G-0', async () => {
      await page.goto('/');
      await page.getByTestId('domain-kv').click();
      // Select store 0 (system store) and group 0. The auto-scan
      // checkbox is ON by default, so selecting the group must
      // trigger a scan automatically — no manual Scan click needed.
      const scanResponse = page.waitForResponse(
        (response) => response.url().includes('/stores/0/groups/0/kv/scan'),
        { timeout: 10_000 },
      );
      await page.getByTestId('kv-store-select').selectOption('0');
      await page.getByTestId('kv-group-select').selectOption('0');
      await scanResponse;
    });

    // The scan table must be visible — even if G-0 has 0 rows, the
    // table DOM is rendered after scan completes. The key assertion
    // is that a scan fired and the table appeared without clicking Scan.
    await expect(page.getByTestId('kv-scan-table')).toBeVisible({ timeout: 3_000 });
    // G-0 is read-only — the system group warning must be visible.
    await expect(page.getByText(/system group.*topology metadata/i)).toBeVisible({ timeout: 3_000 });

    // --- Part 2: writable group auto-scans on re-selection ---
    // Switch to store 99 / group 990, seed a key, then re-select the
    // group to verify auto-scan fires again and picks up the new key.
    await step('kv: seed G-990 through API', async () => {
      await page.getByTestId('kv-store-select').selectOption('99');
      await page.getByTestId('kv-group-select').selectOption('990');
      const putResp = await request.post(`${dataPath}/put`, { data: { key: 'autoscan-reselect-key', value: 'autoscan-reselect-value' } });
      expect(putResp.ok(), await putResp.text()).toBeTruthy();
    });

    // Re-select group 990 — auto-scan must fire and show the key.
    await step('kv: re-select G-990 triggers auto-scan', async () => {
      const rescanResponse = page.waitForResponse(
        (response) => response.url().includes('/stores/99/groups/990/kv/scan'),
        { timeout: 10_000 },
      );
      await page.getByTestId('kv-group-select').selectOption('990');
      await rescanResponse;
      await expect(page.getByTestId('kv-scan-table').getByText('autoscan-reselect-key')).toBeVisible({ timeout: 3_000 });
    });

    expect(errors, errors.join('\n')).toHaveLength(0);
  });
});
