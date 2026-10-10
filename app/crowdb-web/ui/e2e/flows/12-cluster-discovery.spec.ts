// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 3.5s (2026-10-10)

import { spawn, type ChildProcess } from 'node:child_process';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { test, expect } from '../fixtures/realBackend';
import { freePort } from '../fixtures/consoleSetup';

test('discovery: real local monitor appears below the cluster tree and becomes stale on loss', async ({ page, request }) => {
  const root = await mkdtemp(join(tmpdir(), 'crowdb-discovery-ui-'));
  const monitorPort = await freePort();
  const webPort = await freePort();
  const monitorUrl = `http://127.0.0.1:${monitorPort}`;
  const webUrl = `http://127.0.0.1:${webPort}`;
  const children: ChildProcess[] = [];
  const output: string[] = [];
  const start = (binary: string, args: string[]) => {
    const child = spawn(binary, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout?.on('data', chunk => output.push(String(chunk)));
    child.stderr?.on('data', chunk => output.push(String(chunk)));
    children.push(child);
    return child;
  };
  const stop = async (child: ChildProcess) => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    await new Promise<void>((done, reject) => {
      const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('Child did not stop')); }, 3000);
      child.once('exit', () => { clearTimeout(timer); done(); });
      child.kill('SIGTERM');
    });
  };
  try {
    await mkdir(join(root, 'node'));
    const monitor = start(process.env.CROWDB_MONITOR_BINARY ?? resolve('../../../target/debug/crowdb-monitor'), [
      'node', '--data-root', join(root, 'node'), '--bind', `127.0.0.1:${monitorPort}`,
      '--interface', 'lo', '--advertise', '127.0.0.1', '--physical-host-id', 'discovery-ui-host',
    ]);
    start(process.env.CROWDB_WEB_BINARY ?? resolve('../../../target/debug/crowdb-web'), [
      '--bind', '127.0.0.1', '--port', String(webPort), '--runtime-dir', join(root, 'web'),
      '--node-monitor', monitorUrl, '--skip-startup-restore',
    ]);
    await expect.poll(async () => {
      try { return (await request.get(`${webUrl}/api/node/candidates`)).status(); }
      catch { return 0; }
    }, { intervals: [100] }).toBe(200);
    const snapshot = await (await request.get(`${webUrl}/api/node/candidates`)).json();
    await page.goto(`${webUrl}/?domain=Cluster`);
    const candidates = page.getByRole('region', { name: 'Candidate nodes' });
    const local = candidates.getByTestId(`candidate-${snapshot.local.advertisement.discovery_id}`);
    await expect(local).toContainText('Available · This node');
    await expect(local).toContainText(monitorUrl);
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
    await expect(candidates.getByRole('combobox', { name: 'Discovered cluster' })).toBeVisible();
    await candidates.getByRole('combobox', { name: 'Discovered cluster' }).selectOption('unbound');
    await expect(local).toBeVisible();
    await stop(monitor);
    await expect(candidates.getByRole('status')).toContainText('last observations shown');
    await expect(local).toBeVisible();
  } catch (error) {
    throw new Error(`${String(error)}\n${output.join('')}`);
  } finally {
    for (const child of children.reverse()) await stop(child);
    await rm(root, { recursive: true, force: true });
  }
});

// Docker supplies the real node roots, SSH endpoints and candidate identities.
// Baseline: 7.5s (2026-10-10; preparation phase).
test('Docker candidates: prepare nodes in UI and initialize their shared cluster', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_ORIGIN, 'Run through the Docker node acceptance harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const identities: string[] = JSON.parse(process.env.CROWDB_NODE_UI_IDENTITIES!);
  await page.goto(`${origin}/?domain=Cluster`);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  await expect(candidates.getByTestId('deployment-phase')).toContainText('Uncommitted local draft');
  await page.getByRole('button', { name: 'Add Rack', exact: true }).click();
  const rack = page.getByRole('dialog', { name: 'Add Rack' });
  await rack.getByLabel('Rack ID', { exact: true }).fill('1');
  await rack.getByLabel('Name (optional)', { exact: true }).fill('rack-one');
  await rack.getByRole('button', { name: 'Create Rack', exact: true }).click();
  await expect(rack).toHaveCount(0);
  for (const id of identities) {
    const candidate = candidates.getByTestId(`candidate-${id}`);
    await candidate.getByRole('button', { name: 'Move to cluster', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Move candidate to cluster' });
    await expect(dialog.getByLabel('SSH user', { exact: true })).toHaveValue('crowdb');
    await expect(dialog.getByLabel('Initial SSH password', { exact: true })).toHaveValue('crowdb');
    await dialog.getByRole('button', { name: 'Verify and move', exact: true }).click();
    // Admission performs authenticated key exchange and proves every peer pair.
    await expect(dialog).toHaveCount(0, { timeout: 10_000 });
    await expect(candidate).toContainText('Prepared or joining');
  }
  await page.getByTestId('domain-kv').click();
  await page.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  const initialize = page.getByRole('dialog', { name: 'Initialize Cluster' });
  await expect(initialize.getByRole('checkbox')).toHaveCount(identities.length);
  for (const checkbox of await initialize.getByRole('checkbox').all()) {
    await expect(checkbox).toBeChecked();
  }
  if (process.env.CROWDB_NODE_UI_PREPARE_ONLY) return;
  await initialize.getByRole('button', { name: 'Initialize Cluster', exact: true }).click();
  // Group 0 must elect a leader and publish topology before becoming active.
  await expect.poll(async () => {
    const response = await request.get(`${origin}/api/node/status`);
    return (await response.json()).phase;
  }, { timeout: 10_000, intervals: [100] }).toBe('active');
  await expect(initialize).toHaveCount(0);
  await page.getByTestId('domain-cluster').click();
  await expect(candidates.getByTestId('deployment-phase')).toHaveText('Active cluster');
  for (const id of identities) {
    await expect(candidates.getByTestId(`candidate-${id}`)).toContainText('Admitted');
  }
});

// Baseline: 4.7s (2026-10-10).
test('Docker recovery: another UI resumes the fixed bootstrap', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_RECOVERY_ORIGIN, 'Run through the interrupted Docker bootstrap harness');
  const origin = process.env.CROWDB_NODE_UI_RECOVERY_ORIGIN!;
  const before = await (await request.get(`${origin}/api/node/status`)).json();
  expect(before.phase).toBe('bootstrap_in_progress');
  await page.goto(`${origin}/?domain=Cluster`);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  await expect(candidates.getByTestId('deployment-phase')).toContainText('submitted inputs are fixed');
  const edit = await request.post(`${origin}/api/racks`, { data: { id: 99, name: 'sealed-edit' } });
  expect(edit.status()).toBe(409);
  await candidates.getByRole('button', { name: 'Resume bootstrap', exact: true }).click();
  const resume = page.getByRole('dialog', { name: 'Resume fixed bootstrap' });
  await resume.getByRole('button', { name: 'Resume', exact: true }).click();
  await expect.poll(async () => {
    const status = await (await request.get(`${origin}/api/node/status`)).json();
    expect(status.cluster_id).toBe(before.cluster_id);
    return status.phase;
  }, { timeout: 10_000, intervals: [100] }).toBe('active');
  await expect(resume).toHaveCount(0);
  await expect(candidates.getByTestId('deployment-phase')).toHaveText('Active cluster');
});


// Baseline: 2.4s (2026-10-10).
test('Docker independent cluster: select and explicitly clean the other cluster', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_FOREIGN_ORIGIN, 'Run through the disjoint cluster Docker harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const foreign = process.env.CROWDB_NODE_UI_FOREIGN_ORIGIN!;
  const id = process.env.CROWDB_NODE_UI_FOREIGN_UUID!;
  const other = await (await request.get(`${foreign}/api/node/status`)).json();
  const retained = await (await request.get(`${origin}/api/node/status`)).json();
  expect(other.cluster_id).not.toBe(retained.cluster_id);
  await page.goto(`${origin}/?domain=Cluster`);
  const candidates = page.getByRole('region', { name: 'Candidate nodes' });
  await expect(candidates.getByTestId(`candidate-${id}`)).toContainText('Another cluster');
  await candidates.getByRole('combobox', { name: 'Discovered cluster' }).selectOption(other.cluster_id);
  const candidate = candidates.getByTestId(`candidate-${id}`);
  await expect(candidate).toBeVisible();
  await expect(candidate.getByRole('button', { name: 'Move to cluster', exact: true })).toHaveCount(0);
  await expect(candidate.getByRole('link', { name: 'Open this cluster UI', exact: true })).toBeVisible();
  await page.goto(`${foreign}/?domain=Cluster`);
  await page.getByRole('region', { name: 'Candidate nodes' }).getByRole('button', { name: 'Clean up Group 0', exact: true }).click();
  const cleanup = page.getByRole('dialog', { name: 'Delete Group 0 and system store' });
  await cleanup.getByRole('button', { name: 'Delete and release bindings', exact: true }).click();
  await expect(cleanup).toHaveCount(0, { timeout: 10_000 });
  await expect.poll(async () => (await (await request.get(`${foreign}/api/node/status`)).json()).phase,
    { intervals: [100] }).toBe('unbound_draft');
  expect((await (await request.get(`${origin}/api/node/status`)).json()).cluster_id).toBe(retained.cluster_id);
});

// Baseline: 4.9s (2026-10-10).
test('Docker node update: authenticated rack and endpoint changes preserve allocation', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NODE_UI_UPDATE_UUID, 'Run through the persistent IP change Docker harness');
  const origin = process.env.CROWDB_NODE_UI_ORIGIN!;
  const id = process.env.CROWDB_NODE_UI_UPDATE_UUID!;
  const before = (await (await request.get(`${origin}/api/node/admissions`)).json()).find((node: { discovery_id: string }) => node.discovery_id === id);
  await page.goto(`${origin}/?domain=Cluster`);
  const candidate = page.getByRole('region', { name: 'Candidate nodes' }).getByTestId(`candidate-${id}`);
  await candidate.getByRole('button', { name: 'Update node', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Update cluster node' });
  await expect(dialog.getByLabel('Initial SSH password', { exact: true })).toHaveValue('');
  await dialog.getByLabel('Rack', { exact: true }).selectOption('2');
  await dialog.getByRole('button', { name: 'Verify and update', exact: true }).click();
  await expect(dialog).toHaveCount(0, { timeout: 10_000 });
  const after = (await (await request.get(`${origin}/api/node/admissions`)).json()).find((node: { discovery_id: string }) => node.discovery_id === id);
  expect(after.node_id).toBe(before.node_id);
  expect(after.physical_host_id).toBe(before.physical_host_id);
  expect(after.rack_id).toBe(2);
  expect(after.host).not.toBe(before.host);
});
