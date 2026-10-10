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
