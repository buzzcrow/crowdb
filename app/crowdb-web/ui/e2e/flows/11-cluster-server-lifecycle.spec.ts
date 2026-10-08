// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 9s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, createNode, createRack, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';
import { readFileSync } from 'node:fs';

test('native diagnostics: auxiliary menus stop and restart the actual typed process', async ({ page, request }) => {
  const alive = (pid: number) => {
    if (process.platform === 'linux') {
      try { return !readFileSync(`/proc/${pid}/stat`, 'utf8').split(')').pop()!.trimStart().startsWith('Z'); }
      catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false; throw error; }
    }
    try { process.kill(pid, 0); return true; }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false; throw error; }
  };
  const list = async () => {
    const response = await request.get('/api/servers');
    expect(response.ok(), await response.text()).toBe(true);
    return response.json();
  };
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const [kind, label, prefix] of [['chunkdb', 'crowdb-chunk-db', 'CDB'], ['diskio', 'crowdb-disk-io', 'DIO'], ['chunk-kv', 'crowdb-chunk-kv', 'CKV'], ['access-server', 'crowdb-access-server', 'AS']]) {
    const original = (await list()).find((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind);
    expect(original.pid).toBeGreaterThan(0); expect(alive(original.pid)).toBe(true);
    const node = sidebar.getByRole('button', { name: 'N-1', exact: true });
    const item = sidebar.getByText(`${prefix}-1`, { exact: true });
    await expect(item).toBeVisible();
    await node.click({ button: 'right' });
    await page.getByRole('menuitem', { name: `${prefix}-1 Running`, exact: true }).hover();
    const stoppedObservation = page.waitForResponse(async response => {
      if (new URL(response.url()).pathname !== '/api/servers' || !response.ok()) return false;
      const rows = await response.json();
      return rows.some((row: { id: string; pid?: number }) => row.id === original.id && !row.pid);
    });
    await page.getByRole('menuitem', { name: `Stop ${label}`, exact: true }).click();
    await stoppedObservation;
    await expect(page.getByRole('button', { name: 'Refresh', exact: true }).locator('svg')).not.toHaveClass(/tw-animate-spin/);
    await expect.poll(() => alive(original.pid), { intervals: [100] }).toBe(false);
    await expect.poll(async () => {
      const stopped = (await list()).find((row: { id: string }) => row.id === original.id);
      return { id: stopped?.id, pid: stopped?.pid ?? null };
    }, { intervals: [100] }).toEqual({ id: original.id, pid: null });
    try {
      await node.click({ button: 'right' });
      await page.getByRole('menuitem', { name: `${prefix}-1 Stopped`, exact: true }).hover();
      await page.getByRole('menuitem', { name: `Start ${label}`, exact: true }).click();
      await expect.poll(async () => {
        const current = (await list()).find((row: { id: string }) => row.id === original.id);
        return current?.pid && current.pid !== original.pid && alive(current.pid);
      }, { intervals: [100] }).toBe(true);
      expect((await list()).filter((row: { id: string }) => row.id === original.id)).toHaveLength(1);
    } finally {
      const current = (await list()).find((row: { id: string }) => row.id === original.id);
      if (!current?.pid) expect((await request.post(`/api/services/${original.id}/restart`, { data: {} })).ok()).toBe(true);
    }
  }
  expect(await list()).toHaveLength(18);
});

test.describe('cluster · server lifecycle', () => {
  test('context menu items differ for node without server, node with server, and server', async ({ page, baseURL }) => {
    // --- Node without server: Deploy CrowDB Storage + Delete Node, no restart/stop ---
    await step('ctx-menu: resetAll', () => resetAll(baseURL!));
    {
      await step('ctx-menu: setup no-server', async () => {
        await createRack(baseURL!, { id: 490, name: 'Rack 490' });
        await createNode(baseURL!, { id: 490, rack_id: 490 });
      });

      await step('ctx-menu: no-server UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const expandRack = aside.getByRole('treeitem').filter({ hasText: 'R-490' }).locator('button[aria-label="Expand"]');
        if (await expandRack.count() > 0) await expandRack.click();
        await expect(aside.getByText('N-490', { exact: true })).toBeVisible({ timeout: 5_000 });

        // Right-click the node in the tree.
        await aside.getByText('N-490', { exact: true }).click({ button: 'right' });

        // Should have Deploy CrowDB Storage and Delete Node.
        await expect(page.getByRole('menuitem', { name: /deploy crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

        // Should NOT have restart/stop (no server deployed).
        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
    }

    // --- Node with server: Deploy DiskDB + Ping + Delete Node, no restart/stop on node ---
    {
      await step('ctx-menu: setup with-server', async () => {
        await createRack(baseURL!, { id: 491, name: 'Rack 491' });
        await createNode(baseURL!, { id: 491, rack_id: 491 });
        await deployNodeServer(baseURL!, 491, freePort(), freePort());
      });

      await step('ctx-menu: with-server UI', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        // Wait for the server to be deployed via API (KV-xxx tree items
        // are in the KV domain, not the Cluster domain).
        const waitApi = await apiContext(baseURL!);
        try {
          await expect.poll(async () => {
            const r = await waitApi.get('/api/nodes/491/server');
            return r.ok() ? await r.json() : null;
          }, { timeout: 10_000, intervals: [100] }).toBeTruthy();
        } finally {
          await waitApi.dispose();
        }

        // Right-click the node (not the server).
        await aside.getByText('N-491', { exact: true }).click({ button: 'right' });

        // Server is deployed, so no "Deploy CrowDB Storage" but "Deploy DiskDB" appears.
        await expect(page.getByRole('menuitem', { name: /deploy diskdb/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /ping/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toBeVisible();

        // Should NOT have restart/stop on the node — those are on the service.
        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
      await step('ctx-menu: teardown 491', () => stopNodeServer(baseURL!, 491));
    }

    // --- Server node: Restart, Stop, Delete CrowDB Storage ---
    {
      await step('ctx-menu: setup server-node', async () => {
        await createRack(baseURL!, { id: 492, name: 'Rack 492' });
        await createNode(baseURL!, { id: 492, rack_id: 492 });
        await deployNodeServer(baseURL!, 492, freePort(), freePort());
      });

      await step('ctx-menu: server-node UI', async () => {
        await page.goto('/');
        // KV-xxx tree items are in the Cluster domain under their physical node.
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        // Wait for the server node to appear.
        await expect(aside.getByText('PKV-492')).toBeVisible({ timeout: 10_000 });

        // Right-click the server (KV) node.
        await aside.getByText('PKV-492', { exact: true }).click({ button: 'right' });

        await expect(page.getByRole('menuitem', { name: /restart crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /stop crowdb storage/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /delete crowdb storage/i })).toBeVisible();

        // Should NOT have "Deploy" or "Delete Node" on the service.
        await expect(page.getByRole('menuitem', { name: /deploy/i })).toHaveCount(0);
        await expect(page.getByRole('menuitem', { name: /delete node/i })).toHaveCount(0);

        await page.keyboard.press('Escape');
      });
      await step('ctx-menu: teardown 492', () => stopNodeServer(baseURL!, 492));
    }
  });

  test('deploys and stops a real crowdb-kv-server through the UI', async ({ page, baseURL }) => {
    await step('deploy-ui: seedRackAndNode', () => seedRackAndNode(baseURL!, 4, 4));

    const restPort = freePort('kv-mgmt');
    const rpcPort = freePort('kv-listen');
    const api = await apiContext(baseURL!);
    try {
      await step('deploy-ui: deploy dialog', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('N-4', { exact: true })).toBeVisible({ timeout: 3_000 });

        await aside.getByText('N-4', { exact: true }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /deploy CrowDB Storage/i }).click();

        await expect(page.getByRole('dialog', { name: /deploy CrowDB Storage on 4/i })).toBeVisible();
        await page.getByLabel('REST Port').fill(String(restPort));
        await page.getByLabel('RPC Port').fill(String(rpcPort));
        const deploy = page.getByRole('button', { name: 'Deploy' });
        await expect(deploy).toBeEnabled({ timeout: process.platform === 'darwin' ? 30_000 : 3_000 });
        await deploy.click();
      });

      await step('deploy-ui: poll server', () => expect.poll(async () => {
        const server = await api.get('/api/nodes/4/server');
        if (!server.ok()) return null;
        return await server.json();
      }, { timeout: 5_000, intervals: [100] }).toEqual(
        expect.objectContaining({
          node_id: 4,
          url: `http://127.0.0.1:${restPort}`,
          rpc_url: `http://127.0.0.1:${rpcPort}`,
          pid: expect.any(Number),
        }),
      ));
    } finally {
      await step('deploy-ui: teardown', () => stopNodeServer(baseURL!, 4));
      await api.dispose();
    }
  });

  // Kept separate: needs an empty backend so the tree holds a single node.
  test('ping, restart, and stop server via context menu', async ({ page, baseURL }) => {
    await step('ping-restart-stop: resetAll', () => resetAll(baseURL!));
    await step('ping-restart-stop: setup', async () => {
      await createRack(baseURL!, { id: 27, name: 'r27' });
      await createNode(baseURL!, { id: 27, rack_id: 27 });
      await deployNodeServer(baseURL!, 27, freePort(), freePort());
      await clusterInit(baseURL!, [27]);
    });

    try {
      await step('ping-restart-stop: ping', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const nodeItem = page.getByRole('treeitem').filter({ hasText: 'N-27' });
        await expect(nodeItem).toBeVisible({ timeout: 3_000 });

        // Ping — on the node context menu. Verify it actually succeeds.
        await nodeItem.click({ button: 'right' });
        const pingPromise = page.waitForResponse((r: any) => r.url().includes('/ping'));
        await page.getByRole('menuitem', { name: /ping/i }).click();
        const pingResp = await pingPromise;
        expect((await pingResp.json()).ok).toBe(true);
      });

      // Restart and Stop are on the server (KV) context menu, not the
      // node. KV-xxx tree items are in the Cluster domain under the node.
      await page.getByTestId('domain-cluster').click();
      const serverItem = page.getByRole('treeitem').filter({ hasText: 'PKV-27' });
      await expect(serverItem).toBeVisible({ timeout: 5_000 });

      // Restart
      await step('ping-restart-stop: restart', async () => {
        await serverItem.click({ button: 'right' });
        const restartResponse = page.waitForResponse((r: any) => r.url().includes('/server/restart'));
        await page.getByRole('menuitem', { name: /restart CrowDB Storage/i }).click();
        await restartResponse;
      });

      // Stop
      await step('ping-restart-stop: stop', async () => {
        await serverItem.click({ button: 'right' });
        const stopResponse = page.waitForResponse((r: any) => r.url().includes('/server/stop'));
        await page.getByRole('menuitem', { name: /stop CrowDB Storage/i }).click();
        await stopResponse;
      });

      // Health pill: the server badge should drop from Healthy after stop
      // (useClusterTree polls every 1s; monitor_cache is dropped on stop).
      await step('ping-restart-stop: health badge', async () => {
        const healthBadge = serverItem.locator('[title]').filter({ hasText: /^(Healthy|Failed|Unknown|Degraded)$/ });
        await expect(healthBadge.filter({ hasText: 'Healthy' })).toHaveCount(0, { timeout: 10_000 });
      });

      // After stop, verify server is no longer running via API
      const api = await apiContext(baseURL!);
      try {
        await step('ping-restart-stop: verify stopped API', async () => {
          const resp = await api.get('/api/nodes/27');
          expect(resp.ok()).toBeTruthy();
          const node = await resp.json();
          const serverState = node.server?.state ?? node.server?.status ?? 'unknown';
          expect(serverState).not.toBe('running');
        });
      } finally {
        await api.dispose();
      }
    } finally {
      await step('ping-restart-stop: teardown', () => stopNodeServer(baseURL!, 27));
    }
  });

  test('deleting a node cascades service shutdown; deleting the service keeps the node', async ({ page, baseURL }) => {
    // --- Delete node with deployed server cascades service shutdown ---
    await step('cascade: resetAll', () => resetAll(baseURL!));
    {
      await step('cascade: setup delete-node', async () => {
        await createRack(baseURL!, { id: 493, name: 'Rack 493' });
        await createNode(baseURL!, { id: 493, rack_id: 493 });
        await deployNodeServer(baseURL!, 493, freePort(), freePort());
      });

      const api = await apiContext(baseURL!);
      try {
        await step('cascade: delete node UI', async () => {
          await page.goto('/');
          await page.getByTestId('domain-cluster').click();

          const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
          // Wait for the server to be deployed via API (KV-xxx tree
          // items are in the KV domain, not the Cluster domain).
          await expect.poll(async () => {
            const r = await api.get('/api/nodes/493/server');
            return r.ok();
          }, { timeout: 10_000, intervals: [100] }).toBe(true);

          // Server is deployed before the cascade delete.
          expect((await api.get('/api/nodes/493/server')).status()).toBe(200);

          // Right-click node → Delete Node.
          await aside.getByText('N-493', { exact: true }).click({ button: 'right' });
          await page.getByRole('menuitem', { name: /delete node/i }).click();

          // Confirm delete dialog.
          const deleteDialog = page.getByRole('dialog', { name: /delete node/i });
          await expect(deleteDialog).toBeVisible();
          const confirmBtn = deleteDialog.getByRole('button', { name: /delete node/i });
          await confirmBtn.click();

          // Node should disappear from the tree.
          await expect(aside.getByText('N-493', { exact: true })).toHaveCount(0, { timeout: 10_000 });

          // Server record removed by the cascade (not orphaned), node gone.
          expect((await api.get('/api/nodes/493/server')).status()).toBe(404);
          expect((await api.get('/api/nodes/493')).status()).toBe(404);
        });
      } finally {
        await api.dispose();
      }
    }

    // --- Delete crowdb storage service removes server but keeps node ---
    {
      await step('cascade: setup delete-svc', async () => {
        await createRack(baseURL!, { id: 494, name: 'Rack 494' });
        await createNode(baseURL!, { id: 494, rack_id: 494 });
        await deployNodeServer(baseURL!, 494, freePort(), freePort());
      });

      await step('cascade: delete svc UI', async () => {
        await page.goto('/');
        // KV-xxx tree items are in the Cluster domain under their physical node.
        await page.getByTestId('domain-cluster').click();

        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByText('PKV-494')).toBeVisible({ timeout: 10_000 });

        // Right-click server → Delete CrowDB Storage.
        await aside.getByText('PKV-494', { exact: true }).click({ button: 'right' });
        await page.getByRole('menuitem', { name: /delete crowdb storage/i }).click();

        // Confirm.
        const deleteDialog = page.getByRole('dialog', { name: /delete crowdb storage/i });
        await expect(deleteDialog).toBeVisible();
        const confirmBtn = deleteDialog.getByRole('button', { name: /delete crowdb storage/i });
        const deleteResp = page.waitForResponse((r: any) =>
          r.request().method() === 'DELETE' && r.url().includes('/api/nodes/494/server'));
        await confirmBtn.click();
        await deleteResp;

        // Server disappears from tree, node remains.
        await expect(aside.getByText('PKV-494', { exact: true })).toHaveCount(0, { timeout: 10_000 });
        // Switch to Cluster domain to verify node remains.
        await page.getByTestId('domain-cluster').click();
        await expect(aside.getByText('N-494', { exact: true })).toBeVisible();
      });

      // Verify via API: node still exists, server is gone.
      const api = await apiContext(baseURL!);
      try {
        await step('cascade: verify delete-svc API', async () => {
          const nodeResp = await api.get('/api/nodes/494');
          expect(nodeResp.ok()).toBeTruthy();
          const serverResp = await api.get('/api/nodes/494/server');
          expect(serverResp.status()).toBe(404);
        });
      } finally {
        await api.dispose();
      }
    }
  });
});

// Baseline: new native defaults/reopen acceptance (2026-10-04).
test('native diagnostics: auxiliary dialogs reopen with unused IDs ports and valid dependencies', async ({ page, request }) => {
  const response = await request.get('/api/deployment-defaults?node_id=1');
  expect(response.ok(), await response.text()).toBe(true);
  const defaults = await response.json();
  const serversResponse = await request.get('/api/servers');
  expect(serversResponse.ok()).toBe(true);
  const servers = await serversResponse.json();
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  for (const [kind, label] of [['chunkdb', 'crowdb-chunk-db'], ['diskio', 'crowdb-disk-io'], ['chunk-kv', 'crowdb-chunk-kv'], ['access-server', 'crowdb-access-server']]) {
    for (let reopen = 0; reopen < 2; reopen++) {
      await sidebar.getByRole('button', { name: 'N-1', exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: `Deploy ${label}`, exact: true }).click();
      const dialog = page.getByRole('dialog', { name: `Deploy ${label}`, exact: true });
      await expect(dialog.getByRole('button', { name: 'Deploy service', exact: true })).toBeEnabled();
      await expect(dialog.getByLabel('Instance ID', { exact: true })).toHaveValue(defaults[kind].instance_id);
      expect(servers.some((server: { id: string }) => server.id === `${kind}-${defaults[kind].instance_id}`)).toBe(false);
      const fields = kind === 'diskio' ? [['RPC port', 'rpc_port']] : kind === 'access-server'
        ? [['Iceberg HTTP port', 'http_port'], ['S3 HTTP port', 's3_port']]
        : [['Management HTTP port', 'http_port'], ['RPC port', 'rpc_port']];
      for (const [field, key] of fields) await expect(dialog.getByLabel(field, { exact: true })).toHaveValue(String(defaults[kind][key]));
      if (kind === 'diskio') await expect(dialog.getByLabel('Disk group', { exact: true })).toHaveValue('1');
      if (kind === 'chunk-kv') {
        await expect(dialog.getByLabel('Metadata store', { exact: true })).toHaveValue('1');
        await expect(dialog.getByLabel('Journal metadata group', { exact: true })).toHaveValue('1');
      }
      await expect(dialog.getByLabel('Single-node test deployment (reduced redundancy)')).not.toBeChecked();
      await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
      await expect(dialog).toHaveCount(0);
    }
  }
  expect(await (await request.get('/api/servers')).json()).toEqual(servers);
});

// Baseline: 1.3s (2026-10-04); existing instances come from the owned fixture.
test('native diagnostics: interrupted six-service plan reconciles without duplicate deployment', async ({ page, request }) => {
  test.skip(!!process.env.CROWDB_NATIVE_PLAN_PREREQUISITES, 'Requires the complete six-service fixture phase');
  const kinds = ['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'];
  const servers = await request.get('/api/servers');
  expect(servers.ok()).toBe(true);
  const original = await servers.json();
  const identity = (rows: { id: string; pid: number; node_id: number; service_type: string }[]) => rows
    .map(row => `${row.node_id}/${row.service_type}/${row.id}/${row.pid}`).sort();
  expect(original).toHaveLength(18);
  for (const kind of kinds) expect(original.filter((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind)).toHaveLength(1);
  const saved = await request.put('/api/nodes/1/service-plan', { data: {
    revision: 0,
    steps: Object.fromEntries(kinds.map(kind => [kind, { state: kind === 'paxos-kv' ? 'deployed' : kind === 'chunkdb' ? 'deploying' : 'pending' }])),
  } });
  expect(saved.ok(), await saved.text()).toBe(true);
  let deploymentRequests = 0;
  page.on('request', current => {
    if (current.method() === 'POST' && /\/api\/nodes\/\d+\/(server|diskdb|services)\/deploy$/.test(new URL(current.url()).pathname)) deploymentRequests++;
  });
  const dialog = page.getByRole('dialog', { name: 'Node 1 services', exact: true });
  const open = async () => {
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expand = aside.getByRole('treeitem').filter({ hasText: 'R-1' }).locator('button[aria-label="Expand"]');
    if (await expand.count() > 0) await expand.click();
    await aside.getByText('N-1', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  };
  await step('native plan recovery DOM', async () => {
    await page.goto('/?domain=Cluster');
    await page.reload();
    await open();
    await expect(dialog).toContainText('Deployment was interrupted');
    await expect(dialog.getByRole('listitem')).toHaveCount(6);
    await dialog.getByRole('button', { name: 'Retry failed services', exact: true }).click();
    await expect(dialog.getByRole('button', { name: 'Done', exact: true })).toBeEnabled();
  });
  await step('native plan authoritative reconciliation', async () => {
    await expect.poll(async () => {
      const response = await request.get('/api/service-plans');
      expect(response.ok()).toBe(true);
      return Object.values((await response.json())['1'].steps).map((entry: any) => entry.state);
    }, { timeout: 3000, intervals: [100] }).toEqual(kinds.map(() => 'deployed'));
    const current = await request.get('/api/servers');
    expect(current.ok()).toBe(true);
    expect(identity(await current.json())).toEqual(identity(original));
    expect(deploymentRequests).toBe(0);
  });
  await step('native completed plan reload', async () => {
    await page.reload();
    await open();
    await expect(dialog.getByRole('button', { name: 'Done', exact: true })).toBeEnabled();
    await expect(dialog).not.toContainText('Deployment was interrupted');
    expect(deploymentRequests).toBe(0);
  });
});

// Baseline: 7.6s (2026-10-04); prerequisite services are controlled through real lifecycle APIs.
test('native diagnostics: waiting plan resumes when DiskIO prerequisites arrive', async ({ page, request }) => {
  test.skip(!process.env.CROWDB_NATIVE_PLAN_PREREQUISITES, 'Requires the partial deployment fixture phase');
  const kinds = ['paxos-kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'];
  const list = async () => {
    const response = await request.get('/api/servers');
    expect(response.ok()).toBe(true);
    return response.json();
  };
  const original = await list();
  expect(original).toHaveLength(12);
  expect(original.filter((row: { service_type: string }) => ['chunk-kv', 'access-server'].includes(row.service_type))).toHaveLength(0);
  const diskioIds = original.filter((row: { service_type: string; node_id: number }) => row.service_type === 'diskio' && row.node_id !== 1)
    .map((row: { id: string }) => row.id);
  expect(diskioIds).toHaveLength(2);
  const deploys: string[] = [];
  page.on('request', current => {
    if (current.method() === 'POST' && /\/api\/nodes\/\d+\/(server|diskdb|services)\/deploy$/.test(new URL(current.url()).pathname)) deploys.push(current.postDataJSON().kind);
  });
  const dialog = page.getByRole('dialog', { name: 'Node 1 services', exact: true });
  const open = async () => {
    const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
    const expand = aside.getByRole('treeitem').filter({ hasText: 'R-1' }).locator('button[aria-label="Expand"]');
    if (await expand.count() > 0) await expand.click();
    await aside.getByText('N-1', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  };
  try {
    await step('native plan missing prerequisite', async () => {
      for (const id of diskioIds) expect((await request.post(`/api/services/${id}/stop`, { data: {} })).ok()).toBe(true);
      const saved = await request.put('/api/nodes/1/service-plan', { data: {
        revision: 0,
        steps: Object.fromEntries(kinds.map(kind => [kind, { state: ['chunk-kv', 'access-server'].includes(kind) ? 'pending' : 'deployed' }])),
      } });
      expect(saved.ok(), await saved.text()).toBe(true);
      await page.goto('/?domain=Cluster');
      await open();
      await expect(dialog).toContainText('restart registered DiskIO services');
      await expect(dialog).toContainText('deploy Chunk-KV and initialize its catalog');
      expect(deploys).toEqual([]);
      await page.reload();
      await open();
      await expect(dialog).toContainText('restart registered DiskIO services');
      expect(deploys).toEqual([]);
    });
    await step('native plan prerequisite arrival', async () => {
      expect((await request.post(`/api/services/${diskioIds[0]}/restart`, { data: {} })).ok()).toBe(true);
      await expect.poll(async () => {
        const response = await request.get('/api/service-plans');
        expect(response.ok()).toBe(true);
        return (await response.json())['1'].steps['chunk-kv'].state;
      }, { timeout: 3000, intervals: [100] }).toBe('waiting');
      expect(deploys).toEqual([]);
      expect((await request.post(`/api/services/${diskioIds[1]}/restart`, { data: {} })).ok()).toBe(true);
      await expect(dialog.getByRole('listitem').filter({ hasText: 'deploying' })).toHaveCount(1);
      await expect.poll(async () => {
        const response = await request.get('/api/service-plans');
        expect(response.ok()).toBe(true);
        return Object.values((await response.json())['1'].steps).map((entry: any) => entry.state);
      }, { timeout: 3000, intervals: [100] }).toEqual(kinds.map(() => 'deployed'));
      await expect(dialog.getByRole('listitem').filter({ hasText: 'deployed' })).toHaveCount(6);
      expect(deploys).toEqual(['chunk-kv', 'access-server']);
      const current = await list();
      expect(current).toHaveLength(14);
      for (const kind of kinds) expect(current.filter((row: { node_id: number; service_type: string }) => row.node_id === 1 && row.service_type === kind)).toHaveLength(1);
      for (const previous of original.filter((row: { service_type: string }) => row.service_type !== 'diskio')) {
        expect(current.find((row: { id: string }) => row.id === previous.id)?.pid).toBe(previous.pid);
      }
      const buckets = await request.get('/api/access/s3/');
      expect(buckets.ok()).toBe(true);
      expect(await buckets.text()).toContain('ListAllMyBucketsResult');
    });
  } finally {
    for (const id of diskioIds) {
      const current = (await list()).find((row: { id: string }) => row.id === id);
      if (!current.pid) expect((await request.post(`/api/services/${id}/restart`, { data: {} })).ok()).toBe(true);
    }
  }
});
