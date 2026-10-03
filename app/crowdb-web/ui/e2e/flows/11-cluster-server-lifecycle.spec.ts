// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 9s (2026-08-16)

import { test, expect } from '../fixtures/realBackend';
import { apiContext, clusterInit, createNode, createRack, deployNodeServer, freePort, resetAll, seedRackAndNode, stopNodeServer } from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

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
        await expect(aside.getByText('KV-492')).toBeVisible({ timeout: 10_000 });

        // Right-click the server (KV) node.
        await aside.getByText('KV-492', { exact: true }).click({ button: 'right' });

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

    const restPort = freePort();
    const rpcPort = freePort();
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
        await page.getByRole('button', { name: 'Deploy' }).click();
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
      const serverItem = page.getByRole('treeitem').filter({ hasText: 'KV-27' });
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
        await expect(aside.getByText('KV-494')).toBeVisible({ timeout: 10_000 });

        // Right-click server → Delete CrowDB Storage.
        await aside.getByText('KV-494', { exact: true }).click({ button: 'right' });
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
        await expect(aside.getByText('KV-494', { exact: true })).toHaveCount(0, { timeout: 10_000 });
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

// Baseline: auxiliary deployment 2.4s (2026-10-03)
test('auxiliary deployments use typed parameters, identities and lifecycle menus', async ({ page, baseURL }) => {
  page.setDefaultTimeout(3000);
  await seedRackAndNode(baseURL!, 495, 495);
  const services: object[] = [];
  const requests: { kind: string; instance_id: string; [key: string]: unknown }[] = [];
  const stopped: string[] = [];
  await page.route('**/api/servers', route => route.fulfill({ json: services }));
  await page.route('**/api/deployment-defaults', route => route.fulfill({ json: {
    chunkdb: { instance_id: '1', http_port: 12010, rpc_port: 12110 },
    diskio: { instance_id: '1', rpc_port: 13010 },
    'chunk-kv': { instance_id: '1', http_port: 15010, rpc_port: 15110 },
    'access-server': { instance_id: '1', http_port: 9092, s3_port: 9091 },
  } }));
  await page.route('**/api/stores?*', route => route.fulfill({ json: [{ store_id: '7', nodes: [], groups: [] }] }));
  await page.route('**/api/nodes/495/disk-groups', route => route.fulfill({ json: [{ id: 8, node_id: 495, rack_id: 495, name: 'Capacity disks', status: 'active' }] }));
  await page.route('**/api/nodes/495/services/deploy', route => {
    const body = route.request().postDataJSON(); requests.push(body);
    services.push({ id: `${body.kind}-${body.instance_id}`, node_id: 495, service_type: body.kind, pid: 12345, health: 'unknown' });
    return route.fulfill({ status: 201, json: { id: `${body.kind}-${body.instance_id}` } });
  });
  await page.route('**/api/services/*/stop', route => {
    stopped.push(new URL(route.request().url()).pathname);
    return route.fulfill({ json: { pid: null } });
  });
  await page.goto('/?domain=Cluster');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(sidebar.getByText('N-495', { exact: true })).toBeVisible({ timeout: 3000 });
  for (const [kind, label, prefix] of [['chunkdb', 'CDB (ChunkDB)', 'CDB'], ['diskio', 'DiskIO', 'DIO'], ['chunk-kv', 'Chunk-KV', 'CKV'], ['access-server', 'Access Server', 'AS']]) {
    await sidebar.getByText('N-495', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: `Deploy ${label}`, exact: true }).click();
    const dialog = page.getByRole('dialog', { name: `Deploy ${label}`, exact: true });
    await expect(dialog.getByRole('button', { name: 'Deploy service', exact: true })).toBeEnabled();
    await dialog.getByLabel('Instance ID', { exact: true }).fill('9007199254740993');
    if (kind === 'diskio') await dialog.getByLabel('Disk group', { exact: true }).selectOption('8');
    if (kind === 'chunk-kv') await dialog.getByLabel('Metadata store', { exact: true }).selectOption('7');
    await dialog.getByRole('button', { name: 'Deploy service', exact: true }).click();
    await expect(dialog).toHaveCount(0);
    const item = sidebar.getByText(`${prefix}-9007199254740993`, { exact: true });
    await expect(item).toBeVisible();
    await item.click();
    await expect(page.getByRole('complementary', { name: 'Entity inspector' }).getByText(label, { exact: true })).toBeVisible();
    await expect(page.locator(`[data-id="SERVICE-${kind}-9007199254740993"]`)).toContainText(`${prefix}-9007199254740993`);
    await sidebar.getByText('N-495', { exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: `${prefix}-9007199254740993 Running`, exact: true }).hover();
    await expect(page.getByRole('menuitem', { name: 'Stop CrowDB Storage', exact: true })).toHaveCount(0);
    await page.getByRole('menuitem', { name: `Stop ${label}`, exact: true }).click();
    await expect.poll(() => stopped.includes(`/api/services/${kind}-9007199254740993/stop`), { timeout: 3000, intervals: [100] }).toBe(true);
  }
  expect(requests.map(request => request.kind)).toEqual(['chunkdb', 'diskio', 'chunk-kv', 'access-server']);
  expect(requests.every(request => request.instance_id === '9007199254740993' && request.test_single_node === false)).toBe(true);
  expect(requests[1]).toMatchObject({ disk_group_id: 8 });
  expect(requests[1]).not.toHaveProperty('http_port');
  expect(requests[2]).toMatchObject({ metadata_store_id: 7 });
  expect(requests[3]).toMatchObject({ s3_port: 9091, http_port: 9092 });
  expect(requests[3]).not.toHaveProperty('rpc_port');
});

// Baseline: 0.681s (2026-10-03), before session queue.
test('default service plan waits for Group 0 without using single-node mode', async ({ page, baseURL }) => {
  await seedRackAndNode(baseURL!, 496, 496);
  const services = [{ id: '496', node_id: 496, service_type: 'kv', pid: 123, health: 'up' }, { id: 'diskdb-496', node_id: 496, service_type: 'diskdb', pid: 124, health: 'up' }];
  let deployments = 0;
  await page.route('**/api/servers', route => route.fulfill({ json: services }));
  await page.route('**/api/stores?*', route => route.fulfill({ json: [] }));
  await page.route('**/api/nodes/496/services/deploy', route => { deployments++; return route.fulfill({ status: 500, json: { error: 'must not deploy before Group 0' } }); });
  await page.goto('/?domain=Cluster');
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByText('N-496', { exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Node 496 services', exact: true });
  await dialog.getByRole('button', { name: 'Deploy missing services', exact: true }).click();
  await expect(dialog.getByRole('listitem')).toHaveCount(6);
  await expect(dialog.getByRole('status').filter({ hasText: 'initialize Group 0' })).toHaveCount(4);
  await expect(dialog.getByRole('button', { name: 'Done', exact: true })).toBeEnabled();
  expect(deployments).toBe(0);
  await expect.poll(async () => {
    const response = await page.request.get(`${baseURL}/api/service-plans`);
    expect(response.ok()).toBe(true);
    const plans = await response.json();
    return Object.values(plans['496'].steps).filter((step: any) => step.state === 'waiting').length;
  }, { timeout: 3000, intervals: [100] }).toBe(4);
  await page.reload();
  await page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByText('N-496', { exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  await expect(dialog.getByRole('status').filter({ hasText: 'initialize Group 0' })).toHaveCount(4);
  await expect(dialog.getByRole('button', { name: 'Deploy missing services', exact: true })).toHaveCount(0);
  expect(deployments).toBe(0);
});

test('Add Node creates and retries services in one dialog, then waits automatically', async ({ page, baseURL }) => {
  await seedRackAndNode(baseURL!, 497, 497);
  const services: object[] = [];
  let nodeCreates = 0;
  let kvDeploys = 0;
  let diskdbDeploys = 0;
  await page.route('**/api/nodes', async route => {
    if (route.request().method() === 'POST') nodeCreates++;
    await route.continue();
  });
  await page.route('**/api/servers', route => route.fulfill({ json: services }));
  await page.route('**/api/stores?*', route => route.fulfill({ json: [] }));
  await page.route('**/api/nodes/498/server/deploy', route => {
    kvDeploys++;
    services.push({ id: '498', node_id: 498, service_type: 'kv', pid: 123, health: 'up' });
    return route.fulfill({ json: { node_id: 498 } });
  });
  await page.route('**/api/nodes/498/diskdb/deploy', route => {
    diskdbDeploys++;
    if (diskdbDeploys === 1) return route.fulfill({ status: 502, json: { error: 'DiskDB startup failed' } });
    services.push({ id: 'diskdb-498', node_id: 498, service_type: 'diskdb', pid: 124, health: 'up' });
    return route.fulfill({ json: { node_id: 498 } });
  });
  await page.goto('/?domain=Cluster');
  const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await expect(aside.getByText('R-497 (rack-497)', { exact: true })).toBeVisible({ timeout: 3000 });
  await aside.getByText('R-497 (rack-497)', { exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Add Node', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Add Node', exact: true });
  await expect(dialog.getByLabel('Node ID')).toHaveValue('498');
  await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
  await expect(dialog.getByRole('alert')).toContainText('DiskDB startup failed');
  await expect(dialog.getByLabel('Node ID')).toBeDisabled();
  await dialog.getByRole('button', { name: 'Retry failed services', exact: true }).click();
  await expect(dialog.getByRole('listitem')).toHaveCount(6);
  await expect(dialog.getByRole('status').filter({ hasText: 'initialize Group 0' })).toHaveCount(4);
  expect(nodeCreates).toBe(1);
  expect(kvDeploys).toBe(1);
  expect(diskdbDeploys).toBe(2);
  await dialog.getByRole('button', { name: 'Done', exact: true }).click();
  await aside.getByText('N-498', { exact: true }).click({ button: 'right' });
  await page.getByRole('menuitem', { name: 'Deploy default services', exact: true }).click();
  const progress = page.getByRole('dialog', { name: 'Node 498 services', exact: true });
  await expect(progress.getByRole('listitem')).toHaveCount(6);
  await expect(progress.getByRole('button', { name: 'Deploy missing services', exact: true })).toHaveCount(0);
});
