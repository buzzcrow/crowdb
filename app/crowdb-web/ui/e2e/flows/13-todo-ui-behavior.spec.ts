// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createServer } from 'node:net';
import { test, expect } from '../fixtures/realBackend';
import {
  addDiskGroup,
  addDisksBatch,
  addGroup,
  apiContext,
  createRack,
  clusterInit,
  createStore,
  freePort,
  freePortRange,
  resetAll,
  waitForLeader,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

const RACK_ID = 701;
const NODE_IDS = [701, 702, 703];
const STORE_ID = 770;
const GROUP_ID = 7700;
const REPLICA_ID = 77000;
const DISK_GROUP_ID = 7710;
const DISK_ID = '0123456789abcdef-0123456789abcdef';

test.describe('todo-ui behavior · service deployment and view ownership', () => {
  test('fixed-slot warning stays healthy and persists across reload', async ({ page, baseURL }) => {
    await resetAll(baseURL!);
    await createRack(baseURL!, { id: 705, name: 'Slot warning' });
    const api = await apiContext(baseURL!);
    try {
      const node = await api.post('/api/nodes', { data: { id: 705, rack_id: 705, host: '127.0.0.1', ssh: { type: 'KeyDefault', user: '' } } });
      expect(node.ok(), await node.text()).toBeTruthy();
      const detail = 'CDB instance is outside the fixed slot plan; explicit slot migration is required';
      const steps = Object.fromEntries(['access-server', 'chunk-kv', 'chunkdb', 'diskdb', 'diskio', 'paxos-kv'].map(kind => [kind, kind === 'chunkdb' ? { state: 'warning', detail } : { state: 'disabled' }]));
      const saved = await api.put('/api/nodes/705/service-plan', { data: { revision: 0, steps } });
      expect(saved.ok(), await saved.text()).toBeTruthy();
      await page.goto('/?domain=Cluster');
      const item = page.getByTestId('tree-node-SERVICE-chunkdb-705');
      await expect(item.getByTitle('Healthy', { exact: true })).toBeVisible();
      await expect(item.getByRole('button', { name: 'CDB-705', exact: true })).toHaveAttribute('title', `CDB-705 · ${detail}`);
      await page.reload();
      await expect(item.getByTitle('Healthy', { exact: true })).toBeVisible();
      expect((await (await api.get('/api/service-plans')).json())['705'].steps.chunkdb.state).toBe('warning');
    } finally {
      await api.dispose();
      await resetAll(baseURL!);
    }
  });

  test('creates three PKV and DiskDB node plans and keeps derived listeners disjoint', async ({ page, baseURL }) => {
    test.setTimeout(120_000);
    await step('todo-ui: reset', () => resetAll(baseURL!));
    await step('todo-ui: create rack', () => createRack(baseURL!, { id: RACK_ID, name: 'Todo UI Rack' }));

    const deploymentPorts = new Map<number, { rest: number; kvRpc: number; diskdbRpc: number }>();
    const failedResponses: string[] = [];
    page.on('response', (response) => {
      if (response.status() >= 500 && /\/api\/nodes\/\d+\/(?:server|diskdb)\/deploy/.test(response.url())) {
        failedResponses.push(`${response.status()} ${response.url()}`);
      }
    });

    try {
      for (const nodeId of NODE_IDS) {
        const rest = freePort('kv-mgmt');
        const kvRpc = freePort('kv-listen');
        // DiskDB derives its HTTP and public crowdb-rpc listeners from this
        // one UI port. Reserve three consecutive ports for every node.
        const diskdbRpc = freePortRange(3, 'diskdb-listen');
        deploymentPorts.set(nodeId, { rest, kvRpc, diskdbRpc });

        await step(`todo-ui: add node ${nodeId}`, async () => {
          if (nodeId === NODE_IDS[0]) {
            await page.goto('/');
            await page.getByTestId('domain-cluster').click();
          }
          const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
          await aside.getByText(`R-${RACK_ID} (Todo UI Rack)`).click({ button: 'right' });
          await page.getByRole('menuitem', { name: /add node/i }).click();

          const dialog = page.getByRole('dialog', { name: 'Add Node' });
          await expect(dialog).toBeVisible();
          await dialog.getByLabel('Node ID').fill(String(nodeId));
          await dialog.getByLabel('Host').fill('127.0.0.1');
          await expect(dialog.getByRole('checkbox', { name: 'crowdb-paxos-kv', exact: true })).toBeChecked();
          await expect(dialog.getByRole('checkbox', { name: 'crowdb-disk-db', exact: true })).toBeChecked();
          for (const name of ['crowdb-access-server', 'crowdb-chunk-kv', 'crowdb-chunk-db', 'crowdb-disk-io']) await dialog.getByLabel(name, { exact: true }).uncheck();
          await dialog.getByLabel('crowdb-paxos-kv RPC port').fill(String(kvRpc));
          await dialog.getByLabel('crowdb-disk-db RPC port').fill(String(diskdbRpc));
          await dialog.getByRole('button', { name: /create node/i }).click();
          await expect(dialog).toHaveCount(0);
          await expect(aside.getByText(`N-${nodeId}`, { exact: true })).toBeVisible({ timeout: 10_000 });
        });
      }

      const api = await apiContext(baseURL!);
      try {
        await expect.poll(async () => {
          const response = await api.get('/api/servers');
          expect(response.ok()).toBe(true);
          const servers = await response.json();
          return NODE_IDS.every(nodeId => servers.some((server: any) => server.node_id === nodeId && server.service_type === 'paxos-kv' && server.pid));
        }, { timeout: 10_000, intervals: [100] }).toBe(true);
        const queued = await (await api.get('/api/service-plans')).json();
        for (const nodeId of NODE_IDS) expect(queued[nodeId].steps.diskdb.state).toBe('waiting');
        await clusterInit(baseURL!, NODE_IDS);
        await step('todo-ui: verify service registrations and listeners', async () => {
          await expect.poll(async () => {
            const response = await api.get('/api/servers');
            if (!response.ok()) return [];
            const servers = await response.json();
            return NODE_IDS.every((nodeId) =>
              servers.some((s: any) => s.node_id === nodeId && String(s.service_type ?? 'paxos-kv').toLowerCase() === 'paxos-kv') &&
              servers.some((s: any) => s.node_id === nodeId && String(s.service_type).toLowerCase() === 'diskdb'),
            ) ? servers : [];
          }, { timeout: 30_000, intervals: [250] }).not.toEqual([]);

          const servers = await (await api.get('/api/servers')).json();
          const listeners = new Set<number>();
          for (const nodeId of NODE_IDS) {
            const ports = deploymentPorts.get(nodeId)!;
            const diskdb = servers.find((s: any) => s.node_id === nodeId && String(s.service_type).toLowerCase() === 'diskdb');
            expect(diskdb, `missing DiskDB registration for node ${nodeId}`).toBeTruthy();
            if (diskdb.pid !== undefined && diskdb.pid !== null) expect(Number(diskdb.pid)).toBeGreaterThan(0);
            const publicEndpoint = String(diskdb.endpoint ?? diskdb.rpc_url ?? '');
            expect(publicEndpoint).toMatch(new RegExp(`:${ports.diskdbRpc + 2}(?:/|$)`));
            for (const listener of [ports.diskdbRpc, ports.diskdbRpc + 1, ports.diskdbRpc + 2]) {
              expect(listeners.has(listener), `DiskDB listener port ${listener} is reused`).toBe(false);
              listeners.add(listener);
            }
          }
        });
        expect(failedResponses, `service deployment responses failed: ${failedResponses.join(', ')}`).toEqual([]);
      } finally {
        await api.dispose();
      }

      await step('todo-ui: create logical and physical test data', async () => {
        await createStore(baseURL!, STORE_ID, NODE_IDS);
        await waitForLeader(baseURL!, 0, 0, 15_000);
        await addGroup(baseURL!, STORE_ID, GROUP_ID, REPLICA_ID, [NODE_IDS[0]]);
        // Wait for the diskdb instance to register with group-0 before
        // addDiskGroup (auto-assign owner requires a live diskdb instance).
        const diskdbPort = deploymentPorts.get(NODE_IDS[0])!.diskdbRpc + 2;
        {
          const api = await apiContext(baseURL!);
          try {
            await expect.poll(async () => {
              const response = await api.get('/api/diskdb/instances');
              if (!response.ok()) return false;
              const instances = await response.json();
              return instances.some((entry: any) => String(entry.rpc_endpoint).includes(`:${diskdbPort}`));
            }, { timeout: 15_000, intervals: [200] }).toBe(true);
          } finally {
            await api.dispose();
          }
        }
        await addDiskGroup(baseURL!, NODE_IDS[0], DISK_GROUP_ID, 'Physical Group');
        await addDisksBatch(baseURL!, NODE_IDS[0], DISK_GROUP_ID, [{ disk_id: DISK_ID }]);
        const api = await apiContext(baseURL!);
        try {
          const diskdbPort = deploymentPorts.get(NODE_IDS[0])!.diskdbRpc + 2;
          let instanceId = '';
          await expect.poll(async () => {
            const response = await api.get('/api/diskdb/instances');
            if (!response.ok()) return false;
            const instances = await response.json();
            const instance = instances.find((entry: any) => String(entry.rpc_endpoint).includes(`:${diskdbPort}`));
            instanceId = String(instance?.instance_id ?? '');
            return instanceId.length > 0;
          }, { timeout: 10_000, intervals: [100] }).toBe(true);
          await expect.poll(async () => {
            const response = await api.get('/api/diskdb/instances');
            if (!response.ok()) return false;
            const instances = await response.json();
            return instances.some((entry: any) =>
              String(entry.instance_id) === instanceId && entry.owned_dg_ids.includes(DISK_GROUP_ID),
            );
          }, { timeout: 10_000, intervals: [100] }).toBe(true);
        } finally {
          await api.dispose();
        }
      });

      await step('todo-ui: cluster owns physical children and KV server', async () => {
        await page.goto('/');
        await page.getByTestId('domain-cluster').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const rack = aside.getByRole('treeitem').filter({ hasText: `R-${RACK_ID}` });
        if (await rack.getByRole('button', { name: 'Expand' }).count()) await rack.getByRole('button', { name: 'Expand' }).click();
        const node = aside.getByRole('treeitem').filter({ hasText: `N-${NODE_IDS[0]}` });
        await expect(node).toBeVisible({ timeout: 10_000 });
        if (await node.getByRole('button', { name: 'Expand' }).count()) await node.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(`PKV-${NODE_IDS[0]}`, { exact: true })).toBeVisible();
        const diskdbSubtree = aside.getByTestId(`tree-node-DDB-${NODE_IDS[0]}`);
        await expect(diskdbSubtree).toBeVisible();
        await diskdbSubtree.getByRole('button', { name: 'Expand', exact: true }).click();
        await expect(diskdbSubtree.getByText(/Physical Group.*DG-7710/)).toBeVisible({ timeout: 10_000 });
        const diskGroup = diskdbSubtree.getByRole('treeitem').filter({ hasText: /DG-7710/ });
        if (await diskGroup.getByRole('button', { name: 'Expand' }).count()) await diskGroup.getByRole('button', { name: 'Expand' }).click();
        await expect(diskdbSubtree.getByText(DISK_ID.slice(0, 12), { exact: false })).toBeVisible();

        await node.click({ button: 'right' });
        await expect(page.getByRole('menuitem', { name: /ping/i })).toBeVisible();
        await expect(page.getByRole('menuitem', { name: /add disk group/i })).toHaveCount(0);
        await page.keyboard.press('Escape');

        await page.getByRole('button', { name: /^N-701 Expand children/ }).click();
        const canvasDiskGroup = page.locator('.react-flow__node').filter({ hasText: /Physical Group.*DG-7710/ });
        await expect(canvasDiskGroup).toBeVisible({ timeout: 10_000 });
        await expect(canvasDiskGroup.getByTestId('compact-disk-stack')).toContainText(DISK_ID.slice(0, 12));
      });

      await step('todo-ui: Cluster excludes logical children', async () => {
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        await expect(aside.getByTestId(`tree-node-S-${NODE_IDS[0]}-${STORE_ID}`)).toHaveCount(0);
        await expect(page.locator(`.react-flow__node[data-id="S-${NODE_IDS[0]}-${STORE_ID}"]`)).toHaveCount(0);
      });

      await step('todo-ui: KV logical tree, operations center, and inspector', async () => {
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const datacenter = aside.getByRole('treeitem').filter({ hasText: /^datacenter$/ });
        await expect(datacenter).toBeVisible();
        const store = aside.getByRole('treeitem').filter({ hasText: `S-${STORE_ID}` });
        await expect(store).toBeVisible({ timeout: 10_000 });
        if (await store.getByRole('button', { name: 'Expand' }).count()) await store.getByRole('button', { name: 'Expand' }).click();
        const group = aside.getByRole('treeitem').filter({ hasText: `G-${GROUP_ID}` });
        await expect(group).toBeVisible();
        if (await group.getByRole('button', { name: 'Expand' }).count()) await group.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(`LR-${REPLICA_ID}`, { exact: true })).toBeVisible();
        // KV has one logical tree: no KV server or physical node parent exists.
        await expect(aside.getByText(`PKV-${NODE_IDS[0]}`, { exact: true })).toHaveCount(0);

        await page.getByText(/^KV actions · Store/).click();
        await expect(page.getByLabel('Put key')).toBeVisible();
        await expect(page.getByLabel('Put value')).toBeVisible();
        await group.click();
        const inspector = page.locator('aside[aria-label="Entity inspector"]');
        await expect(inspector).toBeVisible();
        await expect(inspector.locator('div').filter({ hasText: /^Group$/ }).first()).toBeVisible();
        await expect(inspector.locator('div.tw-font-semibold').filter({ hasText: `G-${GROUP_ID}` })).toBeVisible();
      });

      await step('todo-ui: chunk hides DiskDB server', async () => {
        // Capacity view must NOT show the DDB server — it's a service
        // item that belongs in the Cluster domain only. The physical
        // disk hierarchy (DG > Disk) remains.
        await page.getByTestId('domain-capacity').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
        const rack = aside.getByRole('treeitem').filter({ hasText: `R-${RACK_ID}` });
        if (await rack.getByRole('button', { name: 'Expand' }).count()) await rack.getByRole('button', { name: 'Expand' }).click();
        const node = aside.getByRole('treeitem').filter({ hasText: `N-${NODE_IDS[0]}` });
        await expect(node).toBeVisible({ timeout: 10_000 });
        if (await node.getByRole('button', { name: 'Expand' }).count()) await node.getByRole('button', { name: 'Expand' }).click();
        await expect(aside.getByText(/Physical Group.*DG-7710/)).toBeVisible();
        // DDB server must NOT appear in the Capacity view.
        await expect(aside.getByText(`DDB-${NODE_IDS[0]}`, { exact: true })).toHaveCount(0, { timeout: 5_000 });
      });
    } finally {
      await resetAll(baseURL!);
    }
  });

  test('retains failed progress and retries DiskDB with the same node and listener values', async ({ page, baseURL }) => {
    await resetAll(baseURL!);
    await createRack(baseURL!, { id: 704, name: 'Failure Rack' });
    const diskdbBase = freePortRange(3, 'diskdb-listen');
    const blocker = createServer();
    await new Promise<void>((resolve, reject) => {
      blocker.once('error', reject); blocker.listen(diskdbBase + 1, '127.0.0.1', resolve);
    });
    const closeBlocker = () => new Promise<void>(resolve => blocker.close(() => resolve()));
    const registrations: string[] = [];
    page.on('request', request => {
      if (request.method() === 'POST' && request.url().endsWith('/api/nodes')) registrations.push(request.url());
    });
    try {
      await page.goto('/?domain=Cluster');
      const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
      await aside.getByText('R-704 (Failure Rack)').click({ button: 'right' });
      await page.getByRole('menuitem', { name: /add node/i }).click();
      const dialog = page.getByRole('dialog', { name: 'Add Node' });
      await dialog.getByLabel('Node ID').fill('704');
      for (const name of ['crowdb-access-server', 'crowdb-chunk-kv', 'crowdb-chunk-db', 'crowdb-disk-io']) await dialog.getByLabel(name, { exact: true }).uncheck();
      await dialog.getByLabel('crowdb-disk-db RPC port').fill(String(diskdbBase));
      await dialog.getByRole('button', { name: 'Create Node' }).click();
      await expect(dialog).toHaveCount(0);
      await expect.poll(async () => (await page.request.get('/api/nodes/704/server')).ok(), { timeout: 10_000, intervals: [100] }).toBe(true);
      await clusterInit(baseURL!, [704]);
      await expect.poll(async () => {
        const plans = await (await page.request.get('/api/service-plans')).json();
        return plans['704'].steps.diskdb.state;
      }, { timeout: 10_000, intervals: [100] }).toBe('failed');
      const saved = (await (await page.request.get('/api/service-plans')).json())['704'];
      expect(saved.overrides.diskdb.rpc_port).toBe(diskdbBase);
      expect(saved.steps.diskdb.detail).toContain('crowdb-disk-db');
      expect(saved.steps.diskdb.detail).toContain(String(diskdbBase));
      await closeBlocker();
      await aside.getByText('N-704', { exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: /Deploy default services/i }).click();
      const progress = page.getByRole('dialog', { name: 'Node 704 services' });
      await progress.getByRole('button', { name: 'Retry failed services' }).click();
      await expect.poll(async () => {
        const plans = await (await page.request.get('/api/service-plans')).json();
        return plans['704'].steps.diskdb.state;
      }, { timeout: 10_000, intervals: [100] }).toBe('deployed');
      expect(registrations).toHaveLength(1);
      const nodes = await (await page.request.get('/api/nodes')).json();
      expect(nodes.filter((node: any) => node.id === 704)).toHaveLength(1);
    } finally {
      if (blocker.listening) await closeBlocker();
      await resetAll(baseURL!);
    }
  });
});
