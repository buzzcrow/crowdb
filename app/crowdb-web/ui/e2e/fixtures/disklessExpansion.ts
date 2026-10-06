// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { APIRequestContext, Page } from '@playwright/test';
import { expect } from './realBackend';
import { waitForLeader } from './consoleSetup';
import type { observeDefaultDeployments } from './defaultDeployments';
import { step } from './stepTimer';

type UIContext = { page: Page; request: APIRequestContext; baseURL: string; deployments: ReturnType<typeof observeDefaultDeployments> };
const disklessNodes = [4, 5, 6];

export async function disklessExpansion(context: UIContext) {
  const { page, request, baseURL, deployments } = context;
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  await step('diskless: add three default node plans through UI', async () => {
    await page.getByTestId('domain-cluster').click();
    for (const id of disklessNodes) {
      await sidebar.getByRole('button', { name: 'R-1', exact: true }).click({ button: 'right' });
      await page.getByRole('menuitem', { name: 'Add Node', exact: true }).click();
      const dialog = page.getByRole('dialog', { name: 'Add Node', exact: true });
      await dialog.getByLabel('Node ID', { exact: true }).fill(String(id));
      await dialog.getByLabel('Host', { exact: true }).fill('127.0.0.1');
      await expect(dialog.getByRole('checkbox', { checked: true })).toHaveCount(8);
      await expect(dialog.getByLabel('Automatically redistribute ChunkDB slots')).toBeChecked();
      await dialog.getByRole('button', { name: 'Create Node', exact: true }).click();
      await expect(dialog).toHaveCount(0);
    }
  });
  await step('diskless: all services become ready without local disks or Retry', async () => {
    for (const kind of ['paxos-kv', 'diskdb', 'diskio', 'chunkdb', 'chunk-kv', 'access-server']) {
      for (const id of disklessNodes) {
        await deployments.verify(id, kind);
        await expect.poll(async () => {
          const response = await request.get('/api/service-plans');
          expect(response.ok(), await response.text()).toBe(true);
          return (await response.json())[id].steps[kind];
        }, { intervals: [100], message: `Diskless Node ${id}: ${kind} automatically deploys` }).toEqual({ state: 'deployed' });
      }
    }
    for (const id of disklessNodes) {
      const response = await request.get(`/api/nodes/${id}/disk-groups`);
      expect(response.ok(), await response.text()).toBe(true);
      expect(await response.json()).toEqual([]);
    }
    const storage = await request.get('/api/chunk-storage-readiness');
    expect(storage.ok(), await storage.text()).toBe(true);
    expect(await storage.json()).toMatchObject({ ready: true });
    const servers = await (await request.get('/api/servers')).json();
    for (const server of servers.filter((server: { node_id: number; service_type: string }) => disklessNodes.includes(server.node_id) && ['chunkdb', 'chunk-kv'].includes(server.service_type))) {
      const response = await request.get(`${server.mgmt_url}/ready`);
      expect(response.status(), `${server.id}: ${await response.text()}`).toBe(200);
      await expect(sidebar.getByTestId(`tree-node-SERVICE-${server.id}`).getByTitle('Healthy', { exact: true })).toBeVisible();
    }
    expect(servers.filter((server: { pid: number }) => server.pid)).toHaveLength(36);
    // New local KV processes need not host Group 0. DiskDB still registers
    // through the cluster seeds while owning no disk groups.
    await expect.poll(async () => {
      const response = await request.get('/api/diskdb/instances');
      expect(response.ok(), await response.text()).toBe(true);
      return (await response.json()).length;
    }, { intervals: [100] }).toBe(6);
  });
  await step('diskless: create a KV group on existing and newly added nodes through UI', async () => {
    await page.getByTestId('domain-kv').click();
    await sidebar.getByRole('button', { name: 'S-0', exact: true }).click({ button: 'right' });
    await page.getByRole('menuitem', { name: 'Add Group', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Add Group', exact: true });
    await dialog.getByLabel('Group ID (numeric)').fill('2');
    await dialog.getByLabel('Starting Replica ID (numeric)').fill('200');
    for (const id of [1, 2, 3, 4, 5, 6]) {
      await dialog.getByLabel(new RegExp(`^${id}\\b`)).setChecked([3, 4, 5].includes(id));
    }
    const created = page.waitForResponse(response => response.request().method() === 'POST' && response.url().endsWith('/api/stores/0/groups'));
    await dialog.getByRole('button', { name: 'Create Group', exact: true }).click();
    const response = await created;
    expect(response.status(), await response.text()).toBe(201);
    await expect(dialog).toHaveCount(0);
    const replicas = await request.get('/api/stores/0/groups/2/replicas');
    expect(replicas.ok(), await replicas.text()).toBe(true);
    expect((await replicas.json()).map((replica: { node_id: number }) => replica.node_id).sort()).toEqual([3, 4, 5]);
    await waitForLeader(baseURL, 0, 2, 10_000);
  });
  await step('diskless: new KV group persists a value through UI', async () => {
    await page.getByTestId('kv-store-select').selectOption('0');
    await page.getByTestId('kv-group-select').selectOption('2');
    await page.getByText(/^KV actions · Store/).click();
    await page.getByLabel('Put key').fill('diskless-node-round-trip');
    await page.getByLabel('Put value').fill('nodes 3, 4 and 5');
    const put = page.waitForResponse(response => response.url().endsWith('/api/stores/0/groups/2/kv/put'));
    await page.getByRole('button', { name: 'Put', exact: true }).click();
    const response = await put;
    expect(response.ok(), await response.text()).toBe(true);
    await page.getByLabel('Get key').fill('diskless-node-round-trip');
    await page.getByRole('button', { name: 'Get', exact: true }).click();
    await expect(page.getByTestId('kv-get-result')).toHaveText('nodes 3, 4 and 5');
  });
}

export async function verifyDisklessSlotBalance({ request }: UIContext) {
  await step('diskless: dynamic slots reach every prepared owner', async () => {
    await expect.poll(async () => {
      const results = await Promise.all([1, 2, 3, 4, 5, 6].map(async instance => {
        const response = await request.get(`/api/chunk-slots?layer=service&instance_id=${instance}`);
        expect(response.ok(), await response.text()).toBe(true);
        return (await response.json()).owned_count as number;
      }));
      return results.every(count => count >= 170 && count <= 171) && results.reduce((sum,count) => sum+count,0) === 1024;
    }, { timeout: 60_000, intervals: [100] }).toBe(true);
    const response = await request.get('/api/chunk-slots?layer=storage&store_id=0&group_id=1');
    expect(response.ok(), await response.text()).toBe(true);
    expect(await response.json()).toMatchObject({ generation: '1', owned_count: 1024 });
  });
}
