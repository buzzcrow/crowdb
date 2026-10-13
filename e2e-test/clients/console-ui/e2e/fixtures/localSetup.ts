// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect } from '@playwright/test';
import { apiContext, createRack, createNode } from './registrySetup';
export { apiContext, createRack, createNode };

async function post(baseURL: string, path: string, data: Record<string, unknown>, status = 201) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post(path, { data });
    expect(response.status(), await response.text()).toBe(status);
  } finally {
    await api.dispose();
  }
}

export async function seedRackAndNode(baseURL: string, rackId: number, nodeId: number) {
  await createRack(baseURL, { id: rackId, name: `rack-${rackId}` });
  await createNode(baseURL, { id: nodeId, rack_id: rackId });
}

export async function deployNodeServer(baseURL: string, nodeId: number) {
  await post(baseURL, `/api/nodes/${nodeId}/server/deploy`, { rest_port: 7000, rpc_port: 7001 });
}

export async function stopNodeServer(baseURL: string, nodeId: number) {
  await post(baseURL, `/api/nodes/${nodeId}/server/stop`, {}, 200);
}

export async function clusterInit(baseURL: string, nodeIds: number[]) {
  const api = await apiContext(baseURL);
  try {
    const response = await api.post('/api/cluster/init', { data: { nodes: nodeIds } });
    expect([201, 409], await response.text()).toContain(response.status());
  } finally {
    await api.dispose();
  }
}

export async function createStore(baseURL: string, storeId: number, nodeIds: number[]) {
  if (storeId !== 0) await clusterInit(baseURL, nodeIds);
  await post(baseURL, '/api/stores', { store_id: storeId, nodes: nodeIds });
  const api = await apiContext(baseURL);
  try {
    await expect.poll(async () => {
      const response = await api.get(`/api/nodes/${nodeIds[0]}/stores`);
      expect(response.ok(), await response.text()).toBe(true);
      return (await response.json()).some((store: { store_id: number }) => store.store_id === storeId);
    }, { timeout: 15_000, intervals: [100] }).toBe(true);
  } finally {
    await api.dispose();
  }
}

export async function addGroup(baseURL: string, storeId: number, groupId: number, replicaId: number, nodeIds: number[]) {
  await post(baseURL, `/api/stores/${storeId}/groups`, { group_id: groupId, replica_id: replicaId, nodes: nodeIds });
}

export async function addReplica(baseURL: string, storeId: number, groupId: number, nodeId: number, replicaId: number) {
  await post(baseURL, `/api/stores/${storeId}/groups/${groupId}/replicas`, { node_id: nodeId, replica_id: replicaId });
}

export async function waitForLeader(baseURL: string, storeId: number, groupId: number, timeout = 3000) {
  const api = await apiContext(baseURL);
  try {
    await expect.poll(async () => {
      const response = await api.get(`/api/stores/${storeId}/groups/${groupId}`);
      if (!response.ok()) return false;
      const group = await response.json();
      return group.replicas?.some((replica: { role: string }) => replica.role.toLowerCase() === 'leader')
        || Number(group.leader_id) > 0;
    }, { timeout, intervals: [100] }).toBe(true);
  } finally {
    await api.dispose();
  }
}
