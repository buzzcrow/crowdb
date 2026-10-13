// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { APIRequestContext } from '@playwright/test';
import { test, expect } from '../fixtures/realBackend';

async function json(request: APIRequestContext, path: string) {
  const response = await request.get(path);
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

async function createStore(request: APIRequestContext, store_id: number, nodes: number[]) {
  const response = await request.post('/api/stores', { data: { store_id, nodes } });
  expect(response.status(), await response.text()).toBe(201);
}

async function createGroup(request: APIRequestContext, store: number, group_id: number, replica_id: number, nodes: number[]) {
  const response = await request.post(`/api/stores/${store}/groups`, { data: { group_id, replica_id, nodes } });
  expect(response.status(), await response.text()).toBe(201);
}

async function leader(request: APIRequestContext, store: number, group: number, nodes: number[]) {
  await expect.poll(async () => {
    const detail = await json(request, `/api/stores/${store}/groups/${group}`);
    const leaders = detail.replicas.filter((replica: { role: string }) => replica.role === 'leader');
    if (leaders.length !== 1 || !(leaders[0].election?.lease_remaining_ms > 0)) return false;
    return detail.replicas.length === nodes.length
      && detail.replicas.map((replica: { node_id: number }) => replica.node_id).sort().join() === nodes.join()
      && detail.replicas.every((replica: any) => replica.state === 'running' && replica.engine_healthy
        && replica.election?.current_term === leaders[0].election?.current_term
        && replica.election?.bulk_phase1_in_flight_slots === 0);
  }, { timeout: 10_000, intervals: [100] }).toBe(true);
}

async function put(request: APIRequestContext, store: number, group: number, key: string, value: string) {
  const response = await request.post(`/api/stores/${store}/groups/${group}/kv/put`, { data: { key, value } });
  expect(response.ok(), await response.text()).toBe(true);
}

async function get(request: APIRequestContext, store: number, group: number, key: string) {
  const result = await json(request, `/api/stores/${store}/groups/${group}/kv/get?key=${encodeURIComponent(key)}`);
  return result.found ? result.value_utf8 : null;
}

// Baseline: 2.2s (2026-10-12).
test('native topology: three racks grow one replica and retain three complete leaders', async ({ page, request }) => {
  const racks = await json(request, '/api/racks?recursive=1');
  expect(racks.items.map((rack: { id: number }) => rack.id).sort()).toEqual([1, 2, 3]);
  for (const rack of racks.items) expect(rack.nodes.map((node: { rack_id: number }) => node.rack_id)).toEqual([rack.id]);
  await createStore(request, 199, [1]);
  await createGroup(request, 199, 1990, 19900, [1]);
  for (const node_id of [2, 3]) {
    const response = await request.post('/api/stores/199/groups/1990/replicas', { data: { node_id, replica_id: 19899 + node_id } });
    expect(response.status(), await response.text()).toBe(201);
  }
  for (const group of [1991, 1992]) await createGroup(request, 199, group, group * 10, [1, 2, 3]);
  for (const group of [1990, 1991, 1992]) await leader(request, 199, group, [1, 2, 3]);
  await page.goto('/?domain=KV');
  const sidebar = page.getByRole('complementary', { name: 'Cluster tree sidebar' });
  const store = sidebar.getByTestId('tree-node-S-199').locator(':scope > [role="treeitem"]');
  if (await store.getAttribute('aria-expanded') !== 'true') await store.getByRole('button', { name: 'Expand', exact: true }).click();
  for (const group of [1990, 1991, 1992]) await expect(sidebar.getByRole('button', { name: `G-${group}`, exact: true })).toBeVisible();
  await put(request, 199, 1990, 'multi-rack-key', 'multi-rack-value');
  expect(await get(request, 199, 1990, 'multi-rack-key')).toBe('multi-rack-value');
});

// Baseline: 1.0s (2026-10-12).
test('native topology: overlapping three-voter subsets retain independent keys and scans', async ({ request }) => {
  await createStore(request, 400, [1, 2, 3, 4, 5]);
  for (const [index, nodes] of [[1, 2, 3], [2, 3, 4], [3, 4, 5]].entries()) {
    const group = 4000 + index;
    await createGroup(request, 400, group, group * 10, nodes);
    await leader(request, 400, group, nodes);
    await put(request, 400, group, `subset-${index}`, `value-${index}`);
  }
  for (let index = 0; index < 3; index++) {
    for (let key = 0; key < 3; key++) {
      expect(await get(request, 400, 4000 + index, `subset-${key}`)).toBe(key === index ? `value-${key}` : null);
    }
    const scan = await json(request, `/api/stores/400/groups/${4000 + index}/kv/scan?limit=500`);
    expect(scan.truncated).toBe(false);
    expect(scan.items.map((row: { key_utf8: string }) => row.key_utf8)).toEqual([`subset-${index}`]);
  }
});

// Baseline: 1.4s (2026-10-12).
test('native topology: SIMPLE and COMPLEX store layouts route every populated group', async ({ request }) => {
  test.setTimeout(90_000);
  // The old COMPLEX fixture registered eight nodes but every group used only
  // its first three. Preserve those voters and remove five idle servers.
  for (const [store, groups] of [[800, [8000]], [900, [9000, 9001]], [901, [9002, 9003]]] as const) {
    await createStore(request, store, [1, 2, 3]);
    for (const group of groups) {
      await createGroup(request, store, group, 1, [1, 2, 3]);
      await leader(request, store, group, [1, 2, 3]);
      await put(request, store, group, 'comparison-key', `${store}/${group}`);
    }
  }
  const stores = await json(request, '/api/stores');
  for (const [store, groups] of [[800, [8000]], [900, [9000, 9001]], [901, [9002, 9003]]] as const) {
    expect(stores).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: store })]));
    for (const group of groups) expect(await get(request, store, group, 'comparison-key')).toBe(`${store}/${group}`);
  }
});
