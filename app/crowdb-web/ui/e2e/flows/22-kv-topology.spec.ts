// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
// Baseline: 33s (2026-08-17)

import { test, expect, consoleBaseURL } from '../fixtures/realBackend';
import {
  apiContext,
  addGroup,
  addReplica,
  createStoreNoInit,
  deployNodeServer,
  seedRackAndNode,
  freePort,
  waitForLeader,
  resetAll,
  clusterInit,
  SIMPLE,
  COMPLEX,
  type TopologyDescriptor,
  type SetupResult,
} from '../fixtures/consoleSetup';
import { step } from '../fixtures/stepTimer';

const apiBase = consoleBaseURL();

async function kvPut(baseURL: string, storeId: number, groupId: number, key: string, value: string) {
  const resp = await step(`kvPut(s${storeId}/g${groupId})`, () => fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/put`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key, value }),
  }));
  expect(resp.ok).toBeTruthy();
}

async function kvGet(baseURL: string, storeId: number, groupId: number, key: string): Promise<string | null> {
  return await step(`kvGet(s${storeId}/g${groupId})`, async () => {
    const resp = await fetch(`${baseURL}/api/stores/${storeId}/groups/${groupId}/kv/get?key=${encodeURIComponent(key)}`);
    expect(resp.ok).toBeTruthy();
    const body = await resp.json();
    return body.found ? body.value_utf8 : null;
  });
}

async function kvScanAll(baseURL: string, storeId: number, groupId: number): Promise<string[]> {
  return await step(`kvScanAll(s${storeId}/g${groupId})`, async () => {
    const keys: string[] = [];
    let startAfter = '';
    for (;;) {
      const url = `/api/stores/${storeId}/groups/${groupId}/kv/scan?limit=500${startAfter ? `&start_after=${encodeURIComponent(startAfter)}` : ''}`;
      const resp = await fetch(`${baseURL}${url}`);
      expect(resp.ok).toBeTruthy();
      const body = await resp.json();
      keys.push(...body.items.map((i: any) => i.key_utf8));
      if (!body.truncated) break;
      startAfter = body.items[body.items.length - 1]?.key_utf8 ?? '';
      if (!startAfter) break;
    }
    return keys;
  });
}

// Build a SetupResult from a topology descriptor (mirrors what
// setupCluster would return, but without deploying — the cluster is
// already up from beforeAll).
function makeSetupResult(topo: TopologyDescriptor): SetupResult {
  const nodes = Array.from({ length: topo.nodeCount }, (_, i) => topo.nodeBase + i);
  const racks = Array.from({ length: topo.nodeCount }, (_, i) => topo.rackBase + i);
  const stores: number[] = [];
  const groups: { storeId: number; groupId: number }[] = [];
  for (let s = 0; s < topo.storeCount; s++) {
    stores.push(topo.storeBase + s);
    for (let g = 0; g < topo.groupsPerStore; g++) {
      groups.push({ storeId: topo.storeBase + s, groupId: topo.groupBase + s * topo.groupsPerStore + g });
    }
  }
  return { racks, nodes, stores, groups, apiBase };
}

async function runSmokeSuite(baseURL: string, label: string, cluster: SetupResult) {
  // Verify all stores and groups have leaders
  await step(`smoke-${label}: verify leaders`, async () => {
    for (const g of cluster.groups) {
      const api = await apiContext(baseURL);
      try {
        const r = await api.get(`/api/stores/${g.storeId}/groups/${g.groupId}`);
        expect(r.ok(), await r.text()).toBeTruthy();
        const body = await r.json();
        const hasLeader =
          (Array.isArray(body.replicas) && body.replicas.some((x: any) => String(x.role).toLowerCase() === 'leader')) ||
          (typeof body.leader_id === 'number' && body.leader_id > 0);
        expect(hasLeader, `${label}: no leader for store ${g.storeId} group ${g.groupId}`).toBe(true);
      } finally {
        await api.dispose();
      }
    }
  });

  // KV put/get on first group
  const firstGroup = cluster.groups[0];
  const testKey = `cmp-${label}-key`;
  const testValue = `cmp-${label}-value`;
  await kvPut(baseURL, firstGroup.storeId, firstGroup.groupId, testKey, testValue);
  expect(await kvGet(baseURL, firstGroup.storeId, firstGroup.groupId, testKey)).toBe(testValue);

  // Verify stores exist via API
  await step(`smoke-${label}: verify stores API`, async () => {
    const api = await apiContext(baseURL);
    try {
      const storesResp = await api.get('/api/stores');
      expect(storesResp.ok(), await storesResp.text()).toBeTruthy();
      const stores = await storesResp.json();
      for (const sid of cluster.stores) {
        expect(stores).toEqual(expect.arrayContaining([expect.objectContaining({ store_id: sid })]));
      }
    } finally {
      await api.dispose();
    }
  });
}

test.describe('kv cluster · multi-rack/multi-store/multi-group topology', () => {
  // One cluster shared by all 5 tests. Each test uses a disjoint node
  // set + store/group IDs. Group-0 is bootstrapped once on nodes
  // 191-193; all stores are created via createStoreNoInit.
  //
  // Node layout:
  //   191-193  store 199  groups 1990-1992  — multi-rack + leader election
  //   381-386  stores 380+381  groups 3800+3810  — store isolation
  //   391-395  store 390  groups 3900+3901  — overlapping groups
  //   401-405  store 400  groups 4000-4002  — 3 independent groups
  //   100-102  store 800  group 8000  — SIMPLE smoke
  //   200-207  stores 900+901  groups 9000-9003  — COMPLEX smoke
  //
  // The beforeAll hook deploys 23 nodes, 8 stores, 15 Paxos groups, and
  // waits for all leaders. On CI runners this can exceed the default
  // 30s test timeout, so the describe-level timeout is raised to 180s.
  // test.describe.configure applies to beforeAll/afterAll hooks; per-
  // test timeouts are still set individually below.
  test.describe.configure({ timeout: 180_000 });

  test.beforeAll(async () => {
    await step('topology: resetAll', () => resetAll(apiBase));

    const allNodes = [
      ...[191, 192, 193],
      ...[381, 382, 383, 384, 385, 386],
      ...[391, 392, 393, 394, 395],
      ...[401, 402, 403, 404, 405],
      ...[100, 101, 102],
      ...[200, 201, 202, 203, 204, 205, 206, 207],
    ];
    await step('topology: seedRackAndNode', () => Promise.all(allNodes.map((r) => seedRackAndNode(apiBase, r, r))));
    await step('topology: deploy Group 0 nodes', () => Promise.all([191, 192, 193].map((n) => deployNodeServer(apiBase, n, freePort(), freePort()))));

    // Bootstrap group-0 on the first 3 nodes (191, 192, 193).
    await step('topology: clusterInit', () => clusterInit(apiBase, [191, 192, 193]));
    await step('topology: deploy remaining nodes', () => Promise.all(allNodes.slice(3).map((n) => deployNodeServer(apiBase, n, freePort(), freePort()))));

    // Create all stores in parallel — stores are independent. This
    // replaces the per-test serial setup phases with a single fan-out,
    // cutting the worst-case wall time from 5 serial phases to 3.
    await step('topology: createStoreNoInit x8', () => Promise.all([
      createStoreNoInit(apiBase, 199, [191]),
      createStoreNoInit(apiBase, 380, [381, 382, 383]),
      createStoreNoInit(apiBase, 381, [384, 385, 386]),
      createStoreNoInit(apiBase, 390, [391, 392, 393, 394, 395]),
      createStoreNoInit(apiBase, 400, [401, 402, 403, 404, 405]),
      createStoreNoInit(apiBase, 800, [100, 101, 102]),
      createStoreNoInit(apiBase, 900, [200, 201, 202]),
      createStoreNoInit(apiBase, 901, [200, 201, 202]),
    ]));

    // Create all initial groups in parallel — each is an independent
    // Paxos group. Groups 1991/1992 are deferred until addReplica
    // extends store 199 to nodes 192+193.
    await step('topology: addGroup x13', () => Promise.all([
      addGroup(apiBase, 199, 1990, 19900, [191]),
      addGroup(apiBase, 380, 3800, 38000, [381, 382, 383]),
      addGroup(apiBase, 381, 3810, 38100, [384, 385, 386]),
      addGroup(apiBase, 390, 3900, 39000, [391, 392, 393]),
      addGroup(apiBase, 390, 3901, 39010, [393, 394, 395]),
      addGroup(apiBase, 400, 4000, 40000, [401, 402, 403]),
      addGroup(apiBase, 400, 4001, 40010, [402, 403, 404]),
      addGroup(apiBase, 400, 4002, 40020, [403, 404, 405]),
      addGroup(apiBase, 800, 8000, 1, [100, 101, 102]),
      addGroup(apiBase, 900, 9000, 1, [200, 201, 202]),
      addGroup(apiBase, 900, 9001, 1, [200, 201, 202]),
      addGroup(apiBase, 901, 9002, 1, [200, 201, 202]),
      addGroup(apiBase, 901, 9003, 1, [200, 201, 202]),
    ]));

    // Extend store 199 to nodes 192+193 via addReplica. Both calls
    // are independent Paxos writes to group-0 sysdata — run them
    // concurrently instead of serially.
    await step('topology: addReplica x2', () => Promise.all([
      addReplica(apiBase, 199, 1990, 192, 19901),
      addReplica(apiBase, 199, 1990, 193, 19902),
    ]));

    // Groups 1991/1992 span all 3 nodes — create after addReplica
    // extends store 199's node set.
    await step('topology: addGroup 1991+1992', () => Promise.all([
      addGroup(apiBase, 199, 1991, 19910, [191, 192, 193]),
      addGroup(apiBase, 199, 1992, 19920, [191, 192, 193]),
    ]));

    // Wait for all leaders in parallel.
    await step('topology: waitForLeader', () => Promise.all([
      ...[1990, 1991, 1992].map((g) => waitForLeader(apiBase, 199, g, 10_000)),
      waitForLeader(apiBase, 380, 3800, 10_000),
      waitForLeader(apiBase, 381, 3810, 10_000),
      waitForLeader(apiBase, 390, 3900, 10_000),
      waitForLeader(apiBase, 390, 3901, 10_000),
      waitForLeader(apiBase, 400, 4000, 10_000),
      waitForLeader(apiBase, 400, 4001, 10_000),
      waitForLeader(apiBase, 400, 4002, 10_000),
      waitForLeader(apiBase, 800, 8000, 10_000),
      waitForLeader(apiBase, 900, 9000, 10_000),
      waitForLeader(apiBase, 900, 9001, 10_000),
      waitForLeader(apiBase, 901, 9002, 10_000),
      waitForLeader(apiBase, 901, 9003, 10_000),
    ]));
  });

  test.afterAll(async () => {
    // resetAll stops all servers + wipes config — one call replaces
    // N per-node stopNodeServer teardowns.
    await step('topology: resetAll', () => resetAll(apiBase));
  });

  test('creates multi-rack cluster with one store and multiple groups, monitors leader election', async ({ page, baseURL }) => {
    test.setTimeout(60_000);

    const api = await apiContext(baseURL!);
    try {
      await step('multi-rack: goto + verify UI', async () => {
        // Navigate to Cluster view and verify all groups appear in UI.
        await page.goto('/');
        await page.getByTestId('domain-kv').click();
        const aside = page.getByRole('complementary', { name: 'Cluster tree sidebar' });

        for (const gid of [1990, 1991, 1992]) {
          await expect(aside.getByText(`G-${gid}`).first()).toBeVisible({ timeout: 3_000 });
        }
      });

      // Monitor leader election via API polling.
      // Three concurrent fresh elections (one per group) need a few
      // election deadlines (default 4-8 s each) plus PreVote/RequestVote
      // round-trip; 30 s gives headroom on a busy CI machine.
      const groups = [1990, 1991, 1992];
      const leaders = new Map<number, number>();

      // Poll until all groups have exactly one leader, or timeout.
      await step('multi-rack: poll leaders', () => expect.poll(async () => {
        for (const gid of groups) {
          if (leaders.has(gid)) continue;
          const response = await api.get(`/api/stores/199/groups/${gid}`);
          if (!response.ok()) continue;
          const detail: { replicas: Array<{ replica_id: number; role: string }> } = await response.json();
          const leaderReplicas = detail.replicas.filter((r) => r.role === 'leader');
          if (leaderReplicas.length === 1) {
            leaders.set(gid, leaderReplicas[0].replica_id);
          }
        }
        return leaders.size;
      }, { timeout: 10_000, intervals: [200] }).toBe(groups.length));

      // Assert every group has elected exactly one leader.
      for (const gid of groups) {
        const leader = leaders.get(gid);
        expect(
          leader,
          `group ${gid} did not elect exactly one leader (leaders so far: ${JSON.stringify(Array.from(leaders.entries()))})`,
        ).toBeTruthy();
        expect(leader).toBeGreaterThan(0);
      }

      // KV put/get verification: write a key to group 1990 and read it back
      // via the console API to confirm the multi-group cluster serves KV.
      await step('multi-rack: KV put/get API', async () => {
        const putResp = await api.post(`/api/stores/199/groups/1990/kv/put`, {
          data: { key: 'e2e-19-key', value: 'e2e-19-value' },
        });
        expect(putResp.ok(), await putResp.text()).toBeTruthy();
        const getResp = await api.get(`/api/stores/199/groups/1990/kv/get?key=e2e-19-key`);
        expect(getResp.ok(), await getResp.text()).toBeTruthy();
        const getBody = await getResp.json();
        expect(getBody.found).toBe(true);
        expect(getBody.value_utf8).toBe('e2e-19-value');
      });
    } finally {
      await api.dispose();
    }
  });



  test('3 groups on different node subsets operate independently', async ({ baseURL }) => {
    test.setTimeout(60_000);

    // Each group gets its own keys
    await kvPut(baseURL!, 400, 4000, 'mg40-key0', 'val0');
    await kvPut(baseURL!, 400, 4001, 'mg40-key1', 'val1');
    await kvPut(baseURL!, 400, 4002, 'mg40-key2', 'val2');

    // Verify per-group get
    expect(await kvGet(baseURL!, 400, 4000, 'mg40-key0')).toBe('val0');
    expect(await kvGet(baseURL!, 400, 4001, 'mg40-key1')).toBe('val1');
    expect(await kvGet(baseURL!, 400, 4002, 'mg40-key2')).toBe('val2');

    // Cross-group isolation: key0 not visible in group 1 or 2
    expect(await kvGet(baseURL!, 400, 4001, 'mg40-key0')).toBeNull();
    expect(await kvGet(baseURL!, 400, 4002, 'mg40-key0')).toBeNull();

    // Per-group scan: each group only has its own keys
    const scan0 = await kvScanAll(baseURL!, 400, 4000);
    expect(scan0).toContain('mg40-key0');
    expect(scan0).not.toContain('mg40-key1');
    expect(scan0).not.toContain('mg40-key2');

    const scan1 = await kvScanAll(baseURL!, 400, 4001);
    expect(scan1).toContain('mg40-key1');
    expect(scan1).not.toContain('mg40-key0');
    expect(scan1).not.toContain('mg40-key2');

    const scan2 = await kvScanAll(baseURL!, 400, 4002);
    expect(scan2).toContain('mg40-key2');
    expect(scan2).not.toContain('mg40-key0');
    expect(scan2).not.toContain('mg40-key1');
  });

  test('comparative smoke suite passes on SIMPLE and COMPLEX topologies', async ({ baseURL }) => {
    test.setTimeout(90_000);
    // --- SIMPLE topology (3 nodes, 1 store, 1 group) ---
    await runSmokeSuite(baseURL!, 'simple', makeSetupResult(SIMPLE));

    // --- COMPLEX topology (8 nodes, 2 stores, 4 groups) ---
    await runSmokeSuite(baseURL!, 'complex', makeSetupResult(COMPLEX));
  });
});
