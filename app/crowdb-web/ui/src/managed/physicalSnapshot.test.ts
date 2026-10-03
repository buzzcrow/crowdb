// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { physicalSnapshot } from './physicalSnapshot';

afterEach(() => { vi.unstubAllGlobals(); });
describe('confirmed physical snapshot', () => {
  it('converts disk units to bytes and keeps 128-bit disk identity exact', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      source: 'group0', racks: [{ id: 1 }], nodes: [{ id: 1, rack_id: 1 }], services: [],
      disk_groups: [{ rack_id: 1, node_id: 1, dg_id: 3, value: { name: 'actual' } }],
      disks: [{ rack_id: 1, node_id: 1, disk_group_id: 3, disk_id: { high: '18446744073709551615', low: '18446744073709551615' },
        value: { disk_type: 0, capacity_units: 1024, zone_size_units: 256, unit_size_bytes: 4096, device_path: '/disk' } }],
    }))));
    const snapshot = await physicalSnapshot();
    expect(snapshot.diskGroups[1].disksByDg[3][0]).toMatchObject({
      disk_id: 'ffffffffffffffffffffffffffffffff', capacity_bytes: 4194304, zone_size_bytes: 1048576, unit_size_bytes: 4096,
    });
  });
  it('does not render a missing authority as a confirmed empty topology', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ source: 'local' }))));
    await expect(physicalSnapshot()).rejects.toThrow('Confirmed Group 0 snapshot unavailable');
  });
  it('projects Chunk-KV instance and node identities independently with its advertised management origin', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      source: 'group0', racks: [{ id: 3 }], nodes: [{ id: 7, rack_id: 3 }], disk_groups: [], disks: [],
      services: [{ kind: 'chunk-kv', instance_id: '9007199254740993', node_id: 7, endpoint: '127.0.0.1:15201', http_endpoint: 'http://127.0.0.1:15101', monitor: null }],
    }))));
    const snapshot = await physicalSnapshot();
    expect(snapshot.servers).toEqual([{ id: 'chunk-kv-9007199254740993', node_id: 7, service_type: 'chunk-kv',
      endpoint: '127.0.0.1:15201', rpc_url: '127.0.0.1:15201', mgmt_url: 'http://127.0.0.1:15101', health: 'unknown', pid: undefined }]);
  });
});
