// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { getApiBase, type ServerSummary } from '../api';
import { readJson } from '../access/native';
import { NodeHealth, ProcState, type Node, type Rack } from '../types';
import type { NodeDiskGroups } from '../data/useClusterTree';

interface Snapshot {
  source: string;
  racks: Array<{ id: number; name?: string }>;
  nodes: Array<{ id: number; rack_id: number; management_host?: string }>;
  disk_groups: Array<{ rack_id: number; node_id: number; dg_id: number; value?: { name?: string } }>;
  disks: Array<{ rack_id: number; node_id: number; disk_group_id: number; disk_id: { high: string | number; low: string | number }; value: { capacity_units: number; zone_size_units: number; unit_size_bytes: number; disk_type: number; device_path: string } }>;
  services: Array<{ kind: string; instance_id?: string; node_id?: number; endpoint: string; http_endpoint?: string; monitor?: { healthy: boolean; pid: number | null } | null }>;
}
export async function physicalSnapshot() {
  const snapshot = await readJson<Snapshot>(await fetch(`${getApiBase()}/preview`, { cache: 'no-store' }));
  if (snapshot.source !== 'group0') throw new Error('Confirmed Group 0 snapshot unavailable');
  const nodes: Node[] = snapshot.nodes.map(node => {
    const server = snapshot.services.find(service => service.kind === 'kv-server' && service.node_id === node.id);
    const running = server?.monitor?.healthy;
    const diskdb = snapshot.services.find(service => service.kind === 'diskdb' && service.node_id === node.id);
    return { id: node.id, rack_id: node.rack_id, host: node.management_host ?? '', ssh: { type: 'KeyDefault', user: '' },
      ...(server ? { has_server: true, kv_server: { mgmt_url: server.endpoint, rpc_url: '', pid: server.monitor?.pid ?? undefined,
        health: running === undefined ? NodeHealth.Unknown : running ? NodeHealth.Up : NodeHealth.Down,
        state: running === undefined ? ProcState.Unknown : running ? ProcState.Running : ProcState.Failed, last_seen_ms: Date.now() } } : {}),
      ...(diskdb ? { diskdb_server: { endpoint: diskdb.endpoint, pid: diskdb.monitor?.pid ?? undefined,
        state: diskdb.monitor?.healthy ? ProcState.Running : ProcState.Unknown,
        health: diskdb.monitor?.healthy ? NodeHealth.Up : NodeHealth.Unknown } } : {}) };
  });
  const racks: Rack[] = snapshot.racks.map(rack => ({ ...rack, nodes: nodes.filter(node => node.rack_id === rack.id) }));
  const diskGroups: Record<number, NodeDiskGroups> = {};
  for (const group of snapshot.disk_groups) {
    const entry = diskGroups[group.node_id] ?? { diskGroups: [], disksByDg: {} };
    entry.diskGroups.push({ id: group.dg_id, rack_id: group.rack_id, node_id: group.node_id, name: group.value?.name });
    entry.disksByDg[group.dg_id] = snapshot.disks.filter(disk => disk.node_id === group.node_id && disk.disk_group_id === group.dg_id).map(disk => ({
      disk_id: BigInt(disk.disk_id.high).toString(16).padStart(16, '0') + BigInt(disk.disk_id.low).toString(16).padStart(16, '0'),
      disk_group_id: disk.disk_group_id, rack_id: disk.rack_id, node_id: disk.node_id, disk_type: ['Hdd', 'Ssd', 'ZoneSsd', 'SmrHdd'][disk.value.disk_type] ?? `Unknown (${disk.value.disk_type})`,
      capacity_bytes: disk.value.capacity_units * disk.value.unit_size_bytes, zone_size_bytes: disk.value.zone_size_units * disk.value.unit_size_bytes, unit_size_bytes: disk.value.unit_size_bytes, device_path: disk.value.device_path,
    }));
    diskGroups[group.node_id] = entry;
  }
  const servers: ServerSummary[] = snapshot.services.map(service => ({
    id: `${service.kind}-${service.instance_id ?? service.endpoint}`, node_id: service.node_id,
    service_type: service.kind === 'kv-server' ? 'paxos-kv' : service.kind,
    endpoint: service.endpoint, rpc_url: service.kind === 'kv-server' ? undefined : service.endpoint,
    mgmt_url: service.http_endpoint ?? (service.kind === 'kv-server' ? service.endpoint : undefined),
    pid: service.monitor?.pid ?? undefined,
    health: service.monitor ? service.monitor.healthy ? 'up' : 'down' : 'unknown',
  }));
  return { racks, nodes, diskGroups, servers };
}
