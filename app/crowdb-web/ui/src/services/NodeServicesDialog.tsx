// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { Dialog } from '../components/Dialog';
import { deployServer, deployDiskdb, listServers, type ServerSummary } from '../api';
import type { DiskGroupEntry, EnrichedStoreView } from '../types';
import { serviceNames, serviceRequest, type AuxiliaryKind } from './client';
import type { DeploymentDefaults } from './useDeploymentDefaults';
const labels = { kv: 'CrowDB Storage', diskdb: 'DiskDB', ...serviceNames };
const order = ['kv', 'diskdb', 'chunkdb', 'diskio', 'chunk-kv', 'access-server'] as const;
export function NodeServicesDialog({ nodeId, servers, stores, diskGroups, onClose, onSuccess }: { nodeId: number; servers: ServerSummary[]; stores: EnrichedStoreView[]; diskGroups: DiskGroupEntry[]; onClose: () => void; onSuccess: () => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [current, setCurrent] = useState('');
  async function run() {
    setBusy(true); setMessage('');
    try {
      let existing = await listServers();
      for (const kind of order) {
        if (existing.some(server => server.node_id === nodeId && server.service_type === kind)) continue;
        setCurrent(labels[kind]);
        if (kind !== 'kv' && kind !== 'diskdb' && !stores.some(store => String(store.store_id) === '0')) throw new Error('Pending: initialize Group 0 in KV, then resume this service plan.');
        if (kind === 'diskio' && !diskGroups.length) throw new Error('Pending: add a disk group and disks with device paths in Capacity, then resume.');
        const metadata = stores.flatMap(store => store.groups.filter(group => String(group.group_id) !== '0').map(group => ({ store: store.store_id, group: group.group_id })))[0];
        if (kind === 'chunk-kv' && !metadata) throw new Error('Pending: create a non-system metadata group in KV, then resume.');
        const defaults = await serviceRequest('/deployment-defaults', 'GET') as Record<string, DeploymentDefaults>;
        const value = defaults[kind];
        if (kind === 'kv') await deployServer(nodeId, { rest_port: value.http_port!, rpc_port: value.rpc_port! });
        else if (kind === 'diskdb') await deployDiskdb(nodeId, { rpc_port: value.rpc_port! });
        else await serviceRequest(`/nodes/${nodeId}/services/deploy`, 'POST', {
          kind: kind as AuxiliaryKind, ...value, test_single_node: false,
          ...(kind === 'diskio' ? { disk_group_id: diskGroups[0].id } : {}),
          ...(kind === 'chunk-kv' ? { metadata_store_id: Number(metadata!.store), bootstrap_group_id: Number(metadata!.group) } : {}),
        });
        await onSuccess();
        existing = await listServers();
      }
      setMessage('All six service types are deployed. Inspect their health in the Cluster tree.');
    } catch (error) { setMessage(String(error)); }
    finally { setBusy(false); setCurrent(''); await onSuccess(); }
  }
  return <Dialog isOpen onClose={onClose} title={`Node ${nodeId} services`} confirmLabel="Deploy missing services" confirmDisabled={busy} confirmLoading={busy} onConfirm={run}>
    <p className="tw-text-sm">One instance of each service per node, using normal multi-node deployment. Existing deployments are retained. Resolve pending prerequisites and resume here.</p>
    <ul className="tw-space-y-2 tw-my-4">{order.map(kind => <li key={kind} className="tw-flex tw-justify-between tw-text-sm"><span>{labels[kind]}</span><span>{current === labels[kind] ? 'Deploying…' : servers.some(server => server.node_id === nodeId && server.service_type === kind) ? 'Deployed' : 'Pending'}</span></li>)}</ul>
    {message && <p role="status" className="tw-text-sm">{message}</p>}
  </Dialog>;
}
