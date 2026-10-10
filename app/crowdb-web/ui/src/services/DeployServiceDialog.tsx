// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { Dialog } from '../components/Dialog';
import { Input } from '../components/ui/Input';
import { inputClass } from '../access/Workbench';
import type { EnrichedStoreView, DiskGroupEntry } from '../types';
import type { ServerSummary } from '../api';
import { useDeploymentDefaults } from './useDeploymentDefaults';
import { serviceNames, serviceRequest, type AuxiliaryKind } from './client';

export function DeployServiceDialog({ nodeId, kind, servers, stores, diskGroups, onClose, onSuccess }: {
  nodeId: number; kind: AuxiliaryKind; servers: ServerSummary[]; stores: EnrichedStoreView[];
  diskGroups: DiskGroupEntry[]; onClose: () => void; onSuccess: () => Promise<void>;
}) {
  const [instance, setInstance] = useState(() => String(servers.reduce((maximum, server) => {
    const suffix = server.service_type === kind ? server.id?.split('-').pop() ?? '' : '';
    const value = /^\d+$/.test(suffix) ? BigInt(suffix) : 0n;
    return value > maximum ? value : maximum;
  }, 0n) + 1n));
  const base = { chunkdb: 43200, diskio: 44000, 'chunk-kv': 45100, 'access-server': 9092 }[kind];
  const [httpPort, setHttpPort] = useState(String(base));
  const [rpcPort, setRpcPort] = useState(String(kind === 'diskio' ? base : base + 100));
  const [s3Port, setS3Port] = useState('9091');
  const [healthPort, setHealthPort] = useState('9094');
  const [diskGroup, setDiskGroup] = useState(String(diskGroups[0]?.id ?? ''));
  const [store, setStore] = useState(String(stores.find(store => String(store.store_id) !== '0')?.store_id ?? stores[0]?.store_id ?? ''));
  const firstGroup = stores.find(entry => String(entry.store_id) === store)?.groups.find(entry => String(entry.group_id) !== '0');
  const [bootstrap, setBootstrap] = useState(!!firstGroup);
  const [group, setGroup] = useState(String(firstGroup?.group_id ?? ''));
  const [dynamicOwnership, setDynamicOwnership] = useState(false);
  const [testMode, setTestMode] = useState(false);
  const defaults = useDeploymentDefaults(true);
  useEffect(() => {
    const value = defaults.values?.[kind];
    if (!value) return;
    setInstance(value.instance_id);
    setDynamicOwnership(value.dynamic_ownership ?? false);
    setHttpPort(String(value.http_port ?? ''));
    setRpcPort(String(value.rpc_port ?? ''));
    setS3Port(String(value.s3_port ?? ''));
    setHealthPort(String(value.health_port ?? ''));
  }, [defaults.values, kind]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const ports = kind === 'diskio' ? [rpcPort] : kind === 'access-server' ? [httpPort, s3Port, healthPort] : [httpPort, rpcPort];
  const valid = /^\d+$/.test(instance) && BigInt(instance) > 0n && BigInt(instance) <= 9223372036854775807n
    && ports.every(port => /^\d+$/.test(port) && Number(port) > 0 && Number(port) <= 65535)
    && new Set(ports.map(Number)).size === ports.length
    && (kind !== 'diskio' || !!diskGroup) && (kind !== 'chunk-kv' || (!!store && (!bootstrap || !!group)));
  async function submit() {
    if (!valid || busy) return;
    setBusy(true); setError('');
    try {
      await serviceRequest(`/nodes/${nodeId}/services/deploy`, 'POST', {
        ...(kind === 'chunkdb' ? { dynamic_ownership: dynamicOwnership } : {}),
        kind, instance_id: instance, test_single_node: testMode,
        ...(kind !== 'diskio' ? { http_port: Number(httpPort) } : {}),
        ...(kind === 'access-server' ? { s3_port: Number(s3Port), health_port: Number(healthPort) } : { rpc_port: Number(rpcPort) }),
        ...(kind === 'diskio' ? { disk_group_id: Number(diskGroup) } : {}),
        ...(kind === 'chunk-kv' ? { metadata_store_id: Number(store), ...(bootstrap ? { bootstrap_group_id: Number(group) } : {}) } : {}),
      });
      await onSuccess(); onClose();
    } catch (error) { setError(String(error)); } finally { setBusy(false); }
  }
  return <Dialog isOpen onClose={onClose} title={`Deploy ${serviceNames[kind]}`} confirmLabel="Deploy service" onConfirm={submit} confirmDisabled={!valid || busy || !defaults.values} confirmLoading={busy}>
    <div className="tw-space-y-3">
      <p className="tw-text-sm">Node {nodeId} · uses this cluster's KV management seeds.</p>
      <Input label="Instance ID" inputMode="numeric" value={instance} onChange={event => setInstance(event.target.value)} />
      {kind !== 'diskio' && <Input label={kind === 'access-server' ? 'Iceberg HTTP port' : 'Management HTTP port'} value={httpPort} onChange={event => setHttpPort(event.target.value)} />}
      {kind !== 'access-server' && <Input label="RPC port" value={rpcPort} onChange={event => setRpcPort(event.target.value)} />}
      {kind === 'access-server' && <Input label="S3 HTTP port" value={s3Port} onChange={event => setS3Port(event.target.value)} />}
      {kind === 'access-server' && <Input label="Health HTTP port" value={healthPort} onChange={event => setHealthPort(event.target.value)} />}
      {kind === 'diskio' && <label className="tw-block tw-text-sm">Disk group<select aria-label="Disk group" className={inputClass} value={diskGroup} onChange={event => setDiskGroup(event.target.value)}>
        <option value="">Select disk group</option>{diskGroups.map(group => <option key={group.id} value={group.id}>DG {group.id} {group.name}</option>)}
      </select></label>}
      {kind === 'chunk-kv' && <>
        <label className="tw-block tw-text-sm">Metadata store<select aria-label="Metadata store" className={inputClass} value={store} onChange={event => { setStore(event.target.value); setGroup(''); }}>
          <option value="">Select store</option>{stores.map(store => <option key={store.store_id} value={store.store_id}>Store {store.store_id}</option>)}
        </select></label>
        <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={bootstrap} onChange={event => setBootstrap(event.target.checked)} />Bootstrap first partition if catalog is empty</label>
        {bootstrap && <label className="tw-block tw-text-sm">Journal metadata group<select aria-label="Journal metadata group" className={inputClass} value={group} onChange={event => setGroup(event.target.value)}>
          <option value="">Select non-system group</option>{stores.find(entry => String(entry.store_id) === store)?.groups.filter(entry => String(entry.group_id) !== '0').map(group => <option key={group.group_id} value={group.group_id}>Group {group.group_id}</option>)}
        </select></label>}
      </>}
      {kind === 'chunkdb' && <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={dynamicOwnership}
        disabled={defaults.values?.chunkdb?.dynamic_ownership != null}
        onChange={event => setDynamicOwnership(event.target.checked)} />Automatically redistribute ChunkDB slots</label>}
      <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={testMode} onChange={event => setTestMode(event.target.checked)} />Single-node test deployment (reduced redundancy)</label>
      {kind === 'diskio' && <p className="tw-text-xs tw-text-muted">Production requires disks with device paths. Test deployments may use in-memory disks.</p>}
      {defaults.error && <p role="alert">{defaults.error}</p>}
      {!defaults.values && !defaults.error && <p role="status">Finding available IDs and ports…</p>}
      {error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
    </div>
  </Dialog>;
}
