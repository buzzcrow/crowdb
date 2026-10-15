// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Input, Select } from '../components/ui/Input';
import { serviceLabels } from '../services/useNodeServicePlans';
import type { DiskInventory } from '../storage/types';
import { formatBytes } from '../utils/capacity';
import { automaticStorage, discoveredStorage, type NodeSetupChoices, type SetupListeners, type SetupService } from './nodeSetup';

interface Props { choices: NodeSetupChoices; inventory: DiskInventory; disabled: boolean; onChange: (choices: NodeSetupChoices) => void }
type Port = 'http_port' | 'rpc_port' | 's3_port' | 'health_port';
function listeners(service: SetupService): Port[] {
  if (service.kind === 'paxos-kv') return [];
  if (service.kind === 'access-server') return ['http_port', 's3_port', 'health_port'];
  return service.kind === 'diskdb' || service.kind === 'diskio' ? ['rpc_port'] : ['http_port', 'rpc_port'];
}

export function NodeSetupOptions({ choices, inventory, disabled, onChange }: Props) {
  const storage = choices.storage;
  const supplied = discoveredStorage(inventory);
  const updateService = (index: number, change: Partial<SetupService>) => onChange({ ...choices, services: choices.services.map((service, position) => position === index ? { ...service, ...change } : service) });
  const updateListener = (index: number, field: keyof SetupListeners, value: number | boolean | undefined) => updateService(index, { listeners: { ...choices.services[index].listeners, [field]: value } });
  return <fieldset disabled={disabled} className="tw-space-y-4">
    <section aria-label="Node service choices" className="tw-space-y-2">
      <h3 className="tw-font-semibold">Services</h3>
      <p className="tw-text-xs tw-text-muted">Selected services start on the node after their prerequisites are ready. Blank listeners use available defaults. KV bootstrap retains ports 10000 / 10100.</p>
      <p className="tw-text-xs tw-text-muted">Unselecting a service retains any existing deployment, including the admitted KV process.</p>
      <div className="tw-grid tw-grid-cols-1 sm:tw-grid-cols-2 tw-gap-2">{choices.services.map((service, index) => <div key={service.kind} className="tw-rounded tw-border tw-border-border tw-p-2 tw-space-y-2">
        <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={service.enabled} onChange={event => updateService(index, { enabled: event.target.checked })} />{serviceLabels[service.kind]}</label>
        <fieldset disabled={!service.enabled} className="tw-space-y-2">{listeners(service).map(field => <Input key={field} label={`${serviceLabels[service.kind]} ${field.replace('_port', '').toUpperCase()} port`}
          inputMode="numeric" placeholder="Automatic" value={Number.isNaN(service.listeners[field]) ? '' : service.listeners[field] ?? ''}
          onChange={event => updateListener(index, field, event.target.value === '' ? undefined : Number(event.target.value))} />)}
          {service.kind === 'diskdb' && <Input label="DiskDB heartbeat interval (seconds)" inputMode="numeric" placeholder="Service default"
            value={service.listeners.keepalive_interval_secs ?? ''} onChange={event => updateListener(index, 'keepalive_interval_secs', event.target.value === '' ? undefined : Number(event.target.value))} />}
          {service.kind === 'chunkdb' && <Select label="ChunkDB ownership policy" value={service.listeners.dynamic_ownership == null ? 'default' : String(service.listeners.dynamic_ownership)}
            onChange={event => updateListener(index, 'dynamic_ownership', event.target.value === 'default' ? undefined : event.target.value === 'true')}>
            <option value="default">Cluster default</option><option value="false">Fixed</option><option value="true">Dynamic</option>
          </Select>}
        </fieldset>
      </div>)}</div>
    </section>
    <section aria-label="Node storage choices" className="tw-space-y-2">
      <h3 className="tw-font-semibold">Storage</h3>
      <Select label="Storage setup" value={storage.mode} onChange={event => onChange({ ...choices, storage: event.target.value === 'diskless' ? { mode: 'diskless' } : event.target.value === 'automatic' ? automaticStorage() : supplied })}>
        {(!inventory.disks.length || storage.mode === 'automatic') && <option value="automatic">Create a managed file</option>}
        {!!inventory.disks.length && <option value="supplied">Use selected discovered disks</option>}
        <option value="diskless">Join without storage</option>
      </Select>
      {storage.mode === 'automatic' && <>
        <Input label="Managed file capacity (GiB)" inputMode="decimal" value={storage.capacity_bytes / 1024 ** 3}
          onChange={event => onChange({ ...choices, storage: { ...storage, capacity_bytes: Number(event.target.value) * 1024 ** 3 } })} />
        <details><summary>Advanced allocation</summary><label className="tw-flex tw-gap-2 tw-text-sm tw-mt-2"><input type="checkbox" checked={storage.sparse}
          onChange={event => onChange({ ...choices, storage: { ...storage, sparse: event.target.checked } })} />Sparse file</label></details>
        <p className="tw-text-xs tw-text-muted">One {formatBytes(storage.capacity_bytes)} file under /opt/crowdb/data/disks · {storage.sparse ? 'Sparse' : 'Preallocated'} · 1 GiB host reserve. Allocation failure never falls back to sparse.</p>
      </>}
      {storage.mode === 'diskless' && <p className="tw-text-xs tw-text-muted">No disk is created or initialized. Storage-dependent services may wait.</p>}
      {inventory.disks.map(disk => {
        const selected = storage.mode === 'supplied' && storage.disks.some(item => item.path === disk.path);
        const eligible = supplied.mode === 'supplied' ? supplied.disks.find(item => item.path === disk.path) : undefined;
        const reason = disk.error ?? (!disk.supplied ? 'Not an explicitly supplied disk.'
          : !disk.resource_identity ? 'Disk source could not be verified.'
          : disk.capacity_bytes !== null && disk.capacity_bytes < 64 * 1024 ** 2 ? 'At least 64 MiB is required.'
          : 'Full blank and ownership checks are required before initialization.');
        return <article key={disk.path} className="tw-rounded tw-border tw-border-border tw-p-2 tw-space-y-1">
          <label className="tw-flex tw-gap-2 tw-text-xs tw-break-all"><input type="checkbox" aria-label={`Use ${disk.path}`} checked={selected} disabled={!eligible || storage.mode !== 'supplied'}
            onChange={event => { if (storage.mode === 'supplied' && eligible) onChange({ ...choices, storage: { ...storage, disks: event.target.checked ? [...storage.disks, eligible] : storage.disks.filter(item => item.path !== disk.path) } }); }} />{disk.path}</label>
          <p className="tw-text-xs tw-text-muted">{disk.backend ?? 'Unknown backend'} · {disk.capacity_bytes === null ? 'Unknown capacity' : formatBytes(disk.capacity_bytes)} · {reason}</p>
        </article>;
      })}
      {storage.mode === 'supplied' && storage.disks.some(disk => disk.backend === 'block-device') && <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox"
        checked={storage.disks.filter(disk => disk.backend === 'block-device').every(disk => disk.initialize)}
        onChange={event => onChange({ ...choices, storage: { ...storage, disks: storage.disks.map(disk => ({ ...disk, initialize: disk.backend === 'file' || event.target.checked })) } })} />I authorize initialization of the selected block devices after blank validation.</label>}
      {!!inventory.disks.length && <p className="tw-text-xs tw-text-muted">Existing inputs prevent automatic fallback files. Unselected or ineligible disks remain candidates.</p>}
    </section>
  </fieldset>;
}
