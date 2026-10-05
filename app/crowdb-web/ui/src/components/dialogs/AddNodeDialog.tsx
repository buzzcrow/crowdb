// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { addNode } from '../../api';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import type { Rack } from '../../types';
import { nextIdFromSuffix } from './defaults';
import { useDeploymentDefaults } from '../../services/useDeploymentDefaults';
import { serviceRequest } from '../../services/client';
import { NodeServiceProgress } from '../../services/NodeServiceProgress';
import { ServicePlanCard, listenerFields } from '../../services/ServicePlanCard';
import { newPlan, serviceOrder, type NodeServicePlan, type ServiceKind, type ServiceOverrides } from '../../services/useNodeServicePlans';

export interface AddNodeDialogProps {
  isOpen: boolean;
  onClose: () => void;
  racks: Rack[];
  defaultRackId?: string;
  existingNodeIds?: string[];
  defaultHost?: string;
  defaultRestPort?: string;
  defaultRpcPort?: string;
  defaultDiskdbRpcPort?: string;
  onDefaultServices?: (nodeId: number, selected?: ServiceKind[], overrides?: ServiceOverrides) => void | Promise<void>;
  servicePlans?: Record<number, NodeServicePlan>;
  onCreatedRackId?: (rackId: number) => void;
  onDiskdbPortReserved?: (port: number) => void;
  onSuccess?: () => void | Promise<void>;
}


export function AddNodeDialog({ isOpen, onClose, racks, defaultRackId,
  existingNodeIds = [], defaultHost = '127.0.0.1', onDefaultServices,
  servicePlans, onCreatedRackId, onDiskdbPortReserved, onSuccess }: AddNodeDialogProps) {
  const [rackId, setRackId] = useState(defaultRackId || String(racks[0]?.id ?? ''));
  const [nodeId, setNodeId] = useState(nextIdFromSuffix(existingNodeIds, 1));
  const [nodeIdTouched, setNodeIdTouched] = useState(false);
  const [host, setHost] = useState(defaultHost);
  const [sshUser, setSshUser] = useState('');
  const [sshKeyPath, setSshKeyPath] = useState('');
  const [selected, setSelected] = useState<ServiceKind[]>([...serviceOrder]);
  const [ports, setPorts] = useState<ServiceOverrides>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [createdId, setCreatedId] = useState<number | null>(null);
  const created = useRef<number | null>(null);
  const submitting = useRef(false);
  const defaults = useDeploymentDefaults(isOpen);

  useEffect(() => {
    if (!isOpen) {
      created.current = null; setCreatedId(null); setError('');
      setSelected([...serviceOrder]); setPorts({}); setNodeIdTouched(false);
      setHost(defaultHost); setSshUser(''); setSshKeyPath('');
    } else if (created.current == null) {
      setRackId(defaultRackId || String(racks[0]?.id ?? ''));
    }
  }, [isOpen, defaultRackId, defaultHost]);
  useEffect(() => {
    if (isOpen && created.current == null && !nodeIdTouched) setNodeId(nextIdFromSuffix(existingNodeIds, 1));
  }, [existingNodeIds.join(','), isOpen, nodeIdTouched]);
  useEffect(() => {
    if (!defaults.values || created.current != null) return;
    setPorts(current => Object.fromEntries(serviceOrder.map(kind => {
      const value = defaults.values![kind];
      return [kind, { ...(value ? Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'instance_id')) : {}), ...current[kind] }];
    })));
  }, [defaults.values]);

  const allPorts = selected.flatMap(kind => [...Object.values(ports[kind] ?? {}), ...(kind === 'diskdb' && ports[kind]?.rpc_port ? [ports[kind]!.rpc_port! + 1, ports[kind]!.rpc_port! + 2] : [])]);
  const validPorts = selected.every(kind => listenerFields(kind).every(([field]) => ports[kind]?.[field] != null))
    && allPorts.every(port => Number.isInteger(port) && port > 0 && port <= 65535)
    && new Set(allPorts).size === allPorts.length
    && (!selected.includes('diskdb') || (ports.diskdb?.rpc_port ?? 0) <= 65533);
  const valid = /^\d+$/.test(nodeId) && Number.isSafeInteger(Number(nodeId)) && Number(nodeId) > 0
    && !!rackId && !!host.trim() && validPorts && !!defaults.values;

  async function submit() {
    if (!valid || submitting.current) return;
    submitting.current = true; setBusy(true); setError('');
    try {
      const id = created.current ?? Number(nodeId);
      if (created.current == null) {
        await addNode({ id, rack_id: Number(rackId), host: host.trim(), ssh_port: 22,
          ssh_user: sshUser.trim(), ...(sshKeyPath.trim() ? { ssh_key: sshKeyPath.trim() } : {}) });
        created.current = id; setCreatedId(id); onCreatedRackId?.(Number(rackId));
      }
      const overrides = Object.fromEntries(selected.map(kind => [kind, ports[kind]])) as ServiceOverrides;
      if (onDefaultServices) await onDefaultServices(id, selected, overrides);
      else {
        const steps = newPlan();
        for (const kind of serviceOrder) if (!selected.includes(kind)) steps[kind] = { state: 'disabled' };
        await serviceRequest(`/nodes/${id}/service-plan`, 'PUT', { revision: 0, steps, overrides });
      }
      if (selected.includes('diskdb')) onDiskdbPortReserved?.(ports.diskdb!.rpc_port!);
      await onSuccess?.();
      onClose();
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : String(failure));
      // Refresh the durable node even if saving its plan failed.
      if (created.current != null) { try { await onSuccess?.(); } catch { /* Keep the original registration or plan error visible. */ } }
    } finally { submitting.current = false; setBusy(false); }
  }
  return <Dialog isOpen={isOpen} onClose={() => { if (!busy) onClose(); }} title="Add Node"
    description="Add a physical node and its service plan"
    confirmLabel={busy ? 'Saving node plan…' : createdId == null ? 'Create Node' : 'Retry service plan'}
    cancelLabel={createdId == null ? 'Cancel' : 'Close'} onConfirm={submit}
    confirmDisabled={!valid || busy} confirmLoading={busy}>
    <div className="tw-space-y-4">
      {defaults.error && <p role="alert">{defaults.error}</p>}
      {!defaults.values && !defaults.error && <p role="status">Finding available ports…</p>}
      {error && <p role="alert" className="tw-text-sm tw-text-failed">{error}</p>}
      {createdId != null && <p role="status">Node {createdId} created · service plan can be retried</p>}
      {createdId != null && servicePlans?.[createdId] && <NodeServiceProgress plan={servicePlans[createdId]} />}
      <fieldset disabled={busy || createdId != null} className="tw-space-y-4">
        <Select label="Rack" value={rackId} onChange={event => setRackId(event.target.value)}>
          <option value="" disabled>Select a rack</option>
          {racks.map(rack => <option key={rack.id} value={rack.id}>{rack.name || rack.id}</option>)}
        </Select>
        <Input label="Node ID" value={nodeId} onChange={event => { setNodeIdTouched(true); setNodeId(event.target.value); }} autoFocus />
        <Input label="Host" value={host} onChange={event => setHost(event.target.value)} />
        <Input label="SSH User (optional)" value={sshUser} onChange={event => setSshUser(event.target.value)} />
        <Input label="SSH Key Path (optional)" value={sshKeyPath} onChange={event => setSshKeyPath(event.target.value)} />
        <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={selected.length > 0}
          onChange={event => setSelected(event.target.checked ? [...serviceOrder] : [])} />Configure services on this node</label>
        <p className="tw-text-xs tw-text-muted">Only PKV starts before Group 0. Other selected services queue and start automatically when their prerequisites are ready.</p>
        <ul aria-label="Default services" className="tw-grid tw-grid-cols-2 tw-gap-2">
          {serviceOrder.map(kind => <ServicePlanCard key={kind} kind={kind} enabled={selected.includes(kind)} ports={ports[kind] ?? {}}
            onToggle={() => setSelected(current => current.includes(kind) ? current.filter(value => value !== kind) : [...current, kind])}
            onChange={(field, value) => setPorts(current => ({ ...current, [kind]: { ...current[kind], [field]: value } }))} />)}
        </ul>
      </fieldset>
    </div>
  </Dialog>;
}
