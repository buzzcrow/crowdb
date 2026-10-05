// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { useDeploymentDefaults } from '../../services/useDeploymentDefaults';
import { Dialog } from '../Dialog';
import { Input, Select } from '../ui/Input';
import { useToast } from '../../contexts/ToastContext';
import { addNode, deployServer, deployDiskdb } from '../../api';
import { Rack } from '../../types';
import { nextIdFromSuffix } from './defaults';
import { NodeServiceProgress } from '../../services/NodeServiceProgress';
import { serviceRequest } from '../../services/client';
import type { DeploymentDefaults } from '../../services/useDeploymentDefaults';
import { serviceLabels, serviceOrder, type NodeServicePlan, type ServiceKind, type ServiceOverrides } from '../../services/useNodeServicePlans';

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
  onDefaultServices?: (nodeId: number, selected?: ServiceKind[], overrides?: ServiceOverrides) => void;
  servicePlans?: Record<number, NodeServicePlan>;
  onCreatedRackId?: (rackId: number) => void;
  onDiskdbPortReserved?: (port: number) => void;
  onSuccess?: () => void | Promise<void>;
}

/**
 * Dialog for adding a new node.
 */
export function AddNodeDialog({
  isOpen,
  onClose,
  racks,
  defaultRackId,
  existingNodeIds = [],
  defaultHost = '127.0.0.1',
  defaultRestPort = '19910',
  defaultRpcPort = '19920',
  defaultDiskdbRpcPort = '29920',
  onDefaultServices,
  servicePlans,
  onCreatedRackId,
  onDiskdbPortReserved,
  onSuccess,
}: AddNodeDialogProps) {
  const initialRackId = defaultRackId || racks[0]?.id || '';
  const initialNodeId = nextIdFromSuffix(existingNodeIds, 1);
  const [rackId, setRackId] = useState(initialRackId);
  const [nodeId, setNodeId] = useState(initialNodeId);
  const [nodeIdTouched, setNodeIdTouched] = useState(false);
  const [host, setHost] = useState(defaultHost);
  const [sshUser, setSshUser] = useState('');
  const [sshKeyPath, setSshKeyPath] = useState('');
  const [enableCrowdbKV, setEnableCrowdbKV] = useState(true);
  const [restPort, setRestPort] = useState(defaultRestPort);
  const [rpcPort, setRpcPort] = useState(defaultRpcPort);
  const [enableDiskdb, setEnableDiskdb] = useState(true);
  const [diskdbRpcPort, setDiskdbRpcPort] = useState(defaultDiskdbRpcPort);
  const [completeSet, setCompleteSet] = useState(true);
  const [selectedServices, setSelectedServices] = useState<ServiceKind[]>([...serviceOrder]);
  const [servicePorts, setServicePorts] = useState<Record<string, { http?: string; rpc?: string; s3?: string }>>({});
  const [isLoading, setIsLoading] = useState(false);
  const { success, error } = useToast();
  const created = useRef<number | null>(null);
  const completed = useRef(new Set<string>());
  const [serviceError, setServiceError] = useState('');
  const [createdId, setCreatedId] = useState<number | null>(null);
  const [initialDone, setInitialDone] = useState(false);
  const plan = createdId == null ? undefined : servicePlans?.[createdId];

  useEffect(() => {
    if (!isOpen || created.current != null) return;
    setNodeId(nextIdFromSuffix(existingNodeIds, 1));
    setNodeIdTouched(false);
    setRackId(initialRackId);
    setCompleteSet(true);
    setRestPort(defaultRestPort);
    setRpcPort(defaultRpcPort);
    setEnableCrowdbKV(true);
    setEnableDiskdb(true);
    setDiskdbRpcPort(defaultDiskdbRpcPort);
  }, [defaultRpcPort, defaultRestPort, defaultDiskdbRpcPort, isOpen]);

  // The cluster tree may finish loading after the dialog opens. Keep the
  // untouched default away from an existing node id, but never overwrite an
  // id the operator has started editing.
  useEffect(() => {
    if (isOpen && created.current == null && !nodeIdTouched) {
      setNodeId(nextIdFromSuffix(existingNodeIds, 1));
    }
  }, [existingNodeIds.join(','), isOpen, nodeIdTouched]);

  const defaults = useDeploymentDefaults(isOpen);
  useEffect(() => {
    if (!defaults.values || created.current != null) return;
    const paxosKv = defaults.values['paxos-kv'] ?? defaults.values.kv;
    if (paxosKv?.http_port != null) setRestPort(String(paxosKv.http_port));
    if (paxosKv?.rpc_port != null) setRpcPort(String(paxosKv.rpc_port));
    setDiskdbRpcPort(String(defaults.values.diskdb.rpc_port));
  }, [defaults.values]);
  useEffect(() => {
    if (!defaults.values || created.current != null) return;
    setServicePorts(Object.fromEntries(serviceOrder.map(kind => [kind, {
      http: defaults.values?.[kind]?.http_port?.toString(), rpc: defaults.values?.[kind]?.rpc_port?.toString(), s3: defaults.values?.[kind]?.s3_port?.toString(),
    }])));
  }, [defaults.values]);

  const isPort = (value: string) => /^\d+$/.test(value) && Number(value) > 0 && Number(value) < 65536;
  const deployPortsValid = isPort(restPort) && isPort(rpcPort) && restPort !== rpcPort;
  const diskdbPortsValid = isPort(diskdbRpcPort);

  const handleSubmit = async () => {
    if (!rackId || !nodeId.trim() || !host.trim() || (enableCrowdbKV && !deployPortsValid) || (enableDiskdb && !diskdbPortsValid)) return;

    setIsLoading(true);
    try {
      const trimmedNodeId = nodeId.trim();
      const numericNodeId = created.current ?? Number(trimmedNodeId);
      if (created.current == null) {
        await addNode({
          id: numericNodeId, rack_id: Number(rackId), host: host.trim(),
          ssh_port: 22, ssh_user: sshUser.trim(),
          ...(sshKeyPath.trim() ? { ssh_key: sshKeyPath.trim() } : {}),
        });
        created.current = numericNodeId;
        setCreatedId(numericNodeId);
        onCreatedRackId?.(Number(rackId));
        if (enableDiskdb) onDiskdbPortReserved?.(Number(diskdbRpcPort));
      }
      let kvRest = Number(restPort), kvRpc = Number(rpcPort), ddbRpc = Number(diskdbRpcPort);
      if (servicePorts['paxos-kv']?.http) kvRest = Number(servicePorts['paxos-kv'].http);
      if (servicePorts['paxos-kv']?.rpc) kvRpc = Number(servicePorts['paxos-kv'].rpc);
      if (servicePorts.diskdb?.rpc) ddbRpc = Number(servicePorts.diskdb.rpc);
      if (serviceError) {
        const fresh = await serviceRequest('/deployment-defaults', 'GET') as Record<string, DeploymentDefaults>;
        const paxosKv = fresh['paxos-kv'] ?? fresh.kv;
        kvRest = paxosKv.http_port!; kvRpc = paxosKv.rpc_port!; ddbRpc = fresh.diskdb.rpc_port!;
      }
      setServiceError('');
      const serviceErrors: string[] = [];
      if (enableCrowdbKV && !completed.current.has('paxos-kv')) {
        try {
          await deployServer(numericNodeId, {
            rest_port: kvRest,
            rpc_port: kvRpc,
          });
          completed.current.add('paxos-kv');
        } catch (err) {
          serviceErrors.push(`crowdb-paxos-kv: ${err instanceof Error ? err.message : 'deployment failed'}`);
        }
      }

      if (enableDiskdb && !completed.current.has('diskdb')) {
        try {
          await deployDiskdb(numericNodeId, {
            rpc_port: ddbRpc,
          });
          completed.current.add('diskdb');
        } catch (err) {
          serviceErrors.push(`crowdb-disk-db: ${err instanceof Error ? err.message : 'deployment failed'}`);
        }
      }

      // Start the complete service plan even when a directly deployed
      // prerequisite failed. The plan records the failure and keeps the
      // remaining default services visible/retriable instead of silently
      // stopping after KV or DiskDB.
      if (completeSet) {
        const serviceOverrides: ServiceOverrides = {};
        for (const kind of serviceOrder) {
          const ports = servicePorts[kind];
          if (ports) serviceOverrides[kind] = { ...(ports.http ? { http_port: Number(ports.http) } : {}), ...(ports.rpc ? { rpc_port: Number(ports.rpc) } : {}), ...(ports.s3 ? { s3_port: Number(ports.s3) } : {}) };
        }
        onDefaultServices?.(numericNodeId, selectedServices, serviceOverrides);
      }
      if (serviceErrors.length > 0) {
        setServiceError(serviceErrors.join('; '));
      } else {
        const parts = [`Node "${trimmedNodeId}" created`];
        if (enableCrowdbKV) parts.push('crowdb-paxos-kv enabled');
        if (enableDiskdb) parts.push('crowdb-disk-db enabled');
        success(parts.join(', '));
      }
      await onSuccess?.();
      if (serviceErrors.length === 0) {
        setInitialDone(true);
        // The submission is the only confirmation.  The service plan is
        // persisted above, so progress remains visible in the Cluster tree
        // after this dialog closes and does not require a second Done click.
        onClose();
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create node';
      setServiceError(message);
      error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    if (isLoading) return;
    setRackId(initialRackId);
    setNodeId(initialNodeId);
    setNodeIdTouched(false);
    setHost(defaultHost);
    setSshUser('');
    setSshKeyPath('');
    setEnableCrowdbKV(true);
    setRestPort(defaultRestPort);
    setRpcPort(defaultRpcPort);
    setEnableDiskdb(true);
    setDiskdbRpcPort(defaultDiskdbRpcPort);
    onClose();
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title="Add Node"
      description="Add a new physical node to your infrastructure"
      confirmLabel={isLoading ? 'Deploying services…' : createdId == null ? 'Create Node' : !initialDone ? 'Retry failed services' : plan && Object.values(plan).some(step => step.state === 'failed') ? 'Retry failed services' : 'Done'}
      cancelLabel={createdId == null ? 'Cancel' : 'Close'}
      onConfirm={initialDone ? () => { if (plan && Object.values(plan).some(step => step.state === 'failed')) onDefaultServices?.(createdId!); else handleClose(); } : handleSubmit}
      confirmDisabled={!defaults.values || !rackId || !nodeId.trim() || !host.trim() || isLoading || (enableCrowdbKV && !deployPortsValid) || (enableDiskdb && !diskdbPortsValid)}
      confirmLoading={isLoading}
    >
      <div className="tw-space-y-4">
        {defaults.error && <p role="alert">{defaults.error}</p>}
        {!defaults.values && !defaults.error && <p role="status">Finding available ports…</p>}
        {serviceError && <p role="alert" className="tw-text-sm tw-text-failed">{serviceError}</p>}
        {createdId != null && <div role="status" className="tw-text-sm">Node {createdId} created{isLoading ? ' · deploying services…' : ''}</div>}
        {plan && <NodeServiceProgress plan={plan} />}
        <fieldset disabled={createdId != null || isLoading} className={initialDone ? 'tw-hidden' : 'tw-space-y-4'}>
        {racks.length > 0 ? (
          <Select
            label="Rack"
            value={rackId}
            onChange={(e) => setRackId(e.target.value)}
          >
            <option value="" disabled>Select a rack</option>
            {racks.map((rack) => (
              <option key={rack.id} value={rack.id}>
                {rack.name || rack.id}
              </option>
            ))}
          </Select>
        ) : (
          <div className="tw-text-sm tw-text-muted">
            No racks available. Create a rack first.
          </div>
        )}
        <Input
          label="Node ID"
          placeholder="N-01"
          value={nodeId}
          onChange={(e) => { setNodeIdTouched(true); setNodeId(e.target.value); }}
          autoFocus
        />
        <Input
          label="Host"
          placeholder="192.168.1.100 or example.com"
          value={host}
          onChange={(e) => setHost(e.target.value)}
        />
        <Input
          label="SSH User (optional)"
          placeholder="root"
          value={sshUser}
          onChange={(e) => setSshUser(e.target.value)}
        />
        <Input
          label="SSH Key Path (optional)"
          placeholder="~/.ssh/id_rsa"
          value={sshKeyPath}
          onChange={(e) => setSshKeyPath(e.target.value)}
        />
        <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={completeSet} onChange={event => { const enabled = event.target.checked; setCompleteSet(enabled); if (enabled) { setEnableCrowdbKV(true); setEnableDiskdb(true); setSelectedServices([...serviceOrder]); } else { setEnableCrowdbKV(false); setEnableDiskdb(false); setSelectedServices([]); } }} />Configure services on this node</label>
        {completeSet && <>
          <p className="tw-text-xs tw-text-muted">Select services and configure their listeners below. Services with cluster prerequisites wait and start automatically when ready.</p>
          <ul aria-label="Default services" className="tw-grid tw-grid-cols-2 tw-gap-2 tw-text-xs">
            {serviceOrder.map(kind => {
              const value = defaults.values?.[kind];
              const listeners = [value?.http_port && `HTTP ${value.http_port}`, value?.rpc_port && `RPC ${value.rpc_port}`, value?.s3_port && `S3 ${value.s3_port}`].filter(Boolean).join(' · ');
              return <li key={kind} className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1.5"><label className="tw-flex tw-items-center tw-gap-2"><input type="checkbox" aria-label={serviceLabels[kind]} checked={selectedServices.includes(kind)} onChange={() => setSelectedServices(current => { const enabled = !current.includes(kind); if (kind === 'paxos-kv') setEnableCrowdbKV(enabled); if (kind === 'diskdb') setEnableDiskdb(enabled); return enabled ? [...current, kind] : current.filter(value => value !== kind); })} /><span>{serviceLabels[kind]}</span></label>{selectedServices.includes(kind) && <div className="tw-mt-1 tw-grid tw-grid-cols-2 tw-gap-1">{(['http','rpc','s3'] as const).filter(port => servicePorts[kind]?.[port]).map(port => <Input key={port} label={port.toUpperCase()} value={servicePorts[kind]?.[port] ?? ''} onChange={event => setServicePorts(current => ({ ...current, [kind]: { ...current[kind], [port]: event.target.value } }))} />)}</div>}<span className="tw-text-muted">{listeners || 'waits for prerequisites'}</span></li>;
            })}
          </ul>
        </>}
        </fieldset>
      </div>
    </Dialog>
  );
}
