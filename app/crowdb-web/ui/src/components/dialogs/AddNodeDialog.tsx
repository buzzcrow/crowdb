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
import { serviceLabels, serviceOrder, type NodeServicePlan } from '../../services/useNodeServicePlans';

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
  onDefaultServices?: (nodeId: number) => void;
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
  const [host, setHost] = useState(defaultHost);
  const [sshUser, setSshUser] = useState('');
  const [sshKeyPath, setSshKeyPath] = useState('');
  const [enableCrowdbKV, setEnableCrowdbKV] = useState(true);
  const [restPort, setRestPort] = useState(defaultRestPort);
  const [rpcPort, setRpcPort] = useState(defaultRpcPort);
  const [enableDiskdb, setEnableDiskdb] = useState(true);
  const [diskdbRpcPort, setDiskdbRpcPort] = useState(defaultDiskdbRpcPort);
  const [completeSet, setCompleteSet] = useState(true);
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
    setRackId(initialRackId);
    setCompleteSet(true);
    setRestPort(defaultRestPort);
    setRpcPort(defaultRpcPort);
    setEnableCrowdbKV(true);
    setEnableDiskdb(true);
    setDiskdbRpcPort(defaultDiskdbRpcPort);
  }, [defaultRpcPort, defaultRestPort, defaultDiskdbRpcPort, isOpen]);

  const defaults = useDeploymentDefaults(isOpen);
  useEffect(() => {
    if (!defaults.values || created.current != null) return;
    setRestPort(String(defaults.values.kv.http_port));
    setRpcPort(String(defaults.values.kv.rpc_port));
    setDiskdbRpcPort(String(defaults.values.diskdb.rpc_port));
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
      if (serviceError) {
        const fresh = await serviceRequest('/deployment-defaults', 'GET') as Record<string, DeploymentDefaults>;
        kvRest = fresh.kv.http_port!; kvRpc = fresh.kv.rpc_port!; ddbRpc = fresh.diskdb.rpc_port!;
      }
      setServiceError('');
      const serviceErrors: string[] = [];
      if (enableCrowdbKV && !completed.current.has('kv')) {
        try {
          await deployServer(numericNodeId, {
            rest_port: kvRest,
            rpc_port: kvRpc,
          });
          completed.current.add('kv');
        } catch (err) {
          serviceErrors.push(`CrowDB Storage: ${err instanceof Error ? err.message : 'deployment failed'}`);
        }
      }

      if (enableDiskdb && !completed.current.has('diskdb')) {
        try {
          await deployDiskdb(numericNodeId, {
            rpc_port: ddbRpc,
          });
          completed.current.add('diskdb');
        } catch (err) {
          serviceErrors.push(`DiskDB: ${err instanceof Error ? err.message : 'deployment failed'}`);
        }
      }

      // Start the complete service plan even when a directly deployed
      // prerequisite failed. The plan records the failure and keeps the
      // remaining default services visible/retriable instead of silently
      // stopping after KV or DiskDB.
      if (completeSet) onDefaultServices?.(numericNodeId);
      if (serviceErrors.length > 0) {
        setServiceError(serviceErrors.join('; '));
      } else {
        const parts = [`Node "${trimmedNodeId}" created`];
        if (enableCrowdbKV) parts.push('CrowDB Storage enabled');
        if (enableDiskdb) parts.push('DiskDB enabled');
        success(parts.join(', '));
      }
      await onSuccess?.();
      if (serviceErrors.length === 0) {
        setInitialDone(true);
        if (!enableCrowdbKV && !enableDiskdb) onClose();
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
        <label className="tw-flex tw-gap-2 tw-text-sm"><input type="checkbox" checked={completeSet} onChange={event => { setCompleteSet(event.target.checked); if (event.target.checked) { setEnableCrowdbKV(true); setEnableDiskdb(true); } }} />Deploy complete service set</label>
        {completeSet && <>
          <p className="tw-text-xs tw-text-muted">The complete default service set will be created for this node. Services with cluster prerequisites wait and start automatically when ready.</p>
          <ul aria-label="Default services" className="tw-grid tw-grid-cols-2 tw-gap-2 tw-text-xs">
            {serviceOrder.map(kind => <li key={kind} className="tw-rounded tw-border tw-border-border tw-px-2 tw-py-1.5 tw-text-muted">{serviceLabels[kind]}</li>)}
          </ul>
        </>}
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
          onChange={(e) => setNodeId(e.target.value)}
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
        <label className="tw-flex tw-items-center tw-gap-2 tw-text-sm tw-text-text">
          <input
            type="checkbox"
            checked={enableCrowdbKV}
            onChange={(e) => { setEnableCrowdbKV(e.target.checked); if (!e.target.checked) setCompleteSet(false); }}
            className="tw-h-4 tw-w-4 tw-rounded tw-border tw-border-border tw-bg-bg tw-text-accent focus:tw-ring-accent"
          />
          <span>Enable CrowDB Storage on this node</span>
        </label>
        {enableCrowdbKV && (
          <>
            <Input
              label="REST Port"
              inputMode="numeric"
              value={restPort}
              onChange={(e) => setRestPort(e.target.value)}
            />
            <Input
              label="RPC Port"
              inputMode="numeric"
              data-testid="kv-rpc-port"
              value={rpcPort}
              onChange={(e) => setRpcPort(e.target.value)}
            />
          </>
        )}
        <label className="tw-flex tw-items-center tw-gap-2 tw-text-sm tw-text-text">
          <input
            type="checkbox"
            checked={enableDiskdb}
            onChange={(e) => { setEnableDiskdb(e.target.checked); if (!e.target.checked) setCompleteSet(false); }}
            className="tw-h-4 tw-w-4 tw-rounded tw-border tw-border-border tw-bg-bg tw-text-accent focus:tw-ring-accent"
          />
          <span>Enable DiskDB on this node</span>
        </label>
        {enableDiskdb && (
          <Input
            label="RPC Port"
            inputMode="numeric"
            data-testid="diskdb-rpc-port"
            value={diskdbRpcPort}
            onChange={(e) => setDiskdbRpcPort(e.target.value)}
          />
        )}
        </fieldset>
      </div>
    </Dialog>
  );
}
