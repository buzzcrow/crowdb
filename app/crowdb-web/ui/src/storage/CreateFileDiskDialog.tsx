// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { Dialog } from '../components/Dialog';
import { Input, Select } from '../components/ui/Input';
import { createNodeDiskFile } from '../api';
import type { DiskGroupEntry } from '../types';
import { storageOperationId, type CreateFileDiskRequest, type FilePreparation } from './types';
import { formatBytes } from '../utils/capacity';

interface Props {
  nodeId: number;
  groups: DiskGroupEntry[];
  groupId?: number;
  available: number | null;
  onClose: () => void;
  onAccepted: (operation: FilePreparation) => void;
}

const GIB = 1024 ** 3;
const MIB = 1024 ** 2;

export function CreateFileDiskDialog({ nodeId, groups, groupId, available, onClose, onAccepted }: Props) {
  const [operationId] = useState(storageOperationId);
  const [group, setGroup] = useState(String(groupId ?? ''));
  const [capacity, setCapacity] = useState('16');
  const [sparse, setSparse] = useState(false);
  const [sealed, setSealed] = useState<CreateFileDiskRequest>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const bytes = Number(capacity) * GIB;
  const valid = Number.isSafeInteger(bytes) && bytes > 0 && bytes % (64 * MIB) === 0 && (bytes <= 32 * GIB || bytes % (32 * GIB) === 0) && groups.some(entry => entry.id === Number(group));
  const enough = available === null || available >= GIB + (sparse ? 0 : bytes);
  const path = `/opt/crowdb/data/disks/managed-${operationId}.img`;
  const submit = async () => {
    if (busy || (!sealed && (!valid || !enough))) return;
    const request = sealed ?? { operation_id: operationId, disk_group_id: Number(group), capacity_bytes: bytes, sparse };
    setSealed(request);
    setBusy(true);
    setError('');
    try {
      const accepted = await createNodeDiskFile(nodeId, request);
      onAccepted(accepted);
      onClose();
    } catch (cause) {
      setError(`${cause instanceof Error ? cause.message : String(cause)} Refresh operations to reconcile the outcome; Retry uses the same identity and inputs.`);
    } finally { setBusy(false); }
  };
  return <Dialog isOpen title="Create File Disk" onClose={onClose}
    description={`Create and register a managed file on Node ${nodeId}. The monitor continues after this dialog closes.`}
    confirmLabel={sealed ? 'Retry same operation' : 'Create and Join'} onConfirm={submit}
    confirmLoading={busy} confirmDisabled={busy || (!sealed && (!valid || !enough))}>
    <div className="tw-space-y-4">
      {error && <p role="alert" className="tw-text-failed tw-text-sm">{error}</p>}
      <p className="tw-text-xs tw-text-muted tw-break-all">Container path: <code>{path}</code></p>
      <Input label="Capacity (GiB)" inputMode="decimal" value={capacity} disabled={!!sealed}
        onChange={event => setCapacity(event.target.value)} />
      <p className="tw-text-xs tw-text-muted">Use multiples of 64 MiB; above 32 GiB, use whole 32 GiB zones.</p>
      <Select label="Disk group" value={group} disabled={!!sealed} onChange={event => setGroup(event.target.value)}>
        <option value="">Select a disk group</option>
        {groups.map(entry => <option key={entry.id} value={entry.id}>{entry.name || `DG-${entry.id}`} · DG-{entry.id}</option>)}
      </Select>
      {groups.length === 0 && <p role="status">Create a disk group before creating a file disk.</p>}
      <details><summary>Advanced allocation</summary>
        <label className="tw-flex tw-gap-2 tw-mt-2"><input type="checkbox" checked={sparse} disabled={!!sealed}
          onChange={event => setSparse(event.target.checked)} />Sparse file</label>
        <p className="tw-text-xs tw-text-muted">Sparse virtual capacity does not reserve host space. Preallocation never falls back to sparse.</p>
      </details>
      <p className="tw-text-sm">Backing filesystem available: {available === null ? 'Unknown' : formatBytes(available)}. Host reserve: 1 GiB.</p>
      {!enough && <p role="status" className="tw-text-failed">Observed host space is insufficient for this request and reserve.</p>}
      <p className="tw-text-xs tw-text-muted">The server rechecks geometry and host space. Capacity becomes usable only after Group 0 registration and DiskIO verification. Other host writers can change available space.</p>
    </div>
  </Dialog>;
}
