// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Button } from '../components/ui/Button';
import { Domain, type CapacityUsageResponse, type HardwareCapacitySummary } from '../types';
import type { NodeDiskGroups } from '../data/useDiskInventory';
import { useSelection } from '../contexts/SelectionContext';
import { formatBytes } from '../utils/capacity';
import { hwStatusLabel } from '../utils/entityDisplay';
import { createNodeDiskFile } from '../api';
import { CreateFileDiskDialog } from './CreateFileDiskDialog';
import { useNodeStorage } from './useNodeStorage';
import { useRemovedDisks } from './useRemovedDisks';
import { DiskLifecycleDialog } from './DiskLifecycleDialog';
import { RemovedDiskRow } from './RemovedDiskRow';
import { filePhaseLabel, type FilePreparation, type CandidateSummary, type DiskLifecycle } from './types';

interface Props {
  nodeId: number; rackId: number; active: boolean; readonly: boolean; ready: boolean;
  inventory?: NodeDiskGroups; hardware: HardwareCapacitySummary | null; usage: CapacityUsageResponse | null;
  candidatesOnly?: boolean;
  loadGroup: (node: number, group: number, fresh?: boolean) => Promise<void>;
  onAddGroup: () => void;
  onCandidates: (nodeId: number, summary: CandidateSummary) => void;
}

export function NodeStorage({ nodeId, rackId, active, readonly, ready, inventory, hardware, usage, candidatesOnly, loadGroup, onAddGroup, onCandidates }: Props) {
  const candidateSection = useRef<HTMLElement>(null);
  const diskElements = useRef(new Map<string, HTMLElement>());
  const [focusTarget, setFocusTarget] = useState<string>();
  const model = useNodeStorage(nodeId, active);
  const removed = useRemovedDisks(nodeId, active && ready);
  const [managing, setManaging] = useState<{ disk: string; group: number }>();
  const [confirmed, setConfirmed] = useState<Record<string, DiskLifecycle>>({});
  const { inspectEntity, selectEntity, selectedEntity } = useSelection();
  const [creating, setCreating] = useState(false);
  const [retryError, setRetryError] = useState('');
  const groupIds = inventory?.diskGroups.map(group => group.id).join(',') ?? '';
  const completed = Object.values(model.operations).filter(operation => operation.phase === 'registered').map(operation => operation.request.operation_id).sort().join(',');
  useEffect(() => {
    if (active) for (const id of groupIds.split(',').filter(Boolean)) void loadGroup(nodeId, Number(id));
  }, [active, nodeId, groupIds, completed, loadGroup]);
  const registered = Object.values(inventory?.disksByDg ?? {}).flat();
  useEffect(() => {
    if (managing || !focusTarget) return;
    const element = focusTarget === 'candidates' ? candidateSection.current : diskElements.current.get(focusTarget);
    if (element) { element.focus(); setFocusTarget(undefined); }
  }, [focusTarget, managing, registered]);
  const paths = new Set(registered.map(disk => disk.device_path).filter(Boolean));
  const pendingPaths = new Set(Object.values(model.operations).filter(operation => operation.request.admission).map(operation => operation.path));
  const removedPaths = new Set(removed.items.map(disk => disk.container_path));
  const candidates = model.inventory?.disks.filter(disk => !paths.has(disk.path) && !pendingPaths.has(disk.path) && !removedPaths.has(disk.path)) ?? [];
  const operationRows = Object.values(model.operations).filter(operation => operation.request.admission);
  const membershipKnown = inventory && inventory.diskGroups.every(group => inventory.disksByDg[group.id] !== undefined);
  const observedAt = model.inventory?.observed_at_ms;
  const stale = !!model.errors.inventory || !!removed.error;
  const candidateCount = candidates.length + removed.items.length;
  const partial = !!removed.next;
  useEffect(() => {
    if (membershipKnown && observedAt !== undefined && removed.observed) onCandidates(nodeId, { count: candidateCount, observed_at_ms: observedAt, stale, partial });
  }, [membershipKnown, observedAt, stale, candidateCount, partial, removed.observed, nodeId, onCandidates]);
  const recommendedGroup = operationRows.find(operation => operation.request.admission?.media_type === null && inventory?.diskGroups.some(group => group.id === operation.request.admission!.disk_group_id))?.request.admission?.disk_group_id
    ?? inventory?.diskGroups.find(group => inventory.disksByDg[group.id]?.length === 0)?.id;
  const backing = operationRows.map(operation => model.inventory?.disks.find(disk => disk.path === operation.path && disk.backing_identity === operation.backing_identity)).find(disk => disk?.backing_filesystem);
  const available = model.inventory?.filesystems.find(filesystem => filesystem.identity === backing?.backing_filesystem)?.available_bytes ?? null;
  const retry = async (operation: FilePreparation) => {
    setRetryError('');
    try {
      model.accept(await createNodeDiskFile(nodeId, { operation_id: operation.request.operation_id,
        disk_group_id: operation.request.admission!.disk_group_id, capacity_bytes: operation.request.capacity_bytes,
        sparse: operation.request.sparse, reserve_bytes: operation.request.reserve_bytes }));
    } catch (error) { setRetryError(String(error)); }
  };
  return <section aria-label={`Node ${nodeId} storage`} className="tw-space-y-4">
    <header className="tw-flex tw-flex-wrap tw-items-center tw-justify-between tw-gap-2">
      <h2 className="tw-font-semibold">Storage</h2>
      <div className="tw-flex tw-gap-2">
        <Button size="sm" variant="secondary" disabled={model.loading} onClick={() => { void model.refresh(); void removed.refresh(); for (const group of inventory?.diskGroups ?? []) void loadGroup(nodeId, group.id); }}>Refresh Discovery</Button>
        <Button size="sm" variant="secondary" disabled={readonly || !ready} onClick={onAddGroup}>Add Disk Group</Button>
        <Button size="sm" disabled={readonly || !ready || !membershipKnown} onClick={() => setCreating(true)}>Create File Disk</Button>
      </div>
    </header>
    {!ready && <p role="status" className="tw-rounded tw-border tw-border-border tw-p-3">Waiting for Group 0 · inventory is provisional; disk membership cannot change.</p>}
    {Object.entries(model.errors).filter(([, error]) => error).map(([source, error]) => <p key={source} role="alert" className="tw-text-sm tw-text-failed">{source}: {error} · Last-known observations retained.</p>)}
    {model.inventory && <p className="tw-text-xs tw-text-muted">Discovery observed {new Date(model.inventory.observed_at_ms).toLocaleString()}{model.errors.inventory ? ' · Stale' : ''}. Virtual capacity, allocated file bytes and shared backing space are separate.</p>}
    {removed.error && <p role="alert">Removed candidates: {removed.error} · Last-known candidates retained.</p>}
    {!model.inventory && <p role="status">{model.loading ? 'Loading discovery…' : 'Discovery unavailable'}</p>}
    {!candidatesOnly && inventory?.diskGroups.map(group => <section key={group.id} aria-label={`Disk group ${group.id}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-space-y-2">
      <button className="tw-text-accent" onClick={() => selectEntity({ domain: Domain.Cluster, type: 'DiskGroup', id: String(group.id), parentIds: { rack_id: rackId, node_id: nodeId } })}>{group.name || `DG-${group.id}`} · DG-{group.id}</button>
      {inventory.disksByDg[group.id] === undefined && <p role="status">Loading disk membership…</p>}
      {inventory.disksByDg[group.id]?.length === 0 && <p className="tw-text-sm tw-text-muted">No registered disks.</p>}
      {inventory.disksByDg[group.id]?.map(disk => {
        const observed = model.inventory?.disks.find(item => item.path === disk.device_path);
        const status = hardware?.disk_groups.find(item => item.disk_group_id === group.id && item.node_id === nodeId)?.disks.find(item => item.disk_id === disk.disk_id)?.status;
        const report = usage?.disk_groups.find(item => item.disk_group_id === group.id && item.node_id === nodeId)?.disks.find(item => item.disk_id === disk.disk_id);
        const preparation = operationRows.find(operation => operation.path === disk.device_path);
        const media = preparation?.request.admission?.media_type === null ? 'Unknown' : disk.disk_type;
        const entity = { domain: Domain.Cluster, type: 'Disk' as const, id: disk.disk_id, parentIds: { rack_id: rackId, node_id: nodeId, disk_group_id: group.id, disk_id: disk.disk_id } };
        return <article tabIndex={-1} ref={element => { if (element) diskElements.current.set(disk.disk_id, element); else diskElements.current.delete(disk.disk_id); }} key={disk.disk_id} aria-label={`Registered disk ${disk.disk_id}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-space-y-1">
          <div className="tw-flex tw-flex-wrap tw-justify-between tw-gap-2">
            <button className="tw-text-accent tw-font-mono tw-text-sm" onClick={() => selectEntity(entity)}>{disk.disk_id}</button>
            <button className="tw-text-xs tw-text-muted" aria-pressed={selectedEntity?.type === 'Disk' && selectedEntity.id === disk.disk_id} onClick={() => inspectEntity(entity)}>Properties</button>
          </div>
          <p className="tw-text-sm">{observed?.backend ?? 'Backend unknown'} · Media {media} · {formatBytes(disk.capacity_bytes)} · {confirmed[disk.disk_id]?.offline ? 'Offline' : status === undefined ? 'Health unknown' : hwStatusLabel(status)}</p>
          <Button size="sm" variant="secondary" disabled={readonly || !ready} onClick={() => setManaging({ disk: disk.disk_id, group: group.id })}>Manage disk</Button>
          <p className="tw-text-xs tw-text-muted">{report ? `${formatBytes(report.busy_bytes)} used` : 'Usage unknown'} · {observed?.error || (model.inventory && !observed ? 'Missing from latest discovery' : disk.device_path)}</p>
        </article>;
      })}
    </section>)}
    <section aria-label="Disk file operations" className="tw-space-y-2">
      <h3 className="tw-text-sm tw-font-semibold">File operations</h3>
      {retryError && <p role="alert">{retryError}</p>}
      {operationRows.length === 0 && <p className="tw-text-sm tw-text-muted">No observed file operations.</p>}
      {operationRows.map(operation => <article key={operation.request.operation_id} aria-label={`File operation ${operation.request.operation_id}`} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm tw-space-y-1">
        <p role="status">{filePhaseLabel[operation.phase]} · Requested {formatBytes(operation.request.capacity_bytes)} · {operation.request.sparse ? 'Sparse' : operation.preparation_complete ? 'Preallocated' : 'Preallocation requested'}</p>
        <p className="tw-text-xs tw-break-all">{operation.path}</p>
        {operation.error && <p className="tw-text-xs tw-text-muted">{operation.error}</p>}
        {['failed', 'unknown'].includes(operation.phase) && <Button size="sm" disabled={readonly || !ready} onClick={() => { void retry(operation); }}>Retry same operation</Button>}
      </article>)}
      {model.next && <Button size="sm" variant="secondary" disabled={model.loading} onClick={() => { void model.refresh(model.next!); }}>More operations</Button>}
    </section>
    <section ref={candidateSection} tabIndex={-1} aria-label="Candidate disks" className="tw-space-y-2">
      <h3 className="tw-text-sm tw-font-semibold">Candidates ({candidateCount}{partial ? '+' : ''})</h3>
      {candidateCount === 0 && model.inventory && removed.observed && <p className="tw-text-sm tw-text-muted">No discovered unregistered media.</p>}
      {removed.items.map(disk => <RemovedDiskRow key={disk.disk_id} disk={disk} inventory={model.inventory} ready={ready} readonly={readonly}
        onInspect={() => inspectEntity({ domain: Domain.Cluster, type: 'Disk', id: disk.disk_id, parentIds: { rack_id: rackId, node_id: nodeId, disk_group_id: disk.disk_group_id, disk_id: disk.disk_id } })}
        onRejoin={() => setManaging({ disk: disk.disk_id, group: disk.disk_group_id })} />)}
      {removed.next && <Button size="sm" variant="secondary" disabled={removed.loading} onClick={() => { void removed.refresh(removed.next!); }}>More retained candidates</Button>}
      {candidates.map(disk => <article key={disk.path} className="tw-rounded tw-border tw-border-border tw-p-3 tw-text-sm tw-space-y-1">
        <p className="tw-break-all">{disk.path}</p><p>{disk.backend ?? 'Backend unknown'} · {disk.capacity_bytes === null ? 'Capacity unknown' : formatBytes(disk.capacity_bytes)}</p>
        <p className="tw-text-muted">{disk.error ?? 'Requires explicit provisioning validation; discovery does not prove blank contents.'}</p>
      </article>)}
    </section>
    {managing && <DiskLifecycleDialog key={`${nodeId}:${managing.group}:${managing.disk}`} nodeId={nodeId} groupId={managing.group} diskId={managing.disk}
      onClose={() => setManaging(undefined)} onCommitted={(state, action) => {
        if (action === 'rejoin') setFocusTarget(state.disk_id);
        if (state.removed) setFocusTarget('candidates');
        if (selectedEntity?.type === 'Disk' && selectedEntity.id === state.disk_id) inspectEntity({ ...selectedEntity });
        setConfirmed(previous => ({ ...previous, [state.disk_id]: state }));
        void loadGroup(nodeId, managing.group, true); void removed.refresh(undefined, true); void model.refresh();
      }} />}
    {creating && <CreateFileDiskDialog nodeId={nodeId} groups={inventory?.diskGroups ?? []} groupId={recommendedGroup} available={available} onClose={() => setCreating(false)} onAccepted={model.accept} />}
  </section>;
}
