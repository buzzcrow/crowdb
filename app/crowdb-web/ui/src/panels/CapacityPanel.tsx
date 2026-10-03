// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useCallback, useMemo, useRef } from 'react';
import { Server, Loader2, RefreshCw } from 'lucide-react';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import { useNavigationSnapshot } from '../contexts/DomainContext';
import { useSelection } from '../contexts/SelectionContext';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { triggerDiskdbScan } from '../api';
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  HardwareCapacitySummary,
  ScanStatusResponse,
} from '../types';
import { Domain } from '../types';
import { busyPct, formatBytes } from '../utils/capacity';
import { observeCapacity } from './capacity/observation';
import { ScannerPanel } from './ScannerPanel';
import { ClusterView } from './capacity/ClusterView';
import { RackView } from './capacity/RackView';
import { NodeView } from './capacity/NodeView';
import { DiskGroupView } from './capacity/DiskGroupView';
import { DiskView, type DiskQuery } from './capacity/DiskView';

interface CapacityPanelProps {
  active?: boolean;
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  hardwareCapacity?: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading?: boolean;
  readonly?: boolean;
  onRefresh?: () => Promise<void>;
  selectedEntity?: SelectedEntity | null;
}

type CapacityScope = 'Cluster' | 'Rack' | 'Node' | 'DiskGroup' | 'Disk';

function scopeFromEntity(entity: SelectedEntity | null | undefined): CapacityScope {
  if (!entity) return 'Cluster';
  switch (entity.type) {
    case 'Rack': return 'Rack';
    case 'Node': return 'Node';
    case 'DiskGroup': return 'DiskGroup';
    case 'Disk': return 'Disk';
    default: return 'Cluster';
  }
}

export function CapacityPanel({
  active = true,
  instances,
  usage,
  hardwareCapacity,
  scanStatus,
  loading,
  readonly,
  onRefresh,
  selectedEntity,
}: CapacityPanelProps) {
  const { selectEntity } = useSelection();
  const { success, error } = useToast();
  const { log } = useActivity();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const scope = scopeFromEntity(selectedEntity);

  // Resolve IDs from the selected entity.
  const rackId = scope === 'Rack' ? Number(selectedEntity?.id) : undefined;
  const nodeId = scope === 'Node' ? Number(selectedEntity?.id) : undefined;
  const dgId = scope === 'DiskGroup'
    ? Number(selectedEntity?.parentIds?.disk_group_id ?? selectedEntity?.id)
    : scope === 'Disk'
      ? Number(selectedEntity?.parentIds?.disk_group_id)
      : undefined;
  const diskId = scope === 'Disk'
    ? String(selectedEntity?.parentIds?.disk_id ?? selectedEntity?.id)
    : undefined;

  const diskQueries = useRef(new Map<string, DiskQuery>());
  const [, updateQueryVersion] = useState(0);
  const diskQueryKey = dgId === undefined || !diskId ? '' : `${dgId}/${diskId}`;
  const diskQuery = diskQueries.current.get(diskQueryKey) ?? { zone: null, page: 0, blockStart: 0 };
  const changeDiskQuery = (value: DiskQuery) => {
    if (!diskQueryKey) return;
    diskQueries.current.delete(diskQueryKey); diskQueries.current.set(diskQueryKey, value);
    while (diskQueries.current.size > 32) diskQueries.current.delete(diskQueries.current.keys().next().value!);
    updateQueryVersion(version => version + 1);
  };
  useNavigationSnapshot(Domain.Capacity, 'disk-query', () => {
    const identity = diskQueryKey; const state = { ...diskQuery };
    return () => { if (identity) { diskQueries.current.set(identity, state); updateQueryVersion(version => version + 1); } };
  });

  const totals = observeCapacity(hardwareCapacity, usage, { rackId, nodeId, dgId, diskId });
  const totalCapacity = totals.capacity;
  const totalBusy = totals.busy;
  const totalFree = totals.free;
  const usageKnown = totalBusy !== null && totalFree !== null && totalCapacity !== null;

  const scopeLabel = useMemo(() => {
    if (!selectedEntity) return 'Cluster';
    switch (selectedEntity.type) {
      case 'Rack': return `Rack ${selectedEntity.id}`;
      case 'Node': return `Node ${selectedEntity.id}`;
      case 'DiskGroup': return `DG-${selectedEntity.parentIds?.disk_group_id ?? selectedEntity.id}`;
      case 'Disk': return `Disk ${String(selectedEntity.parentIds?.disk_id ?? selectedEntity.id).slice(0, 12)}…`;
      default: return 'Cluster';
    }
  }, [selectedEntity]);

  const handleClusterScan = useCallback(() => {
    if (readonly) return;
    setActionLoading('scan-all');
    triggerDiskdbScan()
      .then(() => {
        success('Trigger Scan succeeded for all');
        log({ action: 'Trigger Scan', target: 'all', status: 'Success' });
        return onRefresh?.();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : 'Unknown error';
        error(`Trigger Scan failed: ${msg}`);
        log({ action: 'Trigger Scan', target: 'all', status: 'Failed', message: msg });
      })
      .finally(() => setActionLoading(null));
  }, [readonly, success, error, log, onRefresh]);

  const selectRack = useCallback((id: number) => {
    selectEntity({ type: 'Rack', id: String(id), domain: Domain.Capacity });
  }, [selectEntity]);

  const selectNode = useCallback((id: number) => {
    selectEntity({ type: 'Node', id: String(id), domain: Domain.Capacity });
  }, [selectEntity]);

  const selectDg = useCallback((id: number) => {
    selectEntity({ type: 'DiskGroup', id: String(id), parentIds: { disk_group_id: id }, domain: Domain.Capacity });
  }, [selectEntity]);

  const selectDisk = useCallback((dId: string, dgIdVal: number, rackIdVal: number, nodeIdVal: number) => {
    selectEntity({
      type: 'Disk',
      id: dId,
      parentIds: { rack_id: rackIdVal, node_id: nodeIdVal, disk_group_id: dgIdVal, disk_id: dId },
      domain: Domain.Capacity,
    });
  }, [selectEntity]);

  if (loading && instances.length === 0) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full">
        <Loader2 className="tw-h-6 tw-w-6 tw-animate-spin tw-text-muted" />
      </div>
    );
  }

  if (instances.length === 0 && !hardwareCapacity) {
    return (
      <div className="tw-flex tw-flex-col tw-items-center tw-justify-center tw-h-full tw-text-muted tw-gap-3">
        <Server className="tw-h-12 tw-w-12 tw-opacity-40" />
        <div className="tw-text-lg">No diskdb instances registered</div>
        <div className="tw-text-sm">Deploy a diskdb instance to see capacity data.</div>
      </div>
    );
  }

  return (
    <div className="tw-h-full tw-overflow-auto tw-p-6 tw-space-y-6">
      {/* Summary header */}
      <div className="tw-flex tw-items-center tw-justify-between">
        <div>
          <h2 className="tw-text-xl tw-font-semibold tw-text-text">Capacity — {scopeLabel}</h2>
          <p className="tw-text-sm tw-text-muted tw-mt-1">
            {instances.length} instance(s)
          </p>
        </div>
        <button
          onClick={() => onRefresh?.()}
          className="tw-p-2 tw-rounded-md hover:tw-bg-panel tw-text-muted"
          aria-label="Refresh"
        >
          <RefreshCw className="tw-h-4 tw-w-4" />
        </button>
      </div>

      {/* Scope totals */}
      <div className="tw-grid tw-grid-cols-3 tw-gap-4" data-testid="capacity-summary">
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Total Capacity</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{totalCapacity === null ? 'Unknown' : formatBytes(totalCapacity)}</div>
        </div>
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Busy</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{totalBusy === null ? 'Unknown' : formatBytes(totalBusy)}</div>
          <div className="tw-text-xs tw-text-muted tw-mt-1">{usageKnown ? `${busyPct(totalCapacity, totalBusy)}% used` : 'Usage coverage incomplete'}</div>
        </div>
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Free</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{totalFree === null ? 'Unknown' : formatBytes(totalFree)}</div>
          <div className="tw-text-xs tw-text-muted tw-mt-1">{usageKnown ? `${busyPct(totalCapacity, totalFree)}% free` : 'Free space is unknown'}</div>
        </div>
      </div>

      {/* Scanner panel — cluster scope only (cluster-wide scan status + trigger) */}
      {scope === 'Cluster' && (
        <ScannerPanel
          scanStatus={scanStatus}
          readonly={readonly}
          actionLoading={actionLoading}
          onScan={handleClusterScan}
        />
      )}

      {/* Scope-specific body */}
      {scope === 'Cluster' && (
        <ClusterView
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectRack={selectRack}
        />
      )}
      {scope === 'Rack' && rackId !== undefined && (
        <RackView
          rackId={rackId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectNode={selectNode}
        />
      )}
      {scope === 'Node' && nodeId !== undefined && (
        <NodeView
          nodeId={nodeId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectDg={selectDg}
        />
      )}
      {scope === 'DiskGroup' && dgId !== undefined && (
        <DiskGroupView
          dgId={dgId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectDisk={selectDisk}
        />
      )}
      {scope === 'Disk' && dgId !== undefined && diskId !== undefined && (
        <DiskView key={`${dgId}/${diskId}`} active={active}
          dgId={dgId}
          diskId={diskId}
          query={diskQuery}
          onQueryChange={changeDiskQuery}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          readonly={readonly}
          onRefresh={onRefresh}
        />
      )}
    </div>
  );
}
