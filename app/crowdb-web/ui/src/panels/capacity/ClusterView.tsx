// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { FolderTree, Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';
import { observeCapacity } from './observation';

interface ClusterViewProps {
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectRack: (rackId: number) => void;
}

interface RackAgg {
  rackId: number;
  nodeCount: number;
  dgCount: number;
}

export function ClusterView({ usage, hardwareCapacity, onSelectRack }: ClusterViewProps) {
  const racks = useMemo<RackAgg[]>(() => {
    const hwRacks = hardwareCapacity?.racks ?? [];
    const groups = [...(hardwareCapacity?.disk_groups ?? []), ...(usage?.disk_groups ?? [])];
    const ids = new Set([...hwRacks.map(rack => rack.rack_id), ...groups.map(group => group.rack_id)]);
    return [...ids].sort((a, b) => a - b).map(rackId => {
      const members = groups.filter(group => group.rack_id === rackId);
      return { rackId, dgCount: new Set(members.map(group => group.disk_group_id)).size,
        nodeCount: hwRacks.find(rack => rack.rack_id === rackId)?.node_count ?? new Set(members.map(group => group.node_id)).size };
    });
  }, [usage, hardwareCapacity]);

  if (racks.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No racks with capacity data.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Racks ({racks.length})</div>
      {racks.map((r) => (
        <button
          key={r.rackId}
          onClick={() => onSelectRack(r.rackId)}
          className="tw-w-full tw-flex tw-items-center tw-justify-between tw-p-3 tw-bg-panel tw-rounded-lg hover:tw-bg-bg/50 tw-text-left"
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <FolderTree className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">R-{r.rackId}</div>
              <div className="tw-text-xs tw-text-muted tw-flex tw-items-center tw-gap-1">
                <Boxes className="tw-h-3 tw-w-3" />
                {r.dgCount} DG(s) · {r.nodeCount} node(s)
              </div>
            </div>
          </div>
          <CapacityBar {...observeCapacity(hardwareCapacity, usage, { rackId: r.rackId })} barWidth="tw-w-32" />
        </button>
      ))}
    </div>
  );
}
