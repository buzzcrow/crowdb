// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';
import { observeCapacity } from './observation';

interface NodeViewProps {
  nodeId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectDg: (dgId: number) => void;
}

interface DgAgg {
  dgId: number;
  diskCount: number;
}

export function NodeView({ nodeId, usage, hardwareCapacity, onSelectDg }: NodeViewProps) {
  const dgs = useMemo<DgAgg[]>(() => {
    const hardware = (hardwareCapacity?.disk_groups ?? []).filter(group => group.node_id === nodeId);
    const reports = (usage?.disk_groups ?? []).filter(group => group.node_id === nodeId);
    const ids = new Set((hardwareCapacity ? hardware : reports).map(group => group.disk_group_id));
    return [...ids].sort((a, b) => a - b).map(dgId => ({ dgId,
      diskCount: (hardware.find(group => group.disk_group_id === dgId) ?? reports.find(group => group.disk_group_id === dgId))!.disks.length,
    }));
  }, [nodeId, usage, hardwareCapacity]);

  if (dgs.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No disk-groups on node {nodeId}.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Disk-groups on N-{nodeId} ({dgs.length})</div>
      {dgs.map((d) => (
        <button
          key={d.dgId}
          onClick={() => onSelectDg(d.dgId)}
          className="tw-w-full tw-flex tw-items-center tw-justify-between tw-p-3 tw-bg-panel tw-rounded-lg hover:tw-bg-bg/50 tw-text-left"
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <Boxes className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">DG-{d.dgId}</div>
              <div className="tw-text-xs tw-text-muted">{d.diskCount} disk(s)</div>
            </div>
          </div>
          <CapacityBar {...observeCapacity(hardwareCapacity, usage, { dgId: d.dgId })} barWidth="tw-w-32" />
        </button>
      ))}
    </div>
  );
}
