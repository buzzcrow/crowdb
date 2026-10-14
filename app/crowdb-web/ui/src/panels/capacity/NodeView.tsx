// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';
import { CapacityRow } from './CapacityRow';
import { observeCapacity } from './observation';

interface NodeViewProps {
  nodeId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectDg: (dgId: number) => void;
  selectedId?: string;
  onInspectDg?: (dgId: number) => void;
}

interface DgAgg {
  dgId: number;
  diskCount: number;
}

export function NodeView({ nodeId, usage, hardwareCapacity, onSelectDg, onInspectDg, selectedId }: NodeViewProps) {
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
        <CapacityRow selected={selectedId === String(d.dgId)} key={d.dgId} label={`DG-${d.dgId}`}
          onOpen={() => onSelectDg(d.dgId)} onInspect={() => onInspectDg?.(d.dgId)}
          details={<CapacityBar {...observeCapacity(hardwareCapacity, usage, { dgId: d.dgId })} barWidth="tw-w-32" />}
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <Boxes className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">DG-{d.dgId}</div>
              <div className="tw-text-xs tw-text-muted">{d.diskCount} disk(s)</div>
            </div>
          </div>
        </CapacityRow>
      ))}
    </div>
  );
}
