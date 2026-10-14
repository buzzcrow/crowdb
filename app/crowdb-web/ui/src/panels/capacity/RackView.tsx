// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { Server, Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';
import { CapacityRow } from './CapacityRow';
import { observeCapacity } from './observation';

interface RackViewProps {
  rackId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectNode: (nodeId: number) => void;
  selectedId?: string;
  onInspectNode?: (nodeId: number) => void;
}

interface NodeAgg {
  nodeId: number;
  dgCount: number;
}

export function RackView({ rackId, usage, hardwareCapacity, onSelectNode, onInspectNode, selectedId }: RackViewProps) {
  const nodes = useMemo<NodeAgg[]>(() => {
    const hwNodes = (hardwareCapacity?.nodes ?? []).filter(node => node.rack_id === rackId);
    const groups = (hardwareCapacity ? hardwareCapacity.disk_groups : (usage?.disk_groups ?? [])).filter(group => group.rack_id === rackId);
    const ids = new Set([...hwNodes.map(node => node.node_id), ...groups.map(group => group.node_id)]);
    return [...ids].sort((a, b) => a - b).map(nodeId => ({ nodeId,
      dgCount: new Set(groups.filter(group => group.node_id === nodeId).map(group => group.disk_group_id)).size,
    }));
  }, [rackId, usage, hardwareCapacity]);

  if (nodes.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No nodes in rack {rackId}.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Nodes in R-{rackId} ({nodes.length})</div>
      {nodes.map((n) => (
        <CapacityRow selected={selectedId === String(n.nodeId)} key={n.nodeId} label={`N-${n.nodeId}`}
          onOpen={() => onSelectNode(n.nodeId)} onInspect={() => onInspectNode?.(n.nodeId)}
          details={<CapacityBar {...observeCapacity(hardwareCapacity, usage, { nodeId: n.nodeId })} barWidth="tw-w-32" />}
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <Server className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">N-{n.nodeId}</div>
              <div className="tw-text-xs tw-text-muted tw-flex tw-items-center tw-gap-1">
                <Boxes className="tw-h-3 tw-w-3" />
                {n.dgCount} DG(s)
              </div>
            </div>
          </div>
        </CapacityRow>
      ))}
    </div>
  );
}
