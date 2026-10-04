// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useSelection } from '../contexts/SelectionContext';
import { OwnershipPanel } from '../chunk/ownership/OwnershipPanel';
import { Domain } from '../types';
import { lazy, Suspense } from 'react';
import type { Rack, Node, CrowdbKVServerView, EnrichedStoreView, NodeStore, NodeHealth, DiskdbInstanceInfo } from '../types';
import type { MenuTarget } from '../topology/TopologyCanvas';
import type { NodeDiskGroups } from '../data/useClusterTree';

const TopologyCanvas = lazy(() => import('../topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })));

export interface ClusterViewProps {
  active?: boolean;
  scope?: import('../types').Domain;
  allServers?: import('../api').ServerSummary[];
  racks: Rack[];
  nodes: Node[];
  servers: CrowdbKVServerView[];
  stores: EnrichedStoreView[];
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  diskdbNodeIds: Set<number>;
  diskdbInstances: DiskdbInstanceInfo[];
  diskdbInstanceIdByNodeId: Map<number, string>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  refreshToken: number;
  focusRequest: { targetId: string; subtree: boolean; nonce: number } | null;
  onEntityContextMenu: (target: MenuTarget, event: React.MouseEvent) => void;
}

export function ClusterView(props: ClusterViewProps) {
  const { selectionForDomain, selectEntity } = useSelection();
  const selection = selectionForDomain(Domain.Cluster);
  const ownership = selection && (['Node', 'Rack', 'Datacenter', 'Group', 'Store'].includes(selection.type) || selection.type === 'Server' && selection.serviceType === 'chunkdb');
  return (
    <div className="tw-h-full tw-overflow-auto">
    <div style={{ height: ownership ? 320 : '100%' }}><Suspense fallback={<ViewFallback />}>
      <TopologyCanvas {...props} />
    </Suspense></div>
    <div hidden={!ownership} className="tw-p-4"><OwnershipPanel active={props.active !== false && !!ownership} selection={selection ?? { domain: Domain.Cluster, type: 'Datacenter', id: 'datacenter' }} nodes={props.nodes} servers={props.allServers ?? []} stores={props.stores} onSelect={selectEntity} /></div>
    </div>
  );
}

function ViewFallback() {
  return <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">Loading…</div>;
}
