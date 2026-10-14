// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useMemo } from 'react';
import { FolderTree, Monitor, Database, Boxes, HardDrive, Cog, Plus, Rocket, Building2, ExternalLink } from 'lucide-react';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { Tree, TreeNode } from '../components/Tree';
import { Button } from '../components/ui/Button';
import { Domain, Rack, EnrichedStoreView, NodeStore, CrowdbKVServerView, NodeHealth, DiskdbInstanceInfo, CapacityUsageResponse, HardwareCapacitySummary } from '../types';
import { crowdbKvServerByNodeId } from '../data/crowdbKvServers';
import { DEFAULT_DC_ID, DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, storeLabel, toUiHealth, toUiReplicaRole, toUiRole } from '../utils/entityDisplay';
import type { NodeDiskGroups } from '../data/useCapacityTree';
import type { ServerSummary } from '../api';
import { isAuxiliaryKind, serviceInstanceLabel } from '../services/client';
import { serviceOrder, type NodeServicePlan, type ServiceKind } from '../services/useNodeServicePlans';
import { domainTabs } from './domainTabs';
import { CandidateNodes } from './CandidateNodes';
import { storageTree } from './storageTree';

/** Fixed UI-only datacenter root wrapping the rack/store children. */
function datacenterRoot(children: TreeNode[]): TreeNode {
  return {
    id: `DC-${DEFAULT_DC_ID}`,
    rawId: DEFAULT_DC_ID,
    label: DEFAULT_DC_NAME,
    type: 'Datacenter',
    icon: <Building2 className="tw-h-4 tw-w-4 tw-text-muted" />,
    children,
  };
}

interface SidebarProps {
  onCandidatesChanged?: () => void;
  nodeManagement?: boolean;
  allServers?: ServerSummary[];
  racks?: Rack[];
  servers?: CrowdbKVServerView[];
  stores?: EnrichedStoreView[];
  nodeStores?: Record<string, NodeStore[]>;
  nodeHealthById?: Record<string, NodeHealth>;
  loading?: boolean;
  readonly?: boolean;
  candidateReadonly?: boolean;
  width?: number;
  clusterInitialized?: boolean;
  onNodeClick?: (node: TreeNode) => void;
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
  onAdd?: () => void;
  onLoadNodeDisks?: (nodeId: number) => Promise<void>;
  onLoadGroupDisks?: (nodeId: number, groupId: number) => Promise<void>;
  // Capacity view props (R77)
  diskdbInstances?: DiskdbInstanceInfo[];
  capacityUsage?: CapacityUsageResponse | null;
  hardwareCapacity?: HardwareCapacitySummary | null;
  nodeDiskGroups?: Record<number, NodeDiskGroups>;
  diskdbNodeIds?: Set<number>;
  diskdbHealthById?: Map<number, string>;
  diskdbInstanceIdByNodeId?: Map<number, string>;
  /** Durable deployment plans make queued and failed services visible before a process registers. */
  servicePlans?: Record<number, NodeServicePlan>;
}

export function Sidebar({
  onCandidatesChanged,
  nodeManagement = false,
  allServers = [],
  racks = [],
  servers = [],
  stores = [],
  nodeStores = {},
  nodeHealthById = {},
  loading,
  readonly,
  candidateReadonly,
  width = 280,
  clusterInitialized = true,
  onNodeClick,
  onNodeContextMenu,
  onAdd,
  onLoadNodeDisks,
  onLoadGroupDisks,
  diskdbInstances = [],
  capacityUsage = null,
  hardwareCapacity = null,
  nodeDiskGroups = {},
  diskdbNodeIds,
  diskdbHealthById,
  diskdbInstanceIdByNodeId = new Map(),
  servicePlans = {},
}: SidebarProps) {
  const { domain } = useDomain();
  const help = domainTabs.find(tab => tab.domain === domain);
  const helpTitle = domain === Domain.Cluster ? 'Cluster' : domain === Domain.KV ? 'PaxosKV' : 'Capacity';
  const [expansions, setExpansions] = useState<Partial<Record<Domain, string[]>>>({});
  const serverByNodeId = useMemo(() => crowdbKvServerByNodeId(servers), [servers]);

  const treeNodes = useMemo<TreeNode[]>(() => {
    if (domain === Domain.Cluster) {
      // One physical tree shared by Topology and Capacity.
      return [datacenterRoot(racks.map((rack) => ({
        id: `R-${rack.id}`,
        rawId: rack.id,
        label: rack.name ? `${rackLabel(String(rack.id))} (${rack.name})` : rackLabel(String(rack.id)),
        type: 'Rack' as const,
        icon: <FolderTree className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (rack.nodes || []).map((entry) => {
          const nodeId: number = entry.id;
          const children: TreeNode[] = [];
          const nodeServices = allServers.filter(service => service.node_id === nodeId && service.id);
          const actualByKind = new Map(nodeServices.map(service => [service.service_type, service]));
          const plan = servicePlans[nodeId];
          const kinds: ServiceKind[] = plan ? [...serviceOrder] : [];
          if (!plan) {
            for (const service of nodeServices) if (isAuxiliaryKind(service.service_type)) kinds.push(service.service_type);
            // Keep a registered KV service visible after it is stopped. The
            // physical node projection may omit `kv_server` while the process
            // is down, but `/api/servers` still owns the deployment record and
            // the restart action must remain available for that record.
            if (serverByNodeId.has(nodeId) || nodeServices.some(service => service.service_type === 'paxos-kv')) {
              kinds.push('paxos-kv');
            }
            if (diskdbNodeIds?.has(nodeId)) kinds.push('diskdb');
          }
          for (const kind of [...new Set(kinds)]) {
            const service = actualByKind.get(kind);
            const step = plan?.[kind];
            const rawId = service?.id ?? `${kind}-${nodeId}`;
            const health = service ? toUiHealth(service.health)
              : step?.state === 'failed' ? 'Failed' : step?.state === 'warning' ? 'Healthy' : 'Unknown';
            const childrenEntry: TreeNode = {
              id: kind === 'diskdb' ? `DDB-${nodeId}` : `SERVICE-${rawId}`,
              rawId,
              label: serviceInstanceLabel(kind, rawId),
              type: 'Server',
              serviceType: kind,
              icon: <Cog className="tw-h-4 tw-w-4 tw-text-muted" />,
              health,
              title: step?.detail ? `${serviceInstanceLabel(kind, rawId)} · ${step.detail}` : undefined,
              parentIds: { rack_id: rack.id, node_id: nodeId },
            };
            if (kind === 'diskdb') childrenEntry.health = toUiHealth(diskdbHealthById?.get(nodeId));
            children.push(childrenEntry);
          }

          return {
            id: `N-${nodeId}`,
            rawId: nodeId,
            label: nodeLabel(String(nodeId)),
            type: 'Node' as const,
            icon: <Monitor className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(nodeHealthById[String(nodeId)]),
            parentIds: { rack_id: rack.id },
            children: [
              { id: `SERVICES-${nodeId}`, rawId: nodeId, label: 'Services', type: 'Node', section: 'services',
                parentIds: { rack_id: rack.id }, icon: <Cog className="tw-h-4 tw-w-4 tw-text-muted" />, children },
              { id: `STORAGE-${nodeId}`, rawId: nodeId, label: 'Storage', type: 'Node', section: 'storage',
                parentIds: { rack_id: rack.id }, icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
                expandable: !!onLoadNodeDisks,
                onExpand: () => { void onLoadNodeDisks?.(nodeId); },
                children: storageTree({ rackId: rack.id, nodeId, disks: nodeDiskGroups[nodeId], hardwareCapacity,
                  capacityUsage, onLoadGroupDisks }) },
            ],
          };
        }),
      })))];
    }

    if (domain === Domain.KV) {
      // KV domain is logical only: datacenter → store → group → replica.
      // Keep the synthetic datacenter visible while empty so the root context
      // menu remains available for creating the first store.
      return [datacenterRoot(stores.map((store) => ({
        id: `S-${store.store_id}`,
        rawId: String(store.store_id),
        label: store.name ? `${storeLabel(String(store.store_id))} (${store.name})` : storeLabel(String(store.store_id)),
        type: 'Store' as const,
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (store.groups || []).map((group) => ({
          id: `G-${store.store_id}-${group.group_id}`,
          rawId: String(group.group_id),
          label: groupLabel(String(group.group_id)),
          type: 'Group' as const,
          icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
          health: toUiHealth(group.state),
          parentIds: { store_id: String(store.store_id) },
          children: (group.replicas || []).map((replica) => ({
            id: `LR-${store.store_id}-${group.group_id}-${replica.replica_id}`,
            rawId: String(replica.replica_id),
            label: localReplicaLabel(replica.replica_id),
            type: 'Replica' as const,
            icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
            role: toUiRole(String(replica.role)),
            health: toUiHealth(String(replica.state)),
            parentIds: { store_id: String(store.store_id), group_id: String(group.group_id), node_id: String(replica.node_id) },
          })),
        })),
      })))]
    }

    // Fallback (uninitialized KV domain): logical store tree.
    // The fallback logical tree also needs a root to expose Add Storage.
    return [datacenterRoot(stores.map((store) => {
      const sid = String(store.store_id);
      return {
        id: `S-${sid}`,
        rawId: sid,
        label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
        type: 'Store',
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (store.groups || []).map((group) => {
          const gid = String(group.group_id);
          const replicas = group.replicas;
          return {
            id: `G-${sid}-${gid}`,
            rawId: gid,
            label: groupLabel(gid),
            type: 'Group' as const,
            icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(group.state),
            parentIds: { store_id: sid },
            children: replicas.map((r) => ({
              id: `LR-${sid}-${gid}-${r.replica_id}`,
              rawId: String(r.replica_id),
              label: localReplicaLabel(r.replica_id),
              type: 'Replica' as const,
              icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
              role: toUiReplicaRole(String(r.role), String(r.state)),
              health: toUiHealth(String(r.state)),
              parentIds: { store_id: sid, group_id: gid, node_id: String(r.node_id ?? '') },
            })),
          };
        }),
      };
    }))];
  }, [allServers, nodeHealthById, nodeStores, serverByNodeId, stores, domain, racks, diskdbInstances, capacityUsage, hardwareCapacity, nodeDiskGroups, diskdbNodeIds, diskdbHealthById, diskdbInstanceIdByNodeId, onLoadNodeDisks, onLoadGroupDisks, servicePlans]);

  const expandedIds = useMemo(() => {
    const ids: string[] = [];
    const collect = (ns: TreeNode[]) => {
      for (const n of ns) {
        if (n.type === 'Datacenter' || n.type === 'Rack' || (n.type === 'Node' && n.section !== 'storage') || domain === Domain.KV) ids.push(n.id);
        if (n.children) collect(n.children);
      }
    };
    collect(treeNodes);
    return ids;
  }, [treeNodes, domain]);

  useNavigationSnapshot(domain, 'shared-sidebar', () => {
    const source = domain; const expanded = [...(expansions[source] ?? expandedIds)];
    return () => {
      setExpansions(previous => ({ ...previous, [source]: expanded }));
    };
  });

  return (
    <aside aria-label="Cluster tree sidebar" className="tw-h-[calc(100vh-3.5rem)] tw-mt-14 tw-border-r tw-border-border tw-bg-bg tw-flex tw-flex-col tw-overflow-hidden tw-fixed tw-left-0 tw-top-0" style={{ width }}>
      <div className="tw-m-3 tw-rounded tw-border tw-border-border tw-bg-panel tw-p-3">
        <div className="tw-flex tw-items-center tw-justify-between tw-gap-2">
          <h3 className="tw-text-sm tw-font-semibold tw-text-text">{helpTitle}</h3>
          <div className="tw-flex tw-items-center tw-gap-2">
            {!readonly && onAdd && domain !== Domain.Capacity && (
              <Button variant="ghost" size="sm" onClick={onAdd} aria-label={domain === Domain.KV && !clusterInitialized ? 'Initialize Cluster' : domain === Domain.KV ? 'Add Store' : 'Add Rack'} className="tw-h-7 tw-px-2">
                <Plus className="tw-h-3.5 tw-w-3.5" />
              </Button>
            )}
            {help && <a className="tw-inline-flex tw-items-center tw-gap-1 tw-text-xs tw-text-accent tw-underline" href={help.docs} target="_blank" rel="noreferrer">Help <ExternalLink className="tw-h-3 tw-w-3" /></a>}
          </div>
        </div>
      </div>

      <div className="tw-min-h-0 tw-flex-1 tw-overflow-y-auto">{loading && treeNodes.length === 0 ? (
        <div className="tw-p-4 tw-animate-pulse tw-space-y-2">
          <div className="tw-h-6 tw-bg-panel tw-rounded-md" />
          <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-3/4" />
          <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-1/2" />
        </div>
      ) : treeNodes.length > 0 ? (
        <Tree
          key={domain}
          nodes={treeNodes}
          expandedIds={expansions[domain] ?? expandedIds}
          onExpansionChange={ids => setExpansions(previous => ({ ...previous, [domain]: ids }))}
          onNodeClick={onNodeClick}
          onNodeContextMenu={onNodeContextMenu}
        />
      ) : (
        <div className="tw-flex tw-items-center tw-justify-center tw-flex-1 tw-text-sm tw-text-muted tw-px-4 tw-text-center">
          {domain === Domain.Cluster
            ? 'No racks registered'
            : domain === Domain.Capacity
              ? 'No racks registered'
              : clusterInitialized
                ? 'No stores yet'
                : (
                  <div className="tw-space-y-3">
                    <div>Cluster not initialized.</div>
                    {!readonly && (
                      <Button size="sm" onClick={onAdd} leftIcon={<Rocket className="tw-h-3.5 tw-w-3.5" />}>
                        Initialize Cluster
                      </Button>
                    )}
                  </div>
                )}
        </div>
      )}</div>
      {nodeManagement && domain === Domain.Cluster && <CandidateNodes racks={racks} readonly={candidateReadonly} onChange={onCandidatesChanged} />}
    </aside>
  );
}
