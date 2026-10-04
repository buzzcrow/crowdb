// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { Building2, FolderTree, Monitor, Database, Boxes, Cog } from 'lucide-react';
import type { ServerSummary } from '../api';
import { Domain, type EnrichedStoreView, type Node, type Rack } from '../types';
import { Tree, type TreeNode } from '../components/Tree';
import { DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { serviceInstanceLabel } from '../services/client';
import { useSelection } from '../contexts/SelectionContext';
import { useNavigationSnapshot } from '../contexts/DomainContext';

export function ChunkHierarchy({ racks, nodes, servers, stores }: { active: boolean; racks: Rack[]; nodes: Node[]; servers: ServerSummary[]; stores: EnrichedStoreView[] }) {
  const { selectEntity } = useSelection();
  const defaults = ['chunk-datacenter', ...racks.map(rack => `chunk-rack-${rack.id}`)];
  const [expanded, setExpanded] = useState<string[]>();
  useNavigationSnapshot(Domain.Chunk, 'chunk-hierarchy', () => {
    const saved = [...(expanded ?? defaults)]; return () => setExpanded(saved);
  });
  const icon = 'tw-h-4 tw-w-4 tw-text-muted';
  const tree: TreeNode[] = [{ id: 'chunk-datacenter', rawId: 'datacenter', label: DEFAULT_DC_NAME, type: 'Datacenter', icon: <Building2 className={icon} />,
    children: racks.map(rack => ({ id: `chunk-rack-${rack.id}`, rawId: rack.id, label: `R-${rack.id}`, type: 'Rack', icon: <FolderTree className={icon} />,
      children: nodes.filter(node => node.rack_id === rack.id).map(node => ({ id: `chunk-node-${node.id}`, rawId: node.id, parentIds: { rack_id: rack.id }, label: `N-${node.id}`, type: 'Node', icon: <Monitor className={icon} />,
        children: servers.filter(server => server.node_id === node.id && ['kv', 'chunkdb'].includes(server.service_type)).map(server => ({
          id: `chunk-server-${server.id}`, rawId: server.id, serviceType: server.service_type as 'kv' | 'chunkdb', parentIds: { node_id: node.id, rack_id: rack.id },
          label: serviceInstanceLabel(server.service_type, server.id ?? String(node.id)), type: 'Server', icon: <Cog className={icon} />,
          children: server.service_type !== 'kv' ? undefined : stores.flatMap(store => {
            const groups = store.groups.filter(group => group.replicas.some(replica => String(replica.node_id) === String(node.id)));
            return groups.length ? [{ id: `chunk-store-${node.id}-${store.store_id}`, rawId: store.store_id, parentIds: { node_id: node.id }, label: `S-${store.store_id}`, type: 'Store' as const, icon: <Database className={icon} />,
              children: groups.map(group => ({ id: `chunk-group-${node.id}-${store.store_id}-${group.group_id}`, rawId: group.group_id,
                parentIds: { store_id: store.store_id, node_id: node.id }, label: `G-${group.group_id}`, type: 'Group' as const, icon: <Boxes className={icon} /> })),
            }] : [];
          }),
        })),
      })),
    })),
  }];
  return <nav aria-label="Chunk hierarchy" className="tw--mx-4">
    <Tree nodes={tree} expandedIds={expanded ?? defaults} onExpansionChange={setExpanded}
      onNodeClick={node => selectEntity({ domain: Domain.Chunk, type: node.type, id: String(node.rawId ?? node.id), name: node.label, parentIds: node.parentIds, serviceType: node.serviceType })} />
    {stores.some(store => store.groups.some(group => !group.replicas.length)) && <p role="status" className="tw-p-3 tw-text-xs">Some Group membership is unavailable; unresolved branches are not assigned to a Node.</p>}
  </nav>;
}
