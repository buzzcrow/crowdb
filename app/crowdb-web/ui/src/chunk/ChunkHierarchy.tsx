// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Building2, FolderTree, Monitor, Database, Boxes, Cog } from 'lucide-react';
import { listNodeGroups, listNodeStores, type ServerSummary } from '../api';
import type { Node, NodeGroup, NodeStore, Rack } from '../types';
import { Tree, type TreeNode } from '../components/Tree';
import { DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { serviceInstanceLabel } from '../services/client';

export function ChunkHierarchy({ active, racks, nodes, servers }: { active: boolean; racks: Rack[]; nodes: Node[]; servers: ServerSummary[] }) {
  const [stores, setStores] = useState<Record<string, NodeStore[]>>({});
  const [groups, setGroups] = useState<Record<string, NodeGroup[]>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const pending = useRef(new Map<string, AbortController>());
  useEffect(() => {
    const requests = pending.current;
    return () => { requests.forEach(request => request.abort()); requests.clear(); };
  }, [active]);
  const load = async (node: number, store?: string) => {
    const key = store === undefined ? String(node) : `${node}/${store}`;
    if (!active || pending.current.has(key) || (store === undefined ? stores[key] : groups[key])) return;
    const controller = new AbortController(); pending.current.set(key, controller);
    setErrors(previous => ({ ...previous, [key]: '' }));
    try {
      if (store === undefined) {
        const result = await listNodeStores(node, 0, { signal: controller.signal });
        if (!controller.signal.aborted) setStores(previous => ({ ...previous, [key]: result }));
      } else {
        const result = await listNodeGroups(node, store, 0, { signal: controller.signal });
        if (!controller.signal.aborted) setGroups(previous => ({ ...previous, [key]: result }));
      }
    } catch (error) {
      if (!controller.signal.aborted) setErrors(previous => ({ ...previous, [key]: String(error) }));
    } finally { pending.current.delete(key); }
  };
  const iconClass = 'tw-h-4 tw-w-4 tw-text-muted';
  const tree: TreeNode[] = [{ id: 'chunk-datacenter', label: DEFAULT_DC_NAME, type: 'Datacenter', icon: <Building2 className={iconClass} />,
    children: racks.map(rack => ({ id: `chunk-rack-${rack.id}`, label: `R-${rack.id}`, type: 'Rack', icon: <FolderTree className={iconClass} />,
      children: nodes.filter(node => node.rack_id === rack.id).map(node => ({ id: `chunk-node-${node.id}`, label: `N-${node.id}`, type: 'Node', icon: <Monitor className={iconClass} />,
        children: servers.filter(server => server.node_id === node.id && ['kv', 'chunkdb'].includes(server.service_type)).map(server => ({
          id: `chunk-server-${server.id}`, label: serviceInstanceLabel(server.service_type, server.id ?? String(node.id)), type: 'Server', icon: <Cog className={iconClass} />,
          expandable: server.service_type === 'kv',
          onExpand: () => { if (server.service_type === 'kv') void load(node.id); },
          children: server.service_type !== 'kv' ? undefined : stores[String(node.id)]?.map(store => ({
            id: `chunk-store-${node.id}-${store.store_id}`, label: `S-${store.store_id}`, type: 'Store', icon: <Database className={iconClass} />, expandable: true,
            onExpand: () => { void load(node.id, String(store.store_id)); },
            children: groups[`${node.id}/${store.store_id}`]?.map(group => ({ id: `chunk-group-${node.id}-${store.store_id}-${group.group_id}`, label: `G-${group.group_id}`, type: 'Group', icon: <Boxes className={iconClass} /> })),
          })),
        })),
      })),
    })),
  }];
  return <nav aria-label="Chunk hierarchy" className="tw--mx-4">
    <Tree nodes={tree} defaultExpandedIds={['chunk-datacenter', ...racks.map(rack => `chunk-rack-${rack.id}`)]} />
    {Object.entries(errors).filter(([, error]) => error).map(([key, error]) => <p key={key} role="alert" className="tw-px-3 tw-text-xs tw-text-failed">{error} · Collapse and expand to retry.</p>)}
  </nav>;
}
