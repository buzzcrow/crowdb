// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo, useCallback, useEffect, useRef, useState } from 'react';
import ReactFlow, {
  Background,
  ReactFlowProvider,
  Node,
  NodeMouseHandler,
  Viewport,
  Edge,
  useReactFlow,
  useNodesInitialized,
} from 'reactflow';
import { ScanSearch } from 'lucide-react';
import 'reactflow/dist/style.css';
import { useDomain, useNavigationSnapshot } from '../contexts/DomainContext';
import { useSelection, SelectedEntity } from '../contexts/SelectionContext';
import { Rack, Node as NodeEntity, EnrichedStoreView, NodeStore, Domain, CrowdbKVServerView, NodeHealth } from '../types';
import { DEFAULT_DC_ID } from '../data/defaultDatacenter';
import { buildFlowForDomain, FlowNodeData } from './buildFlow';
import { layoutTree } from './layout';
import { addServiceNodes } from '../services/topology';
import { isAuxiliaryKind } from '../services/client';
import { CrowdbKVNode } from './CrowdbKVNode';
import { Button } from '../components/ui/Button';

export interface MenuTarget {
  type: SelectedEntity['type'];
  id: string;
  rawId?: string | number;
  parentIds?: Record<string, string | number>;
  label?: string;
  /** Service flavor for `Server` targets: KV vs DiskDB. */
  serviceType?: SelectedEntity['serviceType'];
}

interface TopologyCanvasProps {
  active?: boolean;
  scope?: Domain;
  allServers?: import('../api').ServerSummary[];
  racks: Rack[];
  nodes: NodeEntity[];
  servers: CrowdbKVServerView[];
  stores: EnrichedStoreView[];
  nodeStores?: Record<string, NodeStore[]>;
  nodeHealthById?: Record<string, NodeHealth>;
  diskdbNodeIds?: Set<number>;
  diskdbInstances?: { instance_id: string; owned_dg_ids: number[] }[];
  diskdbInstanceIdByNodeId?: Map<number, string>;
  nodeDiskGroups?: Record<number, import('./buildFlow').NodeDiskGroups>;
  refreshToken?: number;
  /** Changes when the main canvas width changes because the inspector opens or resizes. */
  viewportWidthKey?: number;
  focusRequest?: { targetId: string; subtree: boolean; nonce: number } | null;
  onFocusChange?: (targetId: string) => void;
  /** Right-click on a canvas node. */
  onEntityContextMenu?: (target: MenuTarget, event: React.MouseEvent) => void;
}

const EMPTY_COLLAPSED = new Set<string>();
const NODE_TYPES = { crowdbKv: CrowdbKVNode };

function descendantIds(rootId: string, edges: Edge[]): Set<string> {
  const childrenByParent = new Map<string, string[]>();
  for (const edge of edges) {
    const children = childrenByParent.get(edge.source) ?? [];
    children.push(edge.target);
    childrenByParent.set(edge.source, children);
  }

  const ids = new Set<string>();
  const visit = (nodeId: string) => {
    if (ids.has(nodeId)) return;
    ids.add(nodeId);
    for (const childId of childrenByParent.get(nodeId) ?? []) visit(childId);
  };
  visit(rootId);
  return ids;
}

export function TopologyCanvas(props: TopologyCanvasProps) {
  return (
    <ReactFlowProvider>
      <TopologyCanvasInner {...props} />
    </ReactFlowProvider>
  );
}

/** Mirror the node-id scheme used in buildFlow so the current selection can
 * be highlighted on the canvas. */
function selectedNodeId(entity: SelectedEntity): string | null {
  const p = entity.parentIds || {};
  if (entity.domain === Domain.Cluster || entity.domain === Domain.Capacity) {
    switch (entity.type) {
      case 'Datacenter': return `DC-${DEFAULT_DC_ID}`;
      case 'Rack': return `R-${entity.id}`;
      case 'Node': return `N-${entity.id}`;
      case 'Server': {
        if (isAuxiliaryKind(entity.serviceType)) return `SERVICE-${entity.id}`;
        // DDB server nodes use `DDB-` prefix; Paxos-KV servers use `PKV-`.
        if (entity.id?.startsWith?.('DDB-')) return p.node_id ? `DDB-${p.node_id}` : null;
        return p.node_id ? `PKV-${p.node_id}` : null;
      }
      case 'DiskGroup':
        if (!p.node_id) return null;
        return entity.domain === Domain.Cluster ? `CL-DG-${p.node_id}-${entity.id}` : `CDG-${p.node_id}-${entity.id}`;
      case 'Disk':
        return entity.domain === Domain.Capacity && p.node_id && p.disk_group_id
          ? `CD-${p.node_id}-${p.disk_group_id}-${entity.id}`
          : null;
      case 'Store': return p.node_id ? `S-${p.node_id}-${entity.id}` : null;
      case 'Group':
        return p.node_id && p.store_id ? `G-${p.node_id}-${p.store_id}-${entity.id}` : null;
      case 'Replica':
        if (p.remote_on && p.store_id && p.group_id) return `RR-${p.remote_on}-${p.store_id}-${p.group_id}-${entity.id}`;
        return p.node_id && p.store_id && p.group_id ? `LR-${p.node_id}-${p.store_id}-${p.group_id}-${entity.id}` : null;
      default: return null;
    }
  }
  switch (entity.type) {
    case 'Datacenter': return `DC-${DEFAULT_DC_ID}`;
    case 'Store': return `S-${entity.id}`;
    case 'Group': return p.store_id ? `G-${p.store_id}-${entity.id}` : null;
    case 'Replica':
      return p.store_id && p.group_id ? `LR-${p.store_id}-${p.group_id}-${entity.id}` : null;
    default: return null;
  }
}

function TopologyCanvasInner({ active = true, scope, allServers, racks, nodes, servers, stores, nodeStores, nodeHealthById, diskdbNodeIds, diskdbInstances, diskdbInstanceIdByNodeId, nodeDiskGroups, refreshToken, viewportWidthKey, focusRequest, onFocusChange, onEntityContextMenu }: TopologyCanvasProps) {
  const { domain: activeDomain, returning } = useDomain();
  const domain = scope ?? activeDomain;
  const { selectionForDomain, selectEntity } = useSelection();
  const selectedEntity = selectionForDomain(domain);
  const { fitView, setViewport, setCenter, getZoom, getNodes, getViewport } = useReactFlow();
  const canvasRef = useRef<HTMLDivElement>(null);
  const [canvasSizeKey, setCanvasSizeKey] = useState('');
  const nodesInitialized = useNodesInitialized();
  const viewportsRef = useRef<Partial<Record<Domain, Viewport>>>({});
  const fittedOnceRef = useRef<Partial<Record<Domain, boolean>>>({});
  const lastRefreshTokenRef = useRef<number | undefined>(refreshToken);
  const lastFocusNonceRef = useRef<number | undefined>(undefined);
  const lastDomainRef = useRef<Domain | undefined>(undefined);
  const wasActive = useRef(active);
  const nodeIdsKeyRef = useRef<Partial<Record<Domain, string>>>({});
  // Tracks the last (domain, nodeIds, refreshToken) triple that triggered
  // a fit/restore. Polls return new array references for the same data, which
  // would otherwise re-run the effect every cycle and fight user panning.
  const lastActionKeyRef = useRef<string | undefined>(undefined);
  // rAF id for the pending fit, tracked outside the effect cleanup so
  // poll updates (which re-run the effect with a new positioned.nodes
  // reference but the same action key) don't cancel an in-flight fit.
  const fitRafIdRef = useRef<number | undefined>(undefined);
  const lastCanvasSizeRef = useRef('');
  const [collapsedByDomain, setCollapsedByDomain] = useState<Partial<Record<Domain, Set<string>>>>({});
  useEffect(() => {
    const element = canvasRef.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      const width = Math.round(entry.contentRect.width);
      const height = Math.round(entry.contentRect.height);
      if (width <= 0 || height <= 0) return;
      const key = `${width}x${height}`;
      if (key === lastCanvasSizeRef.current) return;
      lastCanvasSizeRef.current = key;
      viewportsRef.current[domain] = undefined;
      fittedOnceRef.current[domain] = false;
      setCanvasSizeKey(key);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [domain]);
  const defaultCollapsed = useMemo(() => domain === Domain.Cluster
    ? new Set(nodes.map(node => `N-${node.id}`)) : EMPTY_COLLAPSED, [domain, nodes]);
  const collapsed = collapsedByDomain[domain] ?? defaultCollapsed;
  useNavigationSnapshot(domain, 'topology', () => {
    const viewport = getViewport();
    const hidden = [...collapsed];
    return () => {
      setCollapsedByDomain(previous => ({ ...previous, [domain]: new Set(hidden) }));
      viewportsRef.current[domain] = viewport;
      fittedOnceRef.current[domain] = true;
      void setViewport(viewport, { duration: 0 });
    };
  });

  const { nodes: rawNodes, edges } = useMemo(
    () => addServiceNodes(domain, buildFlowForDomain(domain, racks, nodes, servers, stores, nodeStores, nodeHealthById, diskdbNodeIds, diskdbInstances, diskdbInstanceIdByNodeId, nodeDiskGroups), allServers ?? [], nodes),
    [domain, allServers, racks, nodes, servers, stores, nodeStores, nodeHealthById, diskdbNodeIds, diskdbInstances, diskdbInstanceIdByNodeId, nodeDiskGroups],
  );

  const childCounts = useMemo(() => {
    const counts = new Map<string, number>();
    edges.forEach(edge => counts.set(edge.source, (counts.get(edge.source) ?? 0) + 1));
    return counts;
  }, [edges]);
  const positioned = useMemo(() => {
    const hidden = new Set<string>();
    collapsed.forEach(id => descendantIds(id, edges).forEach(child => {
      if (child !== id) hidden.add(child);
    }));
    return layoutTree(rawNodes.filter(node => !hidden.has(node.id)),
      edges.filter(edge => !hidden.has(edge.source) && !hidden.has(edge.target)));
  }, [rawNodes, edges, collapsed]);

  // Sidebar focus reveals every ancestor before centering a hidden target.
  useEffect(() => {
    if (!focusRequest) return;
    setCollapsedByDomain(previous => {
      const next = new Set(previous[domain] ?? defaultCollapsed);
      next.forEach(id => {
        if (id !== focusRequest.targetId && descendantIds(id, edges).has(focusRequest.targetId)) next.delete(id);
      });
      if (next.size === (previous[domain] ?? defaultCollapsed).size) return previous;
      return { ...previous, [domain]: next };
    });
  }, [domain, edges, focusRequest, defaultCollapsed]);

  useEffect(() => {
    if (refreshToken !== lastRefreshTokenRef.current) {
      lastRefreshTokenRef.current = refreshToken;
      viewportsRef.current = {};
      fittedOnceRef.current = {};
    }
  }, [refreshToken]);

  useEffect(() => {
    if (!active) { wasActive.current = false; return; }
    if (!wasActive.current && !returning) {
      viewportsRef.current[domain] = undefined;
      fittedOnceRef.current[domain] = false;
      lastActionKeyRef.current = undefined;
    }
    wasActive.current = true;
    // On view-mode switch, always fit to window — don't restore a stale
    // saved viewport from a previous visit to this mode.
    const domainChanged = lastDomainRef.current !== domain;
    if (domainChanged) {
      lastDomainRef.current = domain;
      viewportsRef.current[domain] = undefined;
      fittedOnceRef.current[domain] = false;
      // Reset the action-key guard so the fit actually runs instead of
      // short-circuiting on the stale key from the previous visit.
      lastActionKeyRef.current = undefined;
    }
    if (positioned.nodes.length === 0 || !nodesInitialized) {
      fittedOnceRef.current[domain] = false;
      return;
    }
    // Re-fit when the set of node IDs changes (nodes added/removed),
    // whether from a mutation or a poll update — not just on manual refresh.
    const nodeIdsKey = positioned.nodes.map((n) => n.id).sort().join(',');
    // Only act on meaningful changes: view-mode switch, node-set change, or
    // manual refresh. Polls return new array references for the same data;
    // without this guard the effect re-runs every cycle and the
    // setViewport(savedViewport) call below fights an in-progress pan drag.
    const actionKey = `${domain}:${nodeIdsKey}:${refreshToken ?? ''}:${viewportWidthKey ?? ''}:${canvasSizeKey}`;
    if (actionKey === lastActionKeyRef.current) return;
    // Action key changed — cancel any pending rAF from the previous action.
    if (fitRafIdRef.current != null) {
      cancelAnimationFrame(fitRafIdRef.current);
      fitRafIdRef.current = undefined;
    }
    lastActionKeyRef.current = actionKey;
    if (nodeIdsKey !== nodeIdsKeyRef.current[domain]) {
      nodeIdsKeyRef.current[domain] = nodeIdsKey;
      viewportsRef.current[domain] = undefined;
      fittedOnceRef.current[domain] = false;
    }
    // Retry fit across frames until ReactFlow has measured every node's
    // dimensions — newly added nodes lack width/height on the first frame,
    // so a single rAF fitView would ignore them and never re-fit.
    const tryFit = () => {
      const savedViewport = viewportsRef.current[domain];
      if (savedViewport) {
        void setViewport(savedViewport, { duration: 250 });
        fitRafIdRef.current = undefined;
        return;
      }
      if (!fittedOnceRef.current[domain]) {
        const storeNodes = getNodes();
        if (storeNodes.some((n) => !n.width || !n.height)) {
          fitRafIdRef.current = requestAnimationFrame(tryFit);
          return;
        }
        // Fit the visible topology only. Hidden service descendants belong to
        // collapsed branches and must not expand the bounds during tab return.
        void fitView({ padding: 0.1, duration: 250 });
        fittedOnceRef.current[domain] = true;
      }
      fitRafIdRef.current = undefined;
    };
    fitRafIdRef.current = requestAnimationFrame(tryFit);
  }, [active, returning, fitView, getNodes, nodesInitialized, positioned.nodes, setViewport, domain, refreshToken, viewportWidthKey, canvasSizeKey]);

  // ReactFlow can finish measuring its viewport one tick after a domain tab
  // becomes visible. A delayed fit closes that gap and prevents the viewport
  // calculated while the panel was hidden from surviving a tab return.
  useEffect(() => {
    if (!active || !nodesInitialized || positioned.nodes.length === 0) return;
    const timer = window.setTimeout(() => {
      viewportsRef.current[domain] = undefined;
      fittedOnceRef.current[domain] = false;
      void fitView({ padding: 0.1, duration: 250 });
      fittedOnceRef.current[domain] = true;
    }, 180);
    return () => window.clearTimeout(timer);
  }, [active, canvasSizeKey, domain, fitView, nodesInitialized, positioned.nodes.length]);

  const selId = selectedEntity ? selectedNodeId(selectedEntity) : null;
  const activateNode = useCallback((node: Node) => {
    onFocusChange?.(node.id);
    const entity = (node.data as FlowNodeData).entity;
    if (entity) selectEntity({ ...entity, domain });
    if (childCounts.has(node.id)) setCollapsedByDomain(previous => {
      const next = new Set(previous[domain] ?? defaultCollapsed);
      if (next.has(node.id)) next.delete(node.id); else next.add(node.id);
      return { ...previous, [domain]: next };
    });
  }, [onFocusChange, selectEntity, domain, childCounts, defaultCollapsed]);
  const decoratedNodes: Node[] = useMemo(
    () =>
      positioned.nodes.map((n) => ({
        ...n,
        data: { ...(n.data as FlowNodeData), isSelected: n.id === selId,
          childCount: childCounts.get(n.id) ?? 0, collapsed: collapsed.has(n.id),
          onActivate: () => activateNode(n) },
      })),
    [positioned.nodes, selId, childCounts, collapsed, activateNode],
  );

  useEffect(() => {
    if (!focusRequest || !nodesInitialized) return;
    if (focusRequest.nonce === lastFocusNonceRef.current) return;

    const target = positioned.nodes.find((node) => node.id === focusRequest.targetId);
    if (!target) return;

    lastFocusNonceRef.current = focusRequest.nonce;
    const targetIds = focusRequest.subtree ? descendantIds(focusRequest.targetId, positioned.edges) : new Set([focusRequest.targetId]);
    const focusNodes = positioned.nodes.filter((node) => targetIds.has(node.id));
    if (focusNodes.length === 0) return;

    // Drop any stale saved viewport for this mode. Programmatic setCenter
    // does not reliably fire onMoveEnd with the new viewport, so the saved
    // value still points at the pre-focus position; if a poll lands right
    // after the focus pan, the node-change effect would otherwise restore it
    // and snap the view back to where it was before the click.
    viewportsRef.current[domain] = undefined;

    const frame = requestAnimationFrame(() => {
      // Pan only — keep the current zoom level, just center the focused
      // node(s) in the viewport. Scaling on every click is jarring.
      const xs = focusNodes.map((n) => n.position.x + (n.width ?? 0) / 2);
      const ys = focusNodes.map((n) => n.position.y + (n.height ?? 0) / 2);
      const cx = xs.reduce((a, b) => a + b, 0) / xs.length;
      const cy = ys.reduce((a, b) => a + b, 0) / ys.length;
      void setCenter(cx, cy, { zoom: getZoom(), duration: 250 });
      fittedOnceRef.current[domain] = true;
    });
    return () => cancelAnimationFrame(frame);
  }, [setCenter, getZoom, focusRequest, nodesInitialized, positioned.edges, positioned.nodes, domain]);

  const handleFitAll = useCallback(() => {
    viewportsRef.current[domain] = undefined;
    void fitView({ padding: 0.1, duration: 250 });
    fittedOnceRef.current[domain] = true;
  }, [fitView, domain]);

  const onNodeClick: NodeMouseHandler = useCallback(
    (_e, node) => activateNode(node),
    [activateNode],
  );

  const onNodeContextMenu = useCallback(
    (e: React.MouseEvent, node: Node) => {
      e.preventDefault();
      const data = node.data as FlowNodeData;
      if (!data.entity) return;
      selectEntity({ ...data.entity, domain });
      onEntityContextMenu?.(
        { type: data.entity.type, id: data.entity.id, parentIds: data.entity.parentIds, label: data.label, serviceType: data.entity.serviceType },
        e,
      );
    },
    [onEntityContextMenu, selectEntity, domain],
  );

  return (
    <div ref={canvasRef} className="tw-relative tw-w-full tw-h-full tw-bg-bg tw-overflow-hidden">
      <div className="tw-absolute tw-top-3 tw-right-3 tw-z-20">
        <Button
          variant="secondary"
          size="sm"
          leftIcon={<ScanSearch className="tw-h-3.5 tw-w-3.5" />}
          onClick={handleFitAll}
          data-testid="fit-all-btn"
        >
          Fit All
        </Button>
      </div>
      {decoratedNodes.length === 0 ? (
        <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">
          {domain === Domain.Cluster
            ? 'No racks registered. Add a rack to get started.'
            : domain === Domain.Capacity
              ? 'No racks registered. Add a rack to get started.'
              : 'No stores yet. Switch to a deployed node and add a store.'}
        </div>
      ) : (
      <ReactFlow
        nodes={decoratedNodes}
        edges={positioned.edges}
        nodeTypes={NODE_TYPES}
        onNodeClick={onNodeClick}
        onNodeContextMenu={onNodeContextMenu}
        onMoveEnd={(_event, viewport) => {
          viewportsRef.current[domain] = viewport;
          fittedOnceRef.current[domain] = true;
        }}
        nodesDraggable={false}
        minZoom={0.02}
        maxZoom={2.5}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={24} color="#2e3440" />
      </ReactFlow>
      )}
    </div>
  );
}
