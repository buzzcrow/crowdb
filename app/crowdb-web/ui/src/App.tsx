import { PanelDivider } from './components/PanelDivider';
// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Suspense, useState, useCallback, useMemo, lazy, useEffect, useRef } from 'react';
import type { CenterPanelMode } from './shell/Header';
import { DomainProvider, useDomain } from './contexts/DomainContext';
import { SelectionProvider, useSelection, type SelectedEntity } from './contexts/SelectionContext';
import { ToastProvider, useToast } from './contexts/ToastContext';
import { ActivityProvider, useActivity } from './contexts/ActivityContext';
import { useClusterTree } from './data/useClusterTree';
import { useLogicalTree } from './data/useLogicalTree';
import { useCapacityTree } from './data/useCapacityTree';
import { Header, ClusterHealth } from './shell/Header';
import { Sidebar } from './shell/Sidebar';
import { useNodeServicePlans } from './services/useNodeServicePlans';
import { NodeServicesDialog } from './services/NodeServicesDialog';
import { DeployServiceDialog } from './services/DeployServiceDialog';
import { ToastContainer } from './components/ToastContainer';
import { TreeNode } from './components/Tree';
import { ContextMenu, useContextMenu } from './components/ContextMenu';
import type { MenuTarget } from './topology/TopologyCanvas';
import {
  AddRackDialog,
  AddNodeDialog,
  AddStoreDialog,
  AddGroupDialog,
  AddReplicaDialog,
  AddDiskGroupDialog,
  AddDiskDialog,
  AssignDiskGroupDialog,
  DeployServerDialog,
  DeployDiskdbDialog,
  ConfirmDeleteDialog,
  InitClusterDialog,
  ZoneSelectDialog,
} from './components/dialogs';
import { Domain } from './types';
import type { ConsoleDialogState } from './menus/context';
import { useClusterMenus } from './menus/useClusterMenus';
import { useCapacityMenus } from './menus/useCapacityMenus';
import { setApiBase, resetCluster, compactDiskdbZones, rebuildDiskdbZoneBitmap, listServers } from './api';
import { deployPortDefaultsForNode, diskdbPortDefaultsForNode, nextIdFromSuffix, nextNumericId } from './components/dialogs/defaults';
import { buildCrowdbKVServers, crowdbKvServerNodeIds, extractPort } from './data/crowdbKvServers';
import { isCrowdbKVServerAvailable } from './data/crowdbKvServers';
import { toUiHealth } from './utils/entityDisplay';
import { ClusterView } from './views/ClusterView';
import { KvView } from './views/KvView';
import { CapacityView } from './views/CapacityView';
import { ChunkBrowser } from './chunk/ChunkBrowser';
import { ChunkKvView } from './chunk-kv/ChunkKvView';
import { IcebergView } from './views/IcebergView';
import { S3View } from './views/S3View';
import { ManagementSession } from './managed/ManagementSession';
import { MonitorSummary } from './managed/MonitorSummary';

const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));

export interface CrowdbConsoleProps {
  /** API prefix for all backend calls (default "/api"). */
  apiPrefix?: string;
  /** Mount hint for host routers (default "/"). Not used for navigation in v1. */
  basePath?: string;
  /** Hide all mutating controls. */
  readonly?: boolean;
  /** Opt feature areas in/out. */
  modules?: Partial<Record<'racks' | 'nodes' | 'stores' | 'groups' | 'replicas' | 'kv' | 'activity', boolean>>;
  /** Initial domain (default Cluster). */
  initialDomain?: Domain;
  /** Container topology is immutable; native data credentials remain independent. */
  managed?: boolean;
  /** Structured event callback for host integration. */
  onEvent?: (event: { type: string; payload?: unknown }) => void;
}

function AppContent({ apiPrefix = '/api', readonly = false, modules, onEvent, managed = false }: CrowdbConsoleProps) {
  const { domain, setDomain } = useDomain();
  const { selectedEntity, selectionForDomain, selectEntity, clearSelection } = useSelection();
  const { success, error } = useToast();
  const { log } = useActivity();

  // Re-root data-plane traffic onto the host-provided apiPrefix. The
  // standalone mount also sets this pre-render in `main.tsx`; this keeps
  // an embedding host's prop authoritative.
  useEffect(() => {
    setApiBase(apiPrefix);
  }, [apiPrefix]);

  const [lastUsedRackId, setLastUsedRackId] = useState<number>(0);
  const [rememberedDeployPorts, setRememberedDeployPorts] = useState<{ mgmt: number[]; rpc: number[]; diskdbRpc: number[] }>({ mgmt: [], rpc: [], diskdbRpc: [] });
  const [lastRefreshTime, setLastRefreshTime] = useState<Date>(new Date());
  const [refreshing, setRefreshing] = useState(false);
  const [centerPanel, setCenterPanel] = useState<CenterPanelMode>('topology');
  const [sidebarWidth, setSidebarWidth] = useState(280);
  const [inspectorWidth, setInspectorWidth] = useState(320);
  const [canvasFocusRequest, setCanvasFocusRequest] = useState<{ targetId: string; subtree: boolean; nonce: number } | null>(null);
  // Cross-jumps replace the destination scope once. Manual switches retain
  // each domain's selection.
  const pendingSelectionRef = useRef<SelectedEntity | null>(null);
  const [chunkRequest, setChunkRequest] = useState<{ id: string; nonce: number } | undefined>();

  useEffect(() => {
    if (pendingSelectionRef.current) {
      const pending = pendingSelectionRef.current;
      pendingSelectionRef.current = null;
      selectEntity(pending, false);
    }
    setCanvasFocusRequest(null);
  }, [domain, clearSelection, selectEntity]);

  const [dialog, setDialog] = useState<ConsoleDialogState>({});

  const { menuState, openMenu, closeMenu } = useContextMenu();

  const managementAuthorized = true;
  const topologyReadonly = readonly || managed;
  const logicalReadonly = readonly;
  const ownsSidebar = domain === Domain.Iceberg || domain === Domain.S3 || (domain === Domain.Chunk || domain === Domain.ChunkKV);
  const physicalActive = domain === Domain.Cluster;
  const capacityActive = domain === Domain.Capacity;
  const { racks, nodes, services: managedServers, nodeStores, nodeHealthById, nodeDiskGroups: clusterDiskGroups, loadNodeDisks, loadGroupDisks, loading: physLoading, error: physError, refresh: refreshPhysical } = useClusterTree({
    enabled: true,
    managed,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { stores, groups, loading: logLoading, error: logError, refresh: refreshLogical } = useLogicalTree({
    enabled: true,
    managed,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { instances: diskdbInstances, usage: capacityUsage, hardwareCapacity, scanStatus: capacityScanStatus, loading: capLoading, error: capError, refresh: refreshCapacity,  } = useCapacityTree({
    enabled: domain === Domain.Capacity || domain === Domain.Cluster,
    observeRuntime: capacityActive,
    diskGroupId: selectionForDomain(Domain.Capacity)?.parentIds?.disk_group_id !== undefined
      ? Number(selectionForDomain(Domain.Capacity)!.parentIds!.disk_group_id)
      : selectionForDomain(Domain.Capacity)?.type === 'DiskGroup' ? Number(selectionForDomain(Domain.Capacity)!.id) : undefined,
    diskId: selectionForDomain(Domain.Capacity)?.type === 'Disk' ? selectionForDomain(Domain.Capacity)!.id : undefined,
    pollIntervalActive: 5000,
    pollIntervalInactive: 30000,
  });

  const nodeDiskGroups = clusterDiskGroups;
  const diskSelection = selectionForDomain(Domain.Capacity);
  const clusterSelection = selectionForDomain(Domain.Cluster);
  useEffect(() => {
    if (managed) return;
    const selected = capacityActive ? diskSelection : physicalActive ? clusterSelection : null;
    const nodeId = selected?.type === 'Node' ? Number(selected.id) : Number(selected?.parentIds?.node_id);
    if (!Number.isFinite(nodeId)) return;
    void loadNodeDisks(nodeId);
    const groupId = selected?.type === 'DiskGroup' ? Number(selected.id) : Number(selected?.parentIds?.disk_group_id);
    if (Number.isFinite(groupId)) void loadGroupDisks(nodeId, groupId);
  }, [managed, capacityActive, physicalActive, diskSelection, clusterSelection, loadNodeDisks, loadGroupDisks]);
  useEffect(() => {
    if (managed || !dialog.deployAuxiliary) return;
    void loadNodeDisks(dialog.deployAuxiliary.nodeId);
  }, [managed, dialog.deployAuxiliary, loadNodeDisks]);
  const existingDiskGroupIds = useMemo(
    () => Array.from(new Set([
      ...Object.values(nodeDiskGroups).flatMap((entry) => entry.diskGroups.map((dg) => dg.id)),
      ...(hardwareCapacity?.disk_groups || []).map((group) => group.disk_group_id),
    ])),
    [nodeDiskGroups, hardwareCapacity],
  );

  const loading = physLoading || logLoading || capLoading;
  const dataError = (domain === Domain.Cluster ? physError : domain === Domain.KV ? logError : domain === Domain.Capacity ? capError ?? physError : null);
  const servers = useMemo(() => buildCrowdbKVServers(nodes, racks), [nodes, racks]);
  const serverNodeIds = useMemo(() => crowdbKvServerNodeIds(servers), [servers]);
  const [standaloneServers, setAllServers] = useState<import('./api').ServerSummary[]>([]);
  const allServers = managed ? managedServers : standaloneServers;
  const serverErrorShownRef = useRef(false);
  const refreshAllServers = useCallback(async () => {
    try {
      setAllServers(await listServers());
      serverErrorShownRef.current = false;
    } catch (err) {
      setAllServers([]);
      // Only show the toast once per failure streak — polling retries
      // every few seconds and would otherwise flood the UI with
      // identical "backend unreachable" toasts.
      if (!serverErrorShownRef.current) {
        serverErrorShownRef.current = true;
        error(`Failed to load server list: ${err instanceof Error ? err.message : 'backend unreachable'}`);
      }
    }
  }, [error]);
  useEffect(() => {
    if (!managed && (physicalActive || capacityActive || domain === Domain.Chunk || domain === Domain.ChunkKV)) {
      refreshAllServers();
    }
  }, [managed, physicalActive, capacityActive, domain, diskdbInstances, refreshAllServers]);
  const diskdbNodeIds = useMemo(
    () => new Set([...allServers.filter((s) => s.service_type === 'diskdb' && s.node_id != null).map((s) => s.node_id!), ...nodes.filter(node => node.diskdb_server).map(node => node.id)]),
    [allServers, nodes],
  );
  const diskdbHealthById = useMemo(() => {
    const m = new Map<number, string>();
    for (const s of allServers) {
      if (s.service_type === 'diskdb' && s.node_id != null) m.set(s.node_id, s.health);
    }
    for (const node of nodes) if (node.diskdb_server) m.set(node.id, node.diskdb_server.health);
    return m;
  }, [allServers, nodes]);
  const diskdbInstanceIdByNodeId = useMemo(() => {
    const instanceByPort = new Map(
      diskdbInstances
        .map((instance) => [extractPort(instance.rpc_endpoint), instance.instance_id] as const)
        .filter((entry): entry is readonly [number, string] => entry[0] != null),
    );
    const result = new Map<number, string>();
    for (const node of nodes) {
      const port = extractPort(node.diskdb_server?.endpoint);
      const instanceId = port == null ? undefined : instanceByPort.get(port);
      if (instanceId) result.set(node.id, instanceId);
    }
    for (const server of allServers) {
      if (server.service_type !== 'diskdb' || server.node_id == null) continue;
      const port = extractPort(server.endpoint || server.rpc_url);
      const instanceId = port == null ? undefined : instanceByPort.get(port);
      if (instanceId != null) result.set(server.node_id, instanceId);
    }
    return result;
  }, [allServers, diskdbInstances, nodes]);
  // Cluster is initialized once the system store (store 0) exists.
  const clusterInitialized = useMemo(
    () => stores.some((s) => String(s.store_id) === '0'),
    [stores],
  );

  const clusterHealth: ClusterHealth = useMemo(() => {
    if (dataError) return domain === Domain.Capacity ? 'Degraded' : 'Failed';
    if (groups.length === 0) return 'Unknown';
    const statuses = groups.map((g) => toUiHealth(String((g as any).state || (g as any).health || '')));
    if (statuses.some((status) => status === 'Failed')) return 'Failed';
    if (statuses.some((status) => status === 'Degraded')) return 'Degraded';
    if (statuses.every((status) => status === 'Healthy')) return 'Healthy';
    return 'Unknown';
  }, [groups, dataError, domain]);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const tasks: Promise<unknown>[] = [refreshPhysical(), refreshLogical(), refreshCapacity()];
      if (!managed) tasks.push(refreshAllServers());
      await Promise.all(tasks);
      setLastRefreshTime(new Date());
    } finally {
      setRefreshing(false);
    }
  }, [managed, refreshPhysical, refreshLogical, refreshCapacity, refreshAllServers]);

  const servicePlans = useNodeServicePlans(stores, nodeDiskGroups, handleRefresh, !topologyReadonly);
  useEffect(() => {
    if (managed) return;
    for (const [id, plan] of Object.entries(servicePlans.plans)) {
      if (plan.diskio.state !== 'waiting') continue;
      const nodeId = Number(id);
      if (!nodeDiskGroups[nodeId]) { void loadNodeDisks(nodeId); continue; }
      for (const group of nodeDiskGroups[nodeId].diskGroups) {
        if (!nodeDiskGroups[nodeId].disksByDg[group.id]) void loadGroupDisks(nodeId, group.id);
      }
    }
  }, [managed, servicePlans.plans, nodeDiskGroups, loadNodeDisks, loadGroupDisks]);

  // After cluster init succeeds, refresh the tree so the system group
  // appears. Init only bootstraps store 0 / group 0; store creation is
  // a separate step the user initiates via the "+" button.
  const handleInitSuccess = useCallback(async () => {
    await handleRefresh();
    setDialog((d) => ({ ...d, initCluster: false }));
  }, [handleRefresh]);

  /** Run a mutation, surface toast + activity, then refresh. */
  const runMutation = useCallback(
    async (action: string, target: string, fn: () => Promise<unknown>) => {
      try {
        await fn();
        log({ action, target, status: 'Success' });
        success(`${action}: ${target}`);
        onEvent?.({ type: 'mutation', payload: { action, target } });
        await handleRefresh();
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'failed';
        log({ action, target, status: 'Failed', message: msg });
        error(`${action} failed: ${msg}`);
      }
    },
    [log, success, error, onEvent, handleRefresh],
  );

  const requestDelete = useCallback(
    (type: string, id: string | number, onDelete: () => Promise<void>, cascadeWarning?: string) => {
      setDialog((d) => ({ ...d, delete: { type, id, onDelete, cascadeWarning } }));
    },
    [],
  );

  const handleResetCluster = useCallback(() => {
    setDialog((d) => ({
      ...d,
      delete: {
        type: 'Cluster',
        id: 'all',
        onDelete: async () => {
          await runMutation('Reset Cluster', 'all', async () => {
            await servicePlans.stop();
            await resetCluster();
            clearSelection();
          });
        },
      },
    }));
  }, [runMutation, clearSelection]);

  const menuContext = { readonly, managed, managementAuthorized, domain, physicalActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds, allServers, setDialog, capacityUsage };
  const buildMenuItems = useClusterMenus(menuContext);
  const buildCapacityMenuItems = useCapacityMenus(menuContext);

  const onTreeContextMenu = useCallback(
    (node: TreeNode, event: React.MouseEvent) => {
      const target: MenuTarget = {
        type: node.type,
        id: node.rawId != null ? String(node.rawId) : node.id,
        rawId: node.rawId,
        parentIds: node.parentIds,
        label: node.label,
        serviceType: node.serviceType,
      };
      const items = capacityActive ? buildCapacityMenuItems(target) : buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, buildCapacityMenuItems, capacityActive, openMenu],
  );

  const onCanvasContextMenu = useCallback(
    (target: MenuTarget, event: React.MouseEvent) => {
      const items = capacityActive ? buildCapacityMenuItems(target) : buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, buildCapacityMenuItems, capacityActive, openMenu],
  );

  const onTreeNodeClick = useCallback((node: TreeNode) => {
    setCanvasFocusRequest({ targetId: node.id, subtree: true, nonce: Date.now() });
  }, []);

  const handleAdd = useCallback(() => {
    if (readonly) return;
    if (physicalActive || capacityActive) setDialog((d) => ({ ...d, addRack: true }));
    else if (!clusterInitialized) setDialog((d) => ({ ...d, initCluster: true }));
    else setDialog((d) => ({ ...d, addStore: true }));
  }, [readonly, physicalActive, capacityActive, clusterInitialized]);

  const closeDialogs = useCallback(() => setDialog({}), []);

  const kvEnabled = modules?.kv !== false;

  const rackIds = useMemo(() => racks.map((r) => r.id), [racks]);
  const nodeIds = useMemo(() => nodes.map((n) => n.id), [nodes]);

  const defaultAddNodeRackId = useMemo(() => {
    if (dialog.addNode?.rackId) return dialog.addNode.rackId;
    if (lastUsedRackId && rackIds.includes(lastUsedRackId)) return lastUsedRackId;
    return racks[0]?.id ?? 0;
  }, [dialog.addNode?.rackId, lastUsedRackId, rackIds, racks]);

  const deployDialogDefaults = useMemo(() => {
    if (!dialog.deployServer?.nodeId) {
      return { defaultRestPort: '19910', defaultRpcPort: '19920' };
    }
    return deployPortDefaultsForNode(
      servers,
      dialog.deployServer.nodeId,
      19910,
      19920,
      rememberedDeployPorts.mgmt,
      rememberedDeployPorts.rpc,
    );
  }, [dialog.deployServer?.nodeId, rememberedDeployPorts, servers]);

  const addNodeDeployDefaults = useMemo(
    () => {
      const nextNodeId = Number(nextIdFromSuffix(nodeIds, 1));
      return {
        ...deployPortDefaultsForNode(
          servers,
          nextNodeId,
          19910,
          19920,
          rememberedDeployPorts.mgmt,
          rememberedDeployPorts.rpc,
        ),
        defaultDiskdbRpcPort: diskdbPortDefaultsForNode(
          diskdbInstances,
          nextNodeId,
          undefined,
          rememberedDeployPorts.diskdbRpc,
        ),
      };
    },
    [nodeIds, rememberedDeployPorts, servers, diskdbInstances],
  );

  const deployDiskdbDefaults = useMemo(() => {
    if (!dialog.deployDiskdb?.nodeId) return '29920';
    return diskdbPortDefaultsForNode(
      diskdbInstances,
      dialog.deployDiskdb.nodeId,
      undefined,
      rememberedDeployPorts.diskdbRpc,
    );
  }, [dialog.deployDiskdb?.nodeId, diskdbInstances, rememberedDeployPorts.diskdbRpc]);

  const storeDialogDefaults = useMemo(() => {
    const availableNodeIds = servers.filter((server) => isCrowdbKVServerAvailable(server)).map((server) => server.node_id);
    const defaultNodeIds = availableNodeIds.length <= 7 ? availableNodeIds : availableNodeIds.slice(0, 3);
    return {
      storeId: nextNumericId(stores.map((s) => String(s.store_id)), 1),
      nodeIds: defaultNodeIds.length > 0 ? defaultNodeIds : (nodes[0] ? [nodes[0].id] : []),
    };
  }, [nodes, servers, stores]);

  const groupDialogDefaults = useMemo(() => {
    const defaults: Record<string, { groupId: string; replicaId: string; nodeIds: number[] }> = {};
    const activeNodeIds = servers
      .filter((server) => isCrowdbKVServerAvailable(server))
      .map((server) => server.node_id);
    for (const store of stores) {
      const storeId = String(store.store_id);
      const groupsInStore = groups.filter((g) => String(g.store_id) === storeId);
      const groupId = nextNumericId(groupsInStore.map((g) => String(g.group_id)), 1);

      const replicaIds: string[] = [];
      for (const group of groupsInStore) {
        for (const replica of group.replicas || []) {
          replicaIds.push(String(replica.replica_id));
        }
      }

      const replicaId = nextNumericId(replicaIds, 1);
      const storeNodeIds = store.nodes.filter((nodeId) => activeNodeIds.includes(nodeId));
      const nodeIds = storeNodeIds.length > 0 ? storeNodeIds : activeNodeIds.slice(0, 3);

      defaults[storeId] = { groupId, replicaId, nodeIds };
    }
    return defaults;
  }, [groups, nodes, servers, stores]);

  const replicaDialogDefaults = useMemo(() => {
    const defaults: Record<string, { nodeId: number; replicaId: string }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const existingReplicaIds = (group.replicas || []).map((replica) => String(replica.replica_id));
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      const preferredNode =
        servers.find((server) => !usedNodeIds.has(server.node_id))?.node_id ||
        servers[0]?.node_id ||
        nodes[0];

      defaults[key] = {
        nodeId: typeof preferredNode === 'number' ? preferredNode : (preferredNode?.id ?? 0),
        replicaId: nextNumericId(existingReplicaIds, 1),
      };
    }

    return defaults;
  }, [groups, nodes, servers]);

  const replicaDialogNodeInfo = useMemo(() => {
    const info: Record<string, { allNodes: typeof nodes; usedNodeIds: Set<number> }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      info[key] = { allNodes: nodes, usedNodeIds };
    }

    return info;
  }, [groups, nodes]);

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crowdb-console">
      <Header
        clusterHealth={clusterHealth}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        onShowTopology={() => {}}
        onShowCapacity={() => { if (centerPanel !== 'chunk') setCenterPanel('capacity'); }}
        onResetCluster={topologyReadonly ? undefined : handleResetCluster}
      />

      {dataError && (
        <div
          role="alert"
          className="tw-fixed tw-top-16 tw-left-1/2 -tw-translate-x-1/2 tw-z-50 tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-px-4 tw-py-2 tw-rounded-md tw-text-sm tw-shadow-lg"
        >
          {domain === Domain.Capacity ? `${dataError.message} — retrying` : 'Backend unreachable — retrying'}
        </div>
      )}

      <div hidden={ownsSidebar}><Sidebar
        allServers={allServers}
        racks={racks}
        servers={servers}
        stores={stores}
        nodeStores={nodeStores}
        nodeHealthById={nodeHealthById}
        loading={loading}
        readonly={domain === Domain.KV ? logicalReadonly : topologyReadonly}
        width={sidebarWidth}
        clusterInitialized={clusterInitialized}
        onNodeClick={onTreeNodeClick}
        onNodeContextMenu={onTreeContextMenu}
        onAdd={handleAdd}
        diskdbInstances={diskdbInstances}
        capacityUsage={capacityUsage}
        hardwareCapacity={hardwareCapacity}
        nodeDiskGroups={nodeDiskGroups}
        onLoadNodeDisks={managed ? undefined : loadNodeDisks}
        onLoadGroupDisks={managed ? undefined : loadGroupDisks}
        diskdbNodeIds={diskdbNodeIds}
        diskdbHealthById={diskdbHealthById}
        diskdbInstanceIdByNodeId={diskdbInstanceIdByNodeId}
      /></div>

      {!ownsSidebar && <PanelDivider fixed side="left" width={sidebarWidth} onResize={setSidebarWidth} />}

      <main
        className="tw-mt-14 tw-h-[calc(100vh-3.5rem)] tw-flex tw-flex-col tw-min-h-0"
        style={{
          marginLeft: ownsSidebar ? 0 : sidebarWidth,
          marginRight: selectedEntity && !ownsSidebar ? inspectorWidth : 0,
        }}
      >
        {managed && <ManagementSession />}
        {(
          <div hidden={domain !== Domain.Cluster} style={{ display: domain === Domain.Cluster ? 'flex' : 'none' }} className="tw-flex-1 tw-min-h-0 tw-flex-col"><div className="tw-px-4 tw-py-2 tw-text-xs tw-bg-panel tw-border-b tw-border-border">Physical topology · Rack → Node → Service · {managed ? 'Container: topology is read-only' : 'Deploy and manage services here'}</div>
          {managed && <MonitorSummary apiPrefix={apiPrefix} />}
          {!clusterInitialized && !loading && !logError && <p className="tw-px-4 tw-py-2 tw-text-xs tw-text-muted" data-testid="bootstrap-state">Bootstrap: add racks and nodes, deploy KV servers, then initialize Group 0 in KV. Changes are saved in the default workspace.</p>}
          <div className="tw-flex-1 tw-min-h-0"><ClusterView
            active={domain === Domain.Cluster}
            scope={Domain.Cluster}
            allServers={allServers}
            racks={racks}
            nodes={nodes}
            servers={servers}
            stores={stores}
            nodeStores={nodeStores}
            nodeHealthById={nodeHealthById}
            diskdbNodeIds={diskdbNodeIds}
            diskdbInstances={diskdbInstances}
            diskdbInstanceIdByNodeId={diskdbInstanceIdByNodeId}
            nodeDiskGroups={nodeDiskGroups}
            refreshToken={lastRefreshTime.getTime()}
            focusRequest={canvasFocusRequest}
            onEntityContextMenu={onCanvasContextMenu}
          /></div></div>
        )}
        {kvEnabled && (
          <div hidden={domain !== Domain.KV} className="tw-flex-1 tw-min-h-0"><KvView active={domain === Domain.KV} stores={stores} selectedEntity={selectionForDomain(Domain.KV)} readonly={logicalReadonly} backendError={!!logError} loading={logLoading} /></div>
        )}
        <div hidden={domain !== Domain.Capacity} className="tw-flex-1 tw-min-h-0"><CapacityView
            active={domain === Domain.Capacity}
            instances={diskdbInstances} usage={capacityUsage} hardwareCapacity={hardwareCapacity}
            scanStatus={capacityScanStatus} loading={capLoading} readonly={topologyReadonly}
            onRefresh={refreshCapacity} selectedEntity={selectionForDomain(Domain.Capacity)}
          /></div>
        <div data-testid="chunk-page" hidden={domain !== Domain.Chunk} className="tw-flex-1 tw-min-h-0"><ChunkBrowser stores={stores} racks={racks} nodes={nodes} servers={allServers} active={domain === Domain.Chunk} openRequest={chunkRequest}
          onPlacement={entity => { pendingSelectionRef.current = entity; setDomain(entity.domain); }}
        /></div>
        <div hidden={domain !== Domain.ChunkKV} className="tw-flex-1 tw-min-h-0"><ChunkKvView
          active={domain === Domain.ChunkKV} racks={racks} nodes={nodes} servers={allServers}
          onChunk={id => { setChunkRequest(previous => ({ id, nonce: (previous?.nonce ?? 0) + 1 })); setDomain(Domain.Chunk); }}
        /></div>
        <div hidden={domain !== Domain.Iceberg} className="tw-flex-1 tw-min-h-0"><IcebergView active={domain === Domain.Iceberg} readonly={readonly} /></div>
        <div hidden={domain !== Domain.S3} className="tw-flex-1 tw-min-h-0"><S3View active={domain === Domain.S3} readonly={readonly}
          onChunk={id => { setChunkRequest(previous => ({ id, nonce: (previous?.nonce ?? 0) + 1 })); setDomain(Domain.Chunk); }} /></div>
      </main>

      {!ownsSidebar && <Suspense fallback={null}>
        <Inspector readonly={domain === Domain.KV ? logicalReadonly : topologyReadonly} allServers={allServers} modules={modules} nodes={nodes} racks={racks} servers={servers} stores={stores} capacityUsage={capacityUsage} hardwareCapacity={hardwareCapacity} diskdbInstances={diskdbInstances} width={inspectorWidth} pendingSelectionRef={pendingSelectionRef} />
      </Suspense>}

      {selectedEntity && !ownsSidebar && <PanelDivider fixed side="right" width={inspectorWidth} onResize={setInspectorWidth} />}

      {menuState && <ContextMenu items={menuState.items} position={menuState.position} onClose={closeMenu} />}

      {/* Dialogs */}
      <AddRackDialog
        isOpen={!!dialog.addRack}
        onClose={closeDialogs}
        existingRackIds={rackIds.map(String)}
        onSuccess={handleRefresh}
      />
      {dialog.addNode && (
        <AddNodeDialog
        onDefaultServices={servicePlans.start}
          servicePlans={servicePlans.plans}
          isOpen
          onClose={closeDialogs}
          racks={racks}
          defaultRackId={String(defaultAddNodeRackId)}
          existingNodeIds={nodeIds.map(String)}
          defaultRestPort={addNodeDeployDefaults.defaultRestPort}
          defaultRpcPort={addNodeDeployDefaults.defaultRpcPort}
          defaultDiskdbRpcPort={addNodeDeployDefaults.defaultDiskdbRpcPort}
          onCreatedRackId={(rackId) => setLastUsedRackId(Number(rackId))}
          onDiskdbPortReserved={(port) => setRememberedDeployPorts((prev) => ({
            ...prev,
            diskdbRpc: prev.diskdbRpc.includes(port) ? prev.diskdbRpc : [...prev.diskdbRpc, port],
          }))}
          onSuccess={handleRefresh}
        />
      )}
      <InitClusterDialog
        isOpen={!!dialog.initCluster}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleInitSuccess}
      />
      <AddStoreDialog
        isOpen={!!dialog.addStore}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultStoreId={storeDialogDefaults.storeId}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleRefresh}
      />
      {dialog.addGroup && (
        <AddGroupDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addGroup.storeId}
          stores={stores}
          nodes={nodes}
          servers={servers}
          defaultGroupId={groupDialogDefaults[dialog.addGroup.storeId]?.groupId || '1'}
          defaultReplicaId={groupDialogDefaults[dialog.addGroup.storeId]?.replicaId || '1'}
          defaultNodeIds={groupDialogDefaults[dialog.addGroup.storeId]?.nodeIds || []}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addReplica && (
        <AddReplicaDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addReplica.storeId}
          groupId={dialog.addReplica.groupId}
          nodes={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.allNodes || []}
          usedNodeIds={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.usedNodeIds || new Set()}
          defaultNodeId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.nodeId ?? 0}
          defaultReplicaId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.replicaId || ''}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.deployServer && (
        <DeployServerDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.deployServer.nodeId}
          defaultRestPort={deployDialogDefaults.defaultRestPort}
          defaultRpcPort={deployDialogDefaults.defaultRpcPort}
          onSuccess={async ({ restPort, rpcPort }) => {
            setRememberedDeployPorts((prev) => ({
              mgmt: prev.mgmt.includes(restPort) ? prev.mgmt : [...prev.mgmt, restPort],
              rpc: prev.rpc.includes(rpcPort) ? prev.rpc : [...prev.rpc, rpcPort],
              diskdbRpc: prev.diskdbRpc,
            }));
            await handleRefresh();
          }}
        />
      )}
      {dialog.delete && (
        <ConfirmDeleteDialog
          isOpen
          onClose={closeDialogs}
          resourceType={dialog.delete.type}
          resourceId={String(dialog.delete.id)}
          onDelete={dialog.delete.onDelete}
          cascadeWarning={dialog.delete.cascadeWarning}
        />
      )}
      {dialog.addDiskGroup && (
        <AddDiskGroupDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDiskGroup.nodeId}
          existingDgIds={existingDiskGroupIds}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addDisk && (
        <AddDiskDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDisk.nodeId}
          dgId={dialog.addDisk.dgId}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.assignDiskGroup && (
        <AssignDiskGroupDialog
          isOpen
          onClose={closeDialogs}
          rackId={dialog.assignDiskGroup.rackId}
          nodeId={dialog.assignDiskGroup.nodeId}
          dgId={dialog.assignDiskGroup.dgId}
          dgName={dialog.assignDiskGroup.dgName}
          instances={diskdbInstances}
          stores={stores}
          onSuccess={handleRefresh}
        />
      )}
      <DeployDiskdbDialog
        isOpen={!!dialog.deployDiskdb}
        onClose={closeDialogs}
        nodes={nodes}
        defaultNodeId={dialog.deployDiskdb?.nodeId}
        defaultRpcPort={deployDiskdbDefaults}
        onSuccess={async () => {
          setRememberedDeployPorts((prev) => ({
            mgmt: prev.mgmt,
            rpc: prev.rpc,
            diskdbRpc: prev.diskdbRpc.includes(Number(deployDiskdbDefaults))
              ? prev.diskdbRpc
              : [...prev.diskdbRpc, Number(deployDiskdbDefaults)],
          }));
          await handleRefresh();
        }}
      />
      {dialog.defaultServices && <NodeServicesDialog nodeId={dialog.defaultServices.nodeId} plan={servicePlans.plans[dialog.defaultServices.nodeId]} onClose={closeDialogs} onStart={servicePlans.start} />}
      {dialog.deployAuxiliary && <DeployServiceDialog key={`${dialog.deployAuxiliary.kind}/${dialog.deployAuxiliary.nodeId}`} {...dialog.deployAuxiliary}
        servers={allServers} stores={stores} diskGroups={nodeDiskGroups[dialog.deployAuxiliary.nodeId]?.diskGroups ?? []}
        onClose={closeDialogs} onSuccess={handleRefresh} />}

      {dialog.compactZones && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Compact Zones"
          description={`Compact zones on disk ${dialog.compactZones.diskId.slice(0, 12)}…`}
          confirmLabel="Compact"
          diskId={dialog.compactZones.diskId}
          zoneCount={dialog.compactZones.zoneCount}
          onConfirm={async (diskId, zones) => {
            await compactDiskdbZones(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}
      {dialog.rebuildBitmap && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Rebuild Bitmap"
          description={`Rebuild zone bitmap on disk ${dialog.rebuildBitmap.diskId.slice(0, 12)}…`}
          confirmLabel="Rebuild"
          diskId={dialog.rebuildBitmap.diskId}
          zoneCount={dialog.rebuildBitmap.zoneCount}
          onConfirm={async (diskId, zones) => {
            await rebuildDiskdbZoneBitmap(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}

      <ToastContainer />
    </div>
  );
}

export default function App(props: CrowdbConsoleProps = {}) {
  const apiPrefix = props.apiPrefix ?? '/api';
  const [mode, setMode] = useState<'loading' | 'legacy' | 'docker' | 'bare-metal-pending' | 'unavailable'>('loading');
  useEffect(() => {
    let active = true;
    fetch(`${apiPrefix}/mode`)
      .then(async (response) => {
        if (!response.ok) throw new Error('Console mode unavailable');
        return response.json();
      })
      .then((body) => {
        if (active) setMode(body?.mode === 'docker' || body?.mode === 'bare-metal-pending' || body?.mode === 'legacy' ? body.mode : 'unavailable');
      })
      .catch(() => {
        if (active) setMode('unavailable');
      });
    return () => { active = false; };
  }, [apiPrefix]);
  if (mode === 'loading') return <div className="tw-p-6 tw-text-muted">Loading console…</div>;
  if (mode === 'unavailable') return <div role="alert" className="tw-p-6 tw-text-muted">Console mode unavailable.</div>;
  if (mode === 'bare-metal-pending') return <div role="alert" className="tw-p-6 tw-text-muted">Bare-metal deployment management is not available yet.</div>;

  return (
    <DomainProvider initialDomain={props.initialDomain}>
      <SelectionProvider>
        <ToastProvider>
          <ActivityProvider>
            <AppContent {...props} managed={mode === 'docker'} />
          </ActivityProvider>
        </ToastProvider>
      </SelectionProvider>
    </DomainProvider>
  );
}
