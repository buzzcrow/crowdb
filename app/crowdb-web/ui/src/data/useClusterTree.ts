// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { listRacks, listNodes, type ServerSummary } from '../api';
import { NodeHealth } from '../types';
import type { Rack, Node, NodeStore } from '../types';
import { physicalSnapshot } from '../managed/physicalSnapshot';

import { useDiskInventory, type NodeDiskGroups } from './useDiskInventory';
export type { NodeDiskGroups } from './useDiskInventory';

interface UseClusterTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
  recursive?: number;
  managed?: boolean;
}

interface UseClusterTreeResult {
  services: ServerSummary[];
  racks: Rack[];
  nodes: Node[];
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
  loadNodeDisks: (nodeId: number) => Promise<void>;
  loadGroupDisks: (nodeId: number, groupId: number) => Promise<void>;
  getNodeById: (nodeId: number) => Node | undefined;
}

/**
 * Hook for polling the cluster infrastructure tree:
 * racks -> nodes -> { KV stores/groups, disk-groups/disks }.
 *
 * Merges the former `usePhysicalTree` (rack/node/KV) with the
 * disk-group/disk fetch logic from `useCapacityTree`.
 */
export function useClusterTree({
  pollIntervalActive = 3000,
  pollIntervalInactive = 30000,
  enabled = true,
  recursive = 3,
  managed = false,
}: UseClusterTreeOptions = {}): UseClusterTreeResult {
  const [racks, setRacks] = useState<Rack[]>([]);
  const [services, setServices] = useState<ServerSummary[]>([]);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [nodeStores, setNodeStores] = useState<Record<string, NodeStore[]>>({});
  const [nodeHealthById, setNodeHealthById] = useState<Record<string, NodeHealth>>({});
  const disks = useDiskInventory(enabled);
  const [managedDiskGroups, setManagedDiskGroups] = useState<Record<number, NodeDiskGroups>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const hasLoadedRef = useRef(false);
  const requestRef = useRef<AbortController | null>(null);

  const fetchData = useCallback(async () => {
    if (!enabled || document.visibilityState === 'hidden') return;
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    const options = { signal: controller.signal };

    try {
      if (!hasLoadedRef.current) {
        setLoading(true);
      }

      if (managed) {
        const snapshot = await physicalSnapshot();
        if (controller.signal.aborted) return;
        setRacks(snapshot.racks); setNodes(snapshot.nodes); setManagedDiskGroups(snapshot.diskGroups); setServices(snapshot.servers);
        setNodeStores({}); setNodeHealthById({}); setError(null);
        return;
      }
      const racksData = await listRacks(recursive, options);
      if (controller.signal.aborted) return;
      setRacks(Array.isArray(racksData) ? racksData : []);

      const nodesData = await listNodes(undefined, recursive, options);
      if (controller.signal.aborted) return;
      const nodeList = Array.isArray(nodesData) ? nodesData : [];
      setNodes(nodeList);

      // Node status comes from the authoritative service projection. The physical
      // view has no use for a second per-node KV catalog or reachability fanout.
      setNodeHealthById(Object.fromEntries(nodeList.map(node => [node.id, node.kv_server?.health ?? NodeHealth.Unknown])));
      setNodeStores({});

      setError(null);
    } catch (err) {
      if (controller.signal.aborted) return;
      console.error('Failed to fetch cluster tree:', err);
      if (managed) { setRacks([]); setNodes([]); setManagedDiskGroups({}); setServices([]); }
      setError(err instanceof Error ? err : new Error('Unknown error fetching cluster tree'));
    } finally {
      if (!controller.signal.aborted) { hasLoadedRef.current = true; setLoading(false); }
    }
  }, [enabled, recursive, managed]);

  useEffect(() => {
    void fetchData();
    return () => { requestRef.current?.abort(); };
  }, [fetchData]);

  useEffect(() => {
    if (!enabled) return;

    const scheduleNextPoll = () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
      const interval = isActiveRef.current ? pollIntervalActive : pollIntervalInactive;
      pollTimeoutRef.current = setTimeout(async () => {
        await fetchData();
        scheduleNextPoll();
      }, interval);
    };

    scheduleNextPoll();

    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, [enabled, pollIntervalActive, pollIntervalInactive, fetchData]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      isActiveRef.current = document.visibilityState === 'visible';
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, []);

  const getNodeById = useCallback(
    (nodeId: number): Node | undefined => {
      return nodes.find((n) => n.id === nodeId);
    },
    [nodes],
  );

  const refresh = useCallback(async () => { await Promise.all([fetchData(), disks.refresh()]); }, [fetchData, disks.refresh]);

  return {
    services,
    racks,
    nodes,
    nodeStores,
    nodeHealthById,
    nodeDiskGroups: managed ? managedDiskGroups : disks.inventory,
    loading,
    error: error ?? disks.error,
    refresh,
    loadNodeDisks: disks.loadNode,
    loadGroupDisks: disks.loadGroup,
    getNodeById,
  };
}
