// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from "react";
import {
  listDiskdbInstances,
  getDiskdbUsage,
  getDiskdbScanStatus,
  getHardwareCapacity,
  listNodeDiskGroups,
  listDisksInGroup,
} from "../api";
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  DiskGroupEntry,
  DiskEntry,
  HardwareCapacitySummary,
} from "../types";

interface UseCapacityTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
}

export interface NodeDiskGroups {
  diskGroups: DiskGroupEntry[];
  disksByDg: Record<number, DiskEntry[]>;
}

interface UseCapacityTreeResult {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  fetchNodeDiskGroups: (nodeIds: number[]) => Promise<void>;
}

export function useCapacityTree({
  pollIntervalActive = 3000,
  enabled = true,
}: UseCapacityTreeOptions = {}): UseCapacityTreeResult {
  const [instances, setInstances] = useState<DiskdbInstanceInfo[]>([]);
  const [usage, setUsage] = useState<CapacityUsageResponse | null>(null);
  const [hardwareCapacity, setHardwareCapacity] =
    useState<HardwareCapacitySummary | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatusResponse | null>(null);
  const [nodeDiskGroups, setNodeDiskGroups] = useState<
    Record<number, NodeDiskGroups>
  >({});
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<Error | null>(null);
  const requestRef = useRef<AbortController | null>(null);
  const hasLoadedRef = useRef(false);

  const fetchData = useCallback(async () => {
    if (!enabled || document.visibilityState === 'hidden') return;
    requestRef.current?.abort();
    const controller = new AbortController();
    requestRef.current = controller;
    const options = { signal: controller.signal };
    if (!hasLoadedRef.current) setLoading(true);
    const [instancesResult, usageResult, hwCapResult, scanResult] = await Promise.allSettled([
      listDiskdbInstances(options),
      getDiskdbUsage(undefined, undefined, undefined, options),
      getHardwareCapacity(options),
      getDiskdbScanStatus(undefined, options),
    ]);
    if (controller.signal.aborted) return;
    setInstances(instancesResult.status === 'fulfilled' ? instancesResult.value : []);
    setUsage(usageResult.status === 'fulfilled' ? usageResult.value : null);
    setHardwareCapacity(hwCapResult.status === 'fulfilled' ? hwCapResult.value : null);
    setScanStatus(scanResult.status === 'fulfilled' ? scanResult.value : null);
    const missing = [
      instancesResult.status === 'rejected' && 'DiskDB instances',
      usageResult.status === 'rejected' && 'usage',
      hwCapResult.status === 'rejected' && 'hardware inventory',
      scanResult.status === 'rejected' && 'scan status',
    ].filter(Boolean);
    setError(missing.length ? new Error(`Capacity observation unavailable: ${missing.join(', ')}`) : null);
    hasLoadedRef.current = true;
    setLoading(false);
  }, [enabled]);

  const fetchNodeDiskGroups = useCallback(
    async (nodeIds: number[]) => {
      if (!enabled || nodeIds.length === 0) {
        setNodeDiskGroups({});
        return;
      }
      try {
        // Fetch disk-groups for all nodes first and render them immediately.
        // Disks are loaded afterwards so a slow group-0 query for one DG
        // doesn't block rendering the rest of the tree.
        const dgLists = await Promise.all(
          nodeIds.map(async (nodeId) => {
            try {
              const dgs = await listNodeDiskGroups(nodeId);
              return [nodeId, dgs] as const;
            } catch {
              return [nodeId, [] as DiskGroupEntry[]] as const;
            }
          }),
        );
        setNodeDiskGroups((prev) => {
          const map: Record<number, NodeDiskGroups> = { ...prev };
          for (const [id, dgs] of dgLists) {
            const existing = map[id];
            map[id] = {
              diskGroups: dgs,
              disksByDg: existing?.disksByDg ?? {},
            };
          }
          return map;
        });

        // Load disks for each DG in the background. Each completed fetch
        // merges into the existing node entry without waiting for all DGs.
        await Promise.all(
          dgLists.flatMap(([nodeId, dgs]) =>
            dgs.map(async (dg) => {
              try {
                const disks = await listDisksInGroup(nodeId, dg.id);
                setNodeDiskGroups((prev) => {
                  const node = prev[nodeId] || {
                    diskGroups: dgs,
                    disksByDg: {},
                  };
                  if (node.diskGroups.length === 0 && dgs.length > 0) {
                    node.diskGroups = dgs;
                  }
                  node.disksByDg = { ...node.disksByDg, [dg.id]: disks };
                  return { ...prev, [nodeId]: node };
                });
              } catch {
                // leave disks undefined; the UI will retry on next poll
              }
            }),
          ),
        );
      } catch (err) {
        console.error("Failed to fetch node disk-groups:", err);
      }
    },
    [enabled],
  );

  useEffect(() => {
    let stopped = false;
    let generation = 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async (version = generation) => {
      await fetchData();
      if (!stopped && version === generation && enabled && document.visibilityState !== 'hidden') timer = setTimeout(() => void poll(version), pollIntervalActive);
    };
    const visibility = () => {
      generation++;
      clearTimeout(timer);
      requestRef.current?.abort();
      if (document.visibilityState !== 'hidden' && enabled) void poll();
    };
    if (enabled) void poll();
    else setLoading(false);
    document.addEventListener('visibilitychange', visibility);
    return () => {
      stopped = true;
      clearTimeout(timer);
      requestRef.current?.abort();
      document.removeEventListener('visibilitychange', visibility);
    };
  }, [enabled, pollIntervalActive, fetchData]);

  return {
    instances,
    usage,
    hardwareCapacity,
    scanStatus,
    loading,
    error,
    refresh: fetchData,
    nodeDiskGroups,
    fetchNodeDiskGroups,
  };
}
