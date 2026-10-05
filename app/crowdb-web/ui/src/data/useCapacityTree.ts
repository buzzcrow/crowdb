// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from "react";
import {
  listDiskdbInstances,
  getDiskdbUsage,
  getDiskdbScanStatus,
  getHardwareCapacity,
} from "../api";
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  HardwareCapacitySummary,
} from "../types";

interface UseCapacityTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
  observeRuntime?: boolean;
  diskGroupId?: number;
  diskId?: string;
}

export type { NodeDiskGroups } from './useClusterTree';

interface UseCapacityTreeResult {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
}

export function useCapacityTree({
  pollIntervalActive = 3000,
  enabled = true,
  observeRuntime = true,
  diskGroupId,
  diskId,
}: UseCapacityTreeOptions = {}): UseCapacityTreeResult {
  const [instances, setInstances] = useState<DiskdbInstanceInfo[]>([]);
  const [usage, setUsage] = useState<CapacityUsageResponse | null>(null);
  const [hardwareCapacity, setHardwareCapacity] =
    useState<HardwareCapacitySummary | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatusResponse | null>(null);
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
      observeRuntime ? getDiskdbUsage(diskGroupId, diskId, undefined, options) : Promise.resolve(null),
      getHardwareCapacity(options),
      observeRuntime ? getDiskdbScanStatus(diskGroupId, options) : Promise.resolve(null),
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
  }, [enabled, observeRuntime, diskGroupId, diskId]);

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
  };
}
