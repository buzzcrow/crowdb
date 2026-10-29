// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { diskKey } from '../panels/capacity/observation';
import { getDiskdbUsage } from '../api';
import type { ZoneUsageDto } from '../types';

interface ZoneBitmapState {
  zone: ZoneUsageDto | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
}

/** Load only the selected zone, cancel stale observations and retain the last successful observation only for the same identity. */
export function useZoneBitmap(
  dgId: number | undefined,
  diskId: string | undefined,
  zoneIndex: number | null,
  active = true,
): ZoneBitmapState {
  const identity = active && dgId !== undefined && diskId !== undefined && zoneIndex !== null
    ? JSON.stringify([dgId, diskKey(diskId), zoneIndex]) : null;
  const [observed, setObserved] = useState<{ identity: string; zone: ZoneUsageDto } | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const reqIdRef = useRef(0);
  const requestRef = useRef<AbortController | null>(null);

  const fetchBitmap = useCallback(async () => {
    const myReq = ++reqIdRef.current;
    requestRef.current?.abort();
    setError(null); setLoading(false);
    if (!active || dgId === undefined || diskId === undefined || zoneIndex === null) {
      setObserved(null);
      setError(null);
      return;
    }
    const controller = new AbortController();
    requestRef.current = controller;
    setLoading(true);
    try {
      const resp = await getDiskdbUsage(dgId, diskId, zoneIndex, { signal: controller.signal });
      // Ignore stale responses from a previous selection.
      if (myReq !== reqIdRef.current) return;
      const dg = resp.disk_groups.find((g) => g.disk_group_id === dgId);
      const disk = dg?.disks.find((d) => diskKey(d.disk_id) === diskKey(diskId));
      const zu = disk?.zone_usages.find((z) => z.zone_index === zoneIndex);
      if (myReq !== reqIdRef.current) return;
      if (!zu) {
        setError(new Error(`zone ${zoneIndex} not found on disk ${diskId}`));

      } else {
        setObserved({ identity: identity!, zone: zu });
        setError(null);
      }
    } catch (err) {
      if (myReq !== reqIdRef.current) return;
      setError(err instanceof Error ? err : new Error('Unknown error fetching zone bitmap'));

    } finally {
      if (myReq === reqIdRef.current) setLoading(false);
    }
  }, [dgId, diskId, zoneIndex, active, identity]);

  useEffect(() => {
    void fetchBitmap();
    return () => { ++reqIdRef.current; requestRef.current?.abort(); };
  }, [fetchBitmap]);

  return { zone: observed?.identity === identity ? observed.zone : null, loading, error, refresh: fetchBitmap };
}
