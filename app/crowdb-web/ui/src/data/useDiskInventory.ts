// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useCallback, useEffect, useRef, useState } from 'react';
import { listNodeDiskGroups, listDisksInGroup } from '../api';
import type { DiskGroupEntry, DiskEntry } from '../types';

export interface NodeDiskGroups {
  diskGroups: DiskGroupEntry[];
  disksByDg: Record<number, DiskEntry[]>;
}

/** Inventory is requested by opened branches, not by the number of cluster nodes. */
export function useDiskInventory(enabled: boolean) {
  const [inventory, setInventory] = useState<Record<number, NodeDiskGroups>>({});
  const [error, setError] = useState<Error | null>(null);
  const errors = useRef(new Map<string, Error>());
  const openedNodes = useRef(new Set<number>());
  const openedGroups = useRef(new Map<string, [number, number]>());
  const controller = useRef(new AbortController());
  const pending = useRef(new Map<string, Promise<void>>());
  const queue = useRef<Array<() => void>>([]);
  const running = useRef(0);
  const pump = useCallback(() => {
    while (running.current < 4 && queue.current.length) queue.current.shift()!();
  }, []);
  const schedule = useCallback((identity: string, operation: (signal: AbortSignal) => Promise<void>) => {
    const existing = pending.current.get(identity);
    if (existing) return existing;
    const signal = controller.current.signal;
    let finish!: () => void;
    const result = new Promise<void>(resolve => { finish = resolve; });
    pending.current.set(identity, result);
    queue.current.push(() => {
      running.current++;
      void (async () => {
        try { if (!signal.aborted) { await operation(signal); if (!signal.aborted) { errors.current.delete(identity); setError(errors.current.size ? new Error([...errors.current.values()].map(value => value.message).join('; ')) : null); } } }
        catch (cause) { if (!signal.aborted) { errors.current.set(identity, cause instanceof Error ? cause : new Error(String(cause))); setError(new Error([...errors.current.values()].map(value => value.message).join('; '))); } }
        finally { running.current--; pending.current.delete(identity); finish(); queueMicrotask(pump); }
      })();
    });
    pump();
    return result;
  }, [pump]);
  const loadNode = useCallback((nodeId: number) => {
    openedNodes.current.add(nodeId);
    return schedule(`node:${nodeId}`, async signal => {
      const diskGroups = await listNodeDiskGroups(nodeId, { signal });
      if (signal.aborted) return;
      setInventory(previous => ({ ...previous, [nodeId]: { diskGroups, disksByDg: previous[nodeId]?.disksByDg ?? {} } }));
    });
  }, [schedule]);
  const loadGroup = useCallback((nodeId: number, groupId: number) => {
    openedGroups.current.set(`${nodeId}:${groupId}`, [nodeId, groupId]);
    return schedule(`group:${nodeId}:${groupId}`, async signal => {
      const disks = await listDisksInGroup(nodeId, groupId, { signal });
      if (signal.aborted) return;
      setInventory(previous => {
        const node = previous[nodeId] ?? { diskGroups: [], disksByDg: {} };
        return { ...previous, [nodeId]: { ...node, disksByDg: { ...node.disksByDg, [groupId]: disks } } };
      });
    });
  }, [schedule]);
  const refresh = useCallback(async () => {
    await Promise.all([...openedNodes.current].map(loadNode));
    await Promise.all([...openedGroups.current.values()].map(([nodeId, groupId]) => loadGroup(nodeId, groupId)));
  }, [loadNode, loadGroup]);
  useEffect(() => {
    controller.current = new AbortController();
    return () => { controller.current.abort(); };
  }, [enabled]);
  return { inventory, error, loadNode, loadGroup, refresh };
}
