// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useCallback, useEffect, useRef, useState } from 'react';
import { listNodeDiskFiles, nodeDiskFile, nodeDiskInventory } from '../api';
import { pendingFilePreparation, type DiskInventory, type FilePreparation } from './types';

interface Observation {
  nodeId: number;
  inventory?: DiskInventory;
  operations: Record<string, FilePreparation>;
  next: string | null;
  errors: Record<string, string>;
  observedAt?: number;
}
const empty = (nodeId: number): Observation => ({ nodeId, operations: {}, errors: {}, next: null });

/** Observations never own execution. Leaving the Node only stops browser reads. */
export function useNodeStorage(nodeId: number, enabled: boolean) {
  const [state, setState] = useState(() => empty(nodeId));
  const [loading, setLoading] = useState(false);
  const controller = useRef(new AbortController());
  const pending = useRef<Promise<void>>();
  const cursor = useRef<string>();
  const operations = useRef<Record<string, FilePreparation>>({});
  const currentNode = useRef(nodeId);
  currentNode.current = nodeId;
  const current = state.nodeId === nodeId ? state : empty(nodeId);
  operations.current = current.operations;
  const refresh = useCallback((after?: string) => {
    if (!enabled) return Promise.resolve();
    if (pending.current) return pending.current;
    if (after) cursor.current = after;
    const signal = controller.current.signal;
    setLoading(true);
    const apply = (change: (state: Observation) => Observation) => {
      if (!signal.aborted) setState(previous => change(previous.nodeId === nodeId ? previous : empty(nodeId)));
    };
    const observe = async () => {
      await Promise.allSettled([
        nodeDiskInventory(nodeId, { signal }).then(inventory => apply(previous => ({
          ...previous, inventory, errors: { ...previous.errors, inventory: '' },
        }))).catch(error => apply(previous => ({ ...previous, errors: { ...previous.errors, inventory: String(error) } }))),
        listNodeDiskFiles(nodeId, cursor.current, { signal }).then(page => apply(previous => ({
          ...previous, observedAt: Date.now(), next: page.next, errors: { ...previous.errors, operations: '' },
          operations: { ...previous.operations, ...Object.fromEntries(page.items.map(item => [item.request.operation_id, item])) },
        }))).catch(error => apply(previous => ({ ...previous, errors: { ...previous.errors, operations: String(error) } }))),
      ]);
      const ids = Object.values(operations.current).filter(pendingFilePreparation).map(operation => operation.request.operation_id);
      for (let offset = 0; offset < ids.length && !signal.aborted; offset += 4) {
        await Promise.allSettled(ids.slice(offset, offset + 4).map(id => nodeDiskFile(nodeId, id, { signal })
          .then(operation => apply(previous => ({ ...previous, operations: { ...previous.operations, [id]: operation }, errors: { ...previous.errors, [id]: '' } })))
          .catch(error => apply(previous => ({ ...previous, errors: { ...previous.errors, [id]: String(error) } })))));
      }
    };
    const work = observe().finally(() => {
      if (!signal.aborted) setLoading(false);
      if (pending.current === work) pending.current = undefined;
    });
    pending.current = work;
    return work;
  }, [nodeId, enabled]);
  useEffect(() => {
    controller.current = new AbortController();
    pending.current = undefined;
    cursor.current = undefined;
    setLoading(false);
    void refresh();
    return () => { controller.current.abort(); };
  }, [refresh]);
  const waiting = Object.values(current.operations).some(pendingFilePreparation);
  useEffect(() => {
    if (!enabled || !waiting) return;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      await refresh();
      if (!stopped) timer = setTimeout(poll, 1000);
    };
    timer = setTimeout(poll, 1000);
    return () => { stopped = true; clearTimeout(timer); };
  }, [enabled, waiting, refresh]);
  const accept = useCallback((operation: FilePreparation) => {
    if (currentNode.current !== nodeId || operation.request.node_id !== nodeId) return;
    setState(previous => ({ ...(previous.nodeId === nodeId ? previous : empty(nodeId)),
      operations: { ...(previous.nodeId === nodeId ? previous.operations : {}), [operation.request.operation_id]: operation } }));
  }, [nodeId]);
  return { ...current, loading, refresh, accept };
}
