// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useCallback, useEffect, useRef, useState } from 'react';
import { removedNodeDisks } from '../api';
import type { RemovedDisk } from './types';
interface Observation { node: number; items: RemovedDisk[]; next: string | null; error: string; observed: boolean }
const empty = (node: number): Observation => ({ node, items: [], next: null, error: '', observed: false });

export function useRemovedDisks(node: number, enabled: boolean) {
  const [state, setState] = useState(() => empty(node));
  const [loading, setLoading] = useState(false);
  const controller = useRef(new AbortController());
  const pending = useRef<Promise<void>>();
  const refresh = useCallback((after?: string, fresh = false): Promise<void> => {
    if (!enabled) return Promise.resolve();
    if (pending.current) return fresh ? pending.current.then(() => refresh(after)) : pending.current;
    const signal = controller.current.signal;
    setLoading(true);
    const work = removedNodeDisks(node, after, { signal }).then(page => {
      if (signal.aborted) return;
      setState(previous => {
        const retained = after && previous.node === node ? previous.items : [];
        const items = [...new Map([...retained, ...page.items].map(disk => [disk.disk_id, disk])).values()];
        return { node, items, next: page.next, error: '', observed: true };
      });
    }).catch(cause => {
      if (!signal.aborted) setState(previous => ({ ...(previous.node === node ? previous : empty(node)), error: String(cause) }));
    }).finally(() => {
      if (!signal.aborted) setLoading(false);
      if (pending.current === work) pending.current = undefined;
    });
    pending.current = work;
    return work;
  }, [node, enabled]);
  useEffect(() => {
    controller.current = new AbortController(); pending.current = undefined; setLoading(false);
    void refresh();
    return () => { controller.current.abort(); };
  }, [refresh]);
  return { ...(state.node === node ? state : empty(node)), loading, refresh };
}
