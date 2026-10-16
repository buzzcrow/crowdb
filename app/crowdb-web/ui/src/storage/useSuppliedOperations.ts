// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useCallback, useEffect, useRef, useState } from 'react';
import { listNodeCandidateOperations, nodeCandidateOperation } from '../api';
import { pendingSupplied, type SuppliedOperation } from './types';

/** A bounded page plus accepted operations; the target monitor owns execution. */
export function useSuppliedOperations(node: number, enabled: boolean) {
  const [operations, setOperations] = useState<Record<string, SuppliedOperation>>({});
  const [next, setNext] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const current = useRef(operations); current.current = operations;
  const currentNode = useRef(node); currentNode.current = node;
  const controller = useRef(new AbortController());
  const pending = useRef<Promise<void>>();
  const cursor = useRef<string>();
  const overflow = useRef(false);
  const accept = useCallback((operation: SuppliedOperation) => {
    if (controller.current.signal.aborted || currentNode.current !== node || operation.node_id !== node) return;
    const rows = Object.values(current.current).filter(row => row.operation_id !== operation.operation_id);
    if (rows.length >= 64) {
      const retired = rows.findIndex(row => !pendingSupplied(row));
      if (retired < 0) { overflow.current = true; setError('64 active observations retained; remaining operations are still available on the node. Reopen after completion to load more.'); return; }
      rows.splice(retired, 1);
    }
    const updated = Object.fromEntries([...rows, operation].map(row => [row.operation_id, row]));
    current.current = updated; setOperations(updated);
  }, [node]);
  const refresh = useCallback((after?: string) => {
    if (!enabled) return Promise.resolve();
    if (pending.current) return pending.current;
    if (after) cursor.current = after;
    const signal = controller.current.signal;
    setLoading(true);
    const run = async () => {
      const errors: string[] = [];
      overflow.current = false;
      try {
        const page = await listNodeCandidateOperations(node, cursor.current, { signal });
        if (signal.aborted) return;
        page.items.forEach(accept); setNext(page.next);
      } catch (cause) { errors.push(String(cause)); }
      try {
        const active = Object.values(current.current).filter(pendingSupplied);
        for (let index = 0; index < active.length && !signal.aborted; index += 4) {
          const results = await Promise.allSettled(active.slice(index, index + 4).map(async row => {
            const found = await nodeCandidateOperation(node, row.operation_id, { signal });
            if (!found) throw new Error(`Accepted candidate operation ${row.operation_id} is missing`);
            if (found.node_id !== node || found.operation_id !== row.operation_id || found.disk_group_id !== row.disk_group_id
              || found.medium.path !== row.medium.path || found.medium.backing_identity !== row.medium.backing_identity
              || found.medium.resource_identity !== row.medium.resource_identity) throw new Error(`Accepted candidate operation ${row.operation_id} identity changed`);
            if (!signal.aborted) accept(found);
          }));
          for (const result of results) if (result.status === 'rejected') errors.push(String(result.reason));
        }
      } finally { if (!signal.aborted) { if (overflow.current) errors.push('64 active observations retained; remaining operations stay on the node.'); setError(errors.join('; ').slice(0, 2048)); setLoading(false); } }
    };
    const work = run().finally(() => { if (pending.current === work) pending.current = undefined; });
    pending.current = work; return work;
  }, [node, enabled, accept]);
  useEffect(() => {
    controller.current = new AbortController(); cursor.current = undefined; pending.current = undefined;
    current.current = {}; setOperations({}); setNext(null); setError('');
    void refresh();
    return () => controller.current.abort();
  }, [refresh]);
  const waiting = Object.values(operations).some(pendingSupplied);
  useEffect(() => {
    if (!enabled || !waiting) return;
    let stopped = false; let timer: ReturnType<typeof setTimeout>;
    const poll = async () => { await refresh(); if (!stopped) timer = setTimeout(poll, 2000); };
    timer = setTimeout(poll, 2000);
    return () => { stopped = true; clearTimeout(timer); };
  }, [enabled, waiting, refresh]);
  return { operations, next, error, loading, refresh, accept };
}
