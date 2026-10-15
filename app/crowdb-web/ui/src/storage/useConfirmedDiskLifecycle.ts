// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useCallback, useEffect, useRef, useState } from 'react';
import { diskLifecycle } from '../api';
import type { DiskLifecycle } from './types';

interface Observation { group: number; state: DiskLifecycle; stale: boolean }
function revision(state: DiskLifecycle): bigint { return BigInt(state.disk_revision ?? state.removed_from_revision ?? '0'); }
function canReplace(state: DiskLifecycle, accepted: Observation, started: Observation): boolean {
  const difference = revision(state) - revision(accepted.state);
  return difference > 0n || (difference === 0n && accepted === started && (!accepted.state.removed || state.removed));
}

/** Reobserve locally confirmed actions through the shared inventory budget. */
export function useConfirmedDiskLifecycle(node: number, active: boolean, onChanged: (group: number) => void) {
  const [observations, setObservations] = useState<{ node: number; disks: Record<string, Observation> }>({ node, disks: {} });
  const current = useRef(observations); current.current = observations;
  const changed = useRef(onChanged); changed.current = onChanged;
  const disks = observations.node === node ? observations.disks : {};
  const targets = Object.entries(disks).map(([id, value]) => `${value.group}:${id}`).sort().join(',');
  const confirm = useCallback((group: number, state: DiskLifecycle) => {
    setObservations(previous => ({ node, disks: { ...(previous.node === node ? previous.disks : {}), [state.disk_id]: { group, state, stale: false } } }));
  }, [node]);
  useEffect(() => {
    if (!active || !targets) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const observe = async () => {
      const selected = current.current.node === node ? Object.entries(current.current.disks) : [];
      await Promise.all(selected.map(async ([id, previous]) => {
        try {
          const state = await diskLifecycle(node, previous.group, id, { signal: controller.signal });
          if (controller.signal.aborted) return;
          const accepted = current.current.node === node ? current.current.disks[id] : undefined;
          if (!accepted || !canReplace(state, accepted, previous)) return;
          const advanced = revision(state) > revision(accepted.state) || state.removed !== accepted.state.removed;
          setObservations(latest => {
            const latestAccepted = latest.node === node ? latest.disks[id] : undefined;
            if (!latestAccepted || !canReplace(state, latestAccepted, previous)) return latest;
            return { node, disks: { ...latest.disks, [id]: { group: previous.group, state, stale: false } } };
          });
          if (advanced) changed.current(previous.group);
        } catch {
          if (!controller.signal.aborted) setObservations(latest => latest.node !== node || !latest.disks[id] ? latest :
            { node, disks: { ...latest.disks, [id]: { ...latest.disks[id], stale: true } } });
        }
      }));
      if (!controller.signal.aborted) timer = setTimeout(() => { void observe(); }, 1000);
    };
    timer = setTimeout(() => { void observe(); }, 1000);
    return () => { controller.abort(); clearTimeout(timer); };
  }, [node, active, targets]);
  return { disks, confirm };
}
