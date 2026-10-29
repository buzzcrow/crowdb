// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useCallback, useEffect, useRef, useState } from 'react';
import { listServers, type ServerSummary } from '../api';

export function useServiceObservations(enabled: boolean, reportError: (message: string) => void) {
  const [servers, setServers] = useState<ServerSummary[]>([]);
  const request = useRef<AbortController>();
  const errorShown = useRef(false);
  const observe = useCallback(async (fresh = false) => {
    if (!enabled || document.visibilityState === 'hidden') return;
    if (!fresh && request.current && !request.current.signal.aborted) return;
    request.current?.abort();
    const controller = new AbortController(); request.current = controller;
    try {
      const value = await listServers({ signal: controller.signal });
      if (controller.signal.aborted) return;
      setServers(value); errorShown.current = false;
    } catch (cause) {
      if (controller.signal.aborted) return;
      setServers([]);
      if (!errorShown.current) {
        errorShown.current = true;
        reportError(`Failed to load server list: ${cause instanceof Error ? cause.message : 'backend unreachable'}`);
      }
    } finally { if (request.current === controller) request.current = undefined; }
  }, [enabled, reportError]);
  useEffect(() => {
    if (!enabled) return;
    let stopped = false; let generation = 0;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async (version = generation) => {
      if (document.visibilityState === 'hidden') return;
      await observe();
      if (!stopped && version === generation) timer = setTimeout(() => void poll(version), 1000);
    };
    const visibility = () => {
      generation++; clearTimeout(timer); request.current?.abort(); void poll();
    };
    void poll();
    document.addEventListener('visibilitychange', visibility);
    return () => { stopped = true; clearTimeout(timer); request.current?.abort(); document.removeEventListener('visibilitychange', visibility); };
  }, [enabled, observe]);
  const refresh = useCallback(() => observe(true), [observe]);
  return { servers, refresh };
}
