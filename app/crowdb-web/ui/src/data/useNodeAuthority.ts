// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { nodeDeploymentStatus, type NodeDeploymentStatus } from '../api';

export function useNodeAuthority(enabled: boolean) {
  const [status, setStatus] = useState<NodeDeploymentStatus>();
  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      try {
        const status = await nodeDeploymentStatus({ signal: controller.signal });
        if (!controller.signal.aborted) setStatus(status);
      } catch {
        if (!controller.signal.aborted) setStatus(previous => previous ? { ...previous, available: false, phase: 'authority_unavailable' } : undefined);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, 1000);
      }
    };
    void refresh();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [enabled]);
  return status;
}
