// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState, useEffect } from 'react';

export function MonitorSummary({ apiPrefix }: { apiPrefix: string }) {
  const [snapshot, setSnapshot] = useState<any>(null);
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true; let timer: ReturnType<typeof setTimeout>; let controller: AbortController;
    const refresh = async () => {
      controller = new AbortController();
      try {
        const response = await fetch(`${apiPrefix}/preview`, { cache: 'no-store', signal: controller.signal });
        const body = await response.json();
        if (active) { setSnapshot(body); setError(response.ok ? '' : body.reason === 'monitor_unavailable' ? 'The monitor status is missing or stale.' : 'Group 0 is unavailable or its topology is incomplete.'); }
      } catch (error) { if (active) { setSnapshot(null); setError(String(error)); } }
      finally { if (active) timer = setTimeout(refresh, 3000); }
    };
    void refresh(); return () => { active = false; clearTimeout(timer); controller?.abort(); };
  }, [apiPrefix]);
  const monitor = snapshot?.monitor;
  return <section className="tw-px-4 tw-py-2 tw-text-xs tw-bg-panel tw-border-b tw-border-border" aria-label="Monitor status">
    {error && <p role="alert" data-testid="managed-unavailable">{error}</p>}
    {monitor && <><p data-testid="managed-monitor-phase">Monitor service health · phase: {monitor.phase} · revision {monitor.revision}</p><div className="tw-flex tw-gap-3 tw-flex-wrap">{Object.entries(monitor.services).map(([name, service]: [string, any]) => <span key={name} data-testid={`managed-process-${name}`}>{name} · PID {service.pid ?? '—'} · generation {service.generation} · restarts {service.restart_attempts} · {service.healthy ? 'healthy' : 'unhealthy'}</span>)}</div></>}
  </section>;
}
