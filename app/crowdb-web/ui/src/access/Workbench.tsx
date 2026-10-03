// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState, type ReactNode } from 'react';
import { configure, connections } from './native';
import { PanelDivider } from '../components/PanelDivider';
import { ActivityLog } from '../panels/ActivityLog';

export const inputClass = 'tw-rounded tw-border tw-border-border tw-bg-bg tw-px-2 tw-py-1.5 tw-text-sm tw-text-text';
export const buttonClass = 'tw-rounded tw-border tw-border-border tw-px-3 tw-py-1.5 tw-text-xs hover:tw-bg-accent/10 disabled:tw-opacity-40';
export function Workbench({ sidebar, children, detail, showActivity = true, resizableSidebar = true }: { resizableSidebar?: boolean; showActivity?: boolean; sidebar: ReactNode; children: ReactNode; detail?: ReactNode }) {
  const [width, setWidth] = useState(280);
  const [detailWidth, setDetailWidth] = useState(320);
  return <div className="tw-grid tw-h-full" style={{ gridTemplateColumns: `${resizableSidebar ? width : 280}px ${resizableSidebar ? '6px ' : ''}minmax(0,1fr)${detail ? ` 6px ${detailWidth}px` : ''}` }}>
    <aside className="tw-overflow-auto tw-border-r tw-border-border tw-bg-bg tw-p-4 tw-space-y-3">{sidebar}</aside>
    {resizableSidebar && <PanelDivider side="left" width={width} onResize={setWidth} />}
    <section className="tw-overflow-auto tw-p-5 tw-space-y-4">{children}{showActivity && <details className="tw-border-t tw-border-border tw-pt-3"><summary className="tw-text-xs tw-text-muted tw-cursor-pointer">Session activity</summary><p className="tw-text-xs tw-text-muted">Browser session history; reload clears this log.</p><ActivityLog /></details>}</section>
    {detail && <PanelDivider side="right" width={detailWidth} onResize={setDetailWidth} />}
    {detail && <aside className="tw-overflow-auto tw-border-l tw-border-border tw-bg-panel tw-p-4 tw-space-y-3">{detail}</aside>}
  </div>;
}
export function JsonView({ value }: { value: unknown }) {
  return <pre className="tw-whitespace-pre-wrap tw-break-all tw-rounded tw-bg-panel tw-border tw-border-border tw-p-3 tw-text-xs">{JSON.stringify(value, null, 2)}</pre>;
}
export function Connection({ protocol, active, onOrigin, readonly, busy = false }: { protocol: 's3' | 'iceberg'; active: boolean; onOrigin: (origin: string | null) => void; readonly: boolean; busy?: boolean }) {
  const [origin, setOrigin] = useState('');
  const [configurable, setConfigurable] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!active) return;
    let current = true;
    connections().then(result => { if (current) { setOrigin(result[protocol] ?? ''); setConfigurable(result.configurable); onOrigin(result[protocol]); setError(''); } })
      .catch(error => { if (current) setError(String(error)); });
    return () => { current = false; };
  }, [active, protocol, onOrigin]);
  return <div className="tw-space-y-2">
    <div className="tw-text-xs tw-text-muted">{protocol === 's3' ? 'S3 endpoint' : 'Catalog endpoint'}</div>
    {configurable && !readonly ? <form className="tw-space-y-2" onSubmit={event => { event.preventDefault(); void configure(protocol, origin).then(result => { onOrigin(result[protocol]); setError(''); }).catch(error => setError(String(error))); }}>
      <input className={`${inputClass} tw-w-full`} aria-label={`${protocol} endpoint`} disabled={busy} required type="url" value={origin} onChange={event => setOrigin(event.target.value)} placeholder="http://127.0.0.1:17000" />
      <button className={buttonClass} disabled={busy}>Save endpoint</button>
    </form> : <p className="tw-text-xs tw-break-all">{origin || 'Not configured'}</p>}
    {error && <p role="alert" className="tw-text-xs tw-text-failed">{error}</p>}
  </div>;
}
