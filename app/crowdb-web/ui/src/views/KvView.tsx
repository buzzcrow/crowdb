// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense, useState } from 'react';
import { PaxosOverview } from '../kv/PaxosOverview';
import type { EnrichedStoreView } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';

const KvOperatorPanel = lazy(() => import('../panels/KvOperatorPanel').then((m) => ({ default: m.KvOperatorPanel })));

export interface KvViewProps {
  active: boolean;
  stores: EnrichedStoreView[];
  selectedEntity: SelectedEntity | null;
  readonly: boolean;
  backendError: boolean;
  loading: boolean;
}

export function KvView(props: KvViewProps) {
  const [view, setView] = useState<'overview' | 'data'>('overview');
  const [dataVisited, setDataVisited] = useState(false);
  return (
    <div className="tw-h-full tw-flex tw-flex-col">
      <nav className="tw-flex tw-gap-2 tw-p-3 tw-border-b tw-border-border" aria-label="KV views">
        {(['overview', 'data'] as const).map(value => <button key={value} data-testid={`kv-view-${value}`}
          aria-pressed={view === value} className={`tw-px-3 tw-py-1 tw-rounded tw-text-sm ${view === value ? 'tw-bg-accent tw-text-bg' : 'tw-text-muted'}`}
          onClick={() => { setView(value); if (value === 'data') setDataVisited(true); }}>
          {value === 'overview' ? 'Overview' : 'Data'}
        </button>)}
      </nav>
      <div hidden={view !== 'overview'} className="tw-flex-1 tw-min-h-0"><PaxosOverview {...props} /></div>
      {dataVisited && <div hidden={view !== 'data'} className="tw-flex-1 tw-min-h-0">
        <Suspense fallback={<ViewFallback />}><KvOperatorPanel {...props} active={props.active && view === 'data'} /></Suspense>
      </div>}
    </div>
  );
}

function ViewFallback() {
  return <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">Loading…</div>;
}
