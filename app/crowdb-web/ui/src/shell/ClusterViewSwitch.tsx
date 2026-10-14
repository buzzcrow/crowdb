// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useDomain } from '../contexts/DomainContext';
import { cn } from '../utils/cn';

export function ClusterViewSwitch() {
  const { clusterView, setClusterView } = useDomain();
  return <nav aria-label="Cluster views" className="tw-flex tw-gap-1 tw-border-b tw-border-border tw-bg-panel tw-px-4 tw-py-2">
    {(['topology', 'capacity'] as const).map(view => <button key={view}
      data-testid={view === 'capacity' ? 'domain-capacity' : 'cluster-topology'}
      aria-pressed={clusterView === view} onClick={() => setClusterView(view)}
      className={cn('tw-rounded tw-px-3 tw-py-1 tw-text-sm', clusterView === view ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg')}>
      {view === 'topology' ? 'Topology' : 'Capacity'}
    </button>)}
  </nav>;
}
