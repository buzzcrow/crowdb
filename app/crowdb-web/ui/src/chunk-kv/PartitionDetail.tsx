// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import { identity, range, type Partition } from './catalog';
import { RuntimeObservation } from './RuntimeObservation';
import { useRuntimeObservation } from './useRuntimeObservation';
import { TreeStorage, JournalStorage } from './StorageObservation';

function Properties({ values }: { values: Record<string, string | null> }) {
  return <dl className="tw-grid tw-grid-cols-[auto_minmax(0,1fr)] tw-gap-x-6 tw-gap-y-2 tw-text-sm">
    {Object.entries(values).map(([label, value]) => <div key={label} className="tw-contents"><dt className="tw-text-muted">{label}</dt><dd className="tw-font-mono tw-break-all">{value ?? 'None'}</dd></div>)}
  </dl>;
}

export function PartitionDetail({ partition: p, generation, currentGeneration, active, catalogPage, catalogOffset, onBack }: { partition: Partition; generation: string; currentGeneration?: string; active: boolean; catalogPage: number; catalogOffset: number; onBack: () => void }) {
  const [tab, setTab] = useState('Overview');
  const overlay = p.artifact.tail_overlay;
  const runtime = useRuntimeObservation({ active, partition: p, generation, catalogPage, catalogOffset });
  return <section aria-label="Partition detail" className="tw-space-y-4 tw-border-t tw-border-border tw-pt-4">
    <button className={buttonClass} onClick={onBack}>Back to range map</button>
    <h2 className="tw-text-lg tw-font-semibold tw-break-all">Partition {p.id}</h2>
    <p className="tw-text-xs tw-text-muted">Catalog generation {generation} · metadata observation</p>
    {currentGeneration !== generation && <p role="status" className="tw-text-degraded">This selection belongs to an earlier catalog observation. Select its current entry to refresh ownership and dependencies.</p>}
    <RuntimeObservation active={active} {...runtime} />
    <div role="tablist" aria-label="Partition views" className="tw-flex tw-gap-2">{['Overview', 'Tree', 'Journal', 'Dependencies'].map(name => <button key={name} role="tab" aria-selected={tab === name} className={buttonClass} onClick={() => setTab(name)}>{name}</button>)}</div>
    <div role="tabpanel" aria-label={tab} className="tw-space-y-4">
      {tab === 'Overview' && <><Properties values={{ Range: range(p), 'Assigned server': p.owner_id, Endpoint: p.endpoint, 'Owner epoch': p.epoch, 'Catalog state': p.state, Transition: p.transition_id }} />
        <p className="tw-text-sm tw-text-muted">Catalog assignment and owner runtime are separate observations, checked against the same generation and owner epoch.</p></>}
      {tab === 'Tree' && <><Properties values={{ 'Tree ID': p.artifact.tree_id, 'Overlay base tree manifest': overlay?.base_tree_manifest ?? null, 'Overlay root generation': overlay?.base_root_manifest_generation ?? null, 'Overlay base applied sequence': overlay?.base_applied_seq ?? null }} />
        <TreeStorage value={runtime.value?.tree} /></>}
      {tab === 'Journal' && <>
        {overlay && <div className="tw-rounded tw-border tw-border-degraded tw-p-4 tw-space-y-3"><h3 className="tw-font-semibold">Inherited parent stream</h3><Properties values={{ Stream: identity(overlay.source_stream_name), 'Source partition': identity(overlay.source_partition_id), 'Manifest generation': overlay.source_stream_manifest_generation, 'Replay offset (bytes)': overlay.replay_offset, 'Cutover offset (bytes)': overlay.cutover_offset, 'Cutover sequence': overlay.cutover_seq }} /></div>}
        <div className="tw-rounded tw-border tw-border-accent tw-p-4 tw-space-y-3"><h3 className="tw-font-semibold">Partition journal stream</h3><Properties values={{ Stream: identity(p.artifact.stream_name), 'Start sequence from overlay': overlay?.target_stream_start_seq ?? null }} /></div>
        <p className="tw-text-sm tw-text-muted">Streams have independent byte offsets. Durable and applied sequence numbers refer to this partition's journal.</p>
        <JournalStorage key={`${runtime.value?.journal?.generation}/${runtime.value?.journal?.offset}`} value={runtime.value?.journal} disabled={!active || runtime.busy || !!runtime.error} onPage={runtime.pageStream} />
      </>}
      {tab === 'Dependencies' && (overlay ? <><p className="tw-text-sm">Parent recovery dependency is retained in this catalog generation. Serving does not imply that materialization has completed.</p><Properties values={{ 'Source partition': identity(overlay.source_partition_id), 'Source epoch': overlay.source_epoch, 'Parent stream': identity(overlay.source_stream_name), 'Pinned base root generation': overlay.base_root_manifest_generation, 'Inheritance cutover sequence': overlay.cutover_seq }} /></> : <p className="tw-text-sm">No parent-tail overlay is recorded in this catalog entry. Runtime readiness and other retention pins require separate observations.</p>)}
    </div>
  </section>;
}
