// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { Dialog } from '../components/Dialog';
import { buttonClass, JsonView } from '../access/Workbench';
import * as api from './api';
import type { DatasetSelection, PublicationResult, SplitSource } from './types';
import { savedOperation, type Operation } from './operation';
import { ImportEditor } from './import/Editor';
import { SplitEditor } from './splits/Editor';

export type DatasetAction = 'delete' | 'composition' | 'split' | 'resume' | 'stable' | 'retain' | 'release' | 'reclaim' | 'inspect';
export function Actions({ selected: target, action, readonly, capabilities, complete }: { selected: Exclude<DatasetSelection, { kind: 'root' }>; action: DatasetAction; readonly: boolean; capabilities: string[]; complete: (receipt?: PublicationResult) => void }) {
  const [selected] = useState(target);
  const started = useRef(false);
  const [editor, setEditor] = useState<{ kind: 'snapshot' | 'split'; parent?: string; revision?: string; source?: SplitSource; recovery?: Operation }>();
  const [error, setError] = useState(''); const [busy, setBusy] = useState(false); const [observation, setObservation] = useState<unknown>(); const [confirm, setConfirm] = useState<string>(); const [unknown, setUnknown] = useState<string>();
  const dataset = selected.dataset; const snapshot = 'snapshot' in selected ? selected.snapshot : undefined;
  const run = async (action: () => Promise<void>) => { setBusy(true); setError(''); try { await action(); } catch (reason) { setError(String(reason)); } finally { setBusy(false); } };
  const open = async (kind: 'snapshot' | 'split') => {
    try {
      const recovery = await savedOperation(dataset);
      if (recovery) { setEditor({ kind: recovery.kind, recovery, parent: recovery.snapshot }); return; }
      const source = 'split' in selected && selected.split !== undefined && selected.revision && snapshot ? { snapshot, revision: selected.revision, name: selected.split } : undefined;
      setEditor({ kind, parent: snapshot, source, revision: 'revision' in selected ? selected.revision : undefined });
    } catch (reason) { setError(String(reason)); }
  };
  const done = (receipt: PublicationResult) => { setEditor(undefined); complete(receipt); };
  const mutate = (action: 'retain' | 'release' | 'reclaim', target = snapshot!) => run(async () => {
    try {
    const result = await api.mutate(action === 'reclaim' ? 'snapshot-reclaim' : 'dataset-management', `${api.snapshotPath(dataset, target)}/${action}`, 'POST'); setObservation(result); setConfirm(undefined); complete();
    } catch (reason) { if (action === 'reclaim' && (!(reason instanceof api.DatasetError) || reason.unknownOutcome)) setUnknown(target); throw reason; }
  });
  useEffect(() => {
    if (started.current) return;
    started.current = true;
    if (readonly && action !== 'inspect') return;
    if (action === 'composition' && capabilities.includes('composition-preparation') && capabilities.includes('snapshot-head-update')) void run(() => open('snapshot'));
    if (action === 'split' && snapshot && capabilities.includes('split-preparation') && capabilities.includes('split-head-update')) void run(() => open('split'));
    if (action === 'resume') void run(async () => { const recovery = await savedOperation(dataset); if (!recovery) throw new Error('No saved request for this Dataset.'); setEditor({ kind: recovery.kind, recovery, parent: recovery.snapshot }); });
    if (snapshot && action === 'stable' && capabilities.includes('dataset-management')) void run(async () => { setObservation(await api.mutate('dataset-management', `${api.datasetPath(dataset)}/names/stable`, 'PUT', { snapshot })); complete(); });
    if (snapshot && (action === 'retain' || action === 'release') && capabilities.includes('dataset-management')) void mutate(action);
    if (snapshot && action === 'reclaim' && capabilities.includes('snapshot-reclaim')) setConfirm(snapshot);
    if (snapshot && action === 'inspect') void run(async () => setObservation(await api.control(`${api.snapshotPath(dataset, snapshot)}/reclaim`)));
  }, []);
  return <section aria-label="Dataset operation" className="tw-space-y-3">
    {busy && <p role="status">Working on {dataset}{snapshot ? ` / ${snapshot}` : ''}…</p>}
    {!readonly && confirm && <Dialog isOpen title="Reclaim Snapshot" description={`Target: ${dataset} / ${confirm}`} onClose={() => setConfirm(undefined)} confirmLabel="Confirm reclamation" confirmDisabled={!!unknown} confirmLoading={busy} onConfirm={() => mutate('reclaim', confirm)} destructive>
      <p>Eligible data and metadata become unavailable. Latest, stable, retained or reachable Snapshots remain protected. New reads stop after marking; cleanup follows the grace period.</p>
      {error && <p role="alert">{error}</p>}
    </Dialog>}
    {unknown && <div role="status">Reclamation outcome unknown for {unknown}. Inspect before continuing.
      <button className={buttonClass} disabled={busy} onClick={() => void run(async () => { setObservation(await api.control(`${api.snapshotPath(dataset, unknown)}/reclaim`)); setUnknown(undefined); setConfirm(undefined); })}>Reconcile reclamation</button>
    </div>}
    {error && <p role="alert">{error}</p>}{observation !== undefined && <JsonView value={observation} />}
    {!readonly && editor?.kind === 'snapshot' && <Dialog isOpen title="Import Snapshot" description={`Dataset: ${dataset}`} onClose={() => setEditor(undefined)} footer={null} size="xl"><ImportEditor embedded dataset={dataset} parent={editor.parent} source={editor.source} recovery={editor.recovery} close={() => setEditor(undefined)} complete={done} /></Dialog>}
    {!readonly && editor?.kind === 'split' && editor.parent && <Dialog isOpen title="Edit Split revision" description={`Dataset: ${dataset} / ${editor.parent}`} onClose={() => setEditor(undefined)} footer={null} size="xl"><SplitEditor embedded initialRevision={editor.revision} dataset={dataset} snapshot={editor.parent} recovery={editor.recovery} close={() => setEditor(undefined)} complete={done} /></Dialog>}
  </section>;
}
