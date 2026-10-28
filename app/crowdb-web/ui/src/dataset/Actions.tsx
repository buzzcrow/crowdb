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

export type DatasetAction = 'delete' | 'composition' | 'split' | 'create-split' | 'remove-split' | 'resume' | 'stable' | 'retain' | 'release' | 'reclaim' | 'inspect';
export function Actions({ selected: target, action, draftId, readonly, capabilities, complete, close = () => {} }: { selected: Exclude<DatasetSelection, { kind: 'root' }>; action: DatasetAction; draftId?: string; readonly: boolean; capabilities: string[]; complete: (receipt?: PublicationResult) => void; close?: () => void }) {
  const [selected] = useState(target);
  const started = useRef(false);
  const [editor, setEditor] = useState<{ kind: 'snapshot' | 'split'; parent?: string; revision?: string; source?: SplitSource; recovery?: Operation; sample?: string; field?: string }>();
  const [error, setError] = useState(''); const [busy, setBusy] = useState(false); const [observation, setObservation] = useState<unknown>(); const [confirm, setConfirm] = useState<string>(); const [unknown, setUnknown] = useState<string>();
  const dataset = selected.dataset; const snapshot = 'snapshot' in selected ? selected.snapshot : undefined;
  const run = async (action: () => Promise<void>) => { setBusy(true); setError(''); try { await action(); } catch (reason) { setError(String(reason)); } finally { setBusy(false); } };
  const open = async (kind: 'snapshot' | 'split') => {
    try {
      const source = 'split' in selected && selected.split !== undefined && selected.revision && snapshot ? { snapshot, revision: selected.revision, name: selected.split } : undefined;
      setEditor({ kind, parent: snapshot, source, revision: 'revision' in selected ? selected.revision : undefined, sample: 'sample' in selected ? selected.sample : undefined, field: 'field' in selected ? selected.field : undefined });
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
    if (['split', 'create-split', 'remove-split'].includes(action) && snapshot && capabilities.includes('split-preparation') && capabilities.includes('split-head-update')) void run(() => open('split'));
    if (action === 'resume') void run(async () => { const recovery = await savedOperation(dataset, draftId); if (!recovery) throw new Error('No saved Draft for this Dataset.'); setEditor({ kind: recovery.kind, recovery, parent: recovery.snapshot }); });
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
    {!readonly && editor?.kind === 'snapshot' && <ImportEditor embedded dataset={dataset} parent={editor.parent} source={editor.source} initialSample={editor.sample} initialField={editor.field} recovery={editor.recovery} close={close} complete={done} />}
    {!readonly && editor?.kind === 'split' && editor.parent && <SplitEditor embedded initialRevision={editor.revision} initialChange={editor.recovery ? undefined : action === 'create-split' ? 'create' : action === 'remove-split' ? 'remove' : undefined} splitName={'split' in selected ? selected.split : undefined} dataset={dataset} snapshot={editor.parent} recovery={editor.recovery} close={close} complete={done} />}
  </section>;
}
