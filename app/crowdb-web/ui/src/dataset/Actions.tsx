// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass, JsonView } from '../access/Workbench';
import * as api from './api';
import type { DatasetSelection, Receipt, SplitSource } from './types';
import { savedOperation, type Operation } from './operation';
import { ImportEditor } from './import/Editor';
import { SplitEditor } from './splits/Editor';

export function Actions({ selected, readonly, capabilities, complete }: { selected: DatasetSelection; readonly: boolean; capabilities: string[]; complete: (receipt?: Receipt) => void }) {
  const [editor, setEditor] = useState<{ kind: 'composition' | 'split'; parent?: string; revision?: string; source?: SplitSource; recovery?: Operation }>();
  const [error, setError] = useState(''); const [busy, setBusy] = useState(false); const [observation, setObservation] = useState<unknown>(); const [confirm, setConfirm] = useState<string>(); const [unknown, setUnknown] = useState<string>();
  if (selected.kind === 'root') return null;
  const dataset = selected.dataset; const snapshot = 'snapshot' in selected ? selected.snapshot : undefined;
  const run = async (action: () => Promise<void>) => { setBusy(true); setError(''); try { await action(); } catch (reason) { setError(String(reason)); } finally { setBusy(false); } };
  const open = (kind: 'composition' | 'split') => {
    try {
      const recovery = savedOperation(dataset);
      if (recovery) { setEditor({ kind: recovery.kind, recovery, parent: recovery.snapshot }); return; }
      const source = 'split' in selected && selected.split !== undefined && selected.revision && snapshot ? { snapshot, revision: selected.revision, name: selected.split } : undefined;
      setEditor({ kind, parent: snapshot, source, revision: 'revision' in selected ? selected.revision : undefined });
    } catch (reason) { setError(String(reason)); }
  };
  const done = (receipt: Receipt) => { setEditor(undefined); complete(receipt); };
  const mutate = (action: 'retain' | 'release' | 'reclaim', target = snapshot!) => run(async () => {
    try {
    const result = await api.mutate(action === 'reclaim' ? 'snapshot-reclaim' : 'dataset-management', `${api.snapshotPath(dataset, target)}/${action}`, 'POST'); setObservation(result); setConfirm(undefined); complete();
    } catch (reason) { if (action === 'reclaim' && (!(reason instanceof api.DatasetError) || reason.unknownOutcome)) setUnknown(target); throw reason; }
  });
  return <section aria-label="Dataset actions" className="tw-space-y-3">
    {!readonly && <div className="tw-flex tw-flex-wrap tw-gap-2">
      <button className={buttonClass} disabled={busy || !!editor || !capabilities.includes('composition')} onClick={() => open('composition')}>{snapshot ? ('split' in selected && selected.split !== undefined ? 'Create child from Split' : 'Create child Snapshot') : 'Import / New Snapshot'}</button>
      <button className={buttonClass} disabled={busy || !!editor} onClick={() => { try { const recovery = savedOperation(dataset); if (!recovery) throw new Error('No saved operation for this Dataset.'); setEditor({ kind: recovery.kind, recovery, parent: recovery.snapshot }); } catch (reason) { setError(String(reason)); } }}>Resume saved draft</button>
      {snapshot && <>
        <button className={buttonClass} disabled={busy || !!editor || !capabilities.includes('split-revisions')} onClick={() => open('split')}>Edit Split revision</button>
        <button className={buttonClass} disabled={busy} onClick={() => void run(async () => { setObservation(await api.mutate('dataset-management', `${api.datasetPath(dataset)}/names/stable`, 'PUT', { snapshot })); complete(); })}>Set stable</button>
        <button className={buttonClass} disabled={busy} onClick={() => void mutate('retain')}>Retain Snapshot</button>
        <button className={buttonClass} disabled={busy} onClick={() => void mutate('release')}>Release Snapshot</button>
        <button className={buttonClass} disabled={busy || !!unknown || !capabilities.includes('snapshot-reclaim')} onClick={() => setConfirm(snapshot)}>Reclaim Snapshot</button>
      </>}
    </div>}
    {!readonly && confirm && <div><p>Reclaim {confirm}? Eligible data and metadata become unavailable. Latest, stable, retained, reachable or actively read Snapshots remain protected by the server.</p><button className={buttonClass} disabled={busy || !!unknown} onClick={() => void mutate('reclaim', confirm)}>Confirm reclamation</button><button className={buttonClass} onClick={() => setConfirm(undefined)}>Cancel reclamation</button></div>}
    {snapshot && <button className={buttonClass} disabled={busy} onClick={() => void run(async () => setObservation(await api.control(`${api.snapshotPath(dataset, snapshot)}/reclaim`)))}>Inspect reclaim status</button>}
    {unknown && <div role="status">Reclamation outcome unknown for {unknown}. Inspect before continuing.
      <button className={buttonClass} disabled={busy} onClick={() => void run(async () => { setObservation(await api.control(`${api.snapshotPath(dataset, unknown)}/reclaim`)); setUnknown(undefined); setConfirm(undefined); })}>Reconcile reclamation</button>
    </div>}
    {error && <p role="alert">{error}</p>}{observation !== undefined && <JsonView value={observation} />}
    {!readonly && editor?.kind === 'composition' && <ImportEditor dataset={dataset} parent={editor.parent} source={editor.source} recovery={editor.recovery} close={() => setEditor(undefined)} complete={done} />}
    {!readonly && editor?.kind === 'split' && editor.parent && <SplitEditor initialRevision={editor.revision} dataset={dataset} snapshot={editor.parent} recovery={editor.recovery} close={() => setEditor(undefined)} complete={done} />}
  </section>;
}
