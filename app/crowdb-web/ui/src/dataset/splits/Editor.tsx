// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { defaultPolicy, type DatasetInfo, type DraftSplit, type PublicationResult, type SplitHead, type SplitPolicy } from '../types';
import { newId } from '../import/upload';
import { forgetOperation, prepare, publish, retryIdentical, saveOperation, type Operation } from '../operation';
import { Definitions, validateEditor } from './Definitions';
import { Publication } from '../Publication';
import { addSplit, removeSplit } from './sets';

export function SplitEditor({ embedded = false, dataset, snapshot, initialRevision, initialChange, splitName, recovery, close, complete }: {
  embedded?: boolean; dataset: string; snapshot: string; initialRevision?: string; initialChange?: 'create' | 'remove'; splitName?: string; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const element = useRef<HTMLElement>(null);
  const [op, setOp] = useState<Operation | undefined>(recovery);
  const saving = useRef<Promise<void>>(Promise.resolve());
  const [splits, setSplits] = useState<DraftSplit[]>([]); const [policy, setPolicy] = useState<SplitPolicy>(defaultPolicy);
  const [base, setBase] = useState<SplitHead>(); const [ready, setReady] = useState(false);
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const cancel = new AbortController(); setReady(false);
    void (async () => {
      if (recovery?.input) { setSplits(recovery.input.splits); setPolicy(recovery.input.policy); setReady(true); return; }
      const info = await api.control<DatasetInfo>(api.datasetPath(dataset), 'GET', undefined, cancel.signal);
      const head = await api.control<SplitHead>(`${api.snapshotPath(dataset, snapshot)}/split-head`, 'GET', undefined, cancel.signal, info.id);
      const revision = initialRevision ?? head.revision;
      const value = revision ? await api.revision(dataset, snapshot, revision, cancel.signal) : null;
      if (!cancel.signal.aborted) {
        let splits = value?.splits.map(({ name, definition, origin }) => ({ name, definition, origin })) ?? [];
        if (initialChange === 'create') splits = addSplit(splits);
        if (initialChange === 'remove') { const index = splits.findIndex(value => value.name === splitName); if (index < 0) throw new Error('The selected Split is unavailable in this revision.'); splits = removeSplit(splits, index); }
        const policy = value?.policy ?? defaultPolicy;
        const operation: Operation = { version: 2, dataset, datasetId: info.id, kind: 'split', snapshot, resource: newId(), expectedHead: head.generation, expectedRevision: head.revision, input: { splits, policy }, editing: true };
        await saveOperation(operation);
        setBase(head); setSplits(splits); setPolicy(policy); setOp(operation); setReady(true);
      }
    })().catch(reason => { if (!cancel.signal.aborted) setError(String(reason)); });
    return () => cancel.abort();
  }, [dataset, snapshot, initialRevision, initialChange, splitName, recovery]);
  const save = async () => {
    validateEditor(element.current);
    if (!op) throw new Error('Current Split head is unavailable.');
    await saving.current;
    delete op.editing;
    await saveOperation(op);
    try {
      const retried = op.pending ? await retryIdentical(op) : undefined;
      if (retried) { await forgetOperation(op); complete(retried); return; }
      if (!op.prepared) await prepare(op);
      const result = await publish(op); await forgetOperation(op); complete(result);
    } finally { setOp({ ...op }); }
  };
  return <section ref={element} role={embedded ? 'region' : 'dialog'} aria-label="Edit Split revision" className="tw-space-y-3 tw-border tw-border-border tw-p-4">
    <h2 className="tw-font-semibold">Edit Split revision · {snapshot}</h2>
    <p className="tw-text-xs">Expected revision: {op?.expectedRevision ?? base?.revision ?? 'none'}. Renaming or removing a Split keeps historical revisions available.</p>
    <Definitions splits={splits} policy={policy} disabled={!ready || busy || !op?.editing} change={(values, rules) => {
      setSplits(values); setPolicy(rules);
      if (op) { const next = { ...op, input: { splits: values, policy: rules } }; setOp(next); saving.current = saving.current.then(() => saveOperation(next)).catch(reason => setError(String(reason))); }
    }} />
    {!ready && !error && <p role="status">Loading Split revision…</p>}
    {error && <p role="alert">{error}</p>}
    <button className={buttonClass} disabled={!ready || busy || !!op?.conflict} onClick={() => run(save)}>Submit Split revision</button>
    <button className={buttonClass} disabled={busy} onClick={close}>Close editor</button>
    {op?.editing && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await saving.current; await forgetOperation(op); close(); })}>Discard local request</button>}
    {op?.input && !op.editing && <Publication operation={op} update={setOp} complete={complete} close={close} busy={busy} run={run} />}
  </section>;
}
