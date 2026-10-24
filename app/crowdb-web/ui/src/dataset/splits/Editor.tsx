// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { defaultPolicy, type DraftSplit, type PublicationResult, type SplitHead, type SplitPolicy } from '../types';
import { newId } from '../import/upload';
import { prepare, saveOperation, type Operation } from '../operation';
import { Definitions, validateEditor } from './Definitions';
import { Publication } from '../Publication';

export function SplitEditor({ dataset, snapshot, initialRevision, recovery, close, complete }: {
  dataset: string; snapshot: string; initialRevision?: string; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const element = useRef<HTMLElement>(null);
  const [op, setOp] = useState<Operation | undefined>(recovery);
  const [splits, setSplits] = useState<DraftSplit[]>([]); const [policy, setPolicy] = useState<SplitPolicy>(defaultPolicy);
  const [base, setBase] = useState<SplitHead>(); const [ready, setReady] = useState(false);
  const [busy, setBusy] = useState(false); const [error, setError] = useState('');
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const cancel = new AbortController(); setReady(false);
    void (async () => {
      if (recovery?.input) { setSplits(recovery.input.splits); setPolicy(recovery.input.policy); setReady(true); return; }
      const head = await api.splitHead(dataset, snapshot, cancel.signal);
      const revision = initialRevision ?? head.revision;
      const value = revision ? await api.revision(dataset, snapshot, revision, cancel.signal) : null;
      if (!cancel.signal.aborted) { setBase(head); setSplits(value?.splits.map(({ name, definition, origin }) => ({ name, definition, origin })) ?? []); setPolicy(value?.policy ?? defaultPolicy); setReady(true); }
    })().catch(reason => { if (!cancel.signal.aborted) setError(String(reason)); });
    return () => cancel.abort();
  }, [dataset, snapshot, initialRevision, recovery]);
  const save = async () => {
    validateEditor(element.current);
    if (!base) throw new Error('Current Split head is unavailable.');
    const value: Operation = { version: 2, dataset, kind: 'split', snapshot, resource: newId(), expectedHead: base.generation,
      expectedRevision: base.revision, input: { splits, policy } };
    await saveOperation(value); setOp(value);
    try { await prepare(value); } finally { setOp({ ...value }); }
  };
  return <section ref={element} role="dialog" aria-label="Edit Split revision" className="tw-space-y-3 tw-border tw-border-border tw-p-4">
    <h2 className="tw-font-semibold">Edit Split revision · {snapshot}</h2>
    <p className="tw-text-xs">Expected revision: {op?.expectedRevision ?? base?.revision ?? 'none'}. Renaming or removing a Split keeps historical revisions available.</p>
    <Definitions splits={splits} policy={policy} disabled={!ready || busy || !!op} change={(values, rules) => { setSplits(values); setPolicy(rules); }} />
    {!ready && !error && <p role="status">Loading Split revision…</p>}
    {error && <p role="alert">{error}</p>}
    {!op && <button className={buttonClass} disabled={!ready || busy} onClick={() => run(save)}>Prepare Split revision</button>}
    <button className={buttonClass} disabled={busy} onClick={close}>Close editor</button>
    {op?.input && <Publication operation={op} update={setOp} complete={complete} close={close} busy={busy} run={run} />}
  </section>;
}
