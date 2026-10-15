// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { defaultPolicy, type DraftSplit, type Receipt, type SplitDraft, type SplitPolicy } from '../types';
import { newId } from '../import/upload';
import { operationPath, request, saveOperation, type Operation } from '../operation';
import { Definitions, validateEditor } from './Definitions';
import { BeginRecovery } from '../BeginRecovery';
import { Publication } from '../Publication';

export function SplitEditor({ dataset, snapshot, recovery, close, complete }: {
  dataset: string; snapshot: string; recovery?: Operation; close: () => void; complete: (receipt: Receipt) => void;
}) {
  const element = useRef<HTMLElement>(null);
  const [op, setOp] = useState<Operation | undefined>(recovery);
  const [draft, setDraft] = useState<SplitDraft>();
  const [splits, setSplits] = useState<DraftSplit[]>([]); const [policy, setPolicy] = useState<SplitPolicy>(defaultPolicy);
  const [base, setBase] = useState<{ revision: string | null; generation: string | null }>();
  const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const [dirty, setDirty] = useState(false);
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const cancel = new AbortController();
    void (async () => {
      if (recovery) { const value = await api.control<SplitDraft>(operationPath(recovery), 'GET', undefined, cancel.signal); setDraft(value); setSplits(value.splits); setPolicy(value.policy); return; }
      const head = await api.splitHead(dataset, snapshot, cancel.signal);
      const value = head.revision ? await api.revision(dataset, snapshot, head.revision, cancel.signal) : null;
      if (!cancel.signal.aborted) { setBase(head); setSplits(value?.splits.map(({ name, definition, origin }) => ({ name, definition, origin })) ?? []); setPolicy(value?.policy ?? defaultPolicy); }
    })().catch(reason => { if (!cancel.signal.aborted) setError(String(reason)); });
    return () => cancel.abort();
  }, [dataset, snapshot, recovery]);
  const save = async () => {
    validateEditor(element.current);
    if (op && draft) {
      const value = await request<SplitDraft>(op, operationPath(op), 'PUT', { edit: newId(), expected_revision: draft.revision, splits, policy }); setDraft(value);
    } else {
      if (!base) throw new Error('Current Split revision is unavailable.');
      const value: Operation = { dataset, kind: 'split', snapshot, operation: newId(), expectedHead: base.generation }; saveOperation(value); setOp(value);
      setDraft(await request<SplitDraft>(value, `${api.snapshotPath(dataset, snapshot)}/split-drafts`, 'POST', { operation: value.operation, expected_revision: base.revision, splits, policy }));
    }
    setDirty(false);
  };
  return <section ref={element} role="dialog" aria-label="Edit Split revision" className="tw-space-y-3 tw-border tw-border-border tw-p-4">
    <h2 className="tw-font-semibold">Edit Split revision · {snapshot}</h2>
    <p className="tw-text-xs">Expected revision: {base?.revision ?? draft?.expected_revision ?? 'none'}. Renaming or removing a Split never changes historical revisions.</p>
    <Definitions splits={splits} policy={policy} disabled={busy || !!op?.pending || !!draft && !['editing', 'evaluating', 'evaluated'].includes(draft.progress.state)} change={(values, rules) => { setSplits(values); setPolicy(rules); setDirty(true); }} />
    {error && <p role="alert">{error}</p>}
    <button className={buttonClass} disabled={busy || !!op?.pending || !!draft && !dirty} onClick={() => run(save)}>Save Split draft</button>
    <button className={buttonClass} disabled={busy} onClick={close}>Close editor</button>
    {op && !draft && <BeginRecovery operation={op} busy={busy} run={run} loaded={value => { setDraft(value as SplitDraft); setSplits(value.splits); setPolicy(value.policy); }} />}
    {op && draft && !dirty && <Publication operation={op} draft={draft} update={value => setDraft(value as SplitDraft)} complete={complete} busy={busy} run={run} />}
    {dirty && draft && <p>Unsaved edits invalidate preview. Save before evaluation.</p>}
  </section>;
}
