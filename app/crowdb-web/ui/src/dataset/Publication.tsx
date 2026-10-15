// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass, JsonView } from '../access/Workbench';
import * as api from './api';
import type { CompositionDraft, CompositionPreview, Receipt, SplitDraft, SplitPreview, SplitRevision } from './types';
import { forgetOperation, newAttempt, operationPath, reconcile, request, retryIdentical, saveOperation, type Operation } from './operation';
import { cleanupUploads } from './import/cleanup';
import { newId } from './import/upload';

export function Publication({ operation: op, draft, update, complete, busy, run, restart }: {
  operation: Operation; draft: CompositionDraft | SplitDraft; update: (draft: CompositionDraft | SplitDraft) => void;
  restart?: () => Promise<void>;
  complete: (receipt: Receipt) => void; busy: boolean; run: (action: () => Promise<void>) => void;
}) {
  const [cleaned, setCleaned] = useState(false);
  const [preview, setPreview] = useState<CompositionPreview | SplitPreview>();
  const [cleanup, setCleanup] = useState<unknown>(); const [conflict, setConflict] = useState<{ generation: string | null }>();
  const path = operationPath(op);
  const refresh = async () => update(await api.control<CompositionDraft | SplitDraft>(path));
  const state = draft.progress.state;
  const evaluated = state === 'evaluated' || state === 'prepared';
  const importReady = op.kind === 'split' || op.importsComplete === true;
  const ready = importReady && state === 'prepared' && (op.kind === 'split' || ('uploads' in draft && draft.uploads.publication_ready));
  const evaluate = async () => {
    const expected_page = draft.progress.state === 'evaluating' ? draft.progress.next_page : '0';
    const value = await request<CompositionPreview | SplitPreview>(op, `${path}/evaluate`, 'POST', { expected_revision: draft.revision, expected_page });
    setPreview(value); await refresh();
  };
  const prepare = async () => {
    await request<CompositionDraft | SplitRevision>(op, `${path}/prepare`, 'POST', { expected_revision: draft.revision }); await refresh();
  };
  const publish = async () => {
    if (draft.progress.state !== 'prepared') throw new Error('Prepare this draft first.');
    op.attempt ??= newId(); saveOperation(op);
    const target = draft.progress.target;
    const receipt = await request<Receipt>(op, `${path}/publish`, 'POST', { attempt: op.attempt, expected_head: op.expectedHead,
      ...(target.kind === 'snapshot' ? { snapshot: target.snapshot } : { revision: target.revision }) });
    forgetOperation(op); complete(receipt);
  };
  return <section aria-label="Draft publication" className="tw-space-y-3 tw-border-t tw-border-border tw-pt-3">
    <h3 className="tw-font-semibold">{state === 'prepared' ? 'Prepared candidate' : `Draft · ${state}`}</h3>
    <p className="tw-text-xs tw-break-all">Operation {op.operation} · edit revision {draft.revision}</p>
    <p className="tw-text-xs">{op.kind === 'composition' ? 'Successful publication advances Dataset latest. Stable and the chosen parent stay unchanged.' : 'Publication replaces the complete current Split set. Dataset latest stays unchanged.'}</p>
    {preview && <JsonView value={preview} />}
    {draft.progress.state === 'prepared' && <JsonView value={draft.progress.target} />}
    {op.pending ? <div className="tw-space-y-2">
      <p role="status">Request unresolved. Inspect status before retrying; do not start another operation.</p>
      <button className={buttonClass} disabled={busy} onClick={() => run(async () => { const result = await reconcile(op); update(result.draft); if (result.receipt) { forgetOperation(op); complete(result.receipt); } })}>Reconcile outcome</button>
      <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await retryIdentical(op); const result = await reconcile(op); update(result.draft); if (result.receipt) { forgetOperation(op); complete(result.receipt); } })}>Retry identical request</button>
      {op.kind === 'composition' && op.attempt && op.conflict && <button className={buttonClass} disabled={busy} onClick={() => run(async () => {
        const receipt = await api.control<{ receipt: Receipt | null }>(`${api.datasetPath(op.dataset)}/commits/${op.attempt}`);
        if (receipt.receipt) { forgetOperation(op); complete(receipt.receipt); return; }
        setConflict(await api.head(op.dataset));
      })}>Review changed Dataset head</button>}
      {op.kind === 'split' && op.conflict && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { const result = await reconcile(op); if (result.receipt) { complete(result.receipt); return; } delete op.pending; delete op.attempt; delete op.conflict; saveOperation(op); update(result.draft); })}>Keep stale draft for explicit abandonment</button>}
      {conflict && <div><p className="tw-text-xs tw-break-all">Observed head: {conflict.generation ?? 'none'}. Keep this candidate and parent; publish with a new commit attempt?</p><button className={buttonClass} onClick={() => { newAttempt(op, conflict.generation); setConflict(undefined); update({ ...draft }); }}>Use reviewed head and new attempt</button></div>}
    </div> : <div className="tw-flex tw-flex-wrap tw-gap-2">
      <button className={buttonClass} disabled={busy || !importReady || !['editing', 'evaluating'].includes(state)} onClick={() => run(evaluate)}>{state === 'evaluating' ? 'Continue evaluation' : 'Evaluate complete membership'}</button>
      <button className={buttonClass} disabled={busy || !importReady || !evaluated || ready} onClick={() => run(prepare)}>Prepare candidate</button>
      <button className={buttonClass} disabled={busy || !ready} onClick={() => run(publish)}>Publish {op.kind === 'composition' ? 'Snapshot' : 'Split revision'}</button>
      <button className={buttonClass} disabled={busy} onClick={() => run(refresh)}>Inspect draft status</button>
      <button className={buttonClass} disabled={busy || ['published', 'abandoned', 'committing'].includes(state)} onClick={() => run(async () => {
        update(await request<CompositionDraft | SplitDraft>(op, path, 'DELETE', { expected_revision: draft.revision }));
      })}>Abandon draft</button>
      {state === 'abandoned' && <button className={buttonClass} disabled={busy} onClick={() => run(async () => {
        const result = await request<{ complete: boolean; physical_cleanup_deferred: boolean }>(op, `${path}/cleanup`, 'POST'); setCleanup(result); if (result.complete) { const uploadsComplete = await cleanupUploads(op); setCleanup({ ...result, uploads_complete: uploadsComplete }); if (uploadsComplete) { forgetOperation(op); setCleaned(true); } }
      })}>Advance cleanup</button>}
    </div>}
    {cleaned && restart && <button className={buttonClass} disabled={busy} onClick={() => run(restart)}>Edit as new operation</button>}
    {cleanup !== undefined && <JsonView value={cleanup} />}
  </section>;
}
