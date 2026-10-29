// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import * as api from '../api';
import { defaultPolicy, type DatasetInfo, type DraftSplit, type PublicationResult, type SplitHead } from '../types';
import { newId } from '../import/upload';
import { forgetOperation, saveOperation, type Operation } from '../operation';
import { useDraft } from '../operation/useDraft';
import { Definitions, validateEditor } from './Definitions';
import { Publication } from '../Publication';
import { addSplit, removeSplit } from './sets';
import { SplitPreview } from './Preview';
import { commitDraft } from '../editor/session';

export function SplitEditor({ embedded = false, dataset, snapshot, initialRevision, initialChange, splitName, recovery, close, complete }: {
  embedded?: boolean; dataset: string; snapshot: string; initialRevision?: string; initialChange?: 'create' | 'remove'; splitName?: string; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const element = useRef<HTMLElement>(null); const draft = useDraft(recovery); const { op, install, setError } = draft;
  const [ready, setReady] = useState(false); const [busy, setBusy] = useState(false); const [preview, setPreview] = useState<DraftSplit>(); const [previewEnabled, setPreviewEnabled] = useState(false);
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const cancel = new AbortController();
    void (async () => {
      const info = await api.control<DatasetInfo>(api.datasetPath(dataset), 'GET', undefined, cancel.signal);
      const capabilities = await api.capabilities(cancel.signal);
      if (!capabilities.includes('dataset-identity-fence')) throw new Error('Dataset identity protection is unavailable.');
      setPreviewEnabled(capabilities.includes('split-preview'));
      if (recovery?.input) {
        if (recovery.datasetId && recovery.datasetId !== info.id) throw new Error('This Draft belongs to a deleted or replaced Dataset.');
        if (!cancel.signal.aborted) { install(recovery); setReady(true); } return;
      }
      const head = await api.control<SplitHead>(`${api.snapshotPath(dataset, snapshot)}/split-head`, 'GET', undefined, cancel.signal, info.id);
      const revision = initialRevision ?? head.revision;
      const value = revision ? await api.revision(dataset, snapshot, revision, cancel.signal) : null;
      let splits = value?.splits.map(({ name, definition, origin }) => ({ name, definition, origin })) ?? [];
      if (initialChange === 'create') splits = addSplit(splits);
      if (initialChange === 'remove') { const index = splits.findIndex(value => value.name === splitName); if (index < 0) throw new Error('The selected Split is unavailable in this revision.'); splits = removeSplit(splits, index); }
      if (cancel.signal.aborted) return;
      const operation: Operation = { version: 2, dataset, datasetId: info.id, kind: 'split', snapshot, resource: newId(), expectedHead: head.generation, expectedRevision: head.revision, input: { splits, policy: value?.policy ?? defaultPolicy }, editing: true };
      await saveOperation(operation); if (!cancel.signal.aborted) { install(operation); setReady(true); }
    })().catch(reason => { if (!cancel.signal.aborted) setError(String(reason)); });
    return () => cancel.abort();
  }, []);
  const commit = async () => { validateEditor(element.current); complete(await commitDraft(draft)); };
  return <section ref={element} role={embedded ? 'region' : 'dialog'} aria-label="Edit Split revision" className="tw-space-y-4">
    <div className="tw-flex tw-items-center tw-justify-between"><h2 className="tw-font-semibold">Splits · {op?.editing ? 'Draft · local' : 'Commit'}</h2><button className={buttonClass} disabled={busy} onClick={close}>Close editor</button></div>
    <p className="tw-text-xs tw-text-muted">Snapshot {snapshot}</p>
    <Definitions splits={op?.input?.splits ?? []} policy={op?.input?.policy ?? defaultPolicy} disabled={!ready || busy || !op?.editing} change={(splits, policy) => {
      setPreview(undefined); draft.change(value => ({ ...value, input: { splits, policy } }));
    }} view={previewEnabled ? index => { try { validateEditor(element.current); setPreview(structuredClone(op!.input!.splits[index])); setError(''); } catch (reason) { setError(String(reason)); } } : undefined} />
    {!previewEnabled && ready && <p>Split View requires a service with split-preview capability.</p>}
    {preview && <SplitPreview key={JSON.stringify(preview)} dataset={dataset} snapshot={snapshot} split={preview} />}
    {!ready && !draft.error && <p role="status">Loading Split revision…</p>}{draft.error && <p role="alert">{draft.error}</p>}
    <div className="tw-flex tw-gap-3 tw-border-t tw-border-border tw-pt-4">
      <button className={buttonClass} disabled={!ready || busy || !op?.editing} onClick={() => run(async () => { await draft.persist(); })}>Save Draft</button>
      <button className={`${buttonClass} tw-bg-accent tw-text-white`} disabled={!ready || busy || !!op?.conflict} onClick={() => run(commit)}>Commit</button>
      {op?.editing && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await draft.saving.current; await forgetOperation(draft.current.current!); close(); })}>Discard Draft</button>}
    </div>
    {ready && op?.editing && !draft.error && <p role="status" className="tw-text-xs tw-text-muted">{draft.dirty ? 'Saving…' : 'Saved locally'}</p>}
    {op?.input && !op.editing && <Publication operation={op} update={install} complete={complete} close={close} busy={busy} run={run} />}
  </section>;
}
