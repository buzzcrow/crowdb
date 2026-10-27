// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass, JsonView } from '../../access/Workbench';
import type { PublicationResult, SplitSource } from '../types';
import { forgetOperation, prepare, publish, retryIdentical, saveOperation, type Operation } from '../operation';
import { Definitions, PredicateInput, validateEditor } from '../splits/Definitions';
import { Publication } from '../Publication';
import { parseMapping, type Mapping } from './mapping';
import { saveImport } from './transfer';
import { independentRoot, openDraft, validateDraft } from './draft';
import { Fields, Samples } from './Fields';
import { digest } from '../../access/digest';
import { FileSamples } from './FileSamples';

export function ImportEditor({ dataset, parent, source, initialSample, initialField, recovery, close, complete }: {
  embedded?: boolean; dataset: string; parent?: string; source?: SplitSource; initialSample?: string; initialField?: string; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const element = useRef<HTMLElement>(null); const transfer = useRef<AbortController>();
  const saving = useRef<Promise<void>>(Promise.resolve());
  const revision = useRef(0); const [unsaved, setUnsaved] = useState(false);
  const [op, setOp] = useState<Operation | undefined>(recovery);
  const [files, setFiles] = useState<File[]>([]); const [advanced, setAdvanced] = useState('');
  const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const [progress, setProgress] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    if (!recovery) void openDraft(dataset, parent, source, controller.signal).then(value => { if (!controller.signal.aborted) setOp(value); })
      .catch(reason => { if (!controller.signal.aborted) setError(String(reason)); });
    return () => { controller.abort(); transfer.current?.abort(); };
  }, [dataset, parent, source, recovery]);
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  const change = (next: Operation) => {
    setOp(next); setUnsaved(true); const current = ++revision.current;
    saving.current = saving.current.then(async () => { await saveOperation(next); if (current === revision.current) setUnsaved(false); }).catch(reason => { setError(`Could not save edits: ${String(reason)}`); });
  };
  const choose = (selected: File[]) => run(async () => {
    if (!op) return;
    transfer.current = new AbortController();
    if (new Set([...Object.keys(op.fileChecks ?? {}), ...selected.map(file => file.webkitRelativePath || file.name)]).size > 1000) throw new Error('Select at most 1000 local files in one batch.');
    const checks = { ...op.fileChecks };
    for (const file of selected) {
      if (!file.size) throw new Error(`${file.name}: empty managed files are unsupported; use an empty inline value.`);
      const token = file.webkitRelativePath || file.name;
      if (selected.filter(value => (value.webkitRelativePath || value.name) === token).length !== 1) throw new Error(`Duplicate file name ${token}.`);
      const md5 = await digest(file, transfer.current.signal);
      if (checks[token] && (checks[token].md5 !== md5 || checks[token].size !== file.size)) throw new Error(`Reselect the original bytes for ${token}, or use a different file name.`);
      checks[token] = { size: file.size, md5 };
    }
    setFiles(previous => [...previous.filter(file => !selected.some(value => (value.webkitRelativePath || value.name) === (file.webkitRelativePath || file.name))), ...selected]);
    change({ ...op, fileChecks: checks, fileSamples: [...(op.fileSamples ?? []), ...selected.filter(file => !op.fileSamples?.some(row => row.file === (file.webkitRelativePath || file.name))).map(file => ({ file: file.webkitRelativePath || file.name, sample: file.name }))] });
  });
  const submit = async () => {
    if (!op) return;
    await saving.current; validateEditor(element.current);
    transfer.current = new AbortController(); const signal = transfer.current.signal;
    try {
      if (op.editing) {
        validateDraft(op);
        const mapping = parseMapping(JSON.stringify(op.mapping), files);
        op.mapping = mapping; op.source = { ...op.source!, schema: mapping.schema }; delete op.editing;
        await saveOperation(op); setOp({ ...op });
      }
      if (op.pending) { const result = await retryIdentical(op, signal); if (result) { await forgetOperation(op); complete(result); return; } }
      if (!op.prepared) {
        if (!op.input) await saveImport({ dataset, parent: op.source?.parent ?? '', inherit: true, mode: 'all', predicate: { kind: 'all' }, filterSplit: false,
          splits: op.source!.splits, policy: op.source!.policy, op, mapping: op.mapping, files, signal, setOp, setProgress });
        else await prepare(op, signal);
      }
      setProgress('Publishing Snapshot…');
      const result = await publish(op); await forgetOperation(op); complete(result);
    } finally { transfer.current = undefined; setOp({ ...op }); }
  };
  const editing = !!op?.editing && !!op.mapping && !!op.source;
  return <section ref={element} aria-label="Import Snapshot" className="tw-space-y-4">
    <div className="tw-flex tw-items-center tw-justify-between"><h2 className="tw-text-lg tw-font-semibold">New Snapshot · {dataset}</h2><button className={buttonClass} disabled={busy} onClick={close}>Close editor</button></div>
    {!op && !error && <p role="status">Loading the editing source…</p>}
    {op && <p className="tw-text-xs tw-break-all">Snapshot <span data-testid="editing-resource-id">{op.resource}</span> · Dataset {op.datasetId ?? 'legacy request'} · Parent {op.source?.parent ?? (op.input && 'parent' in op.input ? op.input.parent : null) ?? 'independent root'} · Expected head {op.expectedHead ?? 'empty'}</p>}
    {editing && <fieldset disabled={busy} className="tw-space-y-4">
      <label className="tw-text-sm"><input type="checkbox" checked={!op.source!.parent} disabled={!op.snapshot} onChange={event => change(independentRoot(op, event.target.checked))} /> Independent root (otherwise inherit the fixed parent)</label>
      {op.source!.source.kind === 'split' && <><p className="tw-text-xs">Frozen Split: {op.source!.source.source.name} / {op.source!.source.source.revision}</p><label><input type="checkbox" aria-label="Filter frozen Split membership" checked={op.source!.source.selection !== null} onChange={event => { if (op.source!.source.kind === 'split') change({ ...op, source: { ...op.source!, source: { ...op.source!.source, selection: event.target.checked ? { kind: 'all' } : null } } }); }} /> Filter frozen Split membership</label>{op.source!.source.selection && <section aria-label="Frozen Split filter"><PredicateInput value={op.source!.source.selection} change={selection => { if (op.source!.source.kind === 'split') change({ ...op, source: { ...op.source!, source: { ...op.source!.source, selection } } }); }} /></section>}</>}
      {op.source!.parent && op.source!.source.kind !== 'split' && <details><summary>Parent selection</summary><label>Parent data <select aria-label="Parent data" className={inputClass} value={op.parentMode ?? 'all'} onChange={event => { const mode = event.target.value as 'all' | 'replace' | 'selection'; change({ ...op, parentMode: mode, source: { ...op.source!, source: mode === 'all' ? { kind: 'all' } : { kind: 'selection', predicate: mode === 'replace' ? { kind: 'explicit', ids: [] } : { kind: 'all' } } } }); }}><option value="all">Preserve all parent data</option><option value="replace">Replace membership with imported Samples</option><option value="selection">Filter parent data</option></select></label>{op.parentMode === 'selection' && op.source!.source.kind === 'selection' && <PredicateInput value={op.source!.source.predicate} change={predicate => change({ ...op, source: { ...op.source!, source: { kind: 'selection', predicate } } })} />}</details>}
      <Fields mapping={op.mapping!} required={new Set(op.source!.parent ? op.parentSchema?.fields.filter(field => field.required).map(field => field.name) : [])} change={mapping => change({ ...op, mapping })} />
      <Samples key={op.source!.parent ?? 'root'} dataset={dataset} parent={op.source!.parent} initialSample={initialSample} initialField={initialField} mapping={op.mapping!} files={Object.keys(op.fileChecks ?? {})} choose={choose} change={mapping => change({ ...op, mapping })} />
      <FileSamples op={op} change={change} run={run} />
      <Definitions splits={op.source!.splits} policy={op.source!.policy} change={(splits, policy) => change({ ...op, source: { ...op.source!, splits, policy } })} />
      <details><summary>Advanced batch mapping</summary><label className="tw-block">Schema and Sample edits<textarea aria-label="Sample mapping JSON" className={`${inputClass} tw-w-full tw-font-mono`} rows={8} value={advanced || JSON.stringify(op.mapping, null, 2)} onChange={event => setAdvanced(event.target.value)} /></label>
        <button type="button" className={buttonClass} onClick={() => { try { const mapping: Mapping = parseMapping(advanced || JSON.stringify(op.mapping), files); if (mapping.schema === null) mapping.schema = op.parentSchema!; change({ ...op, mapping }); setAdvanced(''); setError(''); } catch (reason) { setError(String(reason)); } }}>Apply batch mapping</button></details>
      {!!Object.keys(op.fileChecks ?? {}).length && <p className="tw-text-xs">Files: {Object.keys(op.fileChecks!).map(name => `${name}${files.some(file => (file.webkitRelativePath || file.name) === name) ? '' : ' (reselect to upload)'}`).join(', ')}</p>}
    </fieldset>}
    {error && <p role="alert">{error}</p>}{progress && <p role="status">{progress}</p>}
    {editing && <p role="status" className="tw-text-xs">{unsaved ? 'Saving edits…' : 'Edits saved in browser.'}</p>}
    {op && !editing && !op.input && <label>Reselect original local payload files <input aria-label="Local payload files" type="file" multiple disabled={busy} onChange={event => choose(Array.from(event.target.files ?? []))} /></label>}
    {op && <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !!op.conflict} onClick={() => run(submit)}>{busy ? 'Submitting…' : 'Submit Snapshot'}</button>
      {busy && transfer.current && <button className={buttonClass} onClick={() => transfer.current?.abort()}>Cancel local transfer</button>}
      {!op.pending && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await saving.current; await forgetOperation(op); close(); })}>Discard local request</button>}</div>}
    {op && !editing && <details><summary>Submission details and recovery</summary>{op.input && <JsonView value={{ resource: op.resource, input: op.input }} />}<Publication operation={op} update={setOp} complete={complete} close={close} busy={busy} run={run} /></details>}
    <p className="tw-text-xs">Edits are saved in this browser. Submit publishes a new immutable Snapshot. Closing preserves input; local files must be reselected after reload. Cancel stops local transfer; uploaded data and prepared results follow service GC.</p>
  </section>;
}
