// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useState } from 'react';
import { buttonClass, inputClass, JsonView } from '../../access/Workbench';
import * as api from '../api';
import { encoded } from '../identity';
import { defaultPolicy, type CompositionDraft, type CompositionSource, type DraftSplit, type Predicate, type Receipt, type SplitPolicy, type SplitSource } from '../types';
import { operationPath, request, saveOperation, type Operation } from '../operation';
import { Definitions, PredicateInput } from '../splits/Definitions';
import { Publication } from '../Publication';
import { parseMapping, resolveSample, type Mapping } from './mapping';
import { newId, upload } from './upload';

const emptyMapping = JSON.stringify({ version: 1, schema: { version: 1, fields: [] }, samples: [] }, null, 2);
export function ImportEditor({ dataset, parent: initialParent, source: initialSource, recovery, close, complete }: {
  dataset: string; parent?: string; source?: SplitSource; recovery?: Operation; close: () => void; complete: (receipt: Receipt) => void;
}) {
  const [parent, setParent] = useState(initialParent ?? '');
  const [mode, setMode] = useState(initialSource ? 'split' : 'all');
  const [predicate, setPredicate] = useState<Predicate>({ kind: 'all' });
  const [inherit, setInherit] = useState(!!initialParent);
  const [text, setText] = useState(initialParent ? JSON.stringify({ version: 1, schema: null, samples: [] }, null, 2) : emptyMapping);
  const [files, setFiles] = useState<File[]>([]); const [mapping, setMapping] = useState<Mapping>();
  const [splits, setSplits] = useState<DraftSplit[]>([]); const [policy, setPolicy] = useState<SplitPolicy>(defaultPolicy);
  const [op, setOp] = useState<Operation | undefined>(recovery); const [draft, setDraft] = useState<CompositionDraft>();
  const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const [progress, setProgress] = useState('');
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    if (!recovery) return;
    const cancel = new AbortController();
    void api.control<CompositionDraft>(operationPath(recovery), 'GET', undefined, cancel.signal).then(value => {
      if (!cancel.signal.aborted) { setDraft(value); setParent(value.parent ?? ''); setSplits(value.splits); setPolicy(value.policy); }
    }).catch(reason => { if (!cancel.signal.aborted) setError(String(reason)); });
    return () => cancel.abort();
  }, [recovery]);
  const review = () => {
    try { const value = parseMapping(text, files); if (parent) api.opaqueId(parent); setMapping(value); setError(''); }
    catch (reason) { setError(String(reason)); }
  };
  const save = async () => {
    if (!mapping) throw new Error('Review the mapping first.');
    const mappingDigest = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(mapping)))), byte => byte.toString(16).padStart(2, '0')).join('');
    if (op?.mappingDigest && op.mappingDigest !== mappingDigest) throw new Error('Reselect the original mapping; this operation cannot accept a different import.');
    if (op?.pending) throw new Error('Reconcile the pending request first.');
    const current = await api.head(dataset);
    const parentHead = parent && inherit ? await api.splitHead(dataset, parent) : null;
    const source: CompositionSource = mode === 'replace' ? { kind: 'selection', predicate: { kind: 'explicit', ids: [] } }
      : mode === 'selection' ? { kind: 'selection', predicate }
      : mode === 'split' && initialSource ? { kind: 'split', source: initialSource, selection: null } : { kind: 'all' };
    const operation: Operation = op ?? { dataset, kind: 'composition', operation: newId(), expectedHead: current.generation, mappingDigest, importsComplete: false };
    saveOperation(operation); setOp(operation);
    let value = op ? await api.control<CompositionDraft>(operationPath(op)) : await request<CompositionDraft>(operation, `${api.datasetPath(dataset)}/compositions`, 'POST', {
      operation: operation.operation, parent: parent || null, source, schema: mapping.schema,
      inherit_parent_splits: inherit && !!parent, parent_split_revision: parentHead?.revision ?? null, splits, policy,
    });
    setDraft(value);
    const uploaded = new Map<string, string>();
    for (const sample of mapping.samples) {
      if (sample.kind !== 'upsert') continue;
      for (const field of sample.fields) {
        if (field.value.kind !== 'file' || uploaded.has(field.value.file)) continue;
        const token = field.value.file; const file = files.find(file => (file.webkitRelativePath || file.name) === token)!;
        uploaded.set(token, await upload(dataset, file, setProgress));
      }
    }
    for (let offset = 0; offset < mapping.samples.length; offset += 20) {
      const samples = mapping.samples.slice(offset, offset + 20).map(sample => resolveSample(sample, uploaded));
      value = await request<CompositionDraft>(operation, operationPath(operation), 'PUT', { edit: newId(), expected_revision: value.revision, samples });
      setDraft(value); setProgress(`Saved ${Math.min(offset + 20, mapping.samples.length)}/${mapping.samples.length} Sample edits`);
    }
    operation.importsComplete = true; saveOperation(operation); setDraft({ ...value });
    setProgress('All reviewed edits are saved. Evaluate complete membership before preparation.');
  };
  return <section role="dialog" aria-label="Import Snapshot" className="tw-space-y-3 tw-border tw-border-border tw-p-4">
    <h2 className="tw-font-semibold">Import / New Snapshot</h2>
    <p className="tw-text-xs">Local test/demo import · namespace dataset-ns · {dataset}</p>
    {(!op || !op.importsComplete) && <>
      <label>Parent Snapshot (empty for independent root) <input className={`${inputClass} tw-w-full`} value={parent} onChange={event => { setParent(event.target.value); setMapping(undefined); }} /></label>
      <label>Parent data <select className={inputClass} value={mode} onChange={event => { setMode(event.target.value); setMapping(undefined); }}>
        <option value="all">Preserve all parent data</option><option value="replace">Replace membership with imported Samples</option><option value="selection">Filter parent data</option>{initialSource && <option value="split">Frozen Split {initialSource.name} / {initialSource.revision}</option>}
      </select></label>
      {mode === 'selection' && <PredicateInput value={predicate} change={value => { setPredicate(value); setMapping(undefined); }} />}
      <label className="tw-block"><input type="checkbox" checked={inherit} disabled={!parent} onChange={event => { setInherit(event.target.checked); setMapping(undefined); }} /> Inherit parent Splits, intersected with surviving Samples; additions remain unassigned</label>
      <label className="tw-block">Local payload files <input type="file" multiple onChange={event => { setFiles(Array.from(event.target.files ?? [])); setMapping(undefined); }} /></label>
      <QuickMapping add={value => { setText(JSON.stringify(value, null, 2)); setMapping(undefined); }} files={files} />
      <label className="tw-block">Batch mapping JSON (version 1, ≤8 MiB)<input type="file" accept=".json,application/json" onChange={event => {
        const file = event.target.files?.[0]; if (!file) return;
        if (file.size > 8 * 1024 * 1024) { setError('Mapping exceeds 8 MiB.'); return; }
        run(async () => { setText(await file.text()); setMapping(undefined); });
      }} /></label>
      <label className="tw-block tw-text-xs">Schema and Sample edits<textarea aria-label="Sample mapping JSON" className={`${inputClass} tw-w-full tw-font-mono`} rows={8} value={text} onChange={event => { setText(event.target.value); setMapping(undefined); }} /></label>
      <p className="tw-text-xs">Schema null inherits parent schema. Sample IDs and inline data are canonical base64. Fields accept inline, file or tombstone; Sample edits accept upsert or remove. Omitted optional fields inherit only for surviving Samples.</p>
      <Definitions splits={splits} policy={policy} disabled={busy} change={(values, rules) => { setSplits(values); setPolicy(rules); setMapping(undefined); }} />
      <button className={buttonClass} disabled={busy} onClick={review}>Review mapping</button>
      {mapping && <><p>{mapping.samples.filter(s => s.kind === 'upsert').length} additions/replacements · {mapping.samples.filter(s => s.kind === 'remove').length} removals · parent mode: {mode}.</p>
        <JsonView value={{ schema: mapping.schema, first_edits: mapping.samples.slice(0, 20), total_edits: mapping.samples.length, splits, policy }} />
        <button className={buttonClass} disabled={busy || !!op?.pending} onClick={() => run(save)}>{op ? 'Resume reviewed import' : 'Save draft and upload local data'}</button></>}
    </>}
    {error && <p role="alert">{error}</p>}{progress && <p role="status">{progress}</p>}
    {op && draft && <Publication operation={op} draft={draft} update={value => setDraft(value as CompositionDraft)} complete={complete} busy={busy} run={run} />}
    <button className={buttonClass} disabled={busy} onClick={close}>Close draft</button><p className="tw-text-xs">Closing keeps the server draft. Resume it from this Dataset; File handles must be reselected after reload. Abandon explicitly to release eligible ownership.</p>
  </section>;
}
function QuickMapping({ add, files }: { add: (value: Mapping) => void; files: File[] }) {
  const [sample, setSample] = useState('sample-1'); const [field, setField] = useState('text');
  const [text, setText] = useState(''); const [file, setFile] = useState(''); const [required, setRequired] = useState(true);
  return <fieldset className="tw-space-y-2 tw-border tw-border-border tw-p-3"><legend>Single Sample helper</legend>
    <label>Sample ID (UTF-8) <input className={inputClass} value={sample} onChange={event => setSample(event.target.value)} /></label>
    <label>Field name <input className={inputClass} value={field} onChange={event => setField(event.target.value)} /></label>
    <label><input type="checkbox" checked={required} onChange={event => setRequired(event.target.checked)} /> Required</label>
    <label>Local field source <select className={inputClass} value={file} onChange={event => setFile(event.target.value)}><option value="">Inline UTF-8 text</option>{files.map((value, index) => <option key={index} value={value.webkitRelativePath || value.name}>{value.webkitRelativePath || value.name}</option>)}</select></label>
    {!file && <label>Inline text <input className={inputClass} value={text} onChange={event => setText(event.target.value)} /></label>}
    <button className={buttonClass} type="button" disabled={!sample || !field} onClick={() => add({ version: 1, schema: { version: 1, fields: [{ name: field, required }] }, samples: [{ kind: 'upsert', id: encoded(new TextEncoder().encode(sample)), fields: [{ name: field, value: file ? { kind: 'file', file } : { kind: 'inline', data: encoded(new TextEncoder().encode(text)) } }] }] })}>Use single Sample mapping</button>
  </fieldset>;
}
