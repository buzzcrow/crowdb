// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass, JsonView } from '../../access/Workbench';
import * as api from '../api';
import { encoded } from '../identity';
import { defaultPolicy, type DraftSplit, type Predicate, type PublicationResult, type SplitPolicy, type SplitSource } from '../types';
import { forgetOperation, type Operation } from '../operation';
import { Definitions, PredicateInput, validateEditor } from '../splits/Definitions';
import { Publication } from '../Publication';
import { parseMapping, type Mapping } from './mapping';
import { saveImport } from './transfer';

const emptyMapping = JSON.stringify({ version: 1, schema: { version: 1, fields: [] }, samples: [] }, null, 2);
export function ImportEditor({ dataset, parent: initialParent, source: initialSource, recovery, close, complete }: {
  dataset: string; parent?: string; source?: SplitSource; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const transfer = useRef<AbortController>();
  useEffect(() => () => transfer.current?.abort(), []);
  const [parent, setParent] = useState(initialParent ?? '');
  const [frozenSource, setFrozenSource] = useState(initialSource);
  const [mode, setMode] = useState(initialSource ? 'split' : 'all');
  const [filterSplit, setFilterSplit] = useState(false);
  const [predicate, setPredicate] = useState<Predicate>({ kind: 'all' });
  const [inherit, setInherit] = useState(!!initialParent);
  const [text, setText] = useState(initialParent ? JSON.stringify({ version: 1, schema: null, samples: [] }, null, 2) : emptyMapping);
  const [files, setFiles] = useState<File[]>([]); const [mapping, setMapping] = useState<Mapping>();
  const [splits, setSplits] = useState<DraftSplit[]>([]); const [policy, setPolicy] = useState<SplitPolicy>(defaultPolicy);
  const element = useRef<HTMLElement>(null);
  const [op, setOp] = useState<Operation | undefined>(recovery);
  const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const [progress, setProgress] = useState('');
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const input = recovery?.input && 'parent' in recovery.input ? recovery.input : recovery?.source;
    if (!input) return;
    setParent(input.parent ?? ''); setSplits(input.splits); setPolicy(input.policy);
    setMode(input.source.kind);
    if (input.source.kind === 'split') { setFrozenSource(input.source.source); setFilterSplit(!!input.source.selection); if (input.source.selection) setPredicate(input.source.selection); }
    if (input.source.kind === 'selection') setPredicate(input.source.predicate);
    if (recovery?.mapping) setText(JSON.stringify(recovery.mapping, null, 2));
  }, [recovery]);
  const review = () => {
    try { validateEditor(element.current); const value = parseMapping(text, files); if (parent) api.opaqueId(parent); setMapping(value); setError(''); }
    catch (reason) { setError(String(reason)); }
  };
  const save = async () => {
    validateEditor(element.current);
    transfer.current = new AbortController(); const signal = transfer.current.signal;
    try { await saveImport({ dataset, parent, inherit, mode, predicate, frozenSource, filterSplit, splits, policy, op, mapping, files, signal, setOp, setProgress }); }
    finally { transfer.current = undefined; }
  };
  return <section ref={element} role="dialog" aria-label="Import Snapshot" className="tw-space-y-3 tw-border tw-border-border tw-p-4">
    <h2 className="tw-font-semibold">Import / New Snapshot</h2>
    <p className="tw-text-xs">Local test/demo import · namespace dataset-ns · {dataset}</p>
    {(!op || !op.input) && <>
      <label>Parent Snapshot (empty for independent root) <input className={`${inputClass} tw-w-full`} disabled={!!op} value={parent} onChange={event => { setParent(event.target.value); setMapping(undefined); }} /></label>
      <label>Parent data <select className={inputClass} disabled={!!op} value={mode} onChange={event => { setMode(event.target.value); setMapping(undefined); }}>
        <option value="all">Preserve all parent data</option><option value="replace">Replace membership with imported Samples</option><option value="selection">Filter parent data</option>{frozenSource && <option value="split">Frozen Split {frozenSource.name} / {frozenSource.revision}</option>}
      </select></label>
      {mode === 'split' && <label className="tw-block"><input type="checkbox" checked={filterSplit} disabled={!!op} onChange={event => { setFilterSplit(event.target.checked); setMapping(undefined); }} /> Filter frozen Split membership</label>}
      {(mode === 'selection' || mode === 'split' && filterSplit) && !op && <PredicateInput value={predicate} change={value => { setPredicate(value); setMapping(undefined); }} />}
      <label className="tw-block"><input type="checkbox" checked={inherit} disabled={!parent || !!op} onChange={event => { setInherit(event.target.checked); setMapping(undefined); }} /> Inherit parent Splits, intersected with surviving Samples; additions remain unassigned</label>
      <label className="tw-block">Local payload files <input type="file" multiple onChange={event => { setFiles(Array.from(event.target.files ?? [])); setMapping(undefined); }} /></label>
      <QuickMapping add={value => { setText(JSON.stringify(value, null, 2)); setMapping(undefined); }} files={files} />
      <label className="tw-block">Batch mapping JSON (version 1, ≤8 MiB)<input type="file" accept=".json,application/json" onChange={event => {
        const file = event.target.files?.[0]; if (!file) return;
        if (file.size > 8 * 1024 * 1024) { setError('Mapping exceeds 8 MiB.'); return; }
        run(async () => { setText(await file.text()); setMapping(undefined); });
      }} /></label>
      <label className="tw-block tw-text-xs">Schema and Sample edits<textarea aria-label="Sample mapping JSON" className={`${inputClass} tw-w-full tw-font-mono`} rows={8} value={text} onChange={event => { setText(event.target.value); setMapping(undefined); }} /></label>
      <p className="tw-text-xs">Schema null inherits parent schema. Sample IDs and inline data are canonical base64. Fields accept inline, file or tombstone; Sample edits accept upsert or remove. Omitted optional fields inherit only for surviving Samples.</p>
      <Definitions splits={splits} policy={policy} disabled={busy || !!op} change={(values, rules) => { setSplits(values); setPolicy(rules); setMapping(undefined); }} />
      <button className={buttonClass} disabled={busy} onClick={review}>Review mapping</button>
      {mapping && <><p>{mapping.samples.filter(s => s.kind === 'upsert').length} additions/replacements · {mapping.samples.filter(s => s.kind === 'remove').length} removals · parent mode: {mode}.</p>
        <JsonView value={{ schema: mapping.schema, first_edits: mapping.samples.slice(0, 20), total_edits: mapping.samples.length, splits, policy }} />
        <button className={buttonClass} disabled={busy || !!op?.pending} onClick={() => run(save)}>{op ? 'Retry reviewed import' : 'Prepare Snapshot and upload local data'}</button></>}
    </>}
    {op?.input && 'parent' in op.input && <details><summary>Frozen source and inherited Split definitions</summary><JsonView value={{ parent: op.input.parent, source: op.input.source, splits: op.input.splits }} /></details>}
    {error && <p role="alert">{error}</p>}{progress && <p role="status">{progress}</p>}
    {op && !op.input && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await forgetOperation(op); close(); })}>Discard local request</button>}
    {op?.input && <Publication operation={op} update={setOp} complete={complete} close={close} busy={busy} run={run} />}
    {busy && transfer.current && <button className={buttonClass} onClick={() => transfer.current?.abort()}>Cancel local transfer</button>}
    <button className={buttonClass} disabled={busy} onClick={close}>Close import</button><p className="tw-text-xs">Closing keeps complete inputs in this browser. Resume the saved request from this Dataset; reselect local files if their upload has not completed.</p>
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
