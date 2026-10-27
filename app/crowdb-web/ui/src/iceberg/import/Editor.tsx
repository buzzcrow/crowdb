// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { iceberg } from '../../access/native';
import { digest } from '../../access/digest';
import type { TableLoad } from '../types';
import { currentSchema, submitImport, supported, tablePath } from './commit';
import { abortTransfers } from './files';
import { requestKey, snapshotId } from './request';
import { footer, schemaFromParquet, type IceSchema } from './schema';
import { forget, load, save, type ImportFile, type ImportRequest } from './storage';
import { forgetDraft, loadDraft, saveDraft } from './draft';
const initialSchema: IceSchema = { type: 'struct', 'schema-id': 0, fields: [{ id: 1, name: 'id', required: true, type: 'long' }] };

export function TableEditor({ namespace, table, close, complete }: { namespace: string[]; table?: string; close: () => void; complete: (name: string) => Promise<void> }) {
  const [name, setName] = useState(table ?? ''); const [mode, setMode] = useState(table ? 'parquet' : 'empty');
  const [schema, setSchema] = useState<IceSchema>(initialSchema); const [base, setBase] = useState<TableLoad>();
  const [inputs, setInputs] = useState<ImportFile[]>([]); const [files, setFiles] = useState<File[]>([]);
  const [op, setOp] = useState<ImportRequest>(); const [ready, setReady] = useState(!table);
  const [draftReady, setDraftReady] = useState(false);
  const [draftError, setDraftError] = useState('');
  const draftIdentity = useRef(requestKey());
  const [error, setError] = useState(''); const [busy, setBusy] = useState(false); const [progress, setProgress] = useState('');
  const controller = useRef<AbortController>();
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  const recover = () => {
    const saved = load(namespace, name); if (!saved) throw new Error('No saved import for this Table.');
    setOp(saved); setSchema(saved.schema); setInputs(saved.files); setMode(saved.files.length ? 'parquet' : 'empty'); setReady(true);
  };
  useEffect(() => {
    const active = new AbortController();
    void (async () => {
      const draft = loadDraft(namespace, table);
      if (draft) { draftIdentity.current = draft.identity; setName(draft.name); setMode(draft.mode); setSchema(draft.schema); setInputs(draft.files); setBase(draft.base); }
      const saved = table || draft?.name ? load(namespace, table ?? draft!.name) : undefined;
      if (saved) { setOp(saved); setSchema(saved.schema); setInputs(saved.files); setReady(true); return; }
      if (table && !draft) {
        const value: TableLoad = await iceberg(tablePath({ namespace, name: table }), '', 'GET', undefined, active.signal);
        supported(value);
        if (active.signal.aborted) return;
        setBase(value); setSchema(currentSchema(value));
      }
      setReady(true); setDraftReady(true);
    })().catch(reason => { if (!active.signal.aborted) setError(String(reason)); });
    return () => { active.abort(); controller.current?.abort(); };
  }, [namespace, table]);
  useEffect(() => {
    if (!draftReady || !ready || op) return;
    try { saveDraft({ version: 1, identity: draftIdentity.current, namespace, table, name, mode, schema, files: inputs, base }); setDraftError(''); }
    catch (reason) { setDraftError(`Could not save Table edits: ${String(reason)}`); }
  }, [draftReady, ready, op, namespace, table, name, mode, schema, inputs, base]);
  const choose = (selected: File[]) => run(async () => {
    if (!selected.length) return;
    controller.current = new AbortController();
    if (new Set([...inputs, ...selected].map(file => file.name)).size > 1000 || new Set(selected.map(file => file.name)).size !== selected.length) throw new Error('Select at most 1000 uniquely named Parquet files.');
    let expected = table || op || inputs.length ? schema : undefined;
    const values: ImportFile[] = [];
    for (const file of selected) {
      setProgress(`Checking ${file.name}…`);
      const metadata = await footer(file);
      const inferred = schemaFromParquet(metadata, expected);
      expected ??= inferred;
      const md5 = await digest(file, controller.current.signal);
      const old = inputs.find(value => value.name === file.name);
      if (op && !old || old && (old.size !== file.size || old.md5 !== md5)) throw new Error(`Reselect the original bytes for ${file.name}, or remove it before selecting a replacement.`);
      values.push(old ?? { name: file.name, size: file.size, md5, rows: metadata.num_rows.toString(), identity: requestKey() });
    }
    if (!op) { setSchema(expected!); setInputs(previous => [...previous.filter(input => !values.some(value => value.name === input.name)), ...values]); }
    setFiles(previous => [...previous.filter(file => !selected.some(value => value.name === file.name)), ...selected]); setProgress('');
  });
  const submit = async () => {
    if (!name || !schema.fields.length || new Set(schema.fields.map(field => field.name)).size !== schema.fields.length || schema.fields.some(field => !field.name)) throw new Error('Use a Table name and distinct, nonempty fields.');
    if (mode === 'parquet' && !inputs.length) throw new Error('Select one or more Parquet files.');
    controller.current = new AbortController();
    const value: ImportRequest = op ?? { version: 1, namespace, name, create: !table, schema, files: mode === 'parquet' ? inputs : [], identity: draftIdentity.current, snapshot: snapshotId(), table: base };
    save(value); setOp(value);
    try { await submitImport(value, files, setProgress, controller.current.signal); forgetDraft(namespace, table, draftIdentity.current); await complete(name); }
    finally { setOp({ ...value }); controller.current = undefined; }
  };
  return <section aria-label={table ? 'Append Parquet' : 'Create Table'} className="tw-space-y-4">
    <div className="tw-flex tw-justify-between"><h2 className="tw-text-lg tw-font-semibold">{table ? `Append Parquet · ${table}` : 'Create Table'}</h2><button className={buttonClass} disabled={busy} onClick={close}>Close editor</button></div>
    <p className="tw-text-xs">Namespace: {namespace.join(' / ')}</p>
    {!ready && !error && <p role="status">Loading Table…</p>}
    <fieldset disabled={busy || !ready} className="tw-space-y-3">
      <label className="tw-block">Table name <input aria-label="Table name" className={inputClass} value={name} disabled={!!table || !!op} onChange={event => setName(event.target.value)} /></label>
      {!table && <label>Source <select aria-label="Table source" className={inputClass} value={mode} disabled={!!op} onChange={event => setMode(event.target.value)}><option value="empty">Empty Table</option><option value="parquet">Parquet files</option></select></label>}
      {mode === 'parquet' && <><label className="tw-block">Parquet files <input aria-label="Parquet files" type="file" multiple accept=".parquet" onChange={event => { choose(Array.from(event.target.files ?? [])); event.target.value = ''; }} /></label>
        <table aria-label="Import files" className="tw-w-full tw-text-sm"><thead><tr><th>File</th><th>Bytes</th><th>Rows</th><th>State</th><th /></tr></thead><tbody>{inputs.map(input => <tr key={input.name}><td>{input.name}</td><td>{input.size}</td><td>{input.rows}</td><td>{input.location ? 'Uploaded' : files.some(file => file.name === input.name) ? 'Ready' : 'Reselect file'}</td><td>{!op && <button type="button" className={buttonClass} onClick={() => { setInputs(inputs.filter(value => value.name !== input.name)); setFiles(files.filter(value => value.name !== input.name)); }}>Remove {input.name}</button>}</td></tr>)}</tbody></table></>}
      <table aria-label="Table Schema editor" className="tw-w-full tw-text-sm"><thead><tr><th>Field</th><th>Type</th><th>Required</th><th /></tr></thead><tbody>{schema.fields.map((field, index) => <tr key={field.id}><td>{mode === 'empty' && !op ? <input aria-label={`Field ${index + 1} name`} className={inputClass} value={field.name} onChange={event => setSchema({ ...schema, fields: schema.fields.map((value, position) => position === index ? { ...value, name: event.target.value } : value) })} /> : field.name}</td><td>{mode === 'empty' && !op ? <select aria-label={`Field ${index + 1} type`} className={inputClass} value={String(field.type)} onChange={event => setSchema({ ...schema, fields: schema.fields.map((value, position) => position === index ? { ...value, type: event.target.value } : value) })}>{['boolean', 'int', 'long', 'float', 'double', 'string', 'binary', 'date', 'time', 'timestamp', 'timestamptz', 'uuid'].map(type => <option key={type}>{type}</option>)}</select> : typeof field.type === 'string' ? field.type : JSON.stringify(field.type)}</td><td><input aria-label={`Field ${index + 1} required`} type="checkbox" checked={field.required} disabled={mode !== 'empty' || !!op} onChange={event => setSchema({ ...schema, fields: schema.fields.map((value, position) => position === index ? { ...value, required: event.target.checked } : value) })} /></td><td>{mode === 'empty' && !op && <button type="button" className={buttonClass} onClick={() => setSchema({ ...schema, fields: schema.fields.filter(value => value.id !== field.id) })}>Remove field {field.name}</button>}</td></tr>)}</tbody></table>
      {mode === 'empty' && !op && <button type="button" className={buttonClass} disabled={schema.fields.length >= 256} onClick={() => setSchema({ ...schema, fields: [...schema.fields, { id: Math.max(0, ...schema.fields.map(field => field.id)) + 1, name: '', required: false, type: 'string' }] })}>Add field</button>}
    </fieldset>
    {error && <p role="alert">{error}</p>}{draftError && <p role="alert">{draftError}</p>}{progress && <p role="status">{progress}</p>}
    <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={busy || !ready || !name || !!draftError} onClick={() => run(submit)}>{op ? 'Resume submission' : table ? 'Submit files' : 'Create table'}</button>
      {!op && <button className={buttonClass} disabled={busy || !name} onClick={() => { try { recover(); setError(''); } catch (reason) { setError(String(reason)); } }}>Resume saved import</button>}
      {op && !op.uncertain && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { await abortTransfers(op); forget(op); forgetDraft(namespace, table, draftIdentity.current); close(); })}>Discard local request</button>}
      {!op && <button className={buttonClass} disabled={busy || !draftReady} onClick={() => { forgetDraft(namespace, table, draftIdentity.current); close(); }}>Discard local edits</button>}
      {busy && controller.current && <button className={buttonClass} onClick={() => controller.current?.abort()}>Cancel local transfer</button>}</div>
    {draftReady && !draftError && !op && <p role="status" className="tw-text-xs">Edits saved in browser.</p>}
    <p className="tw-text-xs">Submit publishes one immutable data Snapshot for this batch. Empty Tables have no data Snapshot. Closing preserves saved input. Reselect local files after reload; failed submissions keep their exact request. Unreferenced files follow service GC.</p>
  </section>;
}
