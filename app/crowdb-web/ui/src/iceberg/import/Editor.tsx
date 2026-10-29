// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { randomUUID } from '../../utils/randomUUID';
import { useContext, useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { iceberg, AccessError } from '../../access/native';
import { digest } from '../../access/digest';
import type { Selection, TableLoad } from '../types';
import { currentSchema, submitImport, supported, tablePath, namespacePath } from './commit';
import { requestKey, snapshotId } from './request';
import { footer, schemaFromParquet, type IceSchema } from './schema';
import { forget, load, save, type ImportRequest } from './storage';
import { forgetDraft, loadDraft, saveDraft, type TableDraft } from './draft';
import { uploadFile, fileRequest } from './files';
import { filePage, mainSnapshot, validateSource, type FileCursor } from './source';
import { FileGrid, fileName, rowKey, type FileRow } from './Grid';
import { useQuery } from '../../dataset/query';
import { EditorDetails } from '../EditorDetails';
import { Fields } from '../Fields';
import { SchemaTable } from '../SchemaTable';
const initialSchema: IceSchema = { type: 'struct', 'schema-id': 0, fields: [] };
const defaultTableName = () => `table-${randomUUID().replaceAll('-', '').slice(0, 8)}`;
interface Window { additions?: number; source?: FileCursor }

export function TableEditor({ namespace, table, selection, guarded = false, close, complete }: {
  namespace: string[]; table?: string; selection?: Selection; guarded?: boolean; close: () => void; complete: (name: string) => Promise<void>;
}) {
  const requestedSource = selection ? String(selection.snapshot['snapshot-id']) : undefined;
  const [draft, setDraft] = useState<TableDraft>(); const current = useRef<TableDraft>();
  const [op, setOp] = useState<ImportRequest>(); const [busy, setBusy] = useState(false);
  const [error, setError] = useState(''); const [progress, setProgress] = useState(''); const [notice, setNotice] = useState('');
  const [window, setWindow] = useState<Window>({ additions: 0 }); const [previous, setPrevious] = useState<Window[]>([]);
  const [chosen, setChosen] = useState<FileRow>();
  const tableName = useRef<HTMLInputElement>(null);
  const controller = useRef<AbortController>(); const details = useContext(EditorDetails);
  const install = (value: TableDraft) => { current.current = value; setDraft({ ...value }); };
  const persist = (value: TableDraft) => { saveDraft(value); install(value); };
  const change = (update: (value: TableDraft) => TableDraft) => {
    try { persist(update(current.current!)); setError(''); setNotice(''); } catch (reason) { setError(String(reason)); }
  };
  const schemaOrigin = draft?.base ? currentSchema(draft.base) : draft?.files[0]?.schema ?? initialSchema;
  const legacySchema = !!draft && !op?.commit && JSON.stringify(draft.schema.fields) !== JSON.stringify(schemaOrigin.fields);
  const canCommit = !!op || !!draft && !legacySchema && (table ? !!draft.removed?.length || !!draft.files.length : !!draft.files.length)
    && draft.files.every(file => !!file.location);
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); setNotice(''); void action().catch(reason => setError(String(reason))).finally(() => { setBusy(false); setProgress(''); controller.current = undefined; }); };
  useEffect(() => {
    const active = new AbortController();
    void (async () => {
      let value = loadDraft(namespace, table, requestedSource);
      const pending = table || value?.name ? load(namespace, table ?? value!.name) : undefined;
      if (pending && value && pending.identity !== value.identity) throw new Error('Another submission for this Table is pending. Resolve it before editing a different Draft.');
      if (!value) {
        const base: TableLoad | undefined = table ? await iceberg(tablePath({ namespace, name: table }), '', 'GET', undefined, active.signal) : undefined;
        if (active.signal.aborted) return;
        if (base) supported(base);
        const source = base ? requestedSource ?? mainSnapshot(base) : null;
        if (base) validateSource(base, source);
        value = { version: 1, identity: requestKey(), namespace, table, name: table ?? defaultTableName(), mode: 'parquet', schema: base ? currentSchema(base) : initialSchema, files: [], base, source, requestedSource, removed: [] };
        if (selection?.kind === 'file') value.removed = [{ manifest: selection.manifest!.location, location: selection.file!.location }];
      }
      if (value.source === undefined && value.base && table) value.source = mainSnapshot(value.base);
      if (pending) { value = { ...value, identity: pending.identity, schema: pending.schema, files: pending.files, base: pending.table, source: pending.source, removed: pending.removed }; setOp(pending); }
      if (!active.signal.aborted) { install(value); if (!pending) persist(value); }
    })().catch(reason => { if (!active.signal.aborted) setError(String(reason)); });
    return () => { active.abort(); controller.current?.abort(); };
  }, []);
  useEffect(() => {
    if (!draft) return;
    details(<section aria-label="Table Draft properties" className="tw-space-y-3"><h2 className="tw-font-semibold">Properties</h2>
      <Fields stacked values={chosen ? { File: fileName(chosen), ...('added' in chosen ? chosen.added : chosen.source.file) } : { Table: draft.name, State: 'Local Draft', 'Source Snapshot': draft.source ?? 'Empty', 'Expected main': draft.base ? mainSnapshot(draft.base) : null }} /></section>);
    return () => details(null);
  }, [draft, chosen, details]);
  const request = (value: TableDraft): ImportRequest => ({ version: 1, namespace, name: value.name, create: !table, schema: value.schema, files: value.files, identity: value.identity, snapshot: snapshotId(), table: value.base, source: value.source, removed: value.removed, guarded, timestamp: Date.now() });
  const showSource = !!draft?.source && (window.additions === undefined || !draft.files.length);
  const source = useQuery(JSON.stringify([draft?.identity, draft?.source, window.source]), !!draft?.base && showSource, signal => filePage(request(draft!), window.source, signal));
  const page = window.additions ?? 0;
  const rows: FileRow[] = showSource ? (source.value?.rows ?? []).map(value => ({ source: value, removed: !!draft?.removed?.some(item => item.manifest === value.manifest && item.location === value.file.location) })) : draft?.files.slice(page * 20, page * 20 + 20).map(added => ({ added })) ?? [];
  const next: Window | undefined = showSource ? source.value?.next ? { source: source.value.next } : undefined
    : draft && (page + 1) * 20 < draft.files.length ? { additions: page + 1 } : draft?.source ? { source: { manifest: 0 } } : undefined;
  const choose = async (selected: File[]) => {
    if (!selected.length) return;
    if (op?.commit) throw new Error('Resolve the frozen Commit before changing its files.');
    if (!current.current!.name.trim()) throw new Error('Enter a Table name before uploading.');
    if (selected.length > 20 || selected.some(file => !file.size || file.size > 100 * 1024 * 1024)) throw new Error('Choose up to 20 nonempty Parquet files, at most 100 MiB each.');
    if (new Set(selected.map(file => file.name)).size !== selected.length) throw new Error('Choose files with distinct names.');
    controller.current = new AbortController(); const signal = controller.current.signal;
    for (const file of selected) {
      let value = structuredClone(current.current!);
      if (op) value = { ...value, files: structuredClone(op.files), base: op.table, schema: op.schema };
      setProgress(`Checking ${file.name}…`);
      const metadata = await footer(file);
      let schema: IceSchema;
      try { schema = schemaFromParquet(metadata, value.base || value.files.length ? value.schema : undefined); }
      catch (reason) { throw new Error(`${file.name}: ${reason instanceof Error ? reason.message : reason}`); }
      if (!schema.fields.length) throw new Error(`${file.name}: Parquet must contain at least one field.`);
      if (schema.fields.length > 256) throw new Error('The editor supports at most 256 top-level fields.');
      const md5 = await digest(file, signal); const existing = value.files.find(input => input.name === file.name);
      if (op && !existing) throw new Error('Reselect only the files in the frozen submission.');
      if (existing && (existing.size !== file.size || existing.md5 !== md5)) throw new Error(`Remove ${file.name} before uploading replacement bytes.`);
      if (!existing && value.files.length >= 1000) throw new Error('A Draft supports at most 1000 added files.');
      if (!value.base && !value.files.length) value.schema = schema;
      if (!value.base) {
        value.base = await iceberg(`${namespacePath(namespace)}/tables`, '', 'POST', { name: value.name, schema, 'stage-create': true, properties: { 'format-version': '2' } }, signal);
        supported(value.base!); value.schema = currentSchema(value.base!);
      }
      const input = existing ?? { name: file.name, size: file.size, md5, rows: metadata.num_rows.toString(), identity: requestKey(), schema: structuredClone(schema) };
      if (!existing) value.files.push(input);
      const keep = () => { persist(value); if (op) { op.files = value.files; op.table = value.base; save(op); setOp({ ...op }); } };
      if (input.location) {
        try { await fileRequest(request(value), input.location, 'HEAD', undefined, {}, signal); }
        catch (reason) {
          if (!(reason instanceof AccessError) || reason.status !== 404 || op) throw reason;
          input.identity = requestKey(); delete input.location; delete input.multipart;
        }
      }
      keep(); setWindow({ additions: Math.floor(value.files.indexOf(input) / 20) }); setPrevious([]);
      const transfer = request(value);
      await uploadFile(transfer, input, file, setProgress, signal, keep);
      value = { ...value }; keep();
    }
  };
  const commit = async () => {
    const value = current.current!;
    if (!op && table && !value.files.length && !value.removed?.length) throw new Error('Add or remove a Parquet file before Commit.');
    persist(value); controller.current = new AbortController(); const pending = op ?? request(value); save(pending); setOp(pending);
    try {
      await submitImport(pending, [], setProgress, controller.current.signal);
      forgetDraft(namespace, table, value.identity, requestedSource, value.revision); await complete(value.name);
    } finally { setOp({ ...pending }); }
  };
  const startCommit = () => {
    setError(''); setNotice('');
    const value = current.current!;
    if (!value.name.trim()) { tableName.current?.focus(); setError('Enter a Table name.'); return; }
    if (legacySchema) return;
    if (!op?.commit && !table && !value.files.length) { setError('Upload Parquet to create a Table.'); return; }
    run(commit);
  };
  const remove = (row: FileRow) => {
    setChosen(undefined);
    change(value => 'added' in row ? { ...value, files: value.files.filter(file => file.identity !== row.added.identity),
      ...(!table && value.files.length === 1 ? { base: undefined, schema: initialSchema } : {}) }
    : { ...value, removed: row.removed ? value.removed?.filter(item => item.location !== row.source.file.location || item.manifest !== row.source.manifest) : [...value.removed ?? [], { manifest: row.source.manifest, location: row.source.file.location }] });
  };
  return <section aria-label={table ? 'Edit Table Draft' : 'Create Table'} className="tw-space-y-4">
    <div className="tw-flex tw-justify-between"><h2 className="tw-text-lg tw-font-semibold">{table ? `${table} / New Snapshot` : 'Create Table'} <span className="tw-text-xs tw-text-muted">{op ? 'Pending Commit' : 'Draft · local'}</span></h2><button className={buttonClass} disabled={busy} onClick={close}>Close editor</button></div>
    {!draft && !error && <p role="status">Loading Draft…</p>}
    {draft && <><p className="tw-text-xs tw-text-muted">{namespace.join(' / ')}{draft.source ? ` · Source Snapshot ${draft.source}` : ''}</p>
      {table && draft.base && draft.source !== mainSnapshot(draft.base) && <p role="status">Commit replaces current main with this historical Snapshot's edited file set. Files added in newer Snapshots are not included.</p>}
      <fieldset disabled={busy || !!op?.commit || legacySchema} className="tw-min-w-0 tw-space-y-3">
        {!table && <label>Table name <input ref={tableName} aria-label="Table name" aria-invalid={!draft.name.trim()} className={inputClass} value={draft.name} disabled={!!draft.base || !!op} onChange={event => change(value => ({ ...value, name: event.target.value }))} /></label>}
        <label className={`${buttonClass} tw-inline-block tw-cursor-pointer`}>Upload Parquet<input className="tw-sr-only" aria-label="Parquet files" type="file" multiple accept=".parquet" onChange={event => { const files = Array.from(event.target.files ?? []); event.target.value = ''; run(() => choose(files)); }} /></label>
        {draft.schema.fields.length > 0 ? <section aria-label="Schema preview"><h3 className="tw-font-semibold">Schema</h3><div className="tw-max-h-64 tw-overflow-auto"><SchemaTable key={JSON.stringify(draft.schema)} fields={draft.schema.fields} /></div></section>
          : <p className="tw-text-muted">Upload Parquet to create a Table.</p>}
        {(table || draft.files.length > 0) && <section aria-label="Draft files"><h3 className="tw-font-semibold">Files</h3>
          <FileGrid rows={rows} readonly={!!op} selected={chosen && rowKey(chosen)} select={setChosen} remove={remove} /></section>}
        {source.loading && showSource && <p role="status">Loading files…</p>}{source.error && showSource && <p role="alert">{source.error}</p>}
        {(previous.length > 0 || !!next) && <nav aria-label="Draft file pages" className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!previous.length} onClick={() => { setWindow(previous.at(-1)!); setPrevious(previous.slice(0, -1)); }}>Previous files</button><button className={buttonClass} disabled={!next || source.loading && showSource} onClick={() => { setPrevious([...previous, window].slice(-32)); setWindow(next!); }}>Next files</button></nav>}
      </fieldset>
      <div className="tw-flex tw-gap-2 tw-border-t tw-border-border tw-pt-4"><button className={buttonClass} disabled={busy || !!op} onClick={() => { try { setNotice(''); persist(current.current!); setError(''); setNotice('Saved locally'); } catch (reason) { setError(String(reason)); } }}>Save Draft</button>
        <button className={`${buttonClass} tw-bg-accent tw-text-white`} disabled={busy || !canCommit || legacySchema} onClick={startCommit}>{op ? 'Retry Commit' : 'Commit'}</button>
        <button className={buttonClass} disabled={busy || !!op?.uncertain} onClick={() => { try { forgetDraft(namespace, table, draft.identity, requestedSource, draft.revision); if (op) forget(op); close(); } catch (reason) { setError(String(reason)); } }}>Discard Draft</button></div>
    </>}
    {legacySchema && <p role="alert">This saved Draft contains manual Schema edits. Discard it and upload Parquet to start a new Draft.</p>}
    {error && <p role="alert">{error}</p>}{notice && !error && <p role="status">{notice}</p>}{progress && <p role="status">{progress}</p>}
    {busy && controller.current && <button className={buttonClass} onClick={() => controller.current?.abort()}>Cancel transfer</button>}
  </section>;
}
