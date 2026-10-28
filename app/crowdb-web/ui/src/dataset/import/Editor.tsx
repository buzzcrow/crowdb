// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useContext, useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { Dialog } from '../../components/Dialog';
import * as api from '../api';
import { defaultPolicy, type MatrixRow, type PublicationResult, type SplitSource } from '../types';
import { forgetOperation, type Operation } from '../operation';
import { useDraft } from '../operation/useDraft';
import { Publication } from '../Publication';
import { independentRoot, patchSample } from './draft';
import { parseMapping } from './mapping';
import { bytes, encoded } from '../identity';
import { newId } from './upload';
import { checkLimits, limits, parentRows, setCell } from '../editor/model';
import { Grid } from '../editor/Grid';
import { DraftDetails } from '../editor/Details';
import { EditorDetails } from '../EditorDetails';
import { useUploads } from '../editor/uploads';
import { commitDraft, loadSnapshotDraft } from '../editor/session';
import { useQuery } from '../query';
import { SourceSelection } from '../editor/SourceSelection';
import { validateEditor } from '../splits/Definitions';

export function ImportEditor({ dataset, parent, source, initialSample, initialField, recovery, close, complete }: {
  embedded?: boolean; dataset: string; parent?: string; source?: SplitSource; initialSample?: string; initialField?: string; recovery?: Operation; close: () => void; complete: (result: PublicationResult) => void;
}) {
  const element = useRef<HTMLElement>(null);
  const draft = useDraft(recovery); const { op, change, install, setError } = draft;
  const uploads = useUploads(draft); const [busy, setBusy] = useState(false); const [ready, setReady] = useState(false);
  const transfer = useRef<AbortController>(); const details = useContext(EditorDetails);
  const [selection, setSelection] = useState<{ id?: string; field?: string }>({ id: initialSample, field: initialField });
  const [fieldDialog, setFieldDialog] = useState(false); const [fieldName, setFieldName] = useState(''); const [required, setRequired] = useState(false);
  const [page, setPage] = useState(0); const [column, setColumn] = useState(0); const [showParent, setShowParent] = useState(!initialSample);
  const [parentCursor, setParentCursor] = useState<string>(); const [parentPrevious, setParentPrevious] = useState<Array<string | undefined>>([]);
  const [advanced, setAdvanced] = useState(''); const [uploadField, setUploadField] = useState(''); const [notice, setNotice] = useState('');
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); setNotice(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  useEffect(() => {
    const controller = new AbortController();
    void loadSnapshotDraft(dataset, parent, source, recovery, controller.signal).then(({ value, error }) => {
      if (!controller.signal.aborted) { install(value); setError(error); setShowParent(!!value.source?.parent); setReady(true); }
    }).catch(reason => { if (!controller.signal.aborted) setError(String(reason)); });
    return () => { controller.abort(); transfer.current?.abort(); };
  }, []);
  useEffect(() => {
    details(op?.editing ? <DraftDetails key={JSON.stringify(selection)} op={op} {...selection} /> : null);
    return () => details(null);
  }, [op, selection, details]);
  const fields = op?.mapping?.schema?.fields ?? [];
  const visible = fields.slice(column * 12, column * 12 + 12).map(field => field.name);
  const base = op?.source?.parent;
  const inherited = useQuery(JSON.stringify([dataset, base, parentCursor, visible, initialSample]), ready && !!base && showParent, async signal => {
    const samples = initialSample ? { entries: [initialSample], after: null } : await api.samplePage(dataset, base!, parentCursor, undefined, undefined, signal);
    const rows = await parentRows(op!, samples.entries, visible, signal);
    return { samples, rows };
  });
  const changes = op?.mapping?.samples ?? [];
  const rows: MatrixRow[] = showParent && base ? inherited.value?.rows ?? [] : changes.slice(page * 20, page * 20 + 20).map(sample => ({ id: sample.id, fields: visible.map(() => null) }));
  const targetField = uploadField || fields[0]?.name || 'content';
  const checkId = async (id: string) => {
    bytes(id);
    if (!id || changes.some(sample => sample.id === id)) throw new Error('Sample ID already exists in this Draft. Choose another ID.');
    if (base && (await api.control<{ contains: boolean }>(`${api.snapshotPath(dataset, base)}/contains?${new URLSearchParams({ sample: id })}`)).contains) throw new Error('Sample ID already exists in the parent. Edit that row explicitly.');
  };
  const add = async () => {
    const id = encoded(new TextEncoder().encode(`sample-${newId().slice(0, 8)}`)); await checkId(id);
    change(value => { const next = { ...value, newSamples: [...value.newSamples ?? [], id], mapping: patchSample(value.mapping!, { kind: 'upsert', id, fields: [] }) }; checkLimits(next); return next; }); setShowParent(false); setPage(Math.floor(changes.length / 20));
  };
  const choose = async (files: File[]) => {
    if (!fields.length) throw new Error('Add a Field first.');
    if (files.length > limits.batch) throw new Error(`Choose at most ${limits.batch} files at once.`);
    if (files.some(file => !file.size || file.size > limits.fileBytes)) throw new Error('Files must be nonempty and at most 100 MiB.');
    const ids = files.map(file => encoded(new TextEncoder().encode(file.name)));
    if (new Set(ids).size !== ids.length) throw new Error('Files need distinct names for automatic Sample IDs.');
    for (const id of ids) await checkId(id);
    if (changes.length + files.length > limits.edits) throw new Error('The Draft exceeds 1000 Sample changes.');
    change(value => ({ ...value, newSamples: [...value.newSamples ?? [], ...ids] }));
    setShowParent(false); setPage(Math.floor(changes.length / 20));
    await uploads.transfer(files.map((file, index) => ({ id: ids[index], field: targetField, file })));
  };
  const commit = async () => {
    validateEditor(element.current); transfer.current = new AbortController();
    try { complete(await commitDraft(draft, transfer.current.signal)); }
    finally { transfer.current = undefined; }
  };
  return <section ref={element} aria-label="Import Snapshot" className="tw-space-y-4">
    <div className="tw-flex tw-items-center tw-justify-between"><h2 className="tw-text-lg tw-font-semibold">New Snapshot · {dataset} <span className="tw-text-sm tw-text-muted">{op?.editing ? 'Draft · local' : 'Commit'}</span></h2><button className={buttonClass} disabled={busy && !uploads.busy} onClick={close}>Close editor</button></div>
    {!ready && !draft.error && <p role="status">Loading Draft…</p>}
    {op && <p className="tw-text-xs tw-text-muted">{base ? `Parent: ${base.slice(0, 12)}… · existing Samples and fields are kept` : 'Add rows and columns, then Commit an immutable Snapshot.'}</p>}
    {op?.editing && <fieldset disabled={!ready || busy || uploads.busy} className="tw-space-y-4">
      <div className="tw-flex tw-flex-wrap tw-items-center tw-gap-2"><button className={buttonClass} onClick={() => run(add)}>Add Sample</button><button className={buttonClass} onClick={() => { setFieldName(''); setRequired(false); setFieldDialog(true); }}>Add Field</button>
        <label className="tw-text-xs">Upload into <select aria-label="Upload target field" className={inputClass} value={targetField} onChange={event => setUploadField(event.target.value)}>{fields.map(field => <option key={field.name}>{field.name}</option>)}</select></label>
        <label className={`${buttonClass} tw-cursor-pointer`}>Upload files<input aria-label="Local payload files" className="tw-sr-only" type="file" multiple onChange={event => { const files = Array.from(event.target.files ?? []); event.target.value = ''; run(() => choose(files)); }} /></label>
      </div>
      {base && <nav aria-label="Draft rows" className="tw-flex tw-gap-2"><button className={buttonClass} aria-pressed={showParent} onClick={() => setShowParent(true)}>Parent Samples</button><button className={buttonClass} aria-pressed={!showParent} onClick={() => setShowParent(false)}>Changes ({changes.length})</button></nav>}
      {inherited.error && showParent && <p role="alert">{inherited.error}</p>}
      <Grid op={op} selectedId={selection.id} rows={rows} fields={visible} apply={(id, field, value) => change(current => setCell(current, id, field, value))} upload={(id, field, file) => run(() => uploads.transfer([{ id, field, file }]))} select={(id, field) => setSelection({ id, field })} selectField={field => setSelection({ field })}
        rename={(id, next) => run(async () => { await checkId(next); change(value => ({ ...value, newSamples: value.newSamples?.map(value => value === id ? next : value), mapping: { ...value.mapping!, samples: value.mapping!.samples.map(sample => sample.id === id ? { ...sample, id: next } : sample) } })); setSelection(value => value.id === id ? { ...value, id: next } : value); })}
        remove={id => change(value => ({ ...value, mapping: !base || value.newSamples?.includes(id) ? { ...value.mapping!, samples: value.mapping!.samples.filter(sample => sample.id !== id) } : patchSample(value.mapping!, { kind: 'remove', id }) }))}
        restore={id => change(value => ({ ...value, mapping: { ...value.mapping!, samples: value.mapping!.samples.filter(sample => sample.id !== id) } }))}
        removeField={name => change(value => ({ ...value, mapping: { ...value.mapping!, schema: { ...value.mapping!.schema!, fields: fields.filter(field => field.name !== name) }, samples: value.mapping!.samples.map(sample => sample.kind === 'upsert' ? { ...sample, fields: sample.fields.filter(field => field.name !== name) } : sample) } }))} />
      <div className="tw-flex tw-flex-wrap tw-gap-2"><button className={buttonClass} disabled={showParent && !!base ? !parentPrevious.length : !page} onClick={() => { if (showParent && base) { setParentCursor(parentPrevious.at(-1)); setParentPrevious(parentPrevious.slice(0, -1)); } else setPage(page - 1); }}>Previous rows</button>
        <button className={buttonClass} disabled={showParent && !!base ? !inherited.value?.samples.after : (page + 1) * 20 >= changes.length} onClick={() => { if (showParent && base) { setParentPrevious([...parentPrevious, parentCursor]); setParentCursor(inherited.value!.samples.after!); } else setPage(page + 1); }}>Next rows</button>
        <button className={buttonClass} disabled={!column} onClick={() => setColumn(column - 1)}>Previous columns</button><button className={buttonClass} disabled={(column + 1) * 12 >= fields.length} onClick={() => setColumn(column + 1)}>Next columns</button></div>
      <details><summary className="tw-text-xs tw-text-muted">Advanced</summary><div className="tw-space-y-3 tw-pt-3">
        {(op.snapshot || op.inheritedSource) && <label className="tw-block"><input type="checkbox" aria-label="Independent root" checked={!op.source?.parent} onChange={event => { change(value => independentRoot(value, event.target.checked)); setShowParent(!event.target.checked); }} /> Independent root (only edited rows)</label>}
        <SourceSelection op={op} change={change} />
        {!!base && !!op.parentSplits?.splits.length && <label className="tw-block"><input type="checkbox" aria-label="Keep parent Splits" checked={!!op.source?.splits.length} onChange={event => change(value => ({ ...value, source: { ...value.source!, ...(event.target.checked ? value.parentSplits! : { splits: [], policy: defaultPolicy }) } }))} /> Keep frozen parent Splits</label>}
        <label className="tw-block">Sample mapping JSON<textarea aria-label="Sample mapping JSON" className={`${inputClass} tw-w-full tw-font-mono`} rows={5} value={advanced || JSON.stringify(op.mapping)} onChange={event => setAdvanced(event.target.value)} /></label>
        <button className={buttonClass} onClick={() => { try { const mapping = parseMapping(advanced || JSON.stringify(op.mapping), []); if (mapping.schema === null) mapping.schema = op.parentSchema!; change(value => { const next = { ...value, mapping }; checkLimits(next); return next; }); setAdvanced(''); setShowParent(false); } catch (reason) { setError(String(reason)); } }}>Apply batch mapping</button>
        <p>Up to 1000 changes, 64 Fields, 64 KiB per text value and 100 MiB per file.</p><p>Snapshot <span data-testid="editing-resource-id">{op.resource}</span></p>
      </div></details>
    </fieldset>}
    {draft.error && <p role="alert">{draft.error}</p>}{notice && <p role="status">{notice}</p>}
    {uploads.busy && <p role="status">{uploads.progress || 'Uploading…'} <button className={buttonClass} onClick={uploads.cancel}>Cancel upload</button></p>}
    {op && <div className="tw-flex tw-gap-3 tw-border-t tw-border-border tw-pt-4"><button className={buttonClass} disabled={busy || !ready || uploads.busy || !op.editing} onClick={() => run(async () => { await draft.persist(); setNotice('Draft saved in this browser.'); })}>Save Draft</button>
      <button className={`${buttonClass} tw-bg-accent tw-text-white`} disabled={busy || !ready || uploads.busy || !!op.conflict} onClick={() => run(commit)}>{busy && !op.editing ? 'Committing…' : 'Commit'}</button>
      {!op.pending && <button className={buttonClass} disabled={busy || uploads.busy} onClick={() => run(async () => { await draft.saving.current; await forgetOperation(draft.current.current!); close(); })}>Discard Draft</button>}</div>}
    {op?.editing && <p role="status" className="tw-text-xs tw-text-muted">{draft.dirty ? 'Saving edits…' : 'Draft saved locally. Files upload separately; Commit publishes the Snapshot.'}</p>}
    {op && !op.editing && <Publication operation={op} update={install} complete={complete} close={close} busy={busy} run={run} />}
    {fieldDialog && <Dialog isOpen title="Add Field" onClose={() => setFieldDialog(false)} confirmLabel="Add Field" onConfirm={() => { try {
      if (!fieldName.trim() || new TextEncoder().encode(fieldName).length > 1024 || fields.some(field => field.name === fieldName)) throw new Error('Use a unique, nonempty Field name of at most 1024 bytes.');
      change(value => { const next = { ...value, mapping: { ...value.mapping!, schema: { ...value.mapping!.schema!, fields: [...fields, { name: fieldName, required }] } } }; checkLimits(next); return next; }); setFieldDialog(false);
    } catch (reason) { setError(String(reason)); } }}><label>Field name<input aria-label="Field name" className={`${inputClass} tw-w-full`} value={fieldName} onChange={event => setFieldName(event.target.value)} /></label><label><input type="checkbox" aria-label="New field required" checked={required} onChange={event => setRequired(event.target.checked)} /> Required</label></Dialog>}
  </section>;
}
