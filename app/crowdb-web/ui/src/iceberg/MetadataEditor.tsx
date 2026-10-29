// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useState } from 'react';
import { getApiBase } from '../api';
import { buttonClass, inputClass, JsonView } from '../access/Workbench';
import { iceberg, AccessError } from '../access/native';
import { requestKey } from './import/request';
import { tablePath } from './import/commit';
import type { TableLoad } from './types';
import { advancedRequest, propertyRequest, type PropertyEdit } from './metadataEdits';
interface Edit { uuid: string; original: Record<string, string>; properties: PropertyEdit[]; advanced?: string; advancedMode?: boolean; key: string; body?: string; unknown?: boolean }
export function MetadataEditor({ namespace, name, close, complete }: { namespace: string[]; name: string; close: () => void; complete: () => Promise<void> }) {
  const path = tablePath({ namespace, name }); const storage = `crowdb.iceberg.metadata:${getApiBase()}:${path}`;
  const [edit, setEdit] = useState<Edit>(); const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const [observation, setObservation] = useState<unknown>();
  const change = (value: Edit) => { const text = JSON.stringify(value); if (new TextEncoder().encode(text).length > 1024 * 1024) throw new Error('Metadata edit exceeds 1 MiB.'); localStorage.setItem(storage, text); setEdit({ ...value }); };
  useEffect(() => {
    const controller = new AbortController();
    void (async () => {
      const saved = localStorage.getItem(storage);
      if (saved) { setEdit(JSON.parse(saved)); return; }
      const table: TableLoad = await iceberg(path, '', 'GET', undefined, controller.signal);
      const original = table.metadata.properties as Record<string, string> ?? {};
      if (!controller.signal.aborted) setEdit({ uuid: String(table.metadata['table-uuid']), original, key: requestKey(), properties: Object.entries(original).map(([name, value]) => ({ name, value, remove: false })) });
    })().catch(reason => { if (!controller.signal.aborted) setError(String(reason)); });
    return () => controller.abort();
  }, [storage, path]);
  const run = (action: () => Promise<void>) => { setBusy(true); setError(''); void action().catch(reason => setError(String(reason))).finally(() => setBusy(false)); };
  const submit = async () => {
    if (!edit) return;
    if (!edit.body) {
      edit.body = edit.advancedMode ? advancedRequest(edit.uuid, edit.advanced ?? '') : propertyRequest(edit.uuid, edit.original ?? {}, edit.properties);
    }
    edit.unknown = true; change(edit);
    try { await iceberg(path, '', 'POST', edit.body, undefined, edit.key); localStorage.removeItem(storage); await complete(); }
    catch (reason) { if (reason instanceof AccessError && !reason.unknownOutcome) { edit.unknown = false; change(edit); } throw reason; }
  };
  const update = (index: number, value: Partial<Edit['properties'][number]>) => { if (edit) try { change({ ...edit, properties: edit.properties.map((property, position) => position === index ? { ...property, ...value } : property) }); setError(''); } catch (reason) { setError(String(reason)); } };
  return <section aria-label="Commit metadata" className="tw-space-y-3"><div className="tw-flex tw-justify-between"><h2 className="tw-text-lg tw-font-semibold">Edit Table properties · {name}</h2><button className={buttonClass} disabled={busy} onClick={close}>Close editor</button></div>
    {!edit && !error && <p role="status">Loading Table identity…</p>}
    {edit && <fieldset disabled={busy || !!edit.body} className="tw-space-y-2"><table aria-label="Table property edits" className="tw-w-full tw-text-sm"><thead><tr><th>Name</th><th>Value</th><th>Remove</th></tr></thead><tbody>{edit.properties.map((property, index) => <tr key={index}><td><input aria-label={`Property ${index + 1} name`} className={inputClass} value={property.name} onChange={event => update(index, { name: event.target.value })} /></td><td><input aria-label={`Property ${index + 1} value`} className={inputClass} value={property.value} onChange={event => update(index, { value: event.target.value })} /></td><td><input aria-label={`Remove property ${index + 1}`} type="checkbox" checked={property.remove} onChange={event => update(index, { remove: event.target.checked })} /></td></tr>)}</tbody></table>
      <button className={buttonClass} disabled={edit.properties.length >= 256 || edit.advancedMode} onClick={() => { try { change({ ...edit, properties: [...edit.properties, { name: '', value: '', remove: false }] }); } catch (reason) { setError(String(reason)); } }}>Add property</button>
      <details><summary>Advanced metadata request</summary><label><input aria-label="Use advanced metadata request" type="checkbox" checked={!!edit.advancedMode} onChange={event => { try { change({ ...edit, advancedMode: event.target.checked, advanced: edit.advanced ?? propertyRequest(edit.uuid, edit.original ?? {}, edit.properties) }); } catch (reason) { setError(String(reason)); } }} /> Use explicit requirements and updates</label>
        <textarea aria-label="Metadata request JSON" rows={10} className={`${inputClass} tw-w-full tw-font-mono`} disabled={!edit.advancedMode} value={edit.advanced ?? ''} onChange={event => { try { change({ ...edit, advanced: event.target.value }); } catch (reason) { setError(String(reason)); } }} />
      </details></fieldset>}
    {error && <p role="alert">{error}</p>}{edit?.unknown && <p role="status">The commit reply is unavailable. Resume with the saved request and key; inspect the current Table before discarding.</p>}
    <button className={buttonClass} disabled={busy || !edit} onClick={() => run(submit)}>{edit?.body ? 'Retry identical request' : 'Submit metadata'}</button>
    {edit && <button className={buttonClass} disabled={busy} onClick={() => run(async () => { const table = await iceberg(path, ''); setObservation(table); })}>Inspect current Table</button>}
    {edit && !edit.unknown && <button className={buttonClass} disabled={busy} onClick={() => { localStorage.removeItem(storage); close(); }}>Discard local request</button>}
    {observation !== undefined && <JsonView value={observation} />}
  </section>;
}
