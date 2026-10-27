// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { encoded, sampleLabel as displayId } from '../identity';
import type { Schema } from '../types';
import type { Mapping, LocalValue } from './mapping';
import { patchSample } from './draft';
import * as api from '../api';
import { useQuery } from '../query';

export function Fields({ mapping, required, change }: { mapping: Mapping; required: Set<string>; change: (value: Mapping) => void }) {
  const [name, setName] = useState(''); const [mandatory, setMandatory] = useState(false);
  const schema = mapping.schema!;
  const update = (schema: Schema) => change({ ...mapping, schema });
  return <fieldset className="tw-space-y-2"><legend className="tw-font-semibold">Fields</legend>
    <p className="tw-text-xs">Schema changes apply to every Sample. Every retained Sample needs a binding for each Required field; a cleared value is still a binding.</p>
    <table aria-label="Draft fields" className="tw-w-full tw-text-sm"><thead><tr><th>Name</th><th>Required</th><th /></tr></thead><tbody>{schema.fields.map(field => <tr key={field.name}><td>{field.name}</td><td><input aria-label={`Required ${field.name}`} type="checkbox" checked={field.required} disabled={required.has(field.name)} onChange={event => update({ ...schema, fields: schema.fields.map(value => value.name === field.name ? { ...value, required: event.target.checked } : value) })} /></td><td><button type="button" className={buttonClass} disabled={required.has(field.name)} onClick={() => change({ ...mapping, schema: { ...schema, fields: schema.fields.filter(value => value.name !== field.name) }, samples: mapping.samples.map(sample => sample.kind === 'remove' ? sample : { ...sample, fields: sample.fields.filter(value => value.name !== field.name) }) })}>Remove field {field.name}</button></td></tr>)}</tbody></table>
    <label>Field name <input aria-label="Field name" className={inputClass} value={name} maxLength={1024} onChange={event => setName(event.target.value)} /></label>
    <label><input aria-label="New field required" type="checkbox" checked={mandatory} onChange={event => setMandatory(event.target.checked)} /> Required</label>
    <button type="button" className={buttonClass} disabled={!name || schema.fields.some(value => value.name === name) || schema.fields.length >= 256} onClick={() => { update({ ...schema, fields: [...schema.fields, { name, required: mandatory }] }); setName(''); }}>Add field</button>
  </fieldset>;
}

export function Samples({ dataset, parent, initialSample, initialField, mapping, files, choose, change }: { dataset: string; parent?: string | null; initialSample?: string; initialField?: string; mapping: Mapping; files: string[]; choose: (files: File[]) => void; change: (value: Mapping) => void }) {
  const [id, setId] = useState(initialSample ? displayId(initialSample) : ''); const [field, setField] = useState(initialField ?? ''); const [kind, setKind] = useState('inline'); const [text, setText] = useState(''); const [file, setFile] = useState('');
  const [binaryId, setBinaryId] = useState<string | undefined>(initialSample);
  const names = mapping.schema!.fields.map(value => value.name); const selectedField = names.includes(field) ? field : names[0] ?? '';
  const sampleId = binaryId ?? encoded(new TextEncoder().encode(id));
  const select = (value: string) => { setBinaryId(value); setId(displayId(value)); };
  const update = (value: LocalValue) => { change(patchSample(mapping, { kind: 'upsert', id: sampleId, fields: [{ name: selectedField, value }] })); setText(''); };
  return <fieldset className="tw-space-y-3"><legend className="tw-font-semibold">Sample changes</legend>
    <p className="tw-text-xs">Unedited parent Samples and fields are preserved. Add fields to the same Sample ID to accumulate edits. Clear writes a tombstone; removing a Sample changes membership.</p>
    {parent && <ParentSamples dataset={dataset} parent={parent} select={select} />}
    <label className="tw-block">Local payload files <input aria-label="Local payload files" type="file" multiple onChange={event => { choose(Array.from(event.target.files ?? [])); event.target.value = ''; }} /></label>
    <label>Sample ID (UTF-8) <input aria-label="Sample ID (UTF-8)" className={inputClass} value={id} onChange={event => { setId(event.target.value); setBinaryId(undefined); }} /></label>
    <label>Sample field <select aria-label="Sample field" className={inputClass} value={selectedField} onChange={event => setField(event.target.value)}>{names.map(name => <option key={name}>{name}</option>)}</select></label>
    <label>Value source <select aria-label="Value source" className={inputClass} value={kind} onChange={event => setKind(event.target.value)}><option value="inline">Inline text</option><option value="file">Local file</option><option value="clear">Clear field</option></select></label>
    {kind === 'inline' && <label>Inline text <input aria-label="Inline text" className={inputClass} value={text} onChange={event => setText(event.target.value)} /></label>}
    {kind === 'file' && <label>Payload file <select aria-label="Payload file" className={inputClass} value={file} onChange={event => setFile(event.target.value)}><option value="">Select a file</option>{files.map(name => <option key={name}>{name}</option>)}</select></label>}
    <button type="button" className={buttonClass} disabled={!id || !selectedField || kind === 'file' && !file} onClick={() => update(kind === 'file' ? { kind: 'file', file } : kind === 'clear' ? { kind: 'tombstone' } : { kind: 'inline', data: encoded(new TextEncoder().encode(text)) })}>Apply Sample field</button>
    <button type="button" className={buttonClass} disabled={!id} onClick={() => change(patchSample(mapping, { kind: 'remove', id: sampleId }))}>Remove Sample</button>
    <table aria-label="Pending Sample changes" className="tw-w-full tw-text-sm"><thead><tr><th>Sample</th><th>Change</th><th>Fields</th><th /></tr></thead><tbody>{mapping.samples.map(sample => <tr key={sample.id}><td><button type="button" className="tw-text-accent" onClick={() => select(sample.id)}>{displayId(sample.id)}</button></td><td>{sample.kind}</td><td>{sample.kind === 'upsert' && sample.fields.map(value => `${value.name}: ${value.value.kind}`).join(', ')}</td><td><button className={buttonClass} type="button" onClick={() => change({ ...mapping, samples: mapping.samples.filter(value => value.id !== sample.id) })}>Undo {displayId(sample.id)}</button></td></tr>)}</tbody></table>
  </fieldset>;
}
function ParentSamples({ dataset, parent, select }: { dataset: string; parent: string; select: (value: string) => void }) {
  const [after, setAfter] = useState<string>(); const [previous, setPrevious] = useState<Array<string | undefined>>([]);
  const result = useQuery(`${dataset}:${parent}:${after}`, true, signal => api.samplePage(dataset, parent, after, undefined, undefined, signal));
  return <section aria-label="Parent Samples" className="tw-space-y-2 tw-text-sm"><h3>Parent Samples</h3>
    {result.error && <p role="alert">{result.error}</p>}
    <ul>{result.value?.entries.map(id => <li key={id}><button type="button" className="tw-text-accent" onClick={() => select(id)}>{displayId(id)}</button></li>)}</ul>
    <button type="button" className={buttonClass} disabled={result.loading || !previous.length} onClick={() => { setAfter(previous.at(-1)); setPrevious(previous.slice(0, -1)); }}>Previous Samples</button>
    <button type="button" className={buttonClass} disabled={result.loading || !result.value?.after} onClick={() => { setPrevious([...previous, after].slice(-32)); setAfter(result.value!.after!); }}>Next Samples</button>
  </section>;
}
