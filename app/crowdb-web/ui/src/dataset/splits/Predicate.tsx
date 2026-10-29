// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { inputClass } from '../../access/Workbench';
import { encoded } from '../identity';
import type { Predicate } from '../types';

function printable(value: string): string {
  const text = new TextDecoder('utf-8', { fatal: true }).decode(Uint8Array.from(atob(value), value => value.charCodeAt(0)));
  if (/[\u0000-\u001f\u007f]/.test(text)) throw new Error('Binary value');
  return text;
}
function ByteInput({ label, values, change, multiple = false }: { label: string; values: string[]; change: (value: string[]) => void; multiple?: boolean }) {
  const display = () => { try { return { encoding: 'utf8', text: values.map(printable).join('\n') }; } catch { return { encoding: 'base64', text: values.join('\n') }; } };
  const [encoding, setEncoding] = useState(display().encoding); const [text, setText] = useState(display().text); const [error, setError] = useState('');
  const element = useRef<HTMLTextAreaElement>(null);
  const sent = useRef(JSON.stringify(values));
  useEffect(() => { if (sent.current === JSON.stringify(values)) return; sent.current = JSON.stringify(values); const value = display(); setEncoding(value.encoding); setText(value.text); setError(''); element.current?.setCustomValidity(''); }, [JSON.stringify(values)]);
  const input = (text: string, format = encoding) => {
    setText(text); setEncoding(format);
    try {
      const entries = multiple ? text.split('\n').filter(Boolean) : [text];
      const value = entries.map(value => {
        if (format === 'utf8') return encoded(new TextEncoder().encode(value));
        if (btoa(atob(value)) !== value) throw new Error('Use canonical padded base64.');
        return value;
      });
      element.current?.setCustomValidity(''); setError(''); sent.current = JSON.stringify(value); change(value);
    } catch (reason) { element.current?.setCustomValidity('Invalid byte encoding.'); setError(String(reason)); }
  };
  return <div className="tw-space-y-1"><label className="tw-block">{label}<textarea ref={element} aria-label={label} rows={multiple ? 3 : 1} className={`${inputClass} tw-block tw-w-full`} value={text} onChange={event => input(event.target.value)} /></label>
    <label className="tw-text-xs">Encoding <select aria-label={`${label} encoding`} className={inputClass} value={encoding} onChange={event => { const format = event.target.value; try { input(format === 'base64' ? values.join('\n') : values.map(printable).join('\n'), format); } catch { setError('This value is binary; keep base64 encoding.'); } }}><option value="utf8">UTF-8</option><option value="base64">Base64</option></select></label>
    {error && <p role="alert">{error}</p>}
  </div>;
}

export function PredicateInput({ value, change }: { value: Predicate; change: (value: Predicate) => void }) {
  const [text, setText] = useState(JSON.stringify(value)); const [error, setError] = useState('');
  const sent = useRef(value); const input = useRef<HTMLTextAreaElement>(null);
  useEffect(() => { if (value !== sent.current) { setText(JSON.stringify(value)); setError(''); input.current?.setCustomValidity(''); sent.current = value; } }, [value]);
  const kind = ['all', 'explicit', 'prefix', 'keyword', 'range', 'field_match'].includes(value.kind) ? value.kind : 'custom';
  const select = (kind: string) => {
    if (kind === 'all') change({ kind: 'all' });
    if (kind === 'explicit') change({ kind, ids: [] });
    if (kind === 'prefix' || kind === 'keyword') change({ kind, value: '' });
    if (kind === 'range') change({ kind, start: '', end: '' });
    if (kind === 'field_match') change({ kind, field: '', value: '' });
  };
  return <div className="tw-space-y-2"><label>Selection <select aria-label="Selection kind" className={inputClass} value={kind} onChange={event => select(event.target.value)}>
    <option value="all">All Samples</option><option value="explicit">Explicit Sample IDs</option><option value="prefix">Sample ID prefix</option><option value="keyword">Sample ID keyword</option><option value="range">Sample ID range</option><option value="field_match">Field value match</option><option value="custom" disabled>Advanced expression</option>
  </select></label>
    {value.kind === 'explicit' && <ByteInput label="Selected Sample IDs (one per line)" values={value.ids} multiple change={ids => change({ kind: 'explicit', ids })} />}
    {(value.kind === 'prefix' || value.kind === 'keyword') && <ByteInput label="Sample ID match" values={[value.value]} change={values => change({ ...value, value: values[0] })} />}
    {value.kind === 'range' && <><ByteInput label="Range start (inclusive)" values={[value.start]} change={values => change({ ...value, start: values[0] })} /><ByteInput label="Range end (exclusive)" values={[value.end]} change={values => change({ ...value, end: values[0] })} /></>}
    {value.kind === 'field_match' && <><label>Field name <input aria-label="Selection field name" className={inputClass} value={value.field} onChange={event => change({ ...value, field: event.target.value })} /></label><ByteInput label="Field match value" values={[value.value]} change={values => change({ ...value, value: values[0] })} /></>}
    <details><summary>Advanced selection JSON</summary><label className="tw-block tw-text-xs">Selection rule (byte values use base64)
      <textarea ref={input} aria-label="Selection rule" className={`${inputClass} tw-block tw-w-full`} value={text} onChange={event => {
        setText(event.target.value);
        try { const parsed = JSON.parse(event.target.value) as Predicate; if (!parsed || typeof parsed.kind !== 'string') throw new Error('A rule requires kind.'); sent.current = parsed; change(parsed); event.target.setCustomValidity(''); setError(''); }
        catch (reason) { change(value); event.target.setCustomValidity('Invalid selection rule JSON.'); setError(String(reason)); }
      }} />{error && <span role="alert">{error}</span>}
    </label></details>
  </div>;
}
