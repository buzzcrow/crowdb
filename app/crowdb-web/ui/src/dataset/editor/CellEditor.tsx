// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import * as api from '../api';
import type { LocalValue } from '../import/mapping';
import type { FieldMetadata } from '../types';
import { textValue } from './model';

function decode(value?: LocalValue) {
  if (value?.kind !== 'inline') return '';
  try { return new TextDecoder('utf-8', { fatal: true }).decode(Uint8Array.from(atob(value.data), c => c.charCodeAt(0))); }
  catch { return null; }
}
export function CellEditor({ dataset, parent, id, sample, field, value, inherited, fileLabel, disabled, select, apply, upload }: {
  dataset: string; parent?: string; id: string; sample: string; field: string; value?: LocalValue;
  inherited?: FieldMetadata | null; fileLabel?: string; disabled: boolean; select: () => void;
  apply: (value: LocalValue) => void; upload: (file: File) => void;
}) {
  const [text, setText] = useState(() => decode(value) ?? '');
  const [loaded, setLoaded] = useState(false); const [loading, setLoading] = useState(false);
  const [error, setError] = useState(''); const input = useRef<HTMLTextAreaElement>(null);
  const controller = useRef<AbortController>();
  useEffect(() => () => controller.current?.abort(), []);
  useEffect(() => { setText(decode(value) ?? ''); }, [value]);
  const inheritedText = !value && inherited?.storage === 'inline';
  const file = fileLabel || (value?.kind === 'upload' ? `File · ${value.length} B` : !value && inherited?.storage === 'payload' ? `File · ${inherited.length} B` : undefined);
  const focus = () => {
    select();
    if (!parent || !inheritedText || loaded || loading) return;
    controller.current = new AbortController(); const signal = controller.current.signal; setLoading(true);
    void api.preview(dataset, parent, id, field, signal).then(result => {
      if (signal.aborted) return;
      if (result.truncated || result.data === null) throw new Error('Existing text cannot be loaded for editing.');
      const text = decode({ kind: 'inline', data: result.data });
      if (text === null) throw new Error('Binary value: enter replacement text or upload a file.');
      setText(text); setLoaded(true);
    }).catch(reason => { if (!signal.aborted) { setError(String(reason)); setLoaded(true); } })
      .finally(() => { if (!signal.aborted) setLoading(false); });
  };
  const change = (next: string) => {
    setText(next);
    try { apply(textValue(next)); input.current?.setCustomValidity(''); setError(''); }
    catch (reason) { const message = String(reason); input.current?.setCustomValidity(message); setError(message); }
  };
  return <div className="tw-min-w-[240px] tw-max-w-[320px] tw-space-y-1">
    {file && <button type="button" className="tw-block tw-max-w-full tw-truncate tw-text-accent" title={file} onClick={select}>{file}</button>}
    <div className="tw-flex tw-items-center tw-gap-2">
      <label className={`${buttonClass} tw-cursor-pointer tw-whitespace-nowrap`}>{file ? 'Replace file' : 'Upload file'}
        <input type="file" aria-label={`Upload ${field} for ${sample}`} className="tw-sr-only" disabled={disabled} onChange={event => { const file = event.target.files?.[0]; event.target.value = ''; if (file) { select(); upload(file); } }} />
      </label><span className="tw-text-xs tw-text-muted">or</span>
      <textarea ref={input} aria-label={`${field} for ${sample}`} className={`${inputClass} tw-w-40 tw-resize-y`} rows={1}
        disabled={disabled} readOnly={loading || (inheritedText && !loaded)} value={text} onFocus={focus}
        placeholder={loading ? 'Loading…' : inheritedText && !loaded ? 'Read existing text…' : 'Input text…'}
        onChange={event => change(event.target.value)} />
    </div>
    {(value && value.kind !== 'tombstone' || !value && inherited) && <button type="button" className="tw-text-xs tw-text-muted" disabled={disabled} aria-label={`Clear ${field} for ${sample}`} onClick={() => { apply({ kind: 'tombstone' }); input.current?.setCustomValidity(''); setError(''); setText(''); }}>Clear</button>}
    {decode(value) === null && <p className="tw-text-xs">Binary value. Upload a file or enter replacement text.</p>}
    {error && <p role="alert" className="tw-text-xs">{error}</p>}
  </div>;
}
