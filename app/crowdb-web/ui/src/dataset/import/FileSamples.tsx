// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useEffect, useRef, useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { encoded } from '../identity';
import * as api from '../api';
import type { Operation } from '../operation';
import { patchSample } from './draft';

export function FileSamples({ op, change, run }: { op: Operation; change: (value: Operation) => void; run: (action: () => Promise<void>) => void }) {
  const [field, setField] = useState(''); const [overwrite, setOverwrite] = useState(false);
  const controller = useRef<AbortController>();
  useEffect(() => () => controller.current?.abort(), []);
  const names = op.mapping!.schema!.fields.map(value => value.name); const target = names.includes(field) ? field : names[0] ?? '';
  const rows = op.fileSamples ?? [];
  const apply = async () => {
    controller.current = new AbortController(); const signal = controller.current.signal;
      const ids = rows.map(row => encoded(new TextEncoder().encode(row.sample)));
      if (rows.some(row => !row.sample) || new Set(ids).size !== ids.length) throw new Error('Use distinct, nonempty Sample IDs.');
      let mapping = op.mapping!;
      for (let index = 0; index < rows.length; index++) {
        const id = ids[index]; const row = rows[index];
        const pending = mapping.samples.some(sample => sample.id === id);
        const inherited = !overwrite && op.source?.parent ? (await api.control<{ contains: boolean }>(`${api.snapshotPath(op.dataset, op.source.parent)}/contains?${new URLSearchParams({ sample: id })}`, 'GET', undefined, signal, op.datasetId)).contains : false;
        if (!overwrite && (pending || inherited)) throw new Error(`${row.sample} already exists. Choose another ID or explicitly allow updates.`);
        mapping = patchSample(mapping, { kind: 'upsert', id, fields: [{ name: target, value: { kind: 'file', file: row.file } }] });
      }
      signal.throwIfAborted(); change({ ...op, mapping });
  };
  return rows.length ? <fieldset className="tw-space-y-2"><legend className="tw-font-semibold">Files to Samples</legend>
    <p className="tw-text-xs">One file binds one Field of one Sample. Review IDs before applying. Explicit updates preserve other fields.</p>
    <label>Target field <select aria-label="Files target field" className={inputClass} value={target} onChange={event => setField(event.target.value)}>{names.map(name => <option key={name}>{name}</option>)}</select></label>
    <table aria-label="File Sample mappings" className="tw-w-full tw-text-sm"><thead><tr><th>File</th><th>Bytes</th><th>Sample ID</th><th /></tr></thead><tbody>{rows.map((row, index) => <tr key={row.file}><td>{row.file}</td><td>{op.fileChecks?.[row.file]?.size}</td><td><input aria-label={`Sample ID for ${row.file}`} className={inputClass} value={row.sample} onChange={event => change({ ...op, fileSamples: rows.map((value, position) => index === position ? { ...value, sample: event.target.value } : value) })} /></td><td><button className={buttonClass} type="button" onClick={() => {
      const checks = { ...op.fileChecks }; delete checks[row.file];
      change({ ...op, fileChecks: checks, fileSamples: rows.filter(value => value.file !== row.file), mapping: { ...op.mapping!, samples: op.mapping!.samples.map(sample => sample.kind === 'remove' ? sample : { ...sample, fields: sample.fields.filter(field => field.value.kind !== 'file' || field.value.file !== row.file) }).filter(sample => sample.kind === 'remove' || sample.fields.length > 0) } });
    }}>Remove file {row.file}</button></td></tr>)}</tbody></table>
    <label><input aria-label="Allow updating existing Sample IDs" type="checkbox" checked={overwrite} onChange={event => setOverwrite(event.target.checked)} /> Allow updating existing Sample IDs</label>
    <button type="button" className={buttonClass} disabled={!target} onClick={() => run(apply)}>Apply file mappings</button>
  </fieldset> : null;
}
