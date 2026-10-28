// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { buttonClass, inputClass } from '../../access/Workbench';
import type { IceSchema } from './schema';
import type { ImportFile } from './storage';
import type { SourceFile } from './source';

export type FileRow = { added: ImportFile } | { source: SourceFile; removed: boolean };
export const rowKey = (row: FileRow) => 'added' in row ? row.added.identity : JSON.stringify([row.source.manifest, row.source.file.location]);
export const fileName = (row: FileRow) => 'added' in row ? row.added.name : row.source.file.location.split('/').pop()!;
export const typeName = (type: unknown) => typeof type === 'string' ? type : String((type as { type: string }).type);
export function FileGrid({ rows, schema, column, readonly, editableSchema, updateSchema, select, selectField, remove, selected }: {
  readonly: boolean; rows: FileRow[]; schema: IceSchema; column: number; editableSchema: boolean; updateSchema: (schema: IceSchema) => void;
  select: (row: FileRow) => void; selectField: (id: number) => void; remove: (row: FileRow) => void; selected?: string;
}) {
  const fields = schema.fields.slice(column * 12, column * 12 + 12);
  return <div className="tw-overflow-auto tw-max-h-[60vh]"><table aria-label="Parquet editor table" className="tw-w-full tw-text-left tw-text-sm">
    <thead className="tw-sticky tw-top-0 tw-bg-panel tw-z-10"><tr><th className="tw-p-3 tw-min-w-[180px]">Parquet file</th>
      {fields.map((field, index) => <th key={field.id} className="tw-p-3 tw-min-w-[130px]">{editableSchema ? <div className="tw-space-y-1">
        <input aria-label={`Field ${column * 12 + index + 1} name`} className={`${inputClass} tw-w-36`} value={field.name} onFocus={() => selectField(field.id)} onChange={event => updateSchema({ ...schema, fields: schema.fields.map(value => value.id === field.id ? { ...value, name: event.target.value } : value) })} />
        <select aria-label={`Field ${column * 12 + index + 1} type`} className={`${inputClass} tw-w-36`} value={typeName(field.type)} onChange={event => updateSchema({ ...schema, fields: schema.fields.map(value => value.id === field.id ? { ...value, type: event.target.value } : value) })}>
          {(typeof field.type === 'object' || !['boolean', 'int', 'long', 'float', 'double', 'string', 'binary', 'date', 'time', 'timestamp', 'timestamptz', 'uuid'].includes(field.type)) && <option value={typeName(field.type)}>{typeName(field.type)}</option>}
          {['boolean', 'int', 'long', 'float', 'double', 'string', 'binary', 'date', 'time', 'timestamp', 'timestamptz', 'uuid'].map(type => <option key={type}>{type}</option>)}
        </select><label className="tw-block tw-font-normal tw-text-xs"><input type="checkbox" aria-label={`Field ${column * 12 + index + 1} required`} checked={field.required} onChange={event => updateSchema({ ...schema, fields: schema.fields.map(value => value.id === field.id ? { ...value, required: event.target.checked } : value) })} /> Required</label>
        <button type="button" className="tw-text-xs tw-text-muted" aria-label={`Remove field ${field.name}`} onClick={() => updateSchema({ ...schema, fields: schema.fields.filter(value => value.id !== field.id) })}>Remove</button>
      </div> : <button type="button" onClick={() => selectField(field.id)}>{field.name}{field.required ? ' *' : ''}<span className="tw-block tw-font-normal tw-text-xs tw-text-muted">{typeName(field.type)}</span></button>}</th>)}
      <th className="tw-p-3">Change</th><th className="tw-p-3">Upload</th><th />
    </tr></thead><tbody>{rows.map(row => {
      const key = rowKey(row); const removed = 'source' in row && row.removed;
      return <tr key={key} aria-selected={selected === key} className={`tw-border-t tw-border-border ${selected === key ? 'tw-bg-accent/10' : ''} ${removed ? 'tw-opacity-60' : ''}`} onClick={() => select(row)}>
        <th className="tw-p-3 tw-font-normal"><button type="button" className="tw-text-accent tw-max-w-[240px] tw-truncate" title={fileName(row)}>{fileName(row)}</button></th>
        {fields.map(field => <td key={field.id} className="tw-p-3 tw-text-muted">{typeName(field.type)}</td>)}
        <td className="tw-p-3">{'added' in row ? 'Added' : removed ? 'Removed' : 'Kept'}</td>
        <td className="tw-p-3 tw-whitespace-nowrap">{'added' in row ? row.added.location ? 'Uploaded' : 'Reselect to resume' : 'Stored'}</td>
        <td className="tw-p-3"><button type="button" className={buttonClass} disabled={readonly || ('source' in row && (row.source.file.content !== 'Data' || row.source.file.format.toUpperCase() !== 'PARQUET'))} aria-label={`${removed ? 'Undo removal of' : 'Remove'} ${fileName(row)}`} onClick={event => { event.stopPropagation(); remove(row); }}>{removed ? 'Undo removal' : 'Remove'}</button></td>
      </tr>;
    })}</tbody>
  </table>{!rows.length && <p className="tw-p-6 tw-text-muted">No files on this page.</p>}</div>;
}
