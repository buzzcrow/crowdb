// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState } from 'react';
import { buttonClass } from '../access/Workbench';
import { byteSize, Fields, Records, scalar } from './Fields';
import type { Column, Inspection } from './types';

export function ParquetInspector({ data }: { data: Inspection }) {
  const [selected, setSelected] = useState<{ group: string; column: Column } | null>(null);
  const [columnPage, setColumnPage] = useState(0);
  const [view, setView] = useState<'layout' | 'schema' | 'footer'>('layout');
  const groups = data.groups ?? [];
  const maxColumns = Math.max(0, ...groups.map(g => g.columns.length));
  const choose = (group: string, column: Column) => setSelected({ group, column });
  return <div className="tw-space-y-4">
    <Fields values={{ 'File size': byteSize(data.size), 'Physical rows': data.physical_rows, 'Row groups': data.row_group_count, Writer: data.footer?.writer, 'Footer size': byteSize(data.footer?.length), Content: data.content }} />
    <nav aria-label="Parquet sections" className="tw-flex tw-gap-2">{(['layout', 'schema', 'footer'] as const).map(name => <button key={name} className={buttonClass} aria-pressed={view === name} onClick={() => setView(name)}>{name === 'layout' ? 'Row group layout' : name === 'schema' ? 'File schema' : 'Footer'}</button>)}</nav>
    {view === 'schema' && <Records label="Parquet schema" headings={['Field', 'ID', 'Physical / logical type', 'Repetition', 'Children']} rows={(Array.isArray(data.schema) ? data.schema : []).map(f => [scalar(f.name), scalar(f.id), `${scalar(f.physical_type)} / ${scalar(f.logical_type ?? f.converted_type)}`, scalar(f.repetition), scalar(f.children)])} />}
    {view === 'footer' && <><Fields values={{ Offset: data.footer?.offset, Length: data.footer?.length, Version: data.footer?.version, Writer: data.footer?.writer }} /><Records label="Footer properties" headings={['Key', 'Value']} rows={(data.footer?.properties ?? []).map(([k, v]) => [k, <span className="tw-break-all">{scalar(v)}</span>])} /></>}
    {view === 'layout' && <>
      <h2 className="tw-font-medium">File byte layout</h2>
      <p className="tw-text-xs tw-text-muted">Absolute byte offsets · current row-group and column page · unshown ranges are not inferred. Small spans can be selected in the column table.</p>
      <div className="tw-flex tw-justify-between tw-text-xs tw-text-muted"><span>0 B</span><span>{data.size} B</span></div>
      <div className="tw-relative tw-h-12 tw-bg-panel" aria-label="Parquet byte layout">{groups.flatMap(group => group.columns.slice(columnPage * 12, columnPage * 12 + 12).map((column, i) => <button key={`${group.index}:${i}`} title={`${group.index} / ${column.path.join('.')} · ${column.offset} B · ${column.compressed} B`} aria-label={`Byte span row group ${group.index} column ${column.path.join('.')}`} className="tw-absolute tw-top-1 tw-h-10 tw-bg-accent/35 hover:tw-bg-accent/60" style={{ left: `${Number(column.offset) / Number(data.size) * 100}%`, width: `${Number(column.compressed) / Number(data.size) * 100}%` }} onClick={() => choose(group.index, column)} />))}<button aria-label="Footer byte span" title="Footer metadata" className="tw-absolute tw-top-1 tw-h-10 tw-bg-text/60" style={{ left: `${Number(data.footer?.offset) / Number(data.size) * 100}%`, width: `${Number(data.footer?.length) / Number(data.size) * 100}%` }} onClick={() => setView('footer')} /></div>
      <div className="tw-flex tw-items-center tw-gap-2"><h2 className="tw-font-medium">Row groups</h2>{maxColumns > 12 && <><button className={buttonClass} disabled={!columnPage} onClick={() => setColumnPage(p => p - 1)}>Previous columns</button><button className={buttonClass} disabled={(columnPage + 1) * 12 >= maxColumns} onClick={() => setColumnPage(p => p + 1)}>Next columns</button><span className="tw-text-xs">Columns {columnPage * 12 + 1}–{Math.min(maxColumns, (columnPage + 1) * 12)}</span></>}</div>
      <div aria-label="Parquet row groups" className="tw-space-y-3">{groups.map(group => <section key={group.index} className="tw-border tw-border-border tw-rounded tw-p-3 tw-space-y-2">
        <h3 className="tw-text-sm">Row group {group.index} · {group.rows} rows · {byteSize(group.columns.reduce((n, c) => n + BigInt(c.compressed), 0n).toString())} compressed</h3>
        <div className="tw-flex tw-gap-1 tw-h-9" aria-label={`Row group ${group.index} size distribution`}>{group.columns.slice(columnPage * 12, columnPage * 12 + 12).map((c, i) => <button key={i} aria-label={`Row group ${group.index} column ${c.path.join('.')}`} title={`${c.path.join('.')} · ${byteSize(c.compressed)}`} className={`tw-min-w-0 tw-bg-accent/20 hover:tw-bg-accent/40 ${selected?.group === group.index && selected.column.path.join('.') === c.path.join('.') ? 'tw-ring-2 tw-ring-accent' : ''}`} style={{ flex: Number(c.compressed) }} onClick={() => choose(group.index, c)} />)}</div>
        <Records label={`Row group ${group.index} columns`} headings={['Column', 'Type', 'Codec', 'Compressed', 'Uncompressed']} rows={group.columns.slice(columnPage * 12, columnPage * 12 + 12).map(c => [<button className="tw-text-accent tw-underline" onClick={() => choose(group.index, c)}>{c.path.join('.')}</button>, c.logical_type ?? c.physical_type, c.codec, byteSize(c.compressed), byteSize(c.uncompressed)])} />
      </section>)}</div>
      {selected && <section aria-label="Column chunk details" className="tw-border-t tw-border-border tw-pt-4 tw-space-y-3"><h2 className="tw-font-medium">Row group {selected.group} / {selected.column.path.join('.')}</h2><ColumnDetails column={selected.column} /></section>}
    </>}
    <p className="tw-text-xs tw-text-muted">Logical metadata ranges: {byteSize(data.logical_metadata_bytes)} · Data pages read: {data.data_page_bytes} B. Physical storage reads may include block framing. Row counts do not apply delete files.</p>
  </div>;
}
function ColumnDetails({ column: c }: { column: Column }) {
  return <Fields values={{ 'Field ID': c.field_id, 'Physical type': c.physical_type, 'Logical type': c.logical_type, 'Converted type': c.converted_type, Precision: c.precision, Scale: c.scale, Codec: c.codec, Encodings: c.encodings.join(', '), 'Byte offset': c.offset, 'Data page offset': c.data_offset, 'Compressed bytes': c.compressed, 'Uncompressed bytes': c.uncompressed, 'Value count': c.values, 'Null count': c.statistics?.nulls, 'Distinct count': c.statistics?.distinct, 'Lower bound': c.statistics?.lower, 'Upper bound': c.statistics?.upper, 'Lower bound exact': c.statistics?.lower_exact, 'Upper bound exact': c.statistics?.upper_exact }} />;
}
