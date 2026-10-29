// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { buttonClass } from '../../access/Workbench';
import type { ImportFile } from './storage';
import type { SourceFile } from './source';

export type FileRow = { added: ImportFile } | { source: SourceFile; removed: boolean };
export const rowKey = (row: FileRow) => 'added' in row ? row.added.identity : JSON.stringify([row.source.manifest, row.source.file.location]);
export const fileName = (row: FileRow) => 'added' in row ? row.added.name : row.source.file.location.split('/').pop()!;
export function FileGrid({ rows, readonly, select, remove, selected }: {
  readonly: boolean; rows: FileRow[];
  select: (row: FileRow) => void; remove: (row: FileRow) => void; selected?: string;
}) {
  return <div className="tw-overflow-auto tw-max-h-[60vh]"><table aria-label="Parquet editor table" className="tw-w-full tw-text-left tw-text-sm">
    <thead className="tw-sticky tw-top-0 tw-bg-panel tw-z-10"><tr><th className="tw-p-3 tw-min-w-[180px]">Parquet file</th>
      <th className="tw-p-3">Records</th><th className="tw-p-3">Bytes</th>
      <th className="tw-p-3">Status</th><th />
    </tr></thead><tbody>{rows.map(row => {
      const key = rowKey(row); const removed = 'source' in row && row.removed;
      return <tr key={key} aria-selected={selected === key} className={`tw-border-t tw-border-border ${selected === key ? 'tw-bg-accent/10' : ''} ${removed ? 'tw-opacity-60' : ''}`} onClick={() => select(row)}>
        <th scope="row" className="tw-p-3 tw-font-normal"><button type="button" className="tw-text-accent tw-max-w-[240px] tw-truncate" title={fileName(row)}>{fileName(row)}</button></th>
        <td className="tw-p-3">{'added' in row ? row.added.rows : row.source.file.records}</td>
        <td className="tw-p-3">{'added' in row ? row.added.size : row.source.file.size}</td>
        <td className="tw-p-3 tw-whitespace-nowrap">{'added' in row ? row.added.location ? 'Uploaded' : 'Reselect to resume' : removed ? 'Removed' : 'Kept'}</td>
        <td className="tw-p-3"><button type="button" className={buttonClass} disabled={readonly || ('source' in row && (row.source.file.content !== 'Data' || row.source.file.format.toUpperCase() !== 'PARQUET'))} aria-label={`${removed ? 'Undo removal of' : 'Remove'} ${fileName(row)}`} onClick={event => { event.stopPropagation(); remove(row); }}>{removed ? 'Undo removal' : 'Remove'}</button></td>
      </tr>;
    })}</tbody>
  </table>{!rows.length && <p className="tw-p-6 tw-text-muted">No files on this page.</p>}</div>;
}
