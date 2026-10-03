// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useState, type ReactNode } from 'react';
import { buttonClass } from '../access/Workbench';
export const missing = 'Not recorded';
export function scalar(value: unknown): string {
  if (value == null) return missing;
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';
  return String(value);
}
export function byteSize(value: string | number | undefined): string {
  if (value == null) return missing;
  const n = Number(value);
  if (!Number.isFinite(n)) return missing;
  if (n < 1024) return `${value} B`;
  const power = Math.min(4, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** power).toFixed(2)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][power]}`;
}
export function Fields({ values, stacked = false }: { values: Record<string, unknown>; stacked?: boolean }) {
  if (Object.keys(values).length > 50) return <Structured value={values} />;
  return <dl className={stacked ? "tw-space-y-3" : "tw-grid tw-grid-cols-2 xl:tw-grid-cols-3 tw-gap-x-5 tw-gap-y-3"} aria-label="Metadata fields">{Object.entries(values).slice(0, 50).map(([key, value]) => <div key={key} className="tw-min-w-0"><dt className="tw-text-xs tw-text-muted">{key.replaceAll('_', ' ').replaceAll('-', ' ')}</dt><dd className="tw-text-sm tw-break-words">{typeof value === 'object' && value !== null ? stacked ? <Expandable value={value} /> : <Structured value={value} /> : scalar(value)}</dd></div>)}</dl>;
}
export function Structured({ value }: { value: unknown }) {
  const [page, setPage] = useState(0);
  if (value == null || typeof value !== 'object') return <>{scalar(value)}</>;
  const entries = Object.entries(value);
  const start = Math.min(page * 30, Math.max(0, Math.floor((entries.length - 1) / 30) * 30));
  return <><dl className="tw-space-y-1">{entries.slice(start, start + 30).map(([key, item]) => <div key={key} className="tw-pl-2 tw-border-l tw-border-border"><dt className="tw-text-xs tw-text-muted">{key}</dt><dd>{item && typeof item === 'object' ? <Expandable value={item} /> : scalar(item)}</dd></div>)}</dl>{entries.length > 30 && <div className="tw-flex tw-gap-2 tw-text-xs"><button className={buttonClass} disabled={!start} onClick={() => setPage(Math.max(0, start / 30 - 1))}>Previous fields</button><span>{start + 1}–{Math.min(start + 30, entries.length)} of {entries.length}</span><button className={buttonClass} disabled={start + 30 >= entries.length} onClick={() => setPage(start / 30 + 1)}>Next fields</button></div>}</>;
}
function Expandable({ value }: { value: object }) {
  const [open, setOpen] = useState(false);
  return <details onToggle={event => setOpen(event.currentTarget.open)}><summary className="tw-text-xs tw-cursor-pointer">{Object.keys(value).length} {Array.isArray(value) ? 'items' : 'fields'}</summary>{open && <Structured value={value} />}</details>;
}
export function Records({ label, headings, rows }: { label: string; headings: string[]; rows: ReactNode[][] }) {
  const [page, setPage] = useState(0);
  const start = Math.min(page * 100, Math.max(0, Math.floor((rows.length - 1) / 100) * 100));
  return <div className="tw-overflow-auto"><table aria-label={label} className="tw-w-full tw-text-xs"><thead><tr>{headings.map(h => <th key={h} className="tw-text-left tw-p-2 tw-text-muted tw-font-medium">{h}</th>)}</tr></thead><tbody>{rows.slice(start, start + 100).map((row, i) => <tr key={i} className="tw-border-t tw-border-border">{row.map((cell, j) => <td key={j} className="tw-p-2 tw-align-top tw-break-words">{cell}</td>)}</tr>)}</tbody></table>{!rows.length && <p className="tw-text-xs tw-text-muted">No records.</p>}{rows.length > 100 && <div className="tw-flex tw-gap-2"><button className={buttonClass} disabled={!start} onClick={() => setPage(start / 100 - 1)}>Previous {label}</button><span>{start + 1}–{Math.min(start + 100, rows.length)} of {rows.length}</span><button className={buttonClass} disabled={start + 100 >= rows.length} onClick={() => setPage(start / 100 + 1)}>Next {label}</button></div>}</div>;
}
