// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { Fragment, useState } from 'react';
import { Records, byteSize } from './Fields';
import { buttonClass } from '../access/Workbench';
import type { Inspector } from './useInspection';
import type { Manifest, FileEntry, Selection } from './types';
import { filename } from './ReferenceTree';
export function ManifestTable({ inspector, selection }: { inspector: Inspector; selection: Selection }) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const list = inspector.cache[inspector.key({ snapshot: selection.snapshot, kind: 'list' })];
  return <table aria-label="Manifest list records" className="tw-w-full tw-text-xs tw-text-left"><thead><tr>{['Manifest', 'Content', 'Spec', 'Size'].map(label => <th key={label} className="tw-p-2 tw-border-b tw-border-border tw-text-muted">{label}</th>)}</tr></thead><tbody>
    {(list?.rows as Manifest[] ?? []).map(manifest => {
      const target: Selection = { snapshot: selection.snapshot, kind: 'manifest', manifest };
      const files = inspector.cache[inspector.key(target)];
      const open = expanded === manifest.location;
      return <Fragment key={manifest.location}><tr className="tw-border-b tw-border-border"><td className="tw-p-2"><button aria-label={`${open ? 'Collapse' : 'Expand'} manifest ${filename(manifest.location)}`} aria-expanded={open} onClick={() => { setExpanded(open ? null : manifest.location); if (!open) void inspector.select(target, undefined, true); }}>{open ? '▾' : '▸'}</button> <button className="tw-text-accent" onClick={() => void inspector.select(target)}>{filename(manifest.location)}</button></td><td className="tw-p-2">{manifest.content}</td><td className="tw-p-2">{manifest.partition_spec_id}</td><td className="tw-p-2">{byteSize(manifest.size)}</td></tr>
        {open && <tr><td colSpan={4} className="tw-p-3 tw-bg-panel"><div role="region" aria-label={`Files in ${filename(manifest.location)}`}>
          {!files ? <p>{inspector.busy ? 'Loading files…' : 'Files unavailable. Collapse and expand to retry.'}</p> : <><Records label="Manifest files" headings={['File', 'Format', 'Records', 'Size']} rows={(files.rows as FileEntry[] ?? []).map(file => [<button className="tw-text-accent" onClick={() => void inspector.select({ ...target, kind: 'file', file })}>{filename(file.location)}</button>, file.format, file.records, byteSize(file.size)])} />
          <div className="tw-flex tw-gap-2 tw-mt-2"><button className={buttonClass} disabled={inspector.busy || !files.previous} onClick={() => void inspector.select(target, files.previous ?? '0', true)}>Previous files</button><button className={buttonClass} disabled={inspector.busy || files.next == null} onClick={() => void inspector.select(target, files.next!, true)}>Next files</button></div></>}
        </div></td></tr>}
      </Fragment>;
    })}
    {!list?.rows?.length && !inspector.busy && <tr><td colSpan={4} className="tw-p-3 tw-text-muted">No manifests.</td></tr>}
  </tbody></table>;
}
