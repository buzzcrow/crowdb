// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { inputClass } from '../../access/Workbench';
import type { Operation } from '../operation';
import { PredicateInput } from '../splits/Predicate';
export function SourceSelection({ op, change }: { op: Operation; change: (update: (value: Operation) => Operation) => void }) {
  const source = op.source!.source;
  if (source.kind === 'split') return <>
    <p>Frozen Split: {source.source.name} / {source.source.revision}</p>
    <label><input type="checkbox" aria-label="Filter frozen Split membership" checked={source.selection !== null} onChange={event => change(value => ({ ...value, source: { ...value.source!, source: { ...source, selection: event.target.checked ? { kind: 'all' } : null } } }))} /> Filter frozen Split membership</label>
    {source.selection && <section aria-label="Frozen Split filter"><PredicateInput value={source.selection} change={selection => change(value => ({ ...value, source: { ...value.source!, source: { ...source, selection } } }))} /></section>}
  </>;
  if (!op.source?.parent) return null;
  return <details><summary>Parent selection</summary>
    <label>Parent data <select aria-label="Parent data" className={inputClass} value={op.parentMode ?? 'all'} onChange={event => {
      const mode = event.target.value as 'all' | 'replace' | 'selection';
      change(value => ({ ...value, parentMode: mode, source: { ...value.source!, source: mode === 'all' ? { kind: 'all' } : { kind: 'selection', predicate: mode === 'replace' ? { kind: 'explicit', ids: [] } : { kind: 'all' } } } }));
    }}><option value="all">Preserve all parent data</option><option value="replace">Replace membership with edited Samples</option><option value="selection">Filter parent data</option></select></label>
    {op.parentMode === 'selection' && source.kind === 'selection' && <PredicateInput value={source.predicate} change={predicate => change(value => ({ ...value, source: { ...value.source!, source: { kind: 'selection', predicate } } }))} />}
  </details>;
}
