// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { buttonClass, inputClass } from '../../access/Workbench';
import { decimal } from '../api';
import type { DraftSplit, SplitPolicy } from '../types';
import { PredicateInput } from './Predicate';
import { addSplit, removeSplit } from './sets';
export { PredicateInput } from './Predicate';

export function Definitions({ splits, policy, change, disabled = false }: { splits: DraftSplit[]; policy: SplitPolicy; change: (splits: DraftSplit[], policy: SplitPolicy) => void; disabled?: boolean }) {
  const [partition, setPartition] = useState('train:80,test:20'); const [seed, setSeed] = useState('1'); const [error, setError] = useState('');
  const update = (index: number, split: DraftSplit) => change(splits.map((value, position) => index === position ? split : value), policy);
  const remove = (index: number) => change(removeSplit(splits, index), policy);
  const rename = (index: number, name: string) => {
    const original = splits[index];
    const group = original.definition.kind === 'weighted_partition' ? JSON.stringify([original.definition.seed, original.definition.weights]) : null;
    change(splits.map((value, position) => {
      const definition = value.definition;
      const sameGroup = definition.kind === 'weighted_partition' && JSON.stringify([definition.seed, definition.weights]) === group;
      return { ...value, name: position === index ? name : value.name,
        definition: sameGroup ? { ...definition, weights: definition.weights.map(weight => weight.name === original.name ? { ...weight, name } : weight) } : definition };
    }), policy);
  };
  const weighted = () => {
    try {
      decimal(seed);
      const weights = partition.split(',').map(entry => { const [name, weight] = entry.trim().split(':'); return { name, weight: Number(weight) }; });
      if (weights.length < 2 || new Set(weights.map(w => w.name)).size !== weights.length || weights.some(w => !w.name || !Number.isInteger(w.weight) || w.weight < 1 || w.weight > 4294967295)) throw new Error('Use distinct name:positive-weight entries.');
      change([...splits, ...weights.map((weight, index): DraftSplit => ({ name: weight.name, origin: { kind: 'evaluated' }, definition: { kind: 'weighted_partition', seed, weights, index } }))], policy); setError('');
    } catch (reason) { setError(String(reason)); }
  };
  return <fieldset disabled={disabled} className="tw-space-y-3 tw-border tw-border-border tw-p-3">
    <legend className="tw-font-semibold">Named Splits</legend>
    <p className="tw-text-xs">One revision publishes this complete set, including an empty set. Definitions evaluate on the complete Snapshot.</p>
    <label className="tw-text-xs">Overlap <select className={inputClass} value={policy.overlap} onChange={event => change(splits, { ...policy, overlap: event.target.value as SplitPolicy['overlap'] })}><option value="disjoint">Disjoint</option><option value="allow_overlap">Allow overlap</option></select></label>
    <label className="tw-text-xs">Coverage <select className={inputClass} value={policy.coverage} onChange={event => change(splits, { ...policy, coverage: event.target.value as SplitPolicy['coverage'] })}><option value="allow_unassigned">Allow unassigned</option><option value="require_complete">Require complete</option></select></label>
    {splits.map((split, index) => <div key={index} className="tw-border-t tw-border-border tw-pt-2 tw-space-y-2">
      <label>Split name <input aria-label={`Split ${index + 1} name`} className={inputClass} value={split.name} onChange={event => rename(index, event.target.value)} /></label>
      <button type="button" className={buttonClass} onClick={() => remove(index)}>{split.definition.kind === 'weighted_partition' ? `Remove partition group ${split.definition.weights.map(value => value.name).join(', ')}` : `Remove ${split.name || 'Split'}`}</button>
      <p className="tw-text-xs">Membership: {split.origin.kind === 'inherited' ? `frozen from ${split.origin.source.snapshot}/${split.origin.source.revision}/${split.origin.source.name}` : 'evaluated on this Snapshot'}</p>
      {split.origin.kind === 'inherited' && <button type="button" className={buttonClass} onClick={() => update(index, { ...split, origin: { kind: 'evaluated' } })}>Reevaluate definition</button>}
      {split.definition.kind === 'selection' ? <PredicateInput value={split.definition.predicate} change={predicate => update(index, { ...split, definition: { kind: 'selection', predicate }, origin: { kind: 'evaluated' } })} /> : <p className="tw-text-xs">Weighted partition: seed {split.definition.seed}, index {split.definition.index}, {split.definition.weights.map(w => `${w.name}:${w.weight}`).join(', ')}. Remove and recreate the partition group to change its weights.</p>}
    </div>)}
    <button type="button" className={buttonClass} onClick={() => change(addSplit(splits), policy)}>Add selection Split</button>
    <div className="tw-flex tw-flex-wrap tw-gap-2"><label>Partitions <input className={inputClass} value={partition} onChange={event => setPartition(event.target.value)} /></label><label>Seed <input className={inputClass} value={seed} onChange={event => setSeed(event.target.value)} /></label><button type="button" className={buttonClass} onClick={weighted}>Add weighted partitions</button></div>
    {error && <p role="alert">{error}</p>}
  </fieldset>;
}
export function validateEditor(element: HTMLElement | null) {
  const inputs = element?.querySelectorAll<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>('input, textarea, select');
  for (const input of inputs ?? []) {
    if (!input.reportValidity()) throw new Error('Correct the invalid editor input before saving.');
  }
}
