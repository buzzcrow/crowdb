// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { DraftSplit } from '../types';
export function removeSplit(splits: DraftSplit[], index: number): DraftSplit[] {
  const definition = splits[index].definition;
  const group = definition.kind === 'weighted_partition' ? JSON.stringify([definition.seed, definition.weights]) : undefined;
  return splits.filter((value, position) => position !== index && !(group && value.definition.kind === 'weighted_partition' && JSON.stringify([value.definition.seed, value.definition.weights]) === group));
}
export function addSplit(splits: DraftSplit[]): DraftSplit[] {
  let index = 1; while (splits.some(value => value.name === `split-${index}`)) index++;
  return [...splits, { name: `split-${index}`, definition: { kind: 'selection', predicate: { kind: 'all' } }, origin: { kind: 'evaluated' } }];
}
