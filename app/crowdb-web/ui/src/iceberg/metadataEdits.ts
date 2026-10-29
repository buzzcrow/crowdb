// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { parseIcebergJson } from './json';
export interface PropertyEdit { name: string; value: string; remove: boolean }
export function propertyRequest(uuid: string, original: Record<string, string>, properties: PropertyEdit[]): string {
  if (properties.some(value => !value.name) || new Set(properties.map(value => value.name)).size !== properties.length) throw new Error('Property names must be nonempty and unique.');
  const updates = Object.fromEntries(properties.filter(value => !value.remove && original[value.name] !== value.value).map(value => [value.name, value.value]));
  const removals = [...Object.keys(original).filter(name => !properties.some(value => value.name === name && !value.remove))];
  return JSON.stringify({ requirements: [{ type: 'assert-table-uuid', uuid }], updates: [
    ...(Object.keys(updates).length ? [{ action: 'set-properties', updates }] : []),
    ...(removals.length ? [{ action: 'remove-properties', removals }] : []),
  ] });
}
/** Validate the envelope without reserializing 64-bit Snapshot IDs. */
export function advancedRequest(uuid: string, text: string): string {
  const value = parseIcebergJson(text) as { requirements?: Array<{ type: string; uuid?: string }>; updates?: Array<{ action: string }> };
  if (!value || !Array.isArray(value.requirements) || !value.requirements.some(requirement => requirement.type === 'assert-table-uuid' && requirement.uuid === uuid)) throw new Error('Keep the assertion for this Table UUID.');
  if (!Array.isArray(value.updates) || value.updates.length > 1000 || value.updates.some(update => !update || typeof update.action !== 'string')) throw new Error('Provide an array of metadata updates.');
  if (new TextEncoder().encode(text).length > 1024 * 1024) throw new Error('Metadata request exceeds 1 MiB.');
  return text;
}
