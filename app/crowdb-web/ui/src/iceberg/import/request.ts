// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
/** Both Catalog commits and native injections require a timestamp-bearing UUID v7. */
export function requestKey(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16)); let now = BigInt(Date.now());
  for (let index = 5; index >= 0; index--) { bytes[index] = Number(now & 255n); now >>= 8n; }
  bytes[6] = (bytes[6] & 15) | 112; bytes[8] = (bytes[8] & 63) | 128;
  const text = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
  return `${text.slice(0, 8)}-${text.slice(8, 12)}-${text.slice(12, 16)}-${text.slice(16, 20)}-${text.slice(20)}`;
}
export function snapshotId(): string {
  const bytes = crypto.getRandomValues(new Uint32Array(2));
  return (((BigInt(bytes[0] & 0x7fffffff) << 32n) | BigInt(bytes[1])) || 1n).toString();
}
/** Only explicitly typed integers become wire numbers; property strings stay strings. */
export function exactJson(value: unknown): string {
  if (typeof value === 'bigint') return value.toString();
  if (Array.isArray(value)) return `[${value.map(exactJson).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(value).filter(([, value]) => value !== undefined).map(([name, value]) => `${JSON.stringify(name)}:${exactJson(value)}`).join(',')}}`;
  return JSON.stringify(value);
}
