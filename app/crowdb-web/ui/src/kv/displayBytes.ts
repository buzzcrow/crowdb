// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
export interface ByteRun { text: string; binary: boolean }
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const undisplayable = /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}\p{Cn}]/u;

/** Decode scalar by scalar, retaining printable fields around binary bytes. */
export function byteRuns(text: string | undefined, hex: string | undefined): ByteRun[] {
  const bytes = hex != null && /^(?:[0-9a-f]{2})*$/i.test(hex)
    ? Uint8Array.from(hex.match(/../g) ?? [], byte => parseInt(byte, 16))
    : new TextEncoder().encode(text ?? '');
  const runs: ByteRun[] = [];
  for (let offset = 0; offset < bytes.length;) {
    const lead = bytes[offset];
    let width = lead < 0x80 ? 1 : lead >= 0xc2 && lead <= 0xdf ? 2 : lead >= 0xe0 && lead <= 0xef ? 3 : lead >= 0xf0 && lead <= 0xf4 ? 4 : 1;
    let decoded = '';
    try { decoded = decoder.decode(bytes.subarray(offset, offset + width)); }
    catch { width = 1; }
    const binary = !decoded || undisplayable.test(decoded);
    const value = binary ? Array.from(bytes.subarray(offset, offset + width), byte => byte.toString(16).padStart(2, '0').toUpperCase()).join('') : decoded;
    const previous = runs.at(-1);
    if (previous?.binary === binary) previous.text += value;
    else runs.push({ text: value, binary });
    offset += width;
  }
  return runs;
}

export function displayBytes(text: string | undefined, hex: string | undefined): string {
  return byteRuns(text, hex).map(run => run.text).join('');
}

export function printableBytes(text: string | undefined, hex: string | undefined): boolean {
  return byteRuns(text, hex).every(run => !run.binary);
}
