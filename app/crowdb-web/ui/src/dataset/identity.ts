// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export function bytes(value: string): Uint8Array {
  if (value.length > 21848) throw new Error('Sample ID exceeds 16 KiB.');
  const decoded = Uint8Array.from(atob(value), character => character.charCodeAt(0));
  if (!decoded.length || encoded(decoded) !== value) throw new Error('Use a nonempty canonical padded base64 ID.');
  return decoded;
}
export function encoded(value: Uint8Array): string {
  let text = ''; for (const byte of value) text += String.fromCharCode(byte);
  return btoa(text);
}
export function sampleLabel(value: string): string {
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes(value));
    return /[\u0000-\u001f\u007f]/.test(text) ? `base64: ${value}` : text;
  } catch { return `base64: ${value}`; }
}
