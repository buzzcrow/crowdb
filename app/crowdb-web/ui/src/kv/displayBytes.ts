// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
/** Prefer exact UTF-8 when printable; never hex-encode lossy replacement text. */
export function displayBytes(text: string | undefined, hex: string | undefined): string {
  let decoded = text ?? '';
  if (hex != null && /^(?:[0-9a-f]{2})*$/i.test(hex)) {
    try {
      decoded = new TextDecoder('utf-8', { fatal: true }).decode(Uint8Array.from(hex.match(/../g) ?? [], byte => parseInt(byte, 16)));
    } catch { return `0x${hex}`; }
    if (/[\p{Cc}\p{Cf}]/u.test(decoded)) return `0x${hex}`;
  }
  return decoded;
}
