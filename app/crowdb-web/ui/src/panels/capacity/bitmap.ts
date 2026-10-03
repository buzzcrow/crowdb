// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/** UsageBitmap::snapshot emits little-endian words, then the Web API hex encodes bytes. */
export function blockState(bitmap: string | undefined, index: number): 'used' | 'free' | 'unknown' {
  const byte = bitmap?.slice(Math.floor(index / 8) * 2, Math.floor(index / 8) * 2 + 2);
  if (!byte || !/^[0-9a-f]{2}$/i.test(byte)) return 'unknown';
  return (parseInt(byte, 16) & (1 << (index % 8))) !== 0 ? 'used' : 'free';
}
