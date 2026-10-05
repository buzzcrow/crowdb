// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/** Format bytes as a human-readable string (B, KB, MB, GB, TB, PB). */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

/** Busy percentage, rounded. Returns 0 when capacity is non-positive. */
export function busyPct(capacity: number, busy: number): number {
  if (capacity <= 0) return 0;
  return Math.round((busy / capacity) * 100);
}

/** Muted green (free) → amber → red (busy), suitable for large map areas. */
export function busyColor(pct: number): string {
  if (pct < 30) return '#527d68';
  if (pct < 60) return '#9b8957';
  if (pct < 85) return '#ab7956';
  return '#a76565';
}

/** Disk type label from the numeric proto enum. */
export function diskTypeLabel(t: number): string {
  switch (t) {
    case 0: return 'BlockHdd';
    case 1: return 'BlockSsd';
    case 2: return 'ZoneSsd';
    case 3: return 'SmrHdd';
    default: return `type:${t}`;
  }
}
