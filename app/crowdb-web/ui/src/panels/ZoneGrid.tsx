// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import type { ZoneUsageDto } from '../types';

interface ZoneGridProps {
  page: number;
  onPageChange: (page: number) => void;
  zones: ZoneUsageDto[];
  zoneCount: number;
  selectedZone: number | null;
  onZoneClick: (index: number, page?: number) => void;
}
const PAGE_SIZE = 32;

export function ZoneGrid({ zones, zoneCount, selectedZone, onZoneClick, page, onPageChange: setPage }: ZoneGridProps) {
  const byIndex = useMemo(() => new Map(zones.map(zone => [zone.zone_index, zone])), [zones]);
  const start = Math.min(page, Math.max(0, Math.ceil(zoneCount / PAGE_SIZE) - 1)) * PAGE_SIZE;
  return <div className="tw-space-y-3" role="group" aria-label="Disk zones">
    <div className="tw-grid tw-grid-cols-4 lg:tw-grid-cols-8 tw-gap-2">
      {Array.from({ length: Math.min(PAGE_SIZE, zoneCount - start) }, (_, i) => {
        const index = start + i; const zone = byIndex.get(index);
        const pct = zone && zone.capacity_bytes > 0 ? Math.round(zone.busy_bytes / zone.capacity_bytes * 100) : null;
        return <button key={index} aria-label={`Zone ${index}`} aria-pressed={selectedZone === index} onClick={() => onZoneClick(index)}
          className={`tw-p-2 tw-rounded tw-border tw-text-xs tw-text-left ${selectedZone === index ? 'tw-border-accent tw-bg-accent/10' : 'tw-border-border'}`}>
          <div>Zone {index}</div><div className="tw-text-muted">{pct === null ? 'Usage unknown' : `${pct}% used`}</div>
        </button>;
      })}
    </div>
    <label className="tw-block tw-text-xs">Go to zone <input aria-label="Go to zone" type="number" min={0} max={Math.max(0, zoneCount - 1)}
      className="tw-w-28 tw-bg-bg tw-border tw-border-border tw-rounded tw-p-1" placeholder={`0–${Math.max(0, zoneCount - 1)}`}
      onChange={event => { if (!event.target.value) return; const index = Number(event.target.value); if (Number.isSafeInteger(index) && index >= 0 && index < zoneCount) { onZoneClick(index, Math.floor(index / PAGE_SIZE)); } }} /></label>
    {zoneCount > PAGE_SIZE && <div className="tw-flex tw-gap-4 tw-text-xs">
      <button disabled={start === 0} onClick={() => setPage(start / PAGE_SIZE - 1)}>Previous zones</button>
      <span>Zones {start}–{Math.min(start + PAGE_SIZE, zoneCount) - 1} of {zoneCount}</span>
      <button disabled={start + PAGE_SIZE >= zoneCount} onClick={() => setPage(start / PAGE_SIZE + 1)}>Next zones</button>
    </div>}
  </div>;
}
