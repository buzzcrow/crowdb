// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useRef, useEffect, useState } from 'react';
import { blockState } from './capacity/bitmap';

interface ZoneBitmapProps {
  usageBitmap?: string;
  totalUnits: number;
}
const PAGE_SIZE = 4096;
const COLUMNS = 64;
const CELL = 6;
const colors = { used: '#557fa5', free: '#527d68', unknown: '#6b7280' };

/** Draw only one block window; the API snapshot is little-endian by byte. */
export function ZoneBitmap({ usageBitmap, totalUnits }: ZoneBitmapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [start, setStart] = useState(0);
  const [hover, setHover] = useState<number | null>(null);
  const total = Number.isSafeInteger(totalUnits) && totalUnits > 0 ? totalUnits : 0;
  const offset = Math.min(start, Math.max(0, Math.ceil(total / PAGE_SIZE) - 1) * PAGE_SIZE);
  const count = Math.min(PAGE_SIZE, total - offset);
  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    for (let i = 0; i < count; i++) {
      ctx.fillStyle = colors[blockState(usageBitmap, offset + i)];
      ctx.fillRect((i % COLUMNS) * CELL, Math.floor(i / COLUMNS) * CELL, CELL - 1, CELL - 1);
    }
  }, [usageBitmap, offset, count]);

  return <div className="tw-space-y-3" data-testid="zone-bitmap">
    <div className="tw-flex tw-gap-4 tw-text-xs" aria-label="Block usage legend">
      {Object.entries(colors).map(([name, color]) => <span key={name} className="tw-flex tw-items-center tw-gap-1"><span style={{ background: color }} className="tw-w-3 tw-h-3 tw-inline-block" />{name === 'used' ? 'Used (blue)' : name === 'free' ? 'Free (green)' : 'Unknown (gray)'}</span>)}
    </div>
    <p className="tw-text-xs tw-text-muted">{count ? `Blocks ${offset}–${offset + count - 1} of ${total}` : 'No blocks reported'} · one cell per allocation block</p>
    <canvas ref={canvasRef} width={COLUMNS * CELL} height={Math.max(1, Math.ceil(count / COLUMNS)) * CELL}
      aria-label="Zone block usage" className="tw-border tw-border-border tw-max-w-full"
      onMouseLeave={() => setHover(null)} onMouseMove={event => {
        const canvas = event.currentTarget; const rect = canvas.getBoundingClientRect();
        const col = Math.floor((event.clientX - rect.left) * canvas.width / rect.width / CELL);
        const row = Math.floor((event.clientY - rect.top) * canvas.height / rect.height / CELL);
        const index = row * COLUMNS + col;
        setHover(col >= 0 && col < COLUMNS && index >= 0 && index < count ? offset + index : null);
      }} />
    {hover !== null && <p role="status" className="tw-text-xs">Block {hover} · {blockState(usageBitmap, hover)}</p>}
    <div className="tw-flex tw-gap-3 tw-text-xs">
      <button disabled={offset === 0} onClick={() => { setHover(null); setStart(offset - PAGE_SIZE); }}>Previous blocks</button>
      <button disabled={offset + count >= total} onClick={() => { setHover(null); setStart(offset + PAGE_SIZE); }}>Next blocks</button>
      <label>Go to block <input type="number" min={0} max={Math.max(0, total - 1)} value={offset} className="tw-w-28 tw-bg-bg tw-border tw-border-border tw-rounded tw-p-1"
        onChange={event => { const value = Number(event.target.value); if (Number.isSafeInteger(value) && value >= 0 && value < total) { setHover(null); setStart(Math.floor(value / PAGE_SIZE) * PAGE_SIZE); } }} /></label>
    </div>
  </div>;
}
