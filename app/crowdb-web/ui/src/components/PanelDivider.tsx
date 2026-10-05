// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { CSSProperties } from 'react';
export function PanelDivider({ side, width, onResize, fixed = false }: { side: 'left' | 'right'; width: number; onResize: (value: number) => void; fixed?: boolean }) {
  const resize = (value: number) => onResize(Math.max(220, Math.min(600, value)));
  const style: CSSProperties = { touchAction: 'none', ...(fixed ? { [side]: width - 3, width: 6 } : {}) };
  return <div role="separator" aria-label={side === 'left' ? 'Sidebar width' : 'Properties width'} aria-orientation="vertical" aria-valuemin={220} aria-valuemax={600} aria-valuenow={width} tabIndex={0}
    className={`tw-cursor-col-resize tw-bg-border hover:tw-bg-accent focus:tw-bg-accent ${fixed ? 'tw-fixed tw-top-14 tw-bottom-0 tw-z-30' : ''}`} style={style}
    onPointerDown={event => event.currentTarget.setPointerCapture(event.pointerId)}
    onPointerMove={event => { if (event.currentTarget.hasPointerCapture(event.pointerId)) { const box = event.currentTarget.parentElement!.getBoundingClientRect(); resize(side === 'left' ? event.clientX - (fixed ? 0 : box.left) : (fixed ? window.innerWidth : box.right) - event.clientX); } }}
    onPointerUp={event => event.currentTarget.releasePointerCapture(event.pointerId)}
    onKeyDown={event => { if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') { event.preventDefault(); resize(width + (event.key === 'ArrowRight' ? 20 : -20) * (side === 'left' ? 1 : -1)); } }} />;
}
