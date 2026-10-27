// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { KeyboardEvent } from 'react';
export function resourceMenuKey(event: KeyboardEvent<HTMLElement>) {
  if (event.key !== 'ContextMenu' && !(event.shiftKey && event.key === 'F10')) return;
  event.preventDefault(); event.stopPropagation();
  const rect = event.currentTarget.getBoundingClientRect();
  event.currentTarget.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: rect.left + 24, clientY: rect.bottom }));
}
