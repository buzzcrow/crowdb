// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { ReactNode } from 'react';

/** Names navigate; the remaining row and its keyboard button inspect in place. */
export function CapacityRow({ label, onOpen, onInspect, children, details, selected = false }: {
  label: string;
  selected?: boolean;
  onOpen: () => void;
  onInspect: () => void;
  children: ReactNode;
  details: ReactNode;
}) {
  return <article data-selected={selected} aria-label={label} onClick={onInspect}
    className="tw-flex tw-w-full tw-items-center tw-justify-between tw-gap-3 tw-rounded-lg tw-bg-panel tw-p-3 hover:tw-bg-bg/50 data-[selected=true]:tw-ring-1 data-[selected=true]:tw-ring-accent">
    <button onClick={event => { event.stopPropagation(); onOpen(); }}
      className="tw-flex tw-min-w-0 tw-items-center tw-gap-2 tw-text-left hover:tw-text-accent">
      {children}
    </button>
    <button aria-label={`Inspect ${label}`} aria-pressed={selected} onClick={event => { event.stopPropagation(); onInspect(); }}
      className="tw-rounded tw-p-1 tw-text-left hover:tw-bg-bg">
      {details}
    </button>
  </article>;
}
