// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cancelScrollRestore, captureScroll } from './navigationScroll';

let panel: HTMLDivElement;
let position: number;
let maximum: number;
let sequence: number;
let frames: Map<number, FrameRequestCallback>;

beforeEach(() => {
  position = maximum = 1032;
  sequence = 0;
  frames = new Map();
  vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => {
    frames.set(++sequence, callback);
    return sequence;
  });
  vi.stubGlobal('cancelAnimationFrame', (id: number) => frames.delete(id));
  panel = document.createElement('div');
  panel.dataset.navigationScroll = 'capacity-disk';
  Object.defineProperty(panel, 'scrollTop', {
    get: () => position,
    set: (value: number) => { position = Math.min(value, maximum); },
  });
  const rectangles = [new DOMRect()];
  vi.spyOn(panel, 'getClientRects').mockReturnValue(Object.assign(rectangles, {
    item: (index: number) => rectangles[index] ?? null,
  }));
  document.body.append(panel);
});

afterEach(() => {
  cancelScrollRestore();
  panel.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function paint() {
  const pending = [...frames.values()];
  frames.clear();
  for (const callback of pending) callback(0);
}

it('restores again when a later refetch clamps an initially restored position', async () => {
  const restore = captureScroll();
  panel.scrollTop = 0;
  restore();
  paint();
  expect(panel.scrollTop).toBe(1032);
  maximum = 968;
  panel.scrollTop = 1032;
  panel.append('refetching');
  await Promise.resolve();
  paint();
  expect(panel.scrollTop).toBe(968);
  maximum = 1032;
  panel.replaceChildren('loaded');
  await Promise.resolve();
  paint();
  expect(panel.scrollTop).toBe(1032);
});

it.each(['pointerdown', 'wheel', 'touchstart', 'keydown'])('leaves user navigation alone after %s', async input => {
  const restore = captureScroll();
  restore();
  paint();
  panel.dispatchEvent(new Event(input, { bubbles: true }));
  panel.scrollTop = 100;
  panel.append('loaded');
  await Promise.resolve();
  paint();
  expect(panel.scrollTop).toBe(100);
});
