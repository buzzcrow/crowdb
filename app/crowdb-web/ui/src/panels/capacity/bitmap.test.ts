// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, it, expect } from 'vitest';
import { blockState } from './bitmap';

describe('native allocation bitmap', () => {
  it('decodes block zero, byte and word boundaries using the native little-endian layout', () => {
    const bitmap = '81010000000000800100000000000000';
    for (const index of [0, 7, 8, 63, 64]) expect(blockState(bitmap, index)).toBe('used');
    for (const index of [1, 6, 9, 62, 65, 127]) expect(blockState(bitmap, index)).toBe('free');
  });
  it('keeps missing, truncated and malformed bytes unknown', () => {
    expect(blockState(undefined, 0)).toBe('unknown');
    expect(blockState('00', 8)).toBe('unknown');
    expect(blockState('0', 0)).toBe('unknown');
    expect(blockState('g0', 0)).toBe('unknown');
  });
});
