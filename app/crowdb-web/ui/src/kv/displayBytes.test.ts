// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, expect, it } from 'vitest';
import { byteRuns, displayBytes, printableBytes } from './displayBytes';

describe('mixed KV byte presentation', () => {
  it('keeps printable fields around control and invalid UTF-8 bytes', () => {
    expect(byteRuns(undefined, '616200ffe4b8ad6364')).toEqual([
      { text: 'ab', binary: false }, { text: '00FF', binary: true },
      { text: '中cd', binary: false },
    ]);
    expect(printableBytes(undefined, '616200ff')).toBe(false);
  });
  it('recovers at the next printable scalar after malformed sequences', () => {
    expect(byteRuns(undefined, 'e0806142f09f9880')).toEqual([
      { text: 'E080', binary: true }, { text: 'aB😀', binary: false },
    ]);
    expect(displayBytes(undefined, 'e282')).toBe('E282');
  });
  it('distinguishes text that looks like hex and preserves valid Unicode', () => {
    expect(byteRuns('FF00中😀', undefined)).toEqual([{ text: 'FF00中😀', binary: false }]);
    expect(printableBytes('FF00中😀', undefined)).toBe(true);
    expect(byteRuns(undefined, 'efbbbf41')).toEqual([{ text: 'EFBBBF', binary: true }, { text: 'A', binary: false }]);
  });
});
