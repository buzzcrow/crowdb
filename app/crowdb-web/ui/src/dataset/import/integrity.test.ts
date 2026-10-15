// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { describe, expect, it } from 'vitest';
import { Md5 } from './digest';
import { editWindows, parseMapping } from './mapping';
import { bytes, sampleLabel } from '../identity';

describe('local import integrity', () => {
  it('matches independent RFC 1321 vectors across incremental block boundaries', () => {
    for (const [input, expected] of [
      ['', 'd41d8cd98f00b204e9800998ecf8427e'], ['a', '0cc175b9c0f1b6a831c399e269772661'],
      ['abc', '900150983cd24fb0d6963f7d28e17f72'], ['message digest', 'f96b697d7cb7938d525a2f31aaf161d0'],
      ['ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', 'd174ab98d277d9f5a5611c2c9f419d9f'],
      ['12345678901234567890123456789012345678901234567890123456789012345678901234567890', '57edf4a22be3c955ac49da2e2107b67a'],
    ]) {
      const md5 = new Md5(); for (const character of input) md5.update(new TextEncoder().encode(character));
      expect(md5.finish()).toBe(expected);
    }
    const million = new Md5(); const block = new TextEncoder().encode('a'.repeat(1000));
    for (let index = 0; index < 1000; index++) million.update(block);
    expect(million.finish()).toBe('7707d6ae4e027c70eea2a935c2296f21');
  });
  it('preserves binary identities and rejects noncanonical aliases', () => {
    expect([...bytes('/wA=')]).toEqual([255, 0]); expect(sampleLabel('/wA=')).toBe('base64: /wA=');
    expect(() => bytes('YQ')).toThrow(); expect(() => bytes('YR==')).toThrow(); expect(sampleLabel('YQ==')).toBe('a');
  });
  it('rejects duplicate identities and missing or ambiguous local file mapping before uploads', () => {
    const sample = { kind: 'upsert', id: 'YQ==', fields: [{ name: 'payload', value: { kind: 'file', file: 'data.bin' } }] };
    const data = { version: 1, schema: null, samples: [sample] }; const file = new File(['payload'], 'data.bin');
    expect(() => parseMapping(JSON.stringify(data), [])).toThrow('exactly one');
    expect(() => parseMapping(JSON.stringify(data), [file, file])).toThrow('exactly one');
    expect(parseMapping(JSON.stringify(data), [file]).samples).toHaveLength(1);
    expect(() => parseMapping(JSON.stringify({ ...data, samples: [sample, sample] }), [file])).toThrow('Duplicate Sample');
  });
});

it('bounds edit bodies by bytes as well as row count before transfer', () => {
  const sample = { kind: 'upsert' as const, id: 'YQ==', fields: [{ name: 'text', value: { kind: 'inline' as const, data: btoa('x'.repeat(65536)) } }] };
  const samples = Array.from({ length: 20 }, (_, index) => ({ ...sample, id: btoa(String(index)) }));
  const windows = [...editWindows(samples)];
  expect(windows.length).toBeGreaterThan(1); expect(windows.flat()).toEqual(samples);
  for (const window of windows) expect(new TextEncoder().encode(JSON.stringify(window)).length).toBeLessThan(768 * 1024 + 2);
  expect(() => [...editWindows([{ ...sample, fields: Array.from({ length: 13 }, () => sample.fields[0]) }])]).toThrow('768 KiB');
});
