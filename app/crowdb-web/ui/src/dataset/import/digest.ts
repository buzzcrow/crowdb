// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Incremental RFC 1321 digest; retains one 64-byte block, never the whole file.
const shifts = [7, 12, 17, 22, 5, 9, 14, 20, 4, 11, 16, 23, 6, 10, 15, 21];
const constants = Array.from({ length: 64 }, (_, i) => Math.floor(Math.abs(Math.sin(i + 1)) * 4294967296) | 0);
export class Md5 {
  private state = new Int32Array([0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476]);
  private block = new Uint8Array(64);
  private buffered = 0;
  private length = 0n;
  update(input: Uint8Array) {
    this.length += BigInt(input.length);
    let offset = 0;
    while (offset < input.length) {
      const count = Math.min(64 - this.buffered, input.length - offset);
      this.block.set(input.subarray(offset, offset + count), this.buffered);
      this.buffered += count; offset += count;
      if (this.buffered === 64) { this.compress(); this.buffered = 0; }
    }
    return this;
  }
  private compress() {
    const words = new DataView(this.block.buffer);
    let [a, b, c, d] = this.state;
    for (let i = 0; i < 64; i++) {
      let f: number; let index: number;
      if (i < 16) { f = (b & c) | (~b & d); index = i; }
      else if (i < 32) { f = (d & b) | (~d & c); index = (5 * i + 1) % 16; }
      else if (i < 48) { f = b ^ c ^ d; index = (3 * i + 5) % 16; }
      else { f = c ^ (b | ~d); index = (7 * i) % 16; }
      const sum = (a + f + constants[i] + words.getInt32(index * 4, true)) | 0;
      const shift = shifts[Math.floor(i / 16) * 4 + i % 4];
      [a, b, c, d] = [d, (b + ((sum << shift) | (sum >>> (32 - shift)))) | 0, b, c];
    }
    this.state[0] += a; this.state[1] += b; this.state[2] += c; this.state[3] += d;
  }
  finish(): string {
    const bits = this.length * 8n;
    const padding = new Uint8Array(this.buffered < 56 ? 64 - this.buffered : 128 - this.buffered);
    padding[0] = 0x80;
    new DataView(padding.buffer).setBigUint64(padding.length - 8, bits, true);
    this.update(padding);
    const output = new DataView(new ArrayBuffer(16));
    this.state.forEach((value, index) => output.setInt32(index * 4, value, true));
    return Array.from(new Uint8Array(output.buffer), byte => byte.toString(16).padStart(2, '0')).join('');
  }
}
export async function digest(file: Blob, signal?: AbortSignal, progress?: (bytes: number) => void): Promise<string> {
  const md5 = new Md5();
  for (let offset = 0; offset < file.size; offset += 4 * 1024 * 1024) {
    signal?.throwIfAborted();
    md5.update(new Uint8Array(await file.slice(offset, offset + 4 * 1024 * 1024).arrayBuffer()));
    progress?.(Math.min(offset + 4 * 1024 * 1024, file.size));
  }
  signal?.throwIfAborted(); return md5.finish();
}
