// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

interface Read {
  key: string;
  controller: AbortController;
  users: number;
  started: boolean;
  promise: Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  load: (signal: AbortSignal) => Promise<unknown>;
}

/** One bounded inventory budget shared by tree, Node workspaces and properties. */
export class InventoryRequests {
  private readonly reads = new Map<string, Read>();
  private queue: Read[] = [];
  private running = 0;

  constructor(private readonly workers = 4, private readonly limit = 256) {}

  run<T>(key: string, signal: AbortSignal | undefined, load: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (signal?.aborted) return Promise.reject(new DOMException('Observation aborted', 'AbortError'));
    let read = this.reads.get(key);
    if (read?.controller.signal.aborted) read = undefined;
    if (!read) {
      if (this.reads.size >= this.limit && !this.reads.has(key)) return Promise.reject(new Error('Inventory observation queue is full'));
      let resolve!: Read['resolve']; let reject!: Read['reject'];
      const promise = new Promise<unknown>((yes, no) => { resolve = yes; reject = no; });
      read = { key, controller: new AbortController(), users: 0, started: false, promise, resolve, reject, load };
      this.reads.set(key, read);
      this.queue.push(read);
    }
    const current = read;
    current.users++;
    const result = new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (complete: () => void) => {
        if (settled) return;
        settled = true;
        signal?.removeEventListener('abort', abort);
        current.users--;
        complete();
        if (current.users === 0 && this.reads.get(key) === current) {
          current.controller.abort();
          if (!current.started) {
            this.queue = this.queue.filter(entry => entry !== current);
            this.reads.delete(key);
            current.reject(new DOMException('Observation aborted', 'AbortError'));
          }
        }
      };
      const abort = () => finish(() => reject(new DOMException('Observation aborted', 'AbortError')));
      signal?.addEventListener('abort', abort, { once: true });
      current.promise.then(value => finish(() => resolve(value as T)), error => finish(() => reject(error)));
    });
    this.pump();
    return result;
  }

  private pump() {
    while (this.running < this.workers && this.queue.length) {
      const read = this.queue.shift()!;
      read.started = true;
      this.running++;
      void Promise.resolve().then(() => read.load(read.controller.signal)).then(read.resolve, read.reject).finally(() => {
        if (this.reads.get(read.key) === read) this.reads.delete(read.key);
        this.running--;
        this.pump();
      });
    }
  }
}

export const inventoryRequests = new InventoryRequests();
