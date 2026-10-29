// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Operation } from '../operation';

/** Each local draft has its own identity. Transactions fence concurrent tabs. */
function transact<T>(mode: IDBTransactionMode, action: (store: IDBObjectStore, done: (value: T) => void, fail: (error: Error) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    const opening = indexedDB.open('crowdb-dataset-requests-v2', 1);
    opening.onupgradeneeded = () => opening.result.createObjectStore('requests');
    opening.onerror = () => reject(opening.error);
    opening.onblocked = () => reject(new Error('Close older Dataset tabs to open browser request storage.'));
    opening.onsuccess = () => {
      const database = opening.result;
      try {
        const transaction = database.transaction('requests', mode);
        let value: T; let failure: Error | undefined;
        transaction.oncomplete = () => { database.close(); resolve(value); };
        transaction.onabort = transaction.onerror = () => { database.close(); reject(failure ?? transaction.error ?? new Error('Browser could not save the Draft.')); };
        action(transaction.objectStore('requests'), result => { value = result; }, error => { failure = error; transaction.abort(); });
      } catch (reason) { database.close(); reject(reason); }
    };
  });
}
const range = (key: string) => IDBKeyRange.bound(`${key}:`, `${key}:\uffff`);
export async function listRequests(key: string): Promise<Operation[]> {
  return transact('readonly', (store, done) => {
    const request = store.openCursor(range(key));
    const values: Operation[] = [];
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor) {
        if (String(cursor.key).length === key.length + 33) values.push(cursor.value);
        cursor.continue(); return;
      }
      const legacy = store.get(key);
      legacy.onsuccess = () => done([...values, ...(legacy.result ? [legacy.result] : [])].sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0)));
    };
  });
}
export async function readRequest(key: string, resource?: string): Promise<Operation | undefined> {
  const values = await listRequests(key);
  return resource ? values.find(value => value.resource === resource) : values[0];
}
export async function writeRequest(key: string, value: Operation) {
  const revision = (value.localRevision ?? 0) + 1;
  const updatedAt = Date.now();
  await transact<void>('readwrite', (store, done, fail) => {
    const current = store.get(`${key}:${value.resource}`);
    current.onsuccess = () => {
      if ((current.result?.localRevision ?? 0) !== (value.localRevision ?? 0)) { fail(new Error('This Draft changed in another tab. Reopen it before editing.')); return; }
      store.put({ ...value, localRevision: revision, updatedAt }, `${key}:${value.resource}`);
      const legacy = store.get(key);
      legacy.onsuccess = () => { if (legacy.result?.resource === value.resource) store.delete(key); done(); };
    };
  });
  value.localRevision = revision; value.updatedAt = updatedAt;
  window.dispatchEvent(new Event('crowdb-dataset-drafts'));
}
export async function removeRequest(key: string, resource: string, revision?: number) {
  await transact<void>('readwrite', (store, done, fail) => {
    const current = store.get(`${key}:${resource}`);
    current.onsuccess = () => {
      if (current.result && (current.result.localRevision ?? 0) !== (revision ?? 0)) { fail(new Error('This Draft changed in another tab. Reopen it before discarding.')); return; }
      store.delete(`${key}:${resource}`);
      const legacy = store.get(key);
      legacy.onsuccess = () => { if (legacy.result?.resource === resource) store.delete(key); done(); };
    };
  });
  window.dispatchEvent(new Event('crowdb-dataset-drafts'));
}
