// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import type { Operation } from '../operation';

/** Store complete client inputs before network mutation, including requests larger than localStorage. */
function transact<T>(mode: IDBTransactionMode, action: (store: IDBObjectStore, fail: (error: Error) => void) => IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    const opening = indexedDB.open('crowdb-dataset-requests-v2', 1);
    opening.onupgradeneeded = () => opening.result.createObjectStore('requests');
    opening.onerror = () => reject(opening.error);
    opening.onblocked = () => reject(new Error('Close older Dataset tabs to open browser request storage.'));
    opening.onsuccess = () => {
      const database = opening.result;
      try {
        const transaction = database.transaction('requests', mode);
        let failure: Error | undefined;
        const request = action(transaction.objectStore('requests'), error => { failure = error; transaction.abort(); });
        transaction.oncomplete = () => { database.close(); resolve(request.result); };
        transaction.onabort = transaction.onerror = () => { database.close(); reject(failure ?? transaction.error ?? new Error('Browser could not save the complete Dataset request.')); };
      } catch (reason) { database.close(); reject(reason); }
    };
  });
}
export const readRequest = (key: string) => transact<Operation | undefined>('readonly', store => store.get(key));
export async function writeRequest(key: string, value: Operation) {
  await transact('readwrite', (store, fail) => {
    const current = store.get(key);
    current.onsuccess = () => {
      if (current.result && current.result.resource !== value.resource) { fail(new Error('Another request is saved for this Dataset. Resume or discard it first.')); return; }
      store.put(value, key);
    };
    return current;
  });
}
export async function removeRequest(key: string, resource: string) {
  await transact('readwrite', store => {
    const current = store.get(key);
    current.onsuccess = () => { if (current.result?.resource === resource) store.delete(key); };
    return current;
  });
}
