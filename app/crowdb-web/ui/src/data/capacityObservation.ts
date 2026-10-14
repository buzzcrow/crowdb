// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

export interface Observation<T> {
  value: T | null;
  observedAt: number | null;
  error: string | null;
}

export const unobserved = <T>(): Observation<T> => ({ value: null, observedAt: null, error: null });

export function retainObservation<T>(previous: Observation<T>, result: PromiseSettledResult<T>): Observation<T> {
  return result.status === 'fulfilled'
    ? { value: result.value, observedAt: Date.now(), error: null }
    : { ...previous, error: result.reason instanceof Error ? result.reason.message : String(result.reason) };
}

export interface CapacityObservationStatus {
  source: string;
  observedAt: number | null;
  error: string | null;
  cached: boolean;
}
