// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

/** IDs and decimal u64s stay strings across HTTP, navigation and recovery. */
export interface Page<T> { entries: T[]; after: string | null }
export interface DatasetInfo { id: string; name: string; namespace: string; latest: string | null; stable: string | null }
export interface Head { latest: string | null; stable: string | null; generation: string | null }
export interface FieldDefinition { name: string; required: boolean }
export interface Schema { version: number; fields: FieldDefinition[] }
export interface SnapshotSummary {
  id: string; parent: string | null; schema_version: number; field_count: number;
  sample_count: string; manifest_sha256: string; inspection: 'ready' | 'unsupported'; reason: string | null;
}
export interface FieldMetadata {
  name: string; storage: 'inline' | 'payload' | 'tombstone'; length: string;
  md5: string | null; source_snapshot: string;
}
export interface MatrixRow { id: string; fields: Array<FieldMetadata | null> }
export interface Preview { state: 'value' | 'missing' | 'tombstone'; length: string | null; md5: string | null; data: string | null; truncated: boolean }
export interface SplitSource { snapshot: string; revision: string; name: string }
export type Predicate = { kind: 'all' } | { kind: 'explicit'; ids: string[] }
  | { kind: 'prefix' | 'keyword'; value: string } | { kind: 'range'; start: string; end: string }
  | { kind: 'field_match'; field: string; value: string } | { kind: 'and' | 'or'; items: Predicate[] }
  | { kind: 'not'; item: Predicate };
export interface SplitPolicy { overlap: 'disjoint' | 'allow_overlap'; coverage: 'allow_unassigned' | 'require_complete' }
export type SplitDefinition = { kind: 'selection'; predicate: Predicate }
  | { kind: 'weighted_partition'; seed: string; weights: Array<{ name: string; weight: number }>; index: number };
export interface DraftSplit {
  name: string; definition: SplitDefinition;
  origin: { kind: 'evaluated' } | { kind: 'inherited'; source: SplitSource };
}
export interface SplitRevision {
  snapshot: string; revision: string; operation: string; evaluator_version: number;
  policy: SplitPolicy; splits: Array<DraftSplit & { count: string }>;
}
export interface SplitHead { revision: string | null; generation: string | null }
export interface RevisionSummary { snapshot: string; revision: string; operation: string; split_count: number }
export type Target = { kind: 'snapshot'; snapshot: string } | { kind: 'split_revision'; snapshot: string; revision: string };
export type DraftProgress = { state: 'editing' | 'abandoned' }
  | { state: 'evaluating'; input_revision: string; next_page: string }
  | { state: 'evaluated'; input_revision: string; binding: string }
  | { state: 'prepared'; target: Target }
  | { state: 'committing' | 'published'; target: Target; attempt: string };
export interface Receipt { attempt: string; target: Target; expected_head: string | null; committed_head: string }
export type CompositionSource = { kind: 'all' } | { kind: 'selection'; predicate: Predicate }
  | { kind: 'split'; source: SplitSource; selection: Predicate | null };
export interface BeginComposition {
  operation: string; parent: string | null; source: CompositionSource; schema: Schema | null;
  inherit_parent_splits: boolean; parent_split_revision: string | null; splits: DraftSplit[]; policy: SplitPolicy | null;
}
export type FieldValue = { kind: 'inline'; data: string } | { kind: 'upload'; id: string } | { kind: 'tombstone' };
export type SampleChange = { kind: 'remove'; id: string } | { kind: 'upsert'; id: string; fields: Array<{ name: string; value: FieldValue }> };
export interface CompositionDraft extends Omit<BeginComposition, 'schema' | 'policy' | 'inherit_parent_splits'> {
  revision: string; schema: Schema; policy: SplitPolicy; progress: DraftProgress;
  source_lease_deadline: string; preparation: { snapshot: string; split_preview: SplitPreview | null } | null;
  uploads: { publication_ready: boolean; staged_uploads: string; candidate_uploads: string };
}
export interface SplitDraft {
  operation: string; snapshot: string; expected_revision: string | null; revision: string;
  policy: SplitPolicy; splits: DraftSplit[]; progress: DraftProgress;
}
export interface SplitPreview {
  input_revision: string; next_page: string; complete: boolean; member_counts: string[];
  validation: { scanned: string; assigned: string; unassigned: string; overlapping: string; unassigned_examples: string[]; overlap_examples: string[] };
}
export interface CompositionPreview { input_revision: string; next_page: string; members: string; complete: boolean; binding: string }
export interface UploadStatus { id: string; length: string; part_size: number; md5: string; state: 'open' | 'complete' | 'claimed' | 'aborted'; expires_at: string; cleanup_complete: boolean; parts: Array<{ index: number; length: string; md5: string }> }

export type DatasetSelection = { kind: 'root' } | { kind: 'dataset'; dataset: string }
  | ({ dataset: string; snapshot: string; revision?: string; split?: string } & (
    { kind: 'snapshot' | 'revision' | 'split' } | { kind: 'sample'; sample: string }
    | { kind: 'schema'; field: string; required: boolean } | { kind: 'cell'; sample: string; field: string }));
export const defaultPolicy: SplitPolicy = { overlap: 'disjoint', coverage: 'allow_unassigned' };
