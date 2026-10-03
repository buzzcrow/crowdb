// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import type { Selection } from './types';

export function navigationSelection(selection: Selection | null): Selection | null {
  if (!selection) return null;
  const snapshot = { 'snapshot-id': selection.snapshot['snapshot-id'],
    'manifest-list': selection.snapshot['manifest-list'] };
  const manifest = selection.manifest ? {
    location: selection.manifest.location, content: selection.manifest.content,
    size: selection.manifest.size, partition_spec_id: selection.manifest.partition_spec_id,
    sequence: selection.manifest.sequence, file_counts: selection.manifest.file_counts.slice(0, 3),
  } : undefined;
  const file = selection.file ? {
    location: selection.file.location, size: selection.file.size, format: selection.file.format,
    status: selection.file.status, content: selection.file.content, records: selection.file.records,
  } : undefined;
  return { kind: selection.kind, snapshot, manifest, file };
}
