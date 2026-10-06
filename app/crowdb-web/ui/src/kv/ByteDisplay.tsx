// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { byteRuns } from './displayBytes';

export function ByteDisplay({ text, hex }: { text?: string; hex?: string }) {
  return <>{byteRuns(text, hex).map((run, index) => run.binary
    ? <span key={index} className="tw-text-[#858585]" data-byte-format="hex" title="Hexadecimal bytes">{run.text}</span>
    : <span key={index}>{run.text}</span>)}</>;
}
