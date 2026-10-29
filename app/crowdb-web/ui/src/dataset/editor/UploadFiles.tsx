// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { useRef, useState } from 'react';
import { buttonClass } from '../../access/Workbench';
import { Dialog } from '../../components/Dialog';

export function UploadFiles({ fields, choose }: { fields: string[]; choose: (files: File[], field: string | undefined) => void }) {
  const input = useRef<HTMLInputElement>(null); const target = useRef<string>();
  const [selecting, setSelecting] = useState(false);
  const pick = (field: string) => { target.current = field; setSelecting(false); input.current?.click(); };
  return <div>
    <button className={buttonClass} disabled={!fields.length} onClick={() => fields.length === 1 ? pick(fields[0]) : setSelecting(true)}>Upload files</button>
    <input ref={input} aria-label="Local payload files" className="tw-sr-only" type="file" multiple onChange={event => {
      const files = Array.from(event.target.files ?? []); event.target.value = '';
      const field = fields.length === 1 ? fields[0] : target.current; target.current = undefined;
      if (files.length) choose(files, field);
    }} />
    {selecting && <Dialog isOpen title="Choose upload Field" footer={null} onClose={() => setSelecting(false)}>
      <div className="tw-flex tw-flex-wrap tw-gap-2">{fields.map(field => <button key={field} className={buttonClass} onClick={() => pick(field)}>{field}</button>)}</div>
    </Dialog>}
  </div>;
}
