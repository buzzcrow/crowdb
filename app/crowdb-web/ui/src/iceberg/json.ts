// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
/** Preserve integral identities beyond JavaScript's exact range before JSON parsing. */
export function parseIcebergJson(text: string): unknown {
  return JSON.parse(text.replace(/"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g, token => {
    if (token.startsWith('"') || /[.eE]/.test(token)) return token;
    return Number.isSafeInteger(Number(token)) ? token : `"${token}"`;
  }));
}
