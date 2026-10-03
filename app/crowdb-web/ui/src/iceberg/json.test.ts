// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { expect, test } from 'vitest';
import { parseIcebergJson } from './json';
test('Iceberg IDs survive parsing without changing quoted strings or ordinary numbers', () => {
  expect(parseIcebergJson('{"snapshot-id":9223372036854775807,"parent":-9223372036854775808,"count":123,"text":"id 9223372036854775807 \\"quoted\\"","float":1.25}')).toEqual({ 'snapshot-id': '9223372036854775807', parent: '-9223372036854775808', count: 123, text: 'id 9223372036854775807 "quoted"', float: 1.25 });
});
