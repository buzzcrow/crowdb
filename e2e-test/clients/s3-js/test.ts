// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import * as S3 from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

function env(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}
const config: S3.S3ClientConfig = {
  endpoint: env("CROWDB_S3_E2E_ENDPOINT"), region: "us-east-1", forcePathStyle: true,
  credentials: { accessKeyId: env("CROWDB_S3_E2E_ACCESS_KEY"), secretAccessKey: env("CROWDB_S3_E2E_SECRET_KEY") },
  requestHandler: { connectionTimeout: 10_000, requestTimeout: 60_000 },
};
const client = new S3.S3Client(config);
const Bucket = `crowdb-js-${randomUUID()}`;
let created = false;
let checksum = false;
let current = "setup";
let cleanupPromise: Promise<void> | undefined;
class InjectedFailure extends Error {}
function step(name: string) { current = name; console.log(`js: ${name}`); }
client.middlewareStack.add((next, context) => async (args) => {
  if (context.commandName === "PutObjectCommand" && (args.input as S3.PutObjectCommandInput).Key === "prefix/ordinary") {
    const headers = (args.request as { headers: Record<string, string> }).headers;
    checksum ||= Boolean(headers["x-amz-checksum-crc32"] || headers["x-amz-trailer"]?.includes("crc32"));
  }
  return next(args);
}, { step: "finalizeRequest", name: "observeDefaultChecksum", priority: "low" });

async function get(Key: string): Promise<Buffer> {
  const result = await client.send(new S3.GetObjectCommand({ Bucket, Key }));
  return Buffer.from(await result.Body!.transformToByteArray());
}
async function put(Key: string, Body: Buffer) {
  await client.send(new S3.PutObjectCommand({ Bucket, Key, Body }));
}
async function expectCode(code: string, action: () => Promise<unknown>) {
  try { await action(); } catch (e) {
    assert(e instanceof S3.S3ServiceException && e.name === code, `expected ${code}`); return;
  }
  assert.fail(`expected ${code}`);
}
async function failureScenario() {
  step("injected failure after an uploaded part");
  await client.send(new S3.CreateBucketCommand({ Bucket })); created = true;
  const { UploadId } = await client.send(new S3.CreateMultipartUploadCommand({ Bucket, Key: "unfinished" }));
  await client.send(new S3.UploadPartCommand({ Bucket, Key: "unfinished", UploadId, PartNumber: 1, Body: Buffer.from("unfinished bytes") }));
  throw new InjectedFailure();
}
async function scenarios() {
  const small = Buffer.from("SDK exact bytes");
  const Metadata = { mtime: "1700000000.123", origin: "js" };
  step("bucket discovery and ordinary default checksum");
  await client.send(new S3.CreateBucketCommand({ Bucket })); created = true;
  await client.send(new S3.HeadBucketCommand({ Bucket }));
  assert((await client.send(new S3.ListBucketsCommand({}))).Buckets?.some(b => b.Name === Bucket));
  await client.send(new S3.PutObjectCommand({ Bucket, Key: "prefix/ordinary", Body: small, Metadata }));
  assert(checksum, "default CRC32 was not transmitted");
  assert.deepEqual(await get("prefix/ordinary"), small);
  assert.deepEqual((await client.send(new S3.HeadObjectCommand({ Bucket, Key: "prefix/ordinary" }))).Metadata, Metadata);
  const range = await client.send(new S3.GetObjectCommand({ Bucket, Key: "prefix/ordinary", Range: "bytes=1-4" }));
  assert.deepEqual(Buffer.from(await range.Body!.transformToByteArray()), small.subarray(1, 5));
  assert.deepEqual(range.Metadata, Metadata);

  step("copy, replacement metadata and paginated prefix listing");
  await client.send(new S3.CopyObjectCommand({ Bucket, Key: "copied", CopySource: `${Bucket}/prefix/ordinary` }));
  assert.deepEqual(await get("copied"), small);
  assert.deepEqual((await client.send(new S3.HeadObjectCommand({ Bucket, Key: "copied" }))).Metadata, Metadata);
  await client.send(new S3.CopyObjectCommand({ Bucket, Key: "copied", CopySource: `${Bucket}/copied`, MetadataDirective: "REPLACE", Metadata: { origin: "replacement" } }));
  assert.deepEqual((await client.send(new S3.HeadObjectCommand({ Bucket, Key: "copied" }))).Metadata, { origin: "replacement" });
  for (const key of ["prefix/a", "prefix/b", "outside"]) await put(key, small);
  const keys = new Set<string>(); let pages = 0;
  for await (const page of S3.paginateListObjectsV2({ client, pageSize: 1 }, { Bucket, Prefix: "prefix/" })) {
    pages++; assert((page.Contents?.length ?? 0) <= 1);
    for (const item of page.Contents ?? []) { assert(!keys.has(item.Key!)); keys.add(item.Key!); }
  }
  assert(pages >= 3); assert.deepEqual(keys, new Set(["prefix/a", "prefix/b", "prefix/ordinary"]));

  step("low-level multipart and abort");
  const first = Buffer.alloc(5 * 1024 * 1024);
  for (let i = 0; i < first.length; i++) first[i] = i % 251;
  const { UploadId } = await client.send(new S3.CreateMultipartUploadCommand({ Bucket, Key: "multipart", Metadata }));
  const Parts: S3.CompletedPart[] = [];
  for (const [index, Body] of [first, small].entries()) {
    const part = await client.send(new S3.UploadPartCommand({ Bucket, Key: "multipart", UploadId, PartNumber: index + 1, Body }));
    Parts.push({ PartNumber: index + 1, ETag: part.ETag });
  }
  assert.equal((await client.send(new S3.ListPartsCommand({ Bucket, Key: "multipart", UploadId }))).Parts?.length, 2);
  await client.send(new S3.CompleteMultipartUploadCommand({ Bucket, Key: "multipart", UploadId, MultipartUpload: { Parts } }));
  assert.deepEqual(await get("multipart"), Buffer.concat([first, small]));
  assert.deepEqual((await client.send(new S3.HeadObjectCommand({ Bucket, Key: "multipart" }))).Metadata, Metadata);
  const abort = await client.send(new S3.CreateMultipartUploadCommand({ Bucket, Key: "abandoned" }));
  await client.send(new S3.UploadPartCommand({ Bucket, Key: "abandoned", UploadId: abort.UploadId, PartNumber: 1, Body: small }));
  await client.send(new S3.AbortMultipartUploadCommand({ Bucket, Key: "abandoned", UploadId: abort.UploadId }));
  await expectCode("NoSuchKey", () => get("abandoned"));

  step("SDK presigned PUT and GET");
  const putUrl = await getSignedUrl(client, new S3.PutObjectCommand({ Bucket, Key: "presigned", Body: small }), { expiresIn: 120 });
  const uploaded = await fetch(putUrl, { method: "PUT", body: small, signal: AbortSignal.timeout(30_000) });
  assert.equal(uploaded.status, 200); await uploaded.arrayBuffer();
  const corrupted = await fetch(putUrl, { method: "PUT", body: Buffer.from("corrupt payload"), signal: AbortSignal.timeout(30_000) });
  assert.equal(corrupted.status, 400, "presigned checksum must reject changed bytes"); await corrupted.arrayBuffer();
  const getUrl = await getSignedUrl(client, new S3.GetObjectCommand({ Bucket, Key: "presigned" }), { expiresIn: 120 });
  const response = await fetch(getUrl, { signal: AbortSignal.timeout(30_000) });
  assert.equal(response.status, 200); assert.deepEqual(Buffer.from(await response.arrayBuffer()), small);

  step("missing object, invalid credentials and corrupt checksum preservation");
  await expectCode("NoSuchKey", () => get("missing"));
  await expectCode("BadDigest", () => client.send(new S3.PutObjectCommand({ Bucket, Key: "prefix/ordinary", Body: small, ChecksumCRC32: "AAAAAA==" })));
  assert.deepEqual(await get("prefix/ordinary"), small);
  const invalid = new S3.S3Client({ ...config, credentials: { accessKeyId: "invalid", secretAccessKey: "invalid" } });
  try {
    try { await invalid.send(new S3.ListBucketsCommand({})); assert.fail("invalid credentials accepted"); }
    catch (e) { assert(e instanceof S3.S3ServiceException && e.$metadata.httpStatusCode === 403); }
  } finally { invalid.destroy(); }
  step("single and batch deletion");
  await client.send(new S3.DeleteObjectCommand({ Bucket, Key: "outside" }));
  await expectCode("NoSuchKey", () => get("outside"));
  const deleted = await client.send(new S3.DeleteObjectsCommand({ Bucket, Delete: { Objects: [{ Key: "prefix/a" }, { Key: "prefix/b" }] } }));
  assert.equal(deleted.Errors?.length ?? 0, 0); assert.equal(deleted.Deleted?.length, 2);
  for (const key of ["prefix/a", "prefix/b"]) await expectCode("NoSuchKey", () => get(key));
}

async function cleanup() {
  if (!created) return;
  cleanupPromise ??= (async () => {
    const cleanupClient = new S3.S3Client(config);
    try {
      let KeyMarker: string | undefined, UploadIdMarker: string | undefined;
      for (;;) {
        const page: S3.ListMultipartUploadsCommandOutput = await cleanupClient.send(new S3.ListMultipartUploadsCommand({ Bucket, KeyMarker, UploadIdMarker }));
        for (const upload of page.Uploads ?? []) await cleanupClient.send(new S3.AbortMultipartUploadCommand({ Bucket, Key: upload.Key, UploadId: upload.UploadId }));
        if (!page.IsTruncated) break;
        KeyMarker = page.NextKeyMarker; UploadIdMarker = page.NextUploadIdMarker;
      }
      const keys: string[] = [];
      for await (const page of S3.paginateListObjectsV2({ client: cleanupClient }, { Bucket }))
        for (const object of page.Contents ?? []) keys.push(object.Key!);
      for (const Key of keys) await cleanupClient.send(new S3.DeleteObjectCommand({ Bucket, Key }));
      await cleanupClient.send(new S3.DeleteBucketCommand({ Bucket }));
      assert(!(await cleanupClient.send(new S3.ListBucketsCommand({}))).Buckets?.some(b => b.Name === Bucket));
      console.log("js: owned bucket and multipart sessions cleaned");
    } finally { cleanupClient.destroy(); }
  })();
  await cleanupPromise;
}
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => {
  client.destroy();
  void cleanup().then(() => process.exit(130), () => { console.error("js: interrupt cleanup FAILED"); process.exit(1); });
});
let exit = 0;
try {
  if (process.env.CROWDB_S3_SDK_FAULT === "after-mpu") await failureScenario();
  await scenarios();
} catch (e) {
  exit = e instanceof InjectedFailure ? 42 : 1;
  // Exception messages/stacks can contain signed URLs; retain only the operation and class/code.
  console.error(`js: FAILED step=${current} code=${e instanceof Error ? e.name : "UnknownError"}`);
  if (e instanceof assert.AssertionError && typeof e.actual === "number" && typeof e.expected === "number")
    console.error(`js: status assertion actual=${e.actual} expected=${e.expected}`);
} finally {
  try { await cleanup(); } catch { exit = 1; console.error("js: cleanup FAILED"); }
  client.destroy();
}
if (exit === 0) console.log("js: PASS AWS SDK 3.1146.0, Node HTTP, default checksums/retries, sequential low-level MPU");
process.exitCode = exit;
