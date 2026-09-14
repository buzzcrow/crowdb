<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Access Server Dataset Service

The Dataset module is an AI-native peer protocol for datasets, generations,
samples, shards, tensors, batches, streams, and prefetch-aware access.

Depends on: [Access Server](design-crowdb-access-server.md),
[Chunk I/O](../chunkio/design-crowdb-chunkio.md), and
[Chunk-KV](../chunkds/design-crowdb-chunk-kv.md).

Satisfies: dataset-oriented access whose batching and transfer semantics are
not constrained by S3 compatibility.

## Table of contents

1. [Boundary](#1-boundary)
2. [Dataset model](#2-dataset-model)
3. [Read and ingest semantics](#3-read-and-ingest-semantics)
4. [Transfer choices](#4-transfer-choices)
5. [Correctness invariants](#5-correctness-invariants)

## 1. Boundary

Dataset is not an S3 extension. It may share stored bytes or name immutable S3
object generations, but it owns dataset identity, sample selection, batching,
ordering, shuffle, streaming, prefetch, authorization, and error semantics.
Small duplicated protocol code is preferred to a common abstraction that
constrains these operations.

## 2. Dataset model

Chunk-KV stores immutable dataset generations and indexes from dataset concepts
to chunk-backed byte ranges. A manifest may describe shards, samples, tensors,
column groups, encodings, and checksums. Chunk storage owns the bytes and their
physical placement.

Every read selects one dataset generation. Mutable ingest builds a new
generation and publishes it atomically; readers do not observe a partially
constructed manifest.

## 3. Read and ingest semantics

The protocol supports bounded streams and may request multiple disjoint ranges,
sample batches, or tensor shards in one operation. Server-side selection may
decode, filter, transform, or repack data when the request explicitly selects
that behavior. Backpressure and admission bound input, processing, output, and
prefetch independently of total dataset size.

## 4. Transfer choices

CPU streaming uses the ordinary chunk and RPC buffer ownership model. The
optional accelerated-transfer module may implement scatter/gather or GPU-direct
delivery through cuObject-compatible or native DC mechanisms. Dataset semantics
do not extend the S3 protocol and are free to use multiple destination ranges.

## 5. Correctness invariants

- **DATA-I1 — Generation consistency:** one operation uses one immutable
  dataset generation.
- **DATA-I2 — Exact selection:** returned samples, shards, and tensor ranges
  correspond exactly to the selected manifest entries.
- **DATA-I3 — Bounded streaming:** dataset size does not determine Access
  Server memory or prefetch depth.
- **DATA-I4 — Protocol independence:** S3 compatibility cannot restrict native
  batch, stream, or scatter/gather behavior.
- **DATA-I5 — Transfer equivalence:** CPU and accelerated paths return the same
  logical data and integrity result.
