<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R162: access server / S3 — AWS Signature Version 4 authentication boundary

## Problem

Production S3 compatibility requires request authentication over the exact raw
method, URI, query, headers, and payload mode. Adding it after routing or body
normalization without a reserved hook risks accepting a different request than
the client signed. Credential distribution and rotation must also work across
stateless frontends.

The authentication boundary is
`doc/design/accessserver/design-crowdb-access-server.md` §§7 and 11 and
`doc/design/accessserver/design-crowdb-access-server-s3.md` §6.

## Solution

1. Implement standard SigV4 canonical request construction, signing-key
   derivation, constant-time signature comparison, timestamp/skew validation,
   credential scope, signed-header validation, and presigned request handling
   in the S3 library before operation dispatch.
2. Preserve and authenticate the raw HTTP components supplied by the R152
   hook. Authenticate headers before selecting a native body pool. Support only
   the payload-signing modes explicitly added and tested; reject unsupported
   streaming variants.
3. Add a group-0 management operation that accepts an S3 user identity,
   generates a random access-key ID and secret key, encrypts the secret with
   the configured cluster master key, durably commits the versioned credential
   record, and returns the plaintext secret once. The initial milestone issues
   independent user tokens; disable, revoke, stable-user deduplication, and
   rotation workflows are later management work. Never store plaintext secret
   material or expose it through logs, metrics, traces, or a subsequent read
   API.
   The initial master key is a required access-server configuration value;
   hardened provisioning, rotation, and external KMS/HSM integration are
   explicitly outside this milestone.
4. Make every access server scan the credential namespace before authenticated
   readiness and periodically rescan. Publish immutable cache snapshots by
   atomic pointer swap so request-path access-key lookup remains lock-free.
   Fail closed after a configured maximum-staleness interval and zeroize
   retired secrets. Notification-driven refresh is later management work.
5. Retain an explicit trusted-network unauthenticated mode only as a separately
   named configuration. Production mode fails closed if the provider is
   unavailable or a request cannot be verified.
6. Map every authentication failure through R163 without revealing whether an
   inaccessible bucket or key exists.

## Dependencies

- Depends on R152's raw-request authentication hook and R163 error mapping.
- Group 0 is the authoritative user and credential namespace. Its linearizable
  read, watch/notify, and periodic refresh paths must be available to the access
  server.
- R164 defines signed payload checksum modes.

## Acceptance

- Given AWS-published and independently generated positive/negative vectors,
  when canonicalization and verification run, assert exact expected signatures
  for URI, query, duplicate header, Unicode, clock-skew, and presigned cases.
  Invariant: verification matches standard SigV4. Unit test.
- Given a signed request whose raw path, query, header, or payload mode is
  changed before dispatch, when verification runs, assert rejection occurs
  before metadata or chunk access. Invariant: handlers execute only the signed
  request. Integration test.
- Given issuance, provider outage, and several frontend instances, when
  requests arrive, assert one-time secret return, lock-free snapshot
  replacement, maximum-staleness rejection, and no secret leakage. Invariant:
  group 0 is the sole credential authority and a stale cache cannot remain
  trusted indefinitely. E2E test.
- Given explicit trusted-network mode is off and credentials are unavailable,
  when the server starts or authenticates, assert it cannot silently bypass the
  hook. Invariant: unauthenticated service is always deliberate. Integration
  test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-s3 --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
