<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R206: access-s3 — Credential principals and namespace authorization

#### Problem

The general S3 listener selects one configured TenantId for every operation.
Durable credential records retain user binding, but CredentialProvider returns
only secret/session-token/enabled state, and RequestAuthenticator returns unit
after signature verification. Consequently ProductionS3Operations cannot
distinguish authenticated users or enforce per-user source/destination/bucket
rights. Multiple accepted credentials on the same listener share its namespace.

This applies to existing GET/PUT/list/delete as well as server-side copy and
batch deletion. Passing wrong-key/signature tests proves authentication, not
isolation between two accepted users. The [S3 authority design](../design/access-server/s3/design-crowdb-access-s3.md)
must define the authorized scope before its namespace-isolation invariant can
be evaluated across multiple users and listeners.

#### Solution

Select and document the authority model before changing credential routing:

1. Shared-realm credentials: every accepted credential grants full access to
   the listener's configured namespace. Explicitly document this restriction,
   define which credentials a listener may accept, and bind credential vending
   and refresh to that realm rather than accepting unrelated users implicitly.
2. Principal-scoped namespaces/grants: retain authenticated identity through
   lib/crowdb-access-s3/src/auth.rs and auth/sigv4.rs, credential snapshots and
   app/crowdb-access-server/src/s3/dispatcher.rs. Resolve a stable authorized
   tenant/grant in operations rather than trusting a caller-supplied namespace.
   Define user migration, shared buckets and administrative credentials.
3. For either model, authorize every bucket/object operation consistently.
   Copy requires both source read and destination write; DeleteObjects requires
   bucket-level admission and the same per-key rights as DeleteObject. Bind
   continuation state to the resulting authorized scope. Do not add per-request
   global locks or infer tenant identity from untrusted bucket names.

#### Dependencies

- Existing credential issuance, encrypted durable user binding, immutable
  credential snapshots and listener configuration are the baseline.
- Copy and batch deletion preserve the current configured-listener scope.
  Their accepted ordinary-client workflows do not certify per-user bucket ACLs
  or IAM semantics. Client compatibility recipes must state this limitation.

#### Acceptance

- Given two accepted users and two namespace scopes, execute GET/PUT/list/delete
  and copy/batch deletion; assert the selected authority model's allowed and
  denied paths before payload reads or mutations. Integration test.
- Given two listeners and replayed credentials/tokens, access each scope;
  assert credentials and continuation state cannot escape their declared realm.
  E2E test.
- Given credential disable/refresh and an in-flight operation, execute retries;
  assert documented revocation behavior with no privilege gained through stale
  snapshots. Integration test.
- Given an existing deployment and the selected migration policy, restart and
  access existing bucket/object identities; assert no implicit data reassignment
  or accidental sharing. E2E test.

Run pixi run test-access-s3, pixi run test-access-server,
pixi run clean-env && pixi run -e s3-e2e test-boto3-e2e,
pixi run rs-fmt-check, and pixi run rs-lint.

#### Open Questions

- Are issued users full-access keys for one explicitly bound shared realm, or
  independent principals requiring namespace/grant isolation? The former
  preserves current operation routing with restricted credential acceptance;
  the latter changes authentication context and needs a migration/grant model.
