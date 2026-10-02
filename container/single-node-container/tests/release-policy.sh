#!/bin/bash
set -euo pipefail

release=.github/workflows/release-container.yml
preview=.github/workflows/docker-preview.yml

events=$(sed -n '/^on:/,/^concurrency:/p' "$release")
[[ "$events" == *'workflow_dispatch:'* ]]
[[ "$events" != *'candidate_sha:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$events"

for required in \
    'environment: DockerHub' \
    'DOCKERHUB_TOKEN' \
    'ref: ${{ github.sha }}' \
    'ref: ${{ needs.verify.outputs.revision }}' \
    'refs/heads/release/' \
    '[[ "$revision" == "$GITHUB_SHA" ]]' \
    'refs/heads/$RELEASE_BRANCH' \
    'actions: read' \
    'runtime_sha256: ${{ steps.runtime_digest.outputs.sha256 }}' \
    'RUNTIME_SHA256=${{ needs.verify.outputs.runtime_sha256 }}' \
    'needs: verify' \
    'docker.io/crowdb/crowdb-iceberg:${{ needs.verify.outputs.image_tag }}' \
    'docker.io/crowdb/crowdb-iceberg:latest' \
    'docker.io/crowdb/crowdb-s3:${{ needs.verify.outputs.image_tag }}' \
    'docker.io/crowdb/crowdb-s3:latest' \
    'pixi run cosign sign --yes "docker.io/crowdb/crowdb-iceberg@$DIGEST"' \
    'pixi run cosign sign --yes "docker.io/crowdb/crowdb-s3@$DIGEST"' \
    'provenance: mode=max' \
    'sbom: true' \
    'cosign sign --yes'; do
    grep -Fq "$required" "$release"
done
! grep -Eq 'crowdb-(iceberg|s3):preview' "$release"
[[ $(grep -c 'push: true' "$release") == 1 ]]
[[ $(grep -c 'id-token: write' "$release") == 1 ]]
[[ "$events" != *'schedule:'* ]]
[[ "$events" != *'      tag:'* ]]
! grep -Eq 'gh release (create|edit|upload)|git (tag|push origin "refs/tags/)' "$release"
! grep -Fq 'steps.registry.outputs' "$release"

verify_job=$(sed -n '/^  verify:/,/^  publish:/p' "$release")
publish_job=$(sed -n '/^  publish:/,$p' "$release")
[[ "$verify_job" == *'name: verified-container-runtime'* ]]
[[ "$publish_job" == *'name: verified-container-runtime'* ]]
[[ "$events" != *'include_symbols:'* ]]
[[ "$verify_job" != *'verified-container-symbols'* ]]
[[ "$publish_job" == *'context: target/container-runtime'* ]]
[[ "$verify_job" == *'pixi run test-single-node-container'* ]]
[[ "$verify_job" != *'Require CI success for the release branch commit'* ]]
[[ "$verify_job" != *'git push origin'* && "$verify_job" != *'gh release create'* ]]
! grep -Eq 'DOCKERHUB_|push: true|id-token: write' <<<"$verify_job"
[[ "$publish_job" == *'needs: verify'* && "$publish_job" == *'environment: DockerHub'* ]]
[[ "$publish_job" != *'RELEASE_ENABLED'* ]]
[[ "$publish_job" == *'[[ "$(git rev-parse HEAD)" == "$REVISION" ]]'* ]]
[[ "$publish_job" == *'Confirm release branch head before image push'*'Build and replace release branch image with attestations'*'Sign published digest'* ]]
grep -Fq 'org.crowdb.runtime.sha256="$RUNTIME_SHA256"' container/single-node-container/Dockerfile

grep -Fq 'branches: [ "main", "release/**" ]' .github/workflows/ci.yml

preview_events=$(sed -n '/^on:/,/^jobs:/p' "$preview")
[[ "$preview_events" == *'workflow_dispatch:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$preview_events"
preview_job=$(sed -n '/^  DockerPreview:/,$p' "$preview")
[[ "$preview_job" == *'contents: read'* && "$preview_job" == *'pixi run test-single-node-container'* ]]
[[ "$preview_job" == *'Upload preview failure logs'* && "$preview_job" == *'CROWDB_PREVIEW_TEST_ARTIFACTS'* ]]
! grep -Eq 'secrets\.|docker/login-action|docker/build-push-action' <<<"$preview_job"

release_tool=tools/release.py
for required in '--dry-run' '--execute' '"workflow", "run"' \
    '"--ref", branch'; do
    grep -Fq -- "$required" "$release_tool"
done
! grep -Eq '"release", "create"|"tag", "-a"|"push", "origin"|candidate_sha' "$release_tool"
! grep -Fq -- '--symbols' "$release_tool"
