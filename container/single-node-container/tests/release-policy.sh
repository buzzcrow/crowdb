#!/bin/bash
set -euo pipefail

release=.github/workflows/release-container.yml
ci=.github/workflows/ci.yml

events=$(sed -n '/^on:/,/^concurrency:/p' "$release")
[[ "$events" == *'workflow_dispatch:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$events"

for required in \
    'environment: preview-release' \
    'PREVIEW_RELEASE_ENABLED' \
    'DOCKERHUB_TOKEN' \
    'ref: ${{ inputs.tag }}' \
    'git rev-parse --verify "refs/tags/$RELEASE_TAG^{commit}"' \
    '[[ "$revision" == "$(git rev-parse HEAD)" ]]' \
    'gh release view "$RELEASE_TAG"' \
    '[[ "$status" == 404 ]]' \
    'needs: verify' \
    'docker.io/crowdb/crowdb-iceberg-single-node:${{ inputs.tag }}' \
    'docker.io/crowdb/crowdb-iceberg-single-node:git-${{ needs.verify.outputs.revision }}' \
    'docker.io/crowdb/crowdb-iceberg-single-node:preview' \
    'provenance: mode=max' \
    'sbom: true' \
    'cosign sign --yes'; do
    grep -Fq "$required" "$release"
done
! grep -Eq 'crowdb-iceberg-single-node:latest' "$release"
[[ $(grep -c 'push: true' "$release") == 1 ]]
[[ $(grep -c 'id-token: write' "$release") == 1 ]]
[[ "$events" != *'schedule:'* ]]

verify_job=$(sed -n '/^  verify:/,/^  publish:/p' "$release")
publish_job=$(sed -n '/^  publish:/,$p' "$release")
for gate in 'pixi run test-single-node-container' 'test-boto3-e2e' 'test-pyiceberg-e2e' \
    'pixi run test-console' 'pixi run test-console-ui' 'pixi run rs-fmt-check && pixi run rs-lint'; do
    [[ "$verify_job" == *"$gate"* ]]
done
! grep -Eq 'DOCKERHUB_|push: true|id-token: write' <<<"$verify_job"
[[ "$publish_job" == *'needs: verify'* && "$publish_job" == *'environment: preview-release'* ]]
[[ "$publish_job" == *'[[ "$RELEASE_ENABLED" == true ]]'* ]]
[[ "$publish_job" == *'[[ "$(git rev-parse HEAD)" == "$REVISION" ]]'* ]]

ci_job=$(sed -n '/^  DockerPreview:/,$p' "$ci")
[[ "$ci_job" == *'contents: read'* && "$ci_job" == *'pixi run test-single-node-container'* ]]
[[ "$ci_job" == *'Upload preview failure logs'* && "$ci_job" == *'CROWDB_PREVIEW_TEST_ARTIFACTS'* ]]
! grep -Eq 'secrets\.|docker/login-action|docker/build-push-action' <<<"$ci_job"
