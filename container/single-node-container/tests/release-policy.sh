#!/bin/bash
set -euo pipefail

release=.github/workflows/release-container.yml
preview=.github/workflows/docker-preview.yml

events=$(sed -n '/^on:/,/^concurrency:/p' "$release")
[[ "$events" == *'workflow_dispatch:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$events"

for required in \
    'environment: DockerHub' \
    'DOCKERHUB_TOKEN' \
    'ref: ${{ inputs.tag }}' \
    'git rev-parse --verify "refs/tags/$RELEASE_TAG^{commit}"' \
    '[[ "$revision" == "$(git rev-parse HEAD)" ]]' \
    'gh release view "$RELEASE_TAG"' \
    '== true ]]' \
    '[[ "$status" == 404 ]]' \
    'needs: verify' \
    'docker.io/crowdb/crowdb-iceberg:${{ inputs.tag }}' \
    'docker.io/crowdb/crowdb-iceberg:git-${{ needs.verify.outputs.revision }}' \
    'provenance: mode=max' \
    'sbom: true' \
    'cosign sign --yes'; do
    grep -Fq "$required" "$release"
done
! grep -Eq 'crowdb-iceberg:(preview|latest)' "$release"
[[ $(grep -c 'push: true' "$release") == 1 ]]
[[ $(grep -c 'id-token: write' "$release") == 1 ]]
[[ "$events" != *'schedule:'* ]]

verify_job=$(sed -n '/^  verify:/,/^  publish:/p' "$release")
publish_job=$(sed -n '/^  publish:/,$p' "$release")
[[ "$verify_job" == *'name: verified-container-runtime'* ]]
[[ "$publish_job" == *'name: verified-container-runtime'* ]]
[[ "$verify_job" == *'name: verified-container-symbols'* ]]
[[ "$publish_job" == *'name: verified-container-symbols'* ]]
[[ "$events" == *'include_symbols:'* && "$events" == *'default: false'* ]]
[[ "$verify_job" == *"CROWDB_PACKAGE_SYMBOLS: \${{ inputs.include_symbols && '1' || '0' }}"* ]]
[[ "$verify_job" == *'pixi run -- python tools/ci-checks/check-container-symbols.py'* ]]
[[ "$verify_job" == *'if: inputs.include_symbols'* ]]
[[ "$publish_job" == *'if: inputs.include_symbols && steps.symbols_download.outcome'* ]]
[[ "$verify_job" == *'continue-on-error: true'* ]]
[[ "$publish_job" == *'continue-on-error: true'* ]]
[[ "$publish_job" == *'gh release upload "$RELEASE_TAG"'* ]]
[[ "$publish_job" == *'gh release edit "$RELEASE_TAG"'* ]]
[[ "$publish_job" == *'context: target/container-runtime'* ]]
for gate in 'pixi run test-single-node-container' 'test-boto3-e2e' 'test-pyiceberg-e2e' \
    'pixi run test-console' 'pixi run test-console-ui' 'pixi run rs-fmt-check && pixi run rs-lint'; do
    [[ "$verify_job" == *"$gate"* ]]
done
! grep -Eq 'DOCKERHUB_|push: true|id-token: write' <<<"$verify_job"
[[ "$publish_job" == *'needs: verify'* && "$publish_job" == *'environment: DockerHub'* ]]
[[ "$publish_job" != *'RELEASE_ENABLED'* ]]
[[ "$publish_job" == *'[[ "$(git rev-parse HEAD)" == "$REVISION" ]]'* ]]

preview_events=$(sed -n '/^on:/,/^jobs:/p' "$preview")
[[ "$preview_events" == *'workflow_dispatch:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$preview_events"
preview_job=$(sed -n '/^  DockerPreview:/,$p' "$preview")
[[ "$preview_job" == *'contents: read'* && "$preview_job" == *'pixi run test-single-node-container'* ]]
[[ "$preview_job" == *'Upload preview failure logs'* && "$preview_job" == *'CROWDB_PREVIEW_TEST_ARTIFACTS'* ]]
! grep -Eq 'secrets\.|docker/login-action|docker/build-push-action' <<<"$preview_job"

release_tool=tools/release.py
for required in '--dry-run' '--execute' '--symbols' 'git", "push", "--atomic"' \
    '"release", "create"' '"workflow", "run"'; do
    grep -Fq -- "$required" "$release_tool"
done
