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
    'ref: ${{ github.sha }}' \
    'ref: ${{ needs.verify.outputs.revision }}' \
    'git rev-parse "refs/tags/$RELEASE_TAG^{commit}"' \
    '[[ "$revision" == "$GITHUB_SHA" ]]' \
    'gh release view "$RELEASE_TAG"' \
    'gh release create "$RELEASE_TAG"' \
    'git push origin "refs/tags/$RELEASE_TAG"' \
    'actions: read' \
    'head_sha=$REVISION&branch=main&event=push' \
    'completed/success) exit 0' \
    'reuse=false' \
    'reuse=true' \
    'runtime_sha256: ${{ steps.runtime_digest.outputs.sha256 }}' \
    'RUNTIME_SHA256=${{ needs.verify.outputs.runtime_sha256 }}' \
    'org.crowdb.runtime.sha256' \
    'needs: verify' \
    'docker.io/crowdb/crowdb-iceberg:${{ needs.verify.outputs.tag }}' \
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
[[ "$events" != *'      tag:'* ]]

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
for gate in 'pixi run test-single-node-container' 'Require CI success for the release commit'; do
    [[ "$verify_job" == *"$gate"* ]]
done
[[ "$verify_job" != *'git push origin'* && "$verify_job" != *'gh release create'* ]]
! grep -Eq 'DOCKERHUB_|push: true|id-token: write' <<<"$verify_job"
[[ "$publish_job" == *'needs: verify'* && "$publish_job" == *'environment: DockerHub'* ]]
[[ "$publish_job" != *'RELEASE_ENABLED'* ]]
[[ "$publish_job" == *'[[ "$(git rev-parse HEAD)" == "$REVISION" ]]'* ]]
[[ "$publish_job" == *'Create release tag after verification'* ]]
grep -Fq 'org.crowdb.runtime.sha256="$RUNTIME_SHA256"' container/single-node-container/Dockerfile

preview_events=$(sed -n '/^on:/,/^jobs:/p' "$preview")
[[ "$preview_events" == *'workflow_dispatch:'* ]]
! grep -Eq '^  (push|pull_request|release|create):' <<<"$preview_events"
preview_job=$(sed -n '/^  DockerPreview:/,$p' "$preview")
[[ "$preview_job" == *'contents: read'* && "$preview_job" == *'pixi run test-single-node-container'* ]]
[[ "$preview_job" == *'Upload preview failure logs'* && "$preview_job" == *'CROWDB_PREVIEW_TEST_ARTIFACTS'* ]]
! grep -Eq 'secrets\.|docker/login-action|docker/build-push-action' <<<"$preview_job"

release_tool=tools/release.py
for required in '--dry-run' '--execute' '--symbols' '"push", "origin", "HEAD:refs/heads/main"' \
    '"workflow", "run"' '"--ref", "main"'; do
    grep -Fq -- "$required" "$release_tool"
done
! grep -Eq '"release", "create"|"tag", "-a"' "$release_tool"
