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
    'docker.io/crowdb/crowdb-iceberg:${{ inputs.tag }}' \
    'docker.io/crowdb/crowdb-iceberg:git-${{ needs.verify.outputs.revision }}' \
    'docker.io/crowdb/crowdb-iceberg:preview' \
    'provenance: mode=max' \
    'sbom: true' \
    'cosign sign --yes'; do
    grep -Fq "$required" "$release"
done
! grep -Eq 'crowdb-iceberg:latest' "$release"

ci_job=$(sed -n '/^  DockerPreview:/,$p' "$ci")
[[ "$ci_job" == *'contents: read'* && "$ci_job" == *'pixi run test-docker-preview'* ]]
! grep -Eq 'secrets\.|docker/login-action|docker/build-push-action' <<<"$ci_job"
