#!/usr/bin/env bash
# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

set -euo pipefail
unset GIT_DIR

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <registry>/<namespace> [repository-name]" >&2
    echo "Example: $0 iad.ocir.io/iwxyzlmkmn63 dfa-images/dfa" >&2
    exit 2
fi

REGISTRY="${1%/}"
REPOSITORY_NAME="${2:-${REPOSITORY_NAME:-dfa-images/dfa}}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"
PACKAGE_VERSION="$(
    python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])'
)"
GIT_SHA="$(git rev-parse HEAD)"
SHORT_SHA="$(git rev-parse --short=7 HEAD)"
IMAGE_VERSION="${DFA_IMAGE_VERSION:-${PACKAGE_VERSION}-${SHORT_SHA}}"
IMAGE="${REGISTRY}/${REPOSITORY_NAME}:${IMAGE_VERSION}"
REPO_URL="$(git config --get remote.origin.url || true)"

"${CONTAINER_TOOL}" build \
    --platform linux/amd64 \
    --label "org.opencontainers.image.revision=${GIT_SHA}" \
    --label "org.opencontainers.image.version=${PACKAGE_VERSION}" \
    --label "org.opencontainers.image.source=${REPO_URL}" \
    -t "${IMAGE}" \
    -f Dockerfile .

mkdir -p dist
GIT_SHA="${GIT_SHA}" \
IMAGE_VERSION="${IMAGE_VERSION}" \
PACKAGE_VERSION="${PACKAGE_VERSION}" \
REPOSITORY_NAME="${REPOSITORY_NAME}" \
IMAGE="${IMAGE}" \
python -c 'import json, os; print(json.dumps({k.lower(): os.environ[k] for k in ("IMAGE", "IMAGE_VERSION", "PACKAGE_VERSION", "GIT_SHA", "REPOSITORY_NAME")}, indent=2))' \
    > dist/image-manifest.json

if [[ "${PUSH_IMAGE:-false}" == "true" ]]; then
    "${CONTAINER_TOOL}" push "${IMAGE}"
fi

echo "${IMAGE}"
