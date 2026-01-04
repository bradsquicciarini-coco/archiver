#!/usr/bin/env bash
set -euo pipefail

# Builds a Docker image from the repo root and pushes it to AWS ECR.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

: "${AWS_REGION:?Set AWS_REGION (e.g. us-east-1)}"
: "${AWS_ACCOUNT_ID:?Set AWS_ACCOUNT_ID (12-digit AWS account ID)}"
: "${ECR_REPO:?Set ECR_REPO (repository name in ECR)}"

IMAGE_TAG="${IMAGE_TAG:-}"
if [[ -z "${IMAGE_TAG}" ]]; then
  if command -v git >/dev/null 2>&1; then
    IMAGE_TAG="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  else
    IMAGE_TAG="latest"
  fi
fi

ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_URI="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"

echo "Building image ${ECR_URI}"
docker build -f Dockerfile.fix -t "${ECR_URI}" "${REPO_ROOT}"

echo "Logging into ECR ${ECR_REGISTRY}"
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "Pushing image ${ECR_URI}"
docker push "${ECR_URI}"

echo "Done."
