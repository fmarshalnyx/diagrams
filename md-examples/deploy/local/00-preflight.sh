#!/usr/bin/env bash
# Design §11: hard-fail unless the current kubectl context is docker-desktop — never touch a
# cloud context from these scripts. Idempotent, re-runnable.
set -euo pipefail

CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
if [[ "$CONTEXT" != "docker-desktop" ]]; then
  echo "ERROR: kubectl context is '$CONTEXT', not 'docker-desktop'. Refusing to continue." >&2
  echo "These scripts only ever target local Docker Desktop Kubernetes — never a cloud context." >&2
  exit 1
fi

for cmd in helm docker mvn kubectl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command '$cmd' not found on PATH." >&2
    exit 1
  fi
done

# Docker Desktop's Kubernetes shares the Docker daemon's image store (design §11) — jib-built
# images with imagePullPolicy: IfNotPresent need no registry, but only if this is actually true.
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker is not reachable (is Docker Desktop running?)." >&2
  exit 1
fi

CPUS="$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)"
MEM_BYTES="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
MEM_GIB=$(( MEM_BYTES / 1024 / 1024 / 1024 ))
if [[ "$CPUS" -lt 6 || "$MEM_GIB" -lt 12 ]]; then
  echo "WARNING: Docker Desktop has ${CPUS} CPUs / ${MEM_GIB} GiB — design §11 recommends >= 6 CPUs / 12 GiB for a comfortable local run." >&2
fi

# design §5.2: Aeron MTU must not exceed the host's real network MTU, or UDP fragments get
# silently dropped. Docker Desktop's default bridge MTU is commonly 1500 (English: don't raise
# clusterNode's Aeron MTU config above that without also raising this).
echo "NOTE: Docker Desktop's default network MTU is typically 1500 bytes — the Aeron channels in"
echo "      this chart use conservative default MTU sizing; do not raise it without checking"
echo "      'docker network inspect bridge' first (design §5.2)."

echo "Preflight OK: context=$CONTEXT, docker/helm/mvn/kubectl present."
