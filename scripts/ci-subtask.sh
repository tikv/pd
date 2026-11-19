#!/usr/bin/env bash

set -euo pipefail

# Keep the same entry point for GitHub Actions and PingCAP CI runners.
args=(run --index "${1:?job index is required}" --count "${2:-${JOB_COUNT:-10}}" --layout "${3:-${JOB_LAYOUT:-standard}}")
if [[ -n "${JOB_PARALLEL:-}" ]]; then
  args+=(--parallel "$JOB_PARALLEL")
fi
if [[ -n "${JOB_DURATIONS:-}" ]]; then
  args+=(--durations "$JOB_DURATIONS")
fi
python3 scripts/ci-shard.py "${args[@]}"
