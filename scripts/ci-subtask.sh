#!/usr/bin/env bash

set -euo pipefail

# Keep the same entry point for GitHub Actions and PingCAP CI runners.
python3 scripts/ci-shard.py run --index "${1:?job index is required}" --count "${2:-${JOB_COUNT:-10}}" --layout "${3:-${JOB_LAYOUT:-standard}}"
