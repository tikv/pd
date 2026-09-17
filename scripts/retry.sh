#!/usr/bin/env bash
# Retry dependency installation after transient download failures. Preserve the
# final error so an unavailable dependency still fails the build.
set -uo pipefail

for attempt in 1 2 3; do
  if "$@"; then
    exit 0
  else
    status=$?
  fi
  if [ "$attempt" -eq 3 ]; then
    exit "$status"
  fi
  echo "Dependency installation failed; retrying (attempt $((attempt + 1))/3)" >&2
  sleep "$attempt"
done
