#!/usr/bin/env bash
set -euo pipefail

if [ "${DASHBOARD-}" == "0" ]; then
  exit 0
fi

DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)
DASHBOARD_RELEASE_VERSION=$(grep -v '^#' "${DASHBOARD_DIR}/release-version")

echo "${DASHBOARD_RELEASE_VERSION}"
