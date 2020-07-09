#!/usr/bin/env bash
set -euo pipefail

if [ "${DASHBOARD-}" == "0" ]; then
  exit 0
fi

DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)

echo "${DASHBOARD_DIR}" | awk -F- '{print substr($NF, 0, 8)}'
