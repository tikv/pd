#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  exit 1
fi

if [ "${DASHBOARD-}" == "0" ]; then
  exit 0
fi

DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)

if [ "$1" = "git-hash" ]; then
  echo "${DASHBOARD_DIR}" | awk -F- '{print $NF}'
elif [ "$1" = "internal-version" ]; then
  grep -v '^#' "${DASHBOARD_DIR}/release-version"
else
  exit 1
fi
