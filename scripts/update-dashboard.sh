#!/usr/bin/env bash
set -euo pipefail

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
BASE_DIR="$(dirname "$CUR_DIR")"
DASHBOARD_VERSION_FILE="$BASE_DIR/scripts/dashboard-version"
# old version
DASHBOARD_VERSION=v$(grep -v '^#' "$DASHBOARD_VERSION_FILE")

# new version
# Usage: ./scripts/update-dashboard.sh <version>
# Example: ./scripts/update-dashboard.sh v7.6.0-f7bbcdcf
if [ "$#" -ge 1 ]; then
  DASHBOARD_VERSION=$1

  # if DASHBOARD_VERSION not start with `v`, then exit
  if [[ ! $DASHBOARD_VERSION =~ ^v[0-9]+\. ]]; then
    echo "Invalid dashboard version: $DASHBOARD_VERSION, it should start with \"v\""
    exit 1
  fi

  # when writing to DASHBOARD_VERSION_FILE, remove the leading `v`,
  # so that we don't need to modify the embed-dashboard-ui.sh logic
  TO_FILE_VERSION=${DASHBOARD_VERSION#v}

  echo "# This file is updated by running scripts/update-dashboard.sh" >$DASHBOARD_VERSION_FILE
  echo "# Don't edit it manually" >>$DASHBOARD_VERSION_FILE
  echo $TO_FILE_VERSION >>$DASHBOARD_VERSION_FILE
fi

echo "+ Update dashboard version to $DASHBOARD_VERSION"

cd $BASE_DIR

# if DASHBOARD_VERSION match "vX.Y.Z-<commit-short-hash>" format,
# then extract the commit hash as tidb-dashboard target
if [[ $DASHBOARD_VERSION =~ ^v[0-9]+\.[0-9]+\.[0-9]+-[0-9a-f]{7,8} ]]; then
  DASHBOARD_VERSION=$(echo $DASHBOARD_VERSION | cut -d'-' -f2)
fi

go get -d github.com/pingcap/tidb-dashboard@$DASHBOARD_VERSION
go mod tidy
make pd-server
go mod tidy

cd ./tests/integrations
go mod tidy

cd ../../tools
go mod tidy
