#!/usr/bin/env bash
set -euo pipefail

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(dirname "$CUR_DIR")"
DASHBOARD_VERSION_FILE="$BASE_DIR/scripts/dashboard-version"
# old version
DASHBOARD_VERSION=$(grep -v '^#' "$DASHBOARD_VERSION_FILE")

# new version
# Usage: ./scripts/update-dashboard.sh <version>
# Example: ./scripts/update-dashboard.sh 2023.12.18.1
if [ "$#" -ge 1 ]; then
  DASHBOARD_VERSION=$1
  echo $1 > $DASHBOARD_VERSION_FILE
fi

echo "+ Update dashboard version to v$DASHBOARD_VERSION"

cd $BASE_DIR

go get -d github.com/pingcap/tidb-dashboard@v$DASHBOARD_VERSION
go mod tidy
make pd-server
go mod tidy

cd tests/integrations/client
go mod tidy
cd ../mcs
go mod tidy
cd ../tso
go mod tidy
