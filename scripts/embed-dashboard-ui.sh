#!/usr/bin/env bash
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(dirname "$DIR")"
CACHE_DIR=${BASE_DIR}/.dashboard_asset_cache

echo '+ Create cache directory'
mkdir -p "${CACHE_DIR}"

echo '+ Fetch Dashboard Go module'
go mod download

echo '+ Discover Dashboard UI version'

DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)
echo "  - Dashboard directory: ${DASHBOARD_DIR}"

DASHBOARD_UI_VERSION=$(grep -v '^#' "${DASHBOARD_DIR}/ui/.github_release_version")
echo "  - Dashboard ui version: ${DASHBOARD_UI_VERSION}"

echo '+ Check embedded assets exists in cache'
CACHE_FILE=${CACHE_DIR}/embedded-assets-golang-${DASHBOARD_UI_VERSION}.zip
if [[ -f "$CACHE_FILE" ]]; then
  echo "  - Cached archive exists: ${CACHE_FILE}"
else
  echo '  - Cached archive does not exist'
  echo '  - Download pre-built embedded assets from GitHub release'

  DOWNLOAD_URL="https://github.com/pingcap-incubator/tidb-dashboard/releases/download/ui_release_${DASHBOARD_UI_VERSION}/embedded-assets-golang.zip"
  echo "  - Download ${DOWNLOAD_URL}"
  curl -L "${DOWNLOAD_URL}" > embedded-assets-golang.zip

  echo "  - Save archive to cache: ${CACHE_FILE}"
  mv embedded-assets-golang.zip "${CACHE_FILE}"
fi

echo '+ Unpack embedded asset from archive'
unzip -o "${CACHE_FILE}"
MOVE_FILE=embedded_assets_handler.go
MOVE_DEST=pkg/dashboard/uiserver/${MOVE_FILE}
mv ${MOVE_FILE} ${MOVE_DEST}
echo "  - Unpacked ${MOVE_DEST}"
