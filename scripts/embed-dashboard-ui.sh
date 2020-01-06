#!/usr/bin/env bash
set -euo pipefail

BASE_DIR=$(git rev-parse --show-toplevel)

echo '+ Fetch Dashboard Go module'
go mod download

echo '+ Discover Dashboard UI version'

DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)
echo "  - Dashboard directory: ${DASHBOARD_DIR}"

DASHBOARD_UI_VERSION=$(grep -v '^#' ${DASHBOARD_DIR}/ui/.github_release_version)
echo "  - Dashboard ui version: ${DASHBOARD_UI_VERSION}"

echo '+ Fetch pre-built embedded assets'

DOWNLOAD_URL="https://github.com/pingcap-incubator/tidb-dashboard/releases/download/ui_release_${DASHBOARD_UI_VERSION}/embedded-assets-golang.zip"
echo "  - Download archive ${DOWNLOAD_URL}"
curl -L ${DOWNLOAD_URL} > embedded-assets-golang.zip

echo '+ Unpack embedded asset'

unzip -o embedded-assets-golang.zip
rm embedded-assets-golang.zip
MOVE_FILE=embedded_assets_handler.go
MOVE_DEST=pkg/dashboard/uiserver
mv ${MOVE_FILE} ${MOVE_DEST}
echo "  - Unpacked ${MOVE_DEST}/${MOVE_FILE}"
