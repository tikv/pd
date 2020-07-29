$DIR = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BASE_DIR = (get-item $DIR).parent.FullName
$CACHE_DIR = Join-Path($BASE_DIR) "\.dashboard_asset_cache"

echo '+ Create asset cache directory'

mkdir -p $CACHE_DIR -Force | Out-Null

echo '+ Fetch TiDB Dashboard Go module'
go mod download

echo '+ Discover TiDB Dashboard release version'

$DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap-incubator/tidb-dashboard)
echo "  - Dashboard directory: $DASHBOARD_DIR"

$DASHBOARD_RELEASE_VERSION= cat "${DASHBOARD_DIR}/release-version" | Select-String -Pattern "^#" -NotMatch
echo "  - TiDB Dashboard release version: $DASHBOARD_RELEASE_VERSION"

echo '+ Check embedded assets exists in cache'
$CACHE_FILE= Join-Path($CACHE_DIR) \embedded-assets-golang-${DASHBOARD_RELEASE_VERSION}.zip

if (Test-Path "$CACHE_FILE" ){
  echo "  - Cached archive exists: $CACHE_FILE"
}
else{
  echo '  - Cached archive does not exist'
  echo '  - Download pre-built embedded assets from GitHub release'
  $DOWNLOAD_URL="https://github.com/pingcap-incubator/tidb-dashboard/releases/download/v${DASHBOARD_RELEASE_VERSION}/embedded-assets-golang.zip"
  $DOWNLOAD_FILE= Join-Path($CACHE_DIR) \embedded-assets-golang.zip
  echo "  - Download ${DOWNLOAD_URL}"
  Invoke-WebRequest -Uri "${DOWNLOAD_URL}" -OutFile "${DOWNLOAD_FILE}"

  echo "  - Save archive to cache: ${CACHE_FILE}"
  mv "${DOWNLOAD_FILE}" "${CACHE_FILE}"
}

echo '+ Unpack embedded asset from archive'
Expand-Archive -Path "${CACHE_FILE}" -DestinationPath "$CACHE_DIR" -Force
$MOVE_FILE="${CACHE_DIR}\embedded_assets_handler.go"
gofmt -s -w "$MOVE_FILE"
$MOVE_DEST="${BASE_DIR}\pkg\dashboard\uiserver"
move-item -path "${MOVE_FILE}" -destination "${MOVE_DEST}" -Force
echo "  - Unpacked ${MOVE_DEST}"
