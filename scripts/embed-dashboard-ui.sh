#!/usr/bin/env bash
set -euo pipefail


DISTRIBUTION_DIR=${DASHBOARD_DISTRIBUTION_DIR-}

if [ -n "${DISTRIBUTION_DIR}" ]; then
  DASHBOARD=COMPILE
fi

if [ "${DASHBOARD-}" == "0" ] || [ "${DASHBOARD-}" = "SKIP" ]; then
  # DASHBOARD=0 will completely exclude TiDB Dashboard in building when calling from Makefile
  # while DASHBOARD=SKIP will keep current asset file unchanged and include it in building
  echo '+ Skip TiDB Dashboard'
  exit 0
fi


RED='\033[1;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(dirname "$DIR")"
ASSET_FILE_NAME=embedded_assets_handler.go
ASSET_DEST_PATH=${BASE_DIR}/pkg/dashboard/uiserver/${ASSET_FILE_NAME}


echo '+ Clean up existing asset file'
rm -f ASSET_DEST_PATH

echo '+ Fetch TiDB Dashboard Go module'
go mod download
go mod tidy
DASHBOARD_DIR=$(go list -f "{{.Dir}}" -m github.com/pingcap/tidb-dashboard)
echo "  - TiDB Dashboard directory: ${DASHBOARD_DIR}"

CACHE_DIR=${BASE_DIR}/.dashboard_download_cache
echo "+ Create download cache directory: ${CACHE_DIR}"
mkdir -p "${CACHE_DIR}"

function download_embed_asset {
  echo '+ Discover TiDB Dashboard release version'
  DASHBOARD_RELEASE_VERSION=$(grep -v '^#' "${BASE_DIR}/scripts/dashboard-version")
  echo "  - TiDB Dashboard release version: ${DASHBOARD_RELEASE_VERSION}"

  echo '+ Check whether pre-built assets are available'
  CACHE_FILE=${CACHE_DIR}/embedded-assets-golang-${DASHBOARD_RELEASE_VERSION}.zip
  if [[ -f "$CACHE_FILE" ]]; then
    echo "  - Cached archive exists: ${CACHE_FILE}"
  else
    echo '  - Cached archive does not exist'
    echo '  - Download pre-built embedded assets from GitHub release'

    DOWNLOAD_URL="https://github.com/pingcap/tidb-dashboard/releases/download/v${DASHBOARD_RELEASE_VERSION}/embedded-assets-golang.zip"
    DOWNLOAD_FILE=${CACHE_DIR}/embedded-assets-golang.zip
    echo "  - Download ${DOWNLOAD_URL}"
    if ! curl -L "${DOWNLOAD_URL}" --fail --output "${DOWNLOAD_FILE}"; then
      echo
      echo -e "${RED}Error: Failed to download assets of TiDB Dashboard release version ${DASHBOARD_RELEASE_VERSION}.${NC}"
      if [ "${DASHBOARD_RELEASE_VERSION}" == "nightly" ]; then
        echo 'This project is using the nightly version of TiDB Dashboard, which does not have any release.'
      else
        echo 'This may be caused by using a non-release version of TiDB Dashboard, or the release is still in progress.'
      fi
      echo
      echo -e "To compile PD without TiDB Dashboard:                       ${YELLOW}DASHBOARD=0 make${NC}"
      echo -e "To compile PD by building TiDB Dashboard assets on-the-fly: ${YELLOW}DASHBOARD=COMPILE make${NC}  or  ${YELLOW}NO_MINIMIZE=1 DASHBOARD=COMPILE make${NC}"
      exit 1
    fi

    echo "  - Save archive to cache: ${CACHE_FILE}"
    mv "${DOWNLOAD_FILE}" "${CACHE_FILE}"
  fi

  echo '+ Unpack embedded asset from archive'
  unzip -o "${CACHE_FILE}"
  gofmt -s -w ${ASSET_FILE_NAME}
  mv "${ASSET_FILE_NAME}" "${ASSET_DEST_PATH}"
  echo "  - Unpacked ${ASSET_DEST_PATH}"
}


function compile_asset {
  BUILD_DIR=${BASE_DIR}/.dashboard_build_temp

  echo '+ Compiling TiDB Dashboard UI from source code'
  echo '+ Clean up build directory'
  echo "  - Build directory: ${DASHBOARD_DIR}"
  if [ -d "${BUILD_DIR}/ui/node_modules" ]; then
    echo "  - Build dependency exists, keep dependency cache"
    mv "${BUILD_DIR}/ui/node_modules" ./
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}/ui"
    mv ./node_modules "${BUILD_DIR}/ui/"
  else
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}/ui"
  fi

  echo '+ Discover referenced TiDB Dashboard commit'
  DASHBOARD_SHA=$(echo "${DASHBOARD_DIR}" | awk -F '-' '{print $NF}')
  CACHE_FILE=${CACHE_DIR}/tidb-dashboard-${DASHBOARD_SHA}.zip
  echo "  - TiDB Dashboard commit: ${DASHBOARD_SHA}"

  echo '+ Check whether UI source code was downloaded'
  if [[ -f "$CACHE_FILE" ]]; then
    echo "  - Source code archive exists: ${CACHE_FILE}"
  else
    echo '  - Source code archive does not exist'
    echo '  - Download source code archive from GitHub'

    DOWNLOAD_URL="https://github.com/pingcap/tidb-dashboard/archive/${DASHBOARD_SHA}.zip"
    DOWNLOAD_FILE=${CACHE_DIR}/tidb-dashboard.zip
    echo "  - Download ${DOWNLOAD_URL}"
    if ! curl -L "${DOWNLOAD_URL}" --fail --output "${DOWNLOAD_FILE}"; then
      echo
      echo -e "${RED}Error: Failed to download TiDB Dashboard source code archive.${NC}"
      exit 1
    fi

    mv "${DOWNLOAD_FILE}" "${CACHE_FILE}"
    echo "  - Source code archive downloaded to: ${CACHE_FILE}"
  fi

  echo '+ Unpack source code archive'
  DASHBOARD_FULL_SHA=$(unzip -l "${CACHE_FILE}" | sed -n '2p')
  DASHBOARD_UNZIP_PATH=/tmp/tidb-dashboard-${DASHBOARD_FULL_SHA}
  unzip -o "${CACHE_FILE}" -d /tmp

  echo '+ Copy unpacked source code to build directory'
  echo "  - Src:  ${DASHBOARD_UNZIP_PATH}"
  echo "  - Dest: ${BUILD_DIR}"
  cp -r "${DASHBOARD_UNZIP_PATH}/." "${BUILD_DIR}/"

  chmod -R u+w "${BUILD_DIR}"
  chmod u+x "${BUILD_DIR}"/scripts/*.sh

  echo '+ Build UI'
  cd "${BUILD_DIR}"
  # ref https://github.com/tikv/pd/issues/7593
  git init
  make ui

  echo '+ Generating UI assets'
  echo '  - Generating...'
  NO_ASSET_BUILD_TAG=1 scripts/embed_ui_assets.sh
  echo '  - Writing...'
  cp "pkg/uiserver/${ASSET_FILE_NAME}" "${ASSET_DEST_PATH}"
  cd -
  echo "  - Wrote ${ASSET_DEST_PATH}"

  echo '+ Build UI complete'
}


if [ "${DASHBOARD-}" == "COMPILE" ]; then
  compile_asset
else
  download_embed_asset
fi
