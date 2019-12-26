#!/usr/bin/env bash
set -euo pipefail

git submodule update --init --recursive
git submodule foreach --recursive git lfs pull

BASE_DIR=$(git rev-parse --show-toplevel)
WEB_DIR="$BASE_DIR/pkg/ui/pd-web"
TARGET_DIST="$BASE_DIR/pkg/ui/dist"
SCRIPTS_DIR="$BASE_DIR/scripts"

echo "##### Build Web UI"
cd "$WEB_DIR" || { echo "No such directory: $WEB_DIR"; exit 1; }
yarn install --frozen-lockfile
PUBLIC_URL=/web yarn build

echo "##### Build Binary With UI"
"$SCRIPTS_DIR/retool" "do" go-bindata -o "$TARGET_DIST/bindata.go" -pkg dist -prefix="$WEB_DIR/build" "$WEB_DIR/build/..."
