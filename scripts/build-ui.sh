#!/usr/bin/env bash
git submodule update --remote
BASEDIR=$(git rev-parse --show-toplevel)
UI_DIR="$BASEDIR/pkg/ui/pd-web"
TARGET_DIST="$BASEDIR/pkg/ui/dist"
SCRIPTS_DIR="$BASEDIR/scripts"
echo "##### Build Web UI"
cd $UI_DIR
yarn
PUBLIC_URL=/web yarn build
echo "##### Build Binary With UI"
$SCRIPTS_DIR/retool do go-bindata -o $TARGET_DIST/bindata.go -pkg dist -prefix=$UI_DIR/build $UI_DIR/build/...
echo 'func init() { ui.Asset = Asset; ui.AssetDir = AssetDir; ui.AssetInfo = AssetInfo }' >> $TARGET_DIST/bindata.go
gofmt -s -w $TARGET_DIST/bindata.go
$SCRIPTS_DIR/retool do goimports -w $TARGET_DIST/bindata.go
