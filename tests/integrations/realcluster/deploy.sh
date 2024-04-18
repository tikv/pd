#!/bin/bash
# deploy `tiup playground`

TIUP_BIN_DIR=$HOME/.tiup/bin/tiup
CUR_PATH=$(pwd)

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

# Install TiUP
color-green "install TiUP..."
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
$TIUP_BIN_DIR update playground

cd ../../..
if [ ! -d "bin" ] || [ ! -e "bin/tikv-server" ] && [ ! -e "bin/tidb-server" ] && [ ! -e "bin/pd-server" ] && [ ! -e "bin/tiflash" ]; then
	color-green "downloading binaries..."
	color-green "this may take a few minutes, you can also download them manually and put them in the bin directory."
	$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 1 --db 1 --pd 3 --without-monitor --tag pd_test \
		> $CUR_PATH/playground.log 2>&1 &
else
	color-green "using existing binaries..."
	$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 1 --db 1 --pd 3 --without-monitor \
		--pd.binpath ./bin/pd-server --kv.binpath ./bin/tikv-server --db.binpath ./bin/tidb-server --tiflash.binpath ./bin/tiflash --tag pd_test \
		> $CUR_PATH/playground.log 2>&1 &
fi

cd $CUR_PATH
