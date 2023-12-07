#!/bin/bash
# deploy `tiup playground`

TIUP_BIN_DIR=$HOME/.tiup/bin/tiup

# Install TiUP
echo "install tiup"
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
$TIUP_BIN_DIR update playground

# Run TiUP
$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 0 --db 1 --pd 3 --without-monitor \
	--kv.binpath ./bin/tikv-server --db.binpath ./bin/tidb-server --pd.binpath ./bin/pd-server --tag pd_test \
	> ./playground.log 2>&1 &
