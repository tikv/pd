#!/bin/bash
# deploy `tiup playground`

TIUP_BIN_DIR=$HOME/.tiup/bin/tiup

make clean
cd ../../../
#rm -rf bin
RUN_CI=1 make pd-server-basic


# Install TiUP
echo "install tiup"
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
$TIUP_BIN_DIR update playground

# Run TiUP
$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 0 --db 1 --pd 3 --without-monitor --pd.binpath ./bin/pd-server --tag pd_test > ./tests/integrations/realtiup/playground.log 2>&1 &
