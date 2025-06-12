#!/bin/bash
# deploy `tiup playground`

set -x

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
if [ ! -d "bin" ] || [ ! -e "bin/tikv-server" ] && [ ! -e "bin/tidb-server" ] && [ ! -e "bin/tiflash" ]; then
	color-green "downloading binaries..."
	color-green "this may take a few minutes, you can also download them manually and put them in the bin directory."
	make pd-server WITH_RACE=1
	$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 1 --db 1 --pd 3 --without-monitor --tag pd_real_cluster_test \
		--pd.binpath ./bin/pd-server --pd.config ./tests/integrations/realcluster/pd.toml \
		> $CUR_PATH/playground.log 2>&1 &
else
  # CI will download the binaries in the prepare phase.
  # ref https://github.com/PingCAP-QE/ci/blob/387e9e533b365174962ccb1959442a7070f9cd66/pipelines/tikv/pd/latest/pull_integration_realcluster_test.groovy#L55-L68
	color-green "using existing binaries..."
	$TIUP_BIN_DIR playground nightly --kv 3 --tiflash 1 --db 1 --pd 3 --without-monitor --tag pd_real_cluster_test \
		--pd.binpath ./bin/pd-server --kv.binpath ./bin/tikv-server --db.binpath ./bin/tidb-server \
		--tiflash.binpath ./bin/tiflash --pd.config ./tests/integrations/realcluster/pd.toml \
		> $CUR_PATH/playground.log 2>&1 &
fi

cd $CUR_PATH
