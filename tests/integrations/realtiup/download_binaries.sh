#!/bin/bash

# download_binaries.sh will
# * download all the binaries you need for integration testing

# Notice:
# Please don't try the script locally,
# it downloads files for linux platform. We only use it in docker-compose.

set -o errexit
set -o pipefail

# Specify which branch to be utilized for executing the test, which is
# exclusively accessible when obtaining binaries from
# http://fileserver.pingcap.net.
branch=${THIRD_PARTY_TARGET_BRANCH:-master}
# Specify whether to download the community version of binaries, the following
# four arguments are applicable only when utilizing the community version of
# binaries.
community=${2:-false}
# Specify which version of the community binaries that will be utilized.
ver=${3:-v6.5.2}
# Specify which os that will be used to pack the binaries.
os=${4:-linux}
# Specify which architecture that will be used to pack the binaries.
arch=${5:-amd64}

set -o nounset

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

function download() {
	local url=$1
	local file_name=$2
	local file_path=$3
	if [[ -f "${file_path}" ]]; then
		echo "file ${file_name} already exists, skip download"
		return
	fi
	echo ">>>"
	echo "download ${file_name} from ${url}"
	wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

function download_binaries() {
	color-green "Download binaries..."
	# PingCAP file server URL.
	file_server_url="http://fileserver.pingcap.net"

	# Get sha1 based on branch name.
	tidb_sha1=$(curl "${file_server_url}/download/refs/pingcap/tidb/${branch}/sha1")
	tikv_sha1=$(curl "${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1")
	tiflash_sha1=$(curl "${file_server_url}/download/refs/pingcap/tiflash/${branch}/sha1")

	# All download links.
	tidb_download_url="${file_server_url}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
	tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
	tiflash_download_url="${file_server_url}/download/builds/pingcap/tiflash/${branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"
	
	download "$tidb_download_url" "tidb-server.tar.gz" "tmp/tidb-server.tar.gz"
	tar -xz -C third_bin bin/tidb-server -f tmp/tidb-server.tar.gz && mv third_bin/bin/* bin/

	download "$tikv_download_url" "tikv-server.tar.gz" "tmp/tikv-server.tar.gz"
	tar -xz -C third_bin bin/tikv-server -f tmp/tikv-server.tar.gz && mv third_bin/bin/tikv-server bin/

	download "$tiflash_download_url" "tiflash.tar.gz" "tmp/tiflash.tar.gz"
	tar -xz -C third_bin -f tmp/tiflash.tar.gz
	mv third_bin/tiflash third_bin/_tiflash
	mv third_bin/_tiflash/* bin && rm -rf third_bin/_tiflash

	chmod a+x bin/*
}

# download_community_version will try to download required binaries from the
# public accessible community version
function download_community_binaries() {
	local dist="${ver}-${os}-${arch}"
	local tidb_file_name="tidb-community-server-$dist"
	local tidb_tar_name="${tidb_file_name}.tar.gz"
	local tidb_url="https://download.pingcap.org/$tidb_tar_name"

	color-green "Download community binaries..."
	download "$tidb_url" "$tidb_tar_name" "tmp/$tidb_tar_name"
	# extract the tidb community version binaries
	tar -xz -C tmp -f tmp/$tidb_tar_name
	# extract the tikv server
	tar -xz -C third_bin -f tmp/$tidb_file_name/tikv-${dist}.tar.gz
	# extract the tidb server
	tar -xz -C third_bin -f tmp/$tidb_file_name/tidb-${dist}.tar.gz
	# extract the tiflash
	tar -xz -C third_bin -f tmp/$tidb_file_name/tiflash-${dist}.tar.gz
	mv third_bin/tiflash third_bin/_tiflash
	mv third_bin/_tiflash/* bin && rm -rf third_bin/_tiflash

	chmod a+x bin/*
}

function make_pd() {
	echo 'download pd-server'
	CUR_PATH=$(pwd)
	cd ../../../
	rm -rf bin
	RUN_CI=1 make pd-server-basic
	cd $CUR_PATH
	mv ../../../bin/pd-server ./bin/
}

# Some temporary dir.
rm -rf tmp
rm -rf bin
rm -rf third_bin

mkdir -p tmp
mkdir -p bin
mkdir -p third_bin

# build pd-server
make_pd
[ $community == true ] && download_community_binaries || download_binaries

# Copy it to the bin directory in the root directory.
rm -rf tmp
rm -rf third_bin

color-green "Download SUCCESS"
