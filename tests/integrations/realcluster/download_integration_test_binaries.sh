#! /usr/bin/env bash

# help
# download some third party tools for integration test
# example: ./download_integration_test_binaries.sh master


set -o errexit
set -o pipefail


# Specify which branch to be utilized for executing the test, which is
# exclusively accessible when obtaining binaries from
# http://fileserver.pingcap.net.
branch=${1:-master}
file_server_url=${2:-http://fileserver.pingcap.net}

tidb_sha1_url="${file_server_url}/download/refs/pingcap/tidb/${branch}/sha1"
tikv_sha1_url="${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1"
tiflash_sha1_url="${file_server_url}/download/refs/pingcap/tiflash/${branch}/sha1"

tidb_sha1=$(curl "$tidb_sha1_url")
tikv_sha1=$(curl "$tikv_sha1_url")
tiflash_sha1=$(curl "$tiflash_sha1_url")

# download tidb / tikv / tiflash binary build from tibuid multibranch pipeline
tidb_download_url="${file_server_url}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
tiflash_download_url="${file_server_url}/download/builds/pingcap/tiflash/${branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"

set -o nounset

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color_green() { # Green
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
    echo "download ${file_name} from ${url}"
    wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

function main() { 
    rm -rf third_bin
    rm -rf tmp
    mkdir third_bin
    mkdir tmp
    
    # tidb server
    download "$tidb_download_url" "tidb-server.tar.gz" "tmp/tidb-server.tar.gz"
    tar -xzf tmp/tidb-server.tar.gz -C third_bin --wildcards 'bin/*'
    mv third_bin/bin/* third_bin/ 

    # TiKV server
    download "$tikv_download_url" "tikv-server.tar.gz" "tmp/tikv-server.tar.gz"
    tar -xzf tmp/tikv-server.tar.gz -C third_bin --wildcards 'bin/*'
    mv third_bin/bin/* third_bin/

    # TiFlash
    download "$tiflash_download_url" "tiflash.tar.gz" "tmp/tiflash.tar.gz"
    tar -xzf tmp/tiflash.tar.gz -C third_bin
    mv third_bin/tiflash third_bin/_tiflash
    mv third_bin/_tiflash/* third_bin && rm -rf third_bin/_tiflash

    chmod +x third_bin/*
    rm -rf tmp
    rm -rf third_bin/bin
    ls -alh third_bin/
}

main "$@"

color_green "Download SUCCESS"
