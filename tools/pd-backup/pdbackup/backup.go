// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pdbackup

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"

	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	pdRootPath      = "/pd"
	pdClusterIDPath = "/pd/cluster_id"
	pdConfigAPIPath = "/pd/api/v1/config"
)

// BackupInfo is the backup infos.
type BackupInfo struct {
	ClusterID         uint64         `json:"clusterID"`
	AllocIDMax        uint64         `json:"allocIDMax"`
	AllocTimestampMax uint64         `json:"allocTimestampMax"`
	Config            *config.Config `json:"config"`
}

// GetBackupInfo return the BackupInfo
func GetBackupInfo(client *clientv3.Client, pdAddr string) (*BackupInfo, error) {
	backInfo := &BackupInfo{}
	resp, err := etcdutil.EtcdKVGet(client, pdClusterIDPath)
	if err != nil {
		return nil, err
	}
	clusterID, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	backInfo.ClusterID = clusterID

	rootPath := path.Join(pdRootPath, strconv.FormatUint(clusterID, 10))
	allocIDPath := path.Join(rootPath, "alloc_id")
	resp, err = etcdutil.EtcdKVGet(client, allocIDPath)
	if err != nil {
		return nil, err
	}
	var allocIDMax uint64 = 0
	if resp.Count > 0 {
		allocIDMax, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
		if err != nil {
			return nil, err
		}
	}

	backInfo.AllocIDMax = allocIDMax

	resp, err = etcdutil.EtcdKVGet(client, endpoint.TimestampPath(rootPath))
	if err != nil {
		return nil, err
	}
	allocTimestampMax, err := typeutil.BytesToUint64(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	backInfo.AllocTimestampMax = allocTimestampMax

	backInfo.Config, err = getConfig(pdAddr)
	if err != nil {
		return nil, err
	}
	return backInfo, nil
}

// OutputToFile output the backupInfo to the file.
func OutputToFile(backInfo *BackupInfo, f *os.File) error {
	w := bufio.NewWriter(f)
	defer w.Flush()
	backBytes, err := json.Marshal(backInfo)
	if err != nil {
		return err
	}
	var formatBuffer bytes.Buffer
	err = json.Indent(&formatBuffer, backBytes, "", "    ")
	if err != nil {
		return err
	}
	fmt.Fprintln(w, formatBuffer.String())
	return nil
}

func getConfig(pdAddr string) (*config.Config, error) {
	resp, err := http.Get(pdAddr + pdConfigAPIPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	conf := &config.Config{}
	err = json.Unmarshal(body, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
