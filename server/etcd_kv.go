// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"path"
	"strings"
	"time"

	log "github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

type etcdKVBase struct {
	server   *Server
	client   *clientv3.Client
	rootPath string
}

func newEtcdKVBase(s *Server) *etcdKVBase {
	return &etcdKVBase{
		server:   s,
		client:   s.client,
		rootPath: s.rootPath,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	resp, err := etcdutil.EtcdKVGet(kv.server.client, key)
	if err != nil {
		return "", err
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	key = path.Join(kv.rootPath, key)
	endKey = path.Join(kv.rootPath, endKey)

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := etcdutil.EtcdKVGet(kv.server.client, key, withRange, withLimit)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), kv.rootPath), "/"))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

func (kv *etcdKVBase) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)

	resp, err := kv.server.leaderTxn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		log.Error("save to etcd meet error", zap.Error(err))
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.WithStack(errTxnFailed)
	}
	return nil
}

func (kv *etcdKVBase) Delete(key string) error {
	key = path.Join(kv.rootPath, key)

	resp, err := kv.server.leaderTxn().Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		log.Error("delete from etcd meet error", zap.Error(err))
		return errors.WithStack(err)
	}
	if !resp.Succeeded {
		return errors.WithStack(errTxnFailed)
	}
	return nil
}
