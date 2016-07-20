// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/binary"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

const (
	requestTimeout  = 10 * time.Second
	slowRequestTime = 1 * time.Second
)

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	kv := clientv3.NewKV(c)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := kv.Get(ctx, key, opts...)
	cancel()

	if cost := time.Now().Sub(start); cost > slowRequestTime {
		log.Warnf("kv gets %s too slow, resp: %v, err: %v, cost: %s", key, resp, err, cost)
	}

	return resp, errors.Trace(err)
}

// A helper function to get value with key from etcd.
// TODO: return the value revision for outer use.
func getValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := kvGet(c, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

// Return boolean to indicate whether the key exists or not.
// TODO: return the value revision for outer use.
func getProtoMsg(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, error) {
	value, err := getValue(c, key, opts...)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}

	if err = proto.Unmarshal(value, msg); err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	if cost := time.Now().Sub(start); cost > slowRequestTime {
		log.Warnf("txn runs too slow, resp: %v, err: %v, cost: %s", resp, err, cost)
	}

	return resp, errors.Trace(err)
}

// convertName converts variable name to a linux type name.
// Like `AbcDef -> abc_def`.
func convertName(str string) string {
	name := make([]byte, 0, 64)
	for i := 0; i < len(str); i++ {
		if str[i] >= 'A' && str[i] <= 'Z' {
			if i > 0 {
				name = append(name, '_')
			}

			name = append(name, str[i]+'a'-'A')
		} else {
			name = append(name, str[i])
		}
	}

	return string(name)
}

func sliceClone(strs []string) []string {
	data := make([]string, 0, len(strs))
	for _, str := range strs {
		data = append(data, str)
	}

	return data
}

func mapClone(m map[uint64]struct{}) map[uint64]struct{} {
	data := make(map[uint64]struct{}, len(m))
	for k := range m {
		data[k] = struct{}{}
	}

	return data
}

func mergeMap(m1 map[uint64]struct{}, m2 map[uint64]struct{}) map[uint64]struct{} {
	data := make(map[uint64]struct{}, len(m1)+len(m2))
	for k := range m1 {
		data[k] = struct{}{}
	}
	for k := range m2 {
		data[k] = struct{}{}
	}

	return data
}
