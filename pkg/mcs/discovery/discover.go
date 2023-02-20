// Copyright 2023 TiKV Project Authors.
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

package discovery

import (
	"context"

	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// Discover is used to get all the service instances of the specified service name.
func Discover(urls []string, serviceName string) ([]string, error) {
	cli, err := clientv3.NewFromURLs(urls)
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	key := discoveryPath(serviceName) + "/"
	endKey := clientv3.GetPrefixRangeEnd(key) + "/"

	withRange := clientv3.WithRange(endKey)
	resp, err := etcdutil.EtcdKVGet(cli, key, withRange)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		values = append(values, string(item.Value))
	}
	return values, nil
}

// Watch is used to watch the given key and call the putAction when the key is put and call the deleteAction when the key is deleted.
// TODO: the action part may be moved to the caller.
func Watch(ctx context.Context, urls []string, key string, putAction, deleteAction func()) error {
	cli, err := clientv3.NewFromURLs(urls)
	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	watcher := clientv3.NewWatcher(cli)
	defer watcher.Close()

	// The revision is the revision of last modification on this key.
	// If the revision is compacted, will meet required revision has been compacted error.
	// In this case, use the compact revision to re-watch the key.
	revision := resp.Header.GetRevision()
	for {
		rch := watcher.Watch(ctx, key, clientv3.WithRev(revision), clientv3.WithPrefix())
		for wresp := range rch {
			// meet compacted error, use the compact revision.
			if wresp.CompactRevision != 0 {
				log.Warn("required revision has been compacted, use the compact revision",
					zap.Int64("required-revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.Error("watcher is canceled with",
					zap.Int64("revision", revision),
					zap.String("key", key))
				return wresp.Err()
			}

			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.DELETE:
					deleteAction()
				case mvccpb.PUT:
					putAction()
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}
