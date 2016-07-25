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
	"path"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

const (
	// update timestamp every updateTimestampStep milliseconds.
	updateTimestampStep = int64(50)
	maxLogical          = int64(1 << 18)
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

func (s *Server) getTimestampPath() string {
	return path.Join(s.rootPath, "timestamp")
}

func (s *Server) loadTimestamp() (int64, error) {
	data, err := getValue(s.client, s.getTimestampPath())
	if err != nil {
		return 0, errors.Trace(err)
	}
	if data == nil {
		return 0, nil
	}

	ts, err := bytesToUint64(data)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return int64(ts), nil
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (s *Server) saveTimestamp(now time.Time) error {
	data := uint64ToBytes(uint64(now.UnixNano()))
	key := s.getTimestampPath()

	resp, err := s.LeaderTxn().Then(clientv3.OpPut(key, string(data))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("save timestamp failed, maybe we lost leader")
	}

	s.lastSavedTime = now

	return nil
}

func (s *Server) syncTimestamp() error {
	last, err := s.loadTimestamp()
	if err != nil {
		return errors.Trace(err)
	}

	var now time.Time

	for {
		now = time.Now()

		since := (now.UnixNano() - last) / 1e6
		if since <= 0 {
			return errors.Errorf("%s <= last saved time %s", now, time.Unix(0, last))
		}

		// TODO: can we speed up this?
		if wait := 2*s.cfg.TsoSaveInterval - since; wait > 0 {
			log.Warnf("wait %d milliseconds to guarantee valid generated timestamp", wait)
			time.Sleep(time.Duration(wait) * time.Millisecond)
			continue
		}

		break
	}

	if err = s.saveTimestamp(now); err != nil {
		return errors.Trace(err)
	}

	log.Debug("sync and save timestamp ok")

	current := &atomicObject{
		physical: now,
	}
	s.ts.Store(current)

	return nil
}

func (s *Server) updateTimestamp() error {
	prev := s.ts.Load().(*atomicObject)
	now := time.Now()

	since := now.Sub(prev.physical).Nanoseconds() / 1e6
	if since > 3*updateTimestampStep {
		log.Warnf("clock offset: %v, prev: %v, now %v", since, prev.physical, now)
	}
	// Avoid the same physical time stamp
	if since <= 0 {
		log.Warnf("invalid physical timestamp, prev:%v, now:%v, re-update later", prev.physical, now)
		return nil
	}

	if now.Sub(s.lastSavedTime).Nanoseconds()/1e6 > s.cfg.TsoSaveInterval {
		if err := s.saveTimestamp(now); err != nil {
			return errors.Trace(err)
		}
	}

	current := &atomicObject{
		physical: now,
	}
	s.ts.Store(current)

	return nil
}

const maxRetryCount = 100

func (s *Server) getRespTS(count uint32) *pdpb.Timestamp {
	resp := &pdpb.Timestamp{}
	for i := 0; i < maxRetryCount; i++ {
		current, ok := s.ts.Load().(*atomicObject)
		if !ok {
			log.Errorf("we haven't synced timestamp ok, wait and retry, retry count %d", i)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		resp.Physical = proto.Int64(int64(current.physical.UnixNano()) / 1e6)
		resp.Logical = proto.Int64(atomic.AddInt64(&current.logical, int64(count)))
		if *resp.Logical >= maxLogical {
			log.Errorf("logical part outside of max logical interval %v, please check ntp time, retry count %d", resp, i)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return resp
	}
	return nil
}
