// Copyright 2026 TiKV Project Authors.
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

package kv

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// powerLossStorage records how many bytes of each file it creates have been
// synced, so a power loss can be simulated by dropping the unsynced bytes.
type powerLossStorage struct {
	storage.Storage
	mu     sync.Mutex
	synced map[storage.FileDesc]int64
}

func (s *powerLossStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	w, err := s.Storage.Create(fd)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.synced[fd] = 0
	s.mu.Unlock()
	return &powerLossWriter{Writer: w, s: s, fd: fd}, nil
}

// dropUnsynced truncates every file created through s to its synced length.
func (s *powerLossStorage) dropUnsynced(re *require.Assertions, dir string) {
	for fd, size := range s.synced {
		err := os.Truncate(filepath.Join(dir, fd.String()), size)
		if os.IsNotExist(err) {
			continue
		}
		re.NoError(err)
	}
}

type powerLossWriter struct {
	storage.Writer
	s    *powerLossStorage
	fd   storage.FileDesc
	size int64
}

func (w *powerLossWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *powerLossWriter) Sync() error {
	if err := w.Writer.Sync(); err != nil {
		return err
	}
	w.s.mu.Lock()
	w.s.synced[w.fd] = w.size
	w.s.mu.Unlock()
	return nil
}

// TestLevelDBReopenAfterPowerLoss covers https://github.com/tikv/pd/issues/11596:
// every open writes a new MANIFEST and points CURRENT at it, so losing the
// unsynced data right after a restart must not leave the DB unopenable.
func TestLevelDBReopenAfterPowerLoss(t *testing.T) {
	re := require.New(t)
	dir := t.TempDir()
	kv, err := NewLevelDBKV(dir)
	re.NoError(err)
	re.NoError(kv.Save("key", "value"))
	re.NoError(kv.Close())

	// Restart, then lose power before the kernel writes unsynced data back.
	stor, err := storage.OpenFile(dir, false)
	re.NoError(err)
	powerLoss := &powerLossStorage{Storage: stor, synced: make(map[storage.FileDesc]int64)}
	db, err := leveldb.Open(powerLoss, nil)
	re.NoError(err)
	re.NoError(db.Close())
	re.NoError(stor.Close())
	powerLoss.dropUnsynced(re, dir)

	kv, err = NewLevelDBKV(dir)
	re.NoError(err)
	value, err := kv.Load("key")
	re.NoError(err)
	re.Equal("value", value)
	re.NoError(kv.Close())
}
