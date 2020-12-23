// Copyright 2020 TiKV Project Authors.
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

package encryptionkm

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/pkg/tempurl"
	"github.com/tikv/pd/pkg/typeutil"
	"github.com/tikv/pd/server/election"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func TestKeyManager(t *testing.T) {
	TestingT(t)
}

type testKeyManagerSuite struct{}

var _ = Suite(&testKeyManagerSuite{})

const (
	testMasterKey     = "8fd7e3e917c170d92f3e51a981dd7bc8fba11f3df7d8df994842f6e86f69b530"
	testMasterKey2    = "8fd7e3e917c170d92f3e51a981dd7bc8fba11f3df7d8df994842f6e86f69b531"
	testCiphertextKey = "8fd7e3e917c170d92f3e51a981dd7bc8fba11f3df7d8df994842f6e86f69b532"
	testDataKey       = "be798242dde0c40d9a65cdbc36c1c9ac"
)

func getTestDataKey() []byte {
	key, _ := hex.DecodeString(testDataKey)
	return key
}

func newTestEtcd(c *C) (client *clientv3.Client, cleanup func()) {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir, _ = ioutil.TempDir("/tmp", "test_etcd")
	cfg.Logger = "zap"
	pu, err := url.Parse(tempurl.Alloc())
	c.Assert(err, IsNil)
	cfg.LPUrls = []url.URL{*pu}
	cfg.APUrls = cfg.LPUrls
	cu, err := url.Parse(tempurl.Alloc())
	c.Assert(err, IsNil)
	cfg.LCUrls = []url.URL{*cu}
	cfg.ACUrls = cfg.LCUrls
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.LPUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew
	server, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)
	<-server.Server.ReadyNotify()

	client, err = clientv3.New(clientv3.Config{
		Endpoints: []string{cfg.LCUrls[0].String()},
	})
	c.Assert(err, IsNil)

	cleanup = func() {
		client.Close()
		server.Close()
		os.RemoveAll(cfg.Dir)
	}

	return client, cleanup
}

func newTestKeyFile(c *C, key ...string) (keyFilePath string, cleanup func()) {
	testKey := testMasterKey
	for _, k := range key {
		testKey = k
	}
	tempDir, err := ioutil.TempDir("/tmp", "test_key_file")
	c.Assert(err, IsNil)
	keyFilePath = tempDir + "/key"
	err = ioutil.WriteFile(keyFilePath, []byte(testKey), 0644)
	c.Assert(err, IsNil)

	cleanup = func() {
		os.RemoveAll(tempDir)
	}

	return keyFilePath, cleanup
}

func newTestLeader(c *C, client *clientv3.Client) *election.Leadership {
	leader := election.NewLeadership(client, "test_leader", "test")
	timeout := int64(30000000) // about a year.
	err := leader.Campaign(timeout, "")
	c.Assert(err, IsNil)
	return leader
}

func checkMasterKeyMeta(c *C, value []byte, meta *encryptionpb.MasterKey, ciphertextKey []byte) {
	content := &encryptionpb.EncryptedContent{}
	err := content.Unmarshal(value)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(content.MasterKey, meta), IsTrue)
	c.Assert(bytes.Equal(content.CiphertextKey, ciphertextKey), IsTrue)
}

func (s *testKeyManagerSuite) TestNewKeyManagerBasic(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	// Use default config.
	config := &encryption.Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Check config.
	c.Assert(m.method, Equals, encryptionpb.EncryptionMethod_PLAINTEXT)
	c.Assert(m.masterKeyMeta.GetPlaintext(), NotNil)
	// Check loaded keys.
	c.Assert(m.keys.Load(), IsNil)
	// Check etcd KV.
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)
}

func (s *testKeyManagerSuite) TestNewKeyManagerWithCustomConfig(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	// Custom config
	rotatePeriod, err := time.ParseDuration("100h")
	c.Assert(err, IsNil)
	config := &encryption.Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotatePeriod),
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Check config.
	c.Assert(m.method, Equals, encryptionpb.EncryptionMethod_AES128_CTR)
	c.Assert(m.dataKeyRotationPeriod, Equals, rotatePeriod)
	c.Assert(m.masterKeyMeta, NotNil)
	keyFileMeta := m.masterKeyMeta.GetFile()
	c.Assert(keyFileMeta, NotNil)
	c.Assert(keyFileMeta.Path, Equals, config.MasterKey.MasterKeyFileConfig.FilePath)
	// Check loaded keys.
	c.Assert(m.keys.Load(), IsNil)
	// Check etcd KV.
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)
}

func (s *testKeyManagerSuite) TestNewKeyManagerLoadKeys(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Use default config.
	config := &encryption.Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Store initial keys in etcd.
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
		},
	}
	err = saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Check config.
	c.Assert(m.method, Equals, encryptionpb.EncryptionMethod_PLAINTEXT)
	c.Assert(m.masterKeyMeta.GetPlaintext(), NotNil)
	// Check loaded keys.
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	// Check etcd KV.
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
}

func (s *testKeyManagerSuite) TestGetCurrentKey(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	// Use default config.
	config := &encryption.Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Test encryption disabled.
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKeyID, Equals, uint64(disableEncryptionKeyID))
	c.Assert(currentKey, IsNil)
	// Test normal case.
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
		},
	}
	m.keys.Store(keys)
	currentKeyID, currentKey, err = m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKeyID, Equals, keys.CurrentKeyId)
	c.Assert(proto.Equal(currentKey, keys.Keys[keys.CurrentKeyId]), IsTrue)
	// Test current key missing.
	keys = &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys:         make(map[uint64]*encryptionpb.DataKey),
	}
	m.keys.Store(keys)
	_, _, err = m.GetCurrentKey()
	c.Assert(err, NotNil)
}

func (s *testKeyManagerSuite) TestGetKey(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Store initial keys in etcd.
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
			456: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679534),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Use default config.
	config := &encryption.Config{}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Get existing key.
	key, err := m.GetKey(uint64(123))
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(key, keys.Keys[123]), IsTrue)
	// Get key that require a reload.
	// Deliberately cancel watcher, delete a key and check if it has reloaded.
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	loadedKeys = proto.Clone(loadedKeys).(*encryptionpb.KeyDictionary)
	delete(loadedKeys.Keys, 456)
	m.keys.Store(loadedKeys)
	m.mu.keysRevision = 0
	key, err = m.GetKey(uint64(456))
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(key, keys.Keys[456]), IsTrue)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	// Get non-existing key.
	_, err = m.GetKey(uint64(789))
	c.Assert(err, NotNil)
}

func (s *testKeyManagerSuite) TestLoadKeyEmpty(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Store initial keys in etcd.
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Use default config.
	config := &encryption.Config{}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	// Simulate keys get deleted.
	_, err = client.Delete(context.Background(), EncryptionKeysPath)
	c.Assert(err, IsNil)
	_, err = m.loadKeys()
	c.Assert(err, NotNil)
}

func (s *testKeyManagerSuite) TestWatcher(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Use default config.
	config := &encryption.Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	go m.StartBackgroundLoop(ctx)
	_, err = m.GetKey(123)
	c.Assert(err, NotNil)
	_, err = m.GetKey(456)
	c.Assert(err, NotNil)
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
		},
	}
	err = saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	<-reloadEvent
	key, err := m.GetKey(123)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(key, keys.Keys[123]), IsTrue)
	_, err = m.GetKey(456)
	c.Assert(err, NotNil)
	// Update again
	keys = &encryptionpb.KeyDictionary{
		CurrentKeyId: 456,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
			456: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679534),
				WasExposed:   false,
			},
		},
	}
	err = saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	<-reloadEvent
	key, err = m.GetKey(123)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(key, keys.Keys[123]), IsTrue)
	key, err = m.GetKey(456)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(key, keys.Keys[456]), IsTrue)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithEncryptionOff(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	// Use default config.
	config := &encryption.Config{}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := NewKeyManager(client, config)
	c.Assert(err, IsNil)
	c.Assert(m.keys.Load(), IsNil)
	// Set leadership
	leadership := newTestLeader(c, client)
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption stays off.
	c.Assert(m.keys.Load(), IsNil)
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithEncryptionEnabling(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Config with encryption on.
	config := &encryption.Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err := config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(m.keys.Load(), IsNil)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption is on and persisted.
	<-reloadEvent
	c.Assert(m.keys.Load(), NotNil)
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	method, err := config.GetMethod()
	c.Assert(err, IsNil)
	c.Assert(currentKey.Method, Equals, method)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	c.Assert(proto.Equal(loadedKeys.Keys[currentKeyID], currentKey), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(loadedKeys, storedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithEncryptionMethodChanged(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533), 0) }
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with different encrption method.
	config := &encryption.Config{
		DataEncryptionMethod: "aes256-ctr",
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption method is updated.
	<-reloadEvent
	c.Assert(m.keys.Load(), NotNil)
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKey.Method, Equals, encryptionpb.EncryptionMethod_AES256_CTR)
	c.Assert(currentKey.Key, HasLen, 32)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	c.Assert(loadedKeys.CurrentKeyId, Equals, currentKeyID)
	c.Assert(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(loadedKeys, storedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithCurrentKeyExposed(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533), 0) }
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   true,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with different encrption method.
	config := &encryption.Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption method is updated.
	<-reloadEvent
	c.Assert(m.keys.Load(), NotNil)
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKey.Method, Equals, encryptionpb.EncryptionMethod_AES128_CTR)
	c.Assert(currentKey.Key, HasLen, 16)
	c.Assert(currentKey.WasExposed, IsFalse)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	c.Assert(loadedKeys.CurrentKeyId, Equals, currentKeyID)
	c.Assert(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(loadedKeys, storedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithCurrentKeyExpired(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533+101), 0) }
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	c.Assert(err, IsNil)
	config := &encryption.Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption method is updated.
	<-reloadEvent
	c.Assert(m.keys.Load(), NotNil)
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKey.Method, Equals, encryptionpb.EncryptionMethod_AES128_CTR)
	c.Assert(currentKey.Key, HasLen, 16)
	c.Assert(currentKey.WasExposed, IsFalse)
	c.Assert(currentKey.CreationTime, Equals, uint64(helper.now().Unix()))
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	c.Assert(loadedKeys.CurrentKeyId, Equals, currentKeyID)
	c.Assert(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(loadedKeys, storedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithMasterKeyChanged(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	keyFile2, cleanupKeyFile2 := newTestKeyFile(c, testMasterKey2)
	defer cleanupKeyFile2()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533), 0) }
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with a different master key.
	config := &encryption.Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile2,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check keys are the same, but encrypted with the new master key.
	<-reloadEvent
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
	meta, err := config.GetMasterKeyMeta()
	c.Assert(err, IsNil)
	checkMasterKeyMeta(c, resp.Kvs[0].Value, meta, nil)
}

func (s *testKeyManagerSuite) TestSetLeadershipMasterKeyWithCiphertextKey(c *C) {
	// Initialize.
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533), 0) }
	// mock NewMasterKey
	newMasterKeyCalled := 0
	outputMasterKey, _ := hex.DecodeString(testMasterKey)
	outputCiphertextKey, _ := hex.DecodeString(testCiphertextKey)
	helper.newMasterKey = func(
		meta *encryptionpb.MasterKey,
		ciphertext []byte,
	) (*encryption.MasterKey, error) {
		if newMasterKeyCalled < 2 {
			// initial load and save. no ciphertextKey
			c.Assert(ciphertext, IsNil)
		} else if newMasterKeyCalled == 2 {
			// called by loadKeys after saveKeys
			c.Assert(bytes.Equal(ciphertext, outputCiphertextKey), IsTrue)
		}
		newMasterKeyCalled += 1
		return encryption.NewCustomMasterKeyForTest(outputMasterKey, outputCiphertextKey), nil
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with a different master key.
	config := &encryption.Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	c.Assert(newMasterKeyCalled, Equals, 3)
	// Check if keys are the same
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
	meta, err := config.GetMasterKeyMeta()
	c.Assert(err, IsNil)
	// Check ciphertext key is stored with keys.
	checkMasterKeyMeta(c, resp.Kvs[0].Value, meta, outputCiphertextKey)
}

func (s *testKeyManagerSuite) TestSetLeadershipWithEncryptionDisabling(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Use default config.
	config := &encryption.Config{}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check encryption is disabled
	<-reloadEvent
	expectedKeys := proto.Clone(keys).(*encryptionpb.KeyDictionary)
	expectedKeys.CurrentKeyId = disableEncryptionKeyID
	expectedKeys.Keys[123].WasExposed = true
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), expectedKeys), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, expectedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestKeyRotation(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	mockNow := int64(1601679533)
	helper.now = func() time.Time { return time.Unix(atomic.LoadInt64(&mockNow), 0) }
	mockTick := make(chan time.Time)
	helper.tick = func(ticker *time.Ticker) <-chan time.Time { return mockTick }
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Listen on ticker event
	tickerEvent := make(chan struct{}, 10)
	helper.eventAfterTicker = func() {
		var e struct{}
		tickerEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	c.Assert(err, IsNil)
	config := &encryption.Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check keys
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
	// Advance time and trigger ticker
	atomic.AddInt64(&mockNow, int64(101))
	mockTick <- time.Unix(atomic.LoadInt64(&mockNow), 0)
	<-tickerEvent
	<-reloadEvent
	// Check key is rotated.
	currentKeyID, currentKey, err := m.GetCurrentKey()
	c.Assert(err, IsNil)
	c.Assert(currentKeyID, Not(Equals), uint64(123))
	c.Assert(currentKey.Method, Equals, encryptionpb.EncryptionMethod_AES128_CTR)
	c.Assert(currentKey.Key, HasLen, 16)
	c.Assert(currentKey.CreationTime, Equals, uint64(mockNow))
	c.Assert(currentKey.WasExposed, IsFalse)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	c.Assert(loadedKeys.CurrentKeyId, Equals, currentKeyID)
	c.Assert(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]), IsTrue)
	c.Assert(proto.Equal(loadedKeys.Keys[currentKeyID], currentKey), IsTrue)
	resp, err = etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err = extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, loadedKeys), IsTrue)
}

func (s *testKeyManagerSuite) TestKeyRotationConflict(c *C) {
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, cleanupEtcd := newTestEtcd(c)
	defer cleanupEtcd()
	keyFile, cleanupKeyFile := newTestKeyFile(c)
	defer cleanupKeyFile()
	leadership := newTestLeader(c, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	mockNow := int64(1601679533)
	helper.now = func() time.Time { return time.Unix(atomic.LoadInt64(&mockNow), 0) }
	mockTick := make(chan time.Time, 10)
	helper.tick = func(ticker *time.Ticker) <-chan time.Time { return mockTick }
	// Listen on ticker event
	tickerEvent := make(chan struct{}, 10)
	helper.eventAfterTicker = func() {
		var e struct{}
		tickerEvent <- e
	}
	// Listen on leader check event
	shouldResetLeader := int32(0)
	helper.eventAfterLeaderCheckSuccess = func() {
		if atomic.LoadInt32(&shouldResetLeader) != 0 {
			leadership.Reset()
		}
	}
	// Listen on save key failure event
	shouldListenSaveKeysFailure := int32(0)
	saveKeysFailureEvent := make(chan struct{}, 10)
	helper.eventSaveKeysFailure = func() {
		if atomic.LoadInt32(&shouldListenSaveKeysFailure) != 0 {
			var e struct{}
			saveKeysFailureEvent <- e
		}
	}
	// Update keys in etcd
	masterKeyMeta := &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
	keys := &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys: map[uint64]*encryptionpb.DataKey{
			123: {
				Key:          getTestDataKey(),
				Method:       encryptionpb.EncryptionMethod_AES128_CTR,
				CreationTime: uint64(1601679533),
				WasExposed:   false,
			},
		},
	}
	err := saveKeys(leadership, masterKeyMeta, keys, defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	c.Assert(err, IsNil)
	config := &encryption.Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: encryption.MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: encryption.MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	c.Assert(err, IsNil)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	c.Assert(err, IsNil)
	// Check keys
	c.Assert(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys), IsTrue)
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
	// Invalidate leader after leader check.
	atomic.StoreInt32(&shouldResetLeader, 1)
	atomic.StoreInt32(&shouldListenSaveKeysFailure, 1)
	// Advance time and trigger ticker
	atomic.AddInt64(&mockNow, int64(101))
	mockTick <- time.Unix(atomic.LoadInt64(&mockNow), 0)
	<-tickerEvent
	<-saveKeysFailureEvent
	// Check keys is unchanged.
	resp, err = etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	c.Assert(err, IsNil)
	storedKeys, err = extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	c.Assert(err, IsNil)
	c.Assert(proto.Equal(storedKeys, keys), IsTrue)
}
