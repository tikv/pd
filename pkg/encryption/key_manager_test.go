// Copyright 2020 TiKV Project Authors.
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

package encryption

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// #nosec G101
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

func newTestEtcd(t *testing.T) (client *clientv3.Client) {
	_, client, clean := etcdutil.NewTestEtcdCluster(t, 1)
	t.Cleanup(func() {
		clean()
	})
	return client
}

func newTestKeyFile(t *testing.T, re *require.Assertions, key ...string) (keyFilePath string) {
	testKey := testMasterKey
	for _, k := range key {
		testKey = k
	}

	keyFilePath = filepath.Join(t.TempDir(), "key")
	err := os.WriteFile(keyFilePath, []byte(testKey), 0600)
	re.NoError(err)

	return keyFilePath
}

func newTestLeader(re *require.Assertions, client *clientv3.Client) *election.Leadership {
	leader := election.NewLeadership(client, "test_leader", "test")
	timeout := int64(30000000) // about a year.
	err := leader.Campaign(timeout, "")
	re.NoError(err)
	return leader
}

func checkMasterKeyMeta(re *require.Assertions, value []byte, meta *encryptionpb.MasterKey, ciphertextKey []byte) {
	content := &encryptionpb.EncryptedContent{}
	err := content.Unmarshal(value)
	re.NoError(err)
	re.True(proto.Equal(content.MasterKey, meta))
	re.Equal(content.CiphertextKey, ciphertextKey)
}

func TestNewKeyManagerBasic(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	// Use default config.
	config := &Config{}
	err := config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Check config.
	re.Equal(encryptionpb.EncryptionMethod_PLAINTEXT, m.method)
	re.NotNil(m.masterKeyMeta.GetPlaintext())
	// Check loaded keys.
	re.Nil(m.keys.Load())
	// Check etcd KV.
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	re.NoError(err)
	re.Nil(value)
}

func TestNewKeyManagerWithCustomConfig(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	// Custom config
	rotatePeriod, err := time.ParseDuration("100h")
	re.NoError(err)
	config := &Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotatePeriod),
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Check config.
	re.Equal(encryptionpb.EncryptionMethod_AES128_CTR, m.method)
	re.Equal(rotatePeriod, m.dataKeyRotationPeriod)
	re.NotNil(m.masterKeyMeta)
	keyFileMeta := m.masterKeyMeta.GetFile()
	re.NotNil(keyFileMeta)
	re.Equal(config.MasterKey.MasterKeyFileConfig.FilePath, keyFileMeta.Path)
	// Check loaded keys.
	re.Nil(m.keys.Load())
	// Check etcd KV.
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	re.NoError(err)
	re.Nil(value)
}

func TestNewKeyManagerLoadKeys(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Use default config.
	config := &Config{}
	err := config.Adjust()
	re.NoError(err)
	// Store initial keys in etcd.
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Check config.
	re.Equal(encryptionpb.EncryptionMethod_PLAINTEXT, m.method)
	re.NotNil(m.masterKeyMeta.GetPlaintext())
	// Check loaded keys.
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	// Check etcd KV.
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
}

func TestGetCurrentKey(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	// Use default config.
	config := &Config{}
	err := config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Test encryption disabled.
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	re.Equal(uint64(disableEncryptionKeyID), currentKeyID)
	re.Nil(currentKey)
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
	re.NoError(err)
	re.Equal(keys.CurrentKeyId, currentKeyID)
	re.True(proto.Equal(currentKey, keys.Keys[keys.CurrentKeyId]))
	// Test current key missing.
	keys = &encryptionpb.KeyDictionary{
		CurrentKeyId: 123,
		Keys:         make(map[uint64]*encryptionpb.DataKey),
	}
	m.keys.Store(keys)
	_, _, err = m.GetCurrentKey()
	re.Error(err)
}

func TestGetKey(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Store initial keys in etcd.
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Use default config.
	config := &Config{}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Get existing key.
	key, err := m.GetKey(uint64(123))
	re.NoError(err)
	re.True(proto.Equal(key, keys.Keys[123]))
	// Get key that require a reload.
	// Deliberately cancel watcher, delete a key and check if it has reloaded.
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	newLoadedKeys := typeutil.DeepClone(loadedKeys, core.KeyDictionaryFactory)

	delete(newLoadedKeys.Keys, 456)
	m.keys.Store(newLoadedKeys)
	m.mu.keysRevision = 0
	key, err = m.GetKey(uint64(456))
	re.NoError(err)
	re.True(proto.Equal(key, keys.Keys[456]))
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	// Get non-existing key.
	_, err = m.GetKey(uint64(789))
	re.Error(err)
}

func TestLoadKeyEmpty(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Store initial keys in etcd.
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Use default config.
	config := &Config{}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	// Simulate keys get deleted.
	_, err = client.Delete(context.Background(), EncryptionKeysPath)
	re.NoError(err)
	re.Error(m.loadKeys())
}

func TestWatcher(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Use default config.
	config := &Config{}
	err := config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	go m.StartBackgroundLoop(ctx)
	_, err = m.GetKey(123)
	re.Error(err)
	_, err = m.GetKey(456)
	re.Error(err)
	// Update keys in etcd
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	<-reloadEvent
	key, err := m.GetKey(123)
	re.NoError(err)
	re.True(proto.Equal(key, keys.Keys[123]))
	_, err = m.GetKey(456)
	re.Error(err)
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
	re.NoError(err)
	<-reloadEvent
	key, err = m.GetKey(123)
	re.NoError(err)
	re.True(proto.Equal(key, keys.Keys[123]))
	key, err = m.GetKey(456)
	re.NoError(err)
	re.True(proto.Equal(key, keys.Keys[456]))
}

func TestSetLeadershipWithEncryptionOff(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	// Use default config.
	config := &Config{}
	err := config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := NewManager(client, config)
	re.NoError(err)
	re.Nil(m.keys.Load())
	// Set leadership
	leadership := newTestLeader(re, client)
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption stays off.
	re.Nil(m.keys.Load())
	value, err := etcdutil.GetValue(client, EncryptionKeysPath)
	re.NoError(err)
	re.Nil(value)
}

func TestSetLeadershipWithEncryptionEnabling(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Config with encryption on.
	config := &Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err := config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.Nil(m.keys.Load())
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption is on and persisted.
	<-reloadEvent
	re.NotNil(m.keys.Load())
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	method, err := config.GetMethod()
	re.NoError(err)
	re.Equal(method, currentKey.Method)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	re.True(proto.Equal(loadedKeys.Keys[currentKeyID], currentKey))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(loadedKeys, storedKeys))
}

func TestSetLeadershipWithEncryptionMethodChanged(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
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
	re.NoError(err)
	// Config with different encryption method.
	config := &Config{
		DataEncryptionMethod: "aes256-ctr",
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption method is updated.
	<-reloadEvent
	re.NotNil(m.keys.Load())
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	re.Equal(encryptionpb.EncryptionMethod_AES256_CTR, currentKey.Method)
	re.Len(currentKey.Key, 32)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	re.Equal(currentKeyID, loadedKeys.CurrentKeyId)
	re.True(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(loadedKeys, storedKeys))
}

func TestSetLeadershipWithCurrentKeyExposed(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
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
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with different encryption method.
	config := &Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption method is updated.
	<-reloadEvent
	re.NotNil(m.keys.Load())
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	re.Equal(encryptionpb.EncryptionMethod_AES128_CTR, currentKey.Method)
	re.Len(currentKey.Key, 16)
	re.False(currentKey.WasExposed)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	re.Equal(currentKeyID, loadedKeys.CurrentKeyId)
	re.True(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(loadedKeys, storedKeys))
}

func TestSetLeadershipWithCurrentKeyExpired(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
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
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	re.NoError(err)
	config := &Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption method is updated.
	<-reloadEvent
	re.NotNil(m.keys.Load())
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	re.Equal(encryptionpb.EncryptionMethod_AES128_CTR, currentKey.Method)
	re.Len(currentKey.Key, 16)
	re.False(currentKey.WasExposed)
	re.Equal(uint64(helper.now().Unix()), currentKey.CreationTime)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	re.Equal(currentKeyID, loadedKeys.CurrentKeyId)
	re.True(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(loadedKeys, storedKeys))
}

func TestSetLeadershipWithMasterKeyChanged(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	keyFile2 := newTestKeyFile(t, re, testMasterKey2)
	leadership := newTestLeader(re, client)
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
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with a different master key.
	config := &Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile2,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check keys are the same, but encrypted with the new master key.
	<-reloadEvent
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
	meta, err := config.GetMasterKeyMeta()
	re.NoError(err)
	checkMasterKeyMeta(re, resp.Kvs[0].Value, meta, nil)
}

func TestSetLeadershipMasterKeyWithCiphertextKey(t *testing.T) {
	re := require.New(t)
	// Initialize.
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	helper.now = func() time.Time { return time.Unix(int64(1601679533), 0) }
	// mock NewMasterKey
	newMasterKeyCalled := 0
	outputMasterKey, _ := hex.DecodeString(testMasterKey)
	outputCiphertextKey, _ := hex.DecodeString(testCiphertextKey)
	helper.newMasterKey = func(
		_ *encryptionpb.MasterKey,
		ciphertext []byte,
	) (*MasterKey, error) {
		if newMasterKeyCalled < 2 {
			// initial load and save. no ciphertextKey
			re.Nil(ciphertext)
		} else if newMasterKeyCalled == 2 {
			// called by loadKeys after saveKeys
			re.Equal(ciphertext, outputCiphertextKey)
		}
		newMasterKeyCalled += 1
		return NewCustomMasterKeyForTest(outputMasterKey, outputCiphertextKey), nil
	}
	// Update keys in etcd
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with a different master key.
	config := &Config{
		DataEncryptionMethod: "aes128-ctr",
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	re.Equal(3, newMasterKeyCalled)
	// Check if keys are the same
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
	meta, err := config.GetMasterKeyMeta()
	re.NoError(err)
	// Check ciphertext key is stored with keys.
	checkMasterKeyMeta(re, resp.Kvs[0].Value, meta, outputCiphertextKey)
}

func TestSetLeadershipWithEncryptionDisabling(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Listen on watcher event
	reloadEvent := make(chan struct{}, 10)
	helper.eventAfterReloadByWatcher = func() {
		var e struct{}
		reloadEvent <- e
	}
	// Update keys in etcd
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Use default config.
	config := &Config{}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check encryption is disabled
	<-reloadEvent
	expectedKeys := typeutil.DeepClone(keys, core.KeyDictionaryFactory)
	expectedKeys.CurrentKeyId = disableEncryptionKeyID
	expectedKeys.Keys[123].WasExposed = true
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), expectedKeys))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, expectedKeys))
}

func TestKeyRotation(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	mockNow := int64(1601679533)
	helper.now = func() time.Time { return time.Unix(atomic.LoadInt64(&mockNow), 0) }
	mockTick := make(chan time.Time)
	helper.tick = func(_ *time.Ticker) <-chan time.Time { return mockTick }
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
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	re.NoError(err)
	config := &Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check keys
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
	// Advance time and trigger ticker
	atomic.AddInt64(&mockNow, int64(101))
	mockTick <- time.Unix(atomic.LoadInt64(&mockNow), 0)
	<-tickerEvent
	<-reloadEvent
	// Check key is rotated.
	currentKeyID, currentKey, err := m.GetCurrentKey()
	re.NoError(err)
	re.NotEqual(uint64(123), currentKeyID)
	re.Equal(encryptionpb.EncryptionMethod_AES128_CTR, currentKey.Method)
	re.Len(currentKey.Key, 16)
	re.Equal(uint64(mockNow), currentKey.CreationTime)
	re.False(currentKey.WasExposed)
	loadedKeys := m.keys.Load().(*encryptionpb.KeyDictionary)
	re.Equal(currentKeyID, loadedKeys.CurrentKeyId)
	re.True(proto.Equal(loadedKeys.Keys[123], keys.Keys[123]))
	re.True(proto.Equal(loadedKeys.Keys[currentKeyID], currentKey))
	resp, err = etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err = extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, loadedKeys))
}

func TestKeyRotationConflict(t *testing.T) {
	re := require.New(t)
	// Initialize.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestEtcd(t)
	keyFile := newTestKeyFile(t, re)
	leadership := newTestLeader(re, client)
	// Setup helper
	helper := defaultKeyManagerHelper()
	// Mock time
	mockNow := int64(1601679533)
	helper.now = func() time.Time { return time.Unix(atomic.LoadInt64(&mockNow), 0) }
	mockTick := make(chan time.Time, 10)
	helper.tick = func(_ *time.Ticker) <-chan time.Time { return mockTick }
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
	masterKeyMeta := newTestMasterKey(keyFile)
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
	re.NoError(err)
	// Config with 100s rotation period.
	rotationPeriod, err := time.ParseDuration("100s")
	re.NoError(err)
	config := &Config{
		DataEncryptionMethod:  "aes128-ctr",
		DataKeyRotationPeriod: typeutil.NewDuration(rotationPeriod),
		MasterKey: MasterKeyConfig{
			Type: "file",
			MasterKeyFileConfig: MasterKeyFileConfig{
				FilePath: keyFile,
			},
		},
	}
	err = config.Adjust()
	re.NoError(err)
	// Create the key manager.
	m, err := newKeyManagerImpl(client, config, helper)
	re.NoError(err)
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	go m.StartBackgroundLoop(ctx)
	// Set leadership
	err = m.SetLeadership(leadership)
	re.NoError(err)
	// Check keys
	re.True(proto.Equal(m.keys.Load().(*encryptionpb.KeyDictionary), keys))
	resp, err := etcdutil.EtcdKVGet(client, EncryptionKeysPath)
	re.NoError(err)
	storedKeys, err := extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
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
	re.NoError(err)
	storedKeys, err = extractKeysFromKV(resp.Kvs[0], defaultKeyManagerHelper())
	re.NoError(err)
	re.True(proto.Equal(storedKeys, keys))
}

func newTestMasterKey(keyFile string) *encryptionpb.MasterKey {
	return &encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: keyFile,
			},
		},
	}
}
