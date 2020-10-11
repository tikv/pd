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
// See the License for the specific language governing permissions and
// limitations under the License.

package encryptionkm

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/encryption"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/etcdutil"
	"github.com/tikv/pd/server/election"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

const (
	// EncryptionKeysPath is the path to store keys in etcd.
	EncryptionKeysPath = "encryption_keys"

	// Special key id to denote encryption is currently not enabled.
	disableEncryptionKeyID = 0
	// Check interval for data key rotation.
	keyRotationCheckPeriod = time.Minute * 10
	// Times to retry generating new data key.
	keyRotationRetryLimit = 10
)

// Test helpers
var (
	now                   = func() time.Time { return time.Now() }
	tick                  = func(ticker *time.Ticker) <-chan time.Time { return ticker.C }
	eventAfterReload      = func() {}
	eventAfterTicker      = func() {}
	eventAfterLeaderCheck = func() {}
	eventSaveKeysFailure  = func() {}
)

// KeyManager maintains the list to encryption keys. It handles encryption key generation and
// rotation, persisting and loading encryption keys.
type KeyManager struct {
	// Backing storage for key dictionary.
	etcdClient *clientv3.Client
	// Encryption method used to encrypt data
	method encryptionpb.EncryptionMethod
	// Time interval between data key rotation.
	dataKeyRotationPeriod time.Duration
	// Metadata defines the master key to use.
	masterKeyMeta *encryptionpb.MasterKey
	// Mutex for updating keys. Used for both of LoadKeys() and rotateKeyIfNeeded().
	muUpdate sync.Mutex
	// PD leadership of the current PD node. Only the PD leader will rotate data keys,
	// or change current encryption method.
	// Guarded by muUpdate.
	leadership *election.Leadership
	// Revision of keys loaded from etcd. Guarded by muUpdate.
	keysRevision int64
	// List of all encryption keys and current encryption key id,
	// with type *encryptionpb.KeyDictionary
	keys atomic.Value
}

// saveKeys saves encryption keys in etcd. Fail if given leadership is not current.
func saveKeys(
	etcdClient *clientv3.Client,
	leadership *election.Leadership,
	masterKeyMeta *encryptionpb.MasterKey,
	keys *encryptionpb.KeyDictionary,
) error {
	// Get master key.
	masterKey, err := encryption.NewMasterKey(masterKeyMeta)
	if err != nil {
		return err
	}
	// Set was_exposed flag if master key is plaintext (no-op).
	if masterKey.IsPlaintext() {
		for _, key := range keys.Keys {
			key.WasExposed = true
		}
	}
	// Encode and encrypt data keys.
	plaintextContent, err := proto.Marshal(keys)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStack("fail to marshal encrypion keys")
	}
	ciphertextContent, iv, err := masterKey.Encrypt(plaintextContent)
	if err != nil {
		return err
	}
	content := &encryptionpb.EncryptedContent{
		Content:   ciphertextContent,
		MasterKey: masterKeyMeta,
		Iv:        iv,
	}
	value, err := proto.Marshal(content)
	if err != nil {
		return errs.ErrProtoMarshal.Wrap(err).GenWithStack("fail to marshal encrypted encryption keys")
	}
	// Avoid write conflict with PD peer by checking if we are leader.
	resp, err := leadership.LeaderTxn().
		Then(clientv3.OpPut(EncryptionKeysPath, string(value))).
		Commit()
	if err != nil {
		return errs.ErrEtcdTxn.Wrap(err).GenWithStack("fail to save encryption keys")
	}
	if !resp.Succeeded {
		return errs.ErrEncryptionSaveDataKeys.GenWithStack("leader expired")
	}
	// Leave for the watcher to load the updated keys.
	log.Info("saved encryption keys")
	return nil
}

func loadKeysFromKV(kv *mvccpb.KeyValue) (*encryptionpb.KeyDictionary, error) {
	content := &encryptionpb.EncryptedContent{}
	err := content.Unmarshal(kv.Value)
	if err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStack(
			"fail to unmarshal encrypted encryption keys")
	}
	masterKeyConfig := content.MasterKey
	if masterKeyConfig == nil {
		return nil, errs.ErrEncryptionLoadKeys.GenWithStack(
			"no master key config found with encryption keys")
	}
	masterKey, err := encryption.NewMasterKey(masterKeyConfig)
	if err != nil {
		return nil, err
	}
	plaintextContent, err := masterKey.Decrypt(content.Content, content.Iv)
	if err != nil {
		return nil, err
	}
	keys := &encryptionpb.KeyDictionary{}
	err = keys.Unmarshal(plaintextContent)
	if err != nil {
		return nil, errs.ErrProtoUnmarshal.Wrap(err).GenWithStack(
			"fail to unmarshal encryption keys")
	}
	return keys, nil
}

// NewKeyManager creates a new key manager.
func NewKeyManager(
	ctx context.Context,
	etcdClient *clientv3.Client,
	config *encryption.Config,
) (*KeyManager, error) {
	method, err := config.GetMethod()
	if err != nil {
		return nil, err
	}
	masterKeyMeta, err := config.GetMasterKeyMeta()
	if err != nil {
		return nil, err
	}
	m := &KeyManager{
		etcdClient:            etcdClient,
		method:                method,
		dataKeyRotationPeriod: config.DataKeyRotationPeriod.Duration,
		masterKeyMeta:         masterKeyMeta,
	}
	// Load encryption keys from storage.
	_, err = m.loadKeys()
	if err != nil {
		return nil, err
	}
	// Start periodic check for keys change and rotation key if needed.
	go m.startBackgroundLoop(ctx)
	return m, nil
}

func (m *KeyManager) startBackgroundLoop(ctx context.Context) {
	// Create new context for the loop.
	loopCtx, _ := context.WithCancel(ctx)
	// Setup key dictionary watcher
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer watcher.Close()
	watcherCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchChan := watcher.Watch(watcherCtx, EncryptionKeysPath, clientv3.WithRev(m.keysRevision))
	// Check data key rotation every min(dataKeyRotationPeriod, keyRotationCheckPeriod).
	checkPeriod := m.dataKeyRotationPeriod
	if keyRotationCheckPeriod < checkPeriod {
		checkPeriod = keyRotationCheckPeriod
	}
	ticker := time.NewTicker(checkPeriod)
	defer ticker.Stop()
	// Loop
	for {
		select {
		// Reload encryption keys updated by PD leader (could be ourselves).
		case resp := <-watchChan:
			if resp.Canceled {
				// If the watcher failed, we rely solely on rotateKeyIfNeeded() to reload encryption keys.
				log.Warn("encryption key watcher canceled")
				continue
			}
			for _, event := range resp.Events {
				if event.Type != mvccpb.PUT {
					log.Warn("encryption keys is deleted unexpectely")
					continue
				}
				{
					m.muUpdate.Lock()
					m.loadKeysFromKV(event.Kv)
					m.muUpdate.Unlock()
				}
			}
			eventAfterReload()
			// Check data key rotation in case we are the PD leader.
		case <-tick(ticker):
			m.muUpdate.Lock()
			err := m.rotateKeyIfNeeded(false /*forceUpdate*/)
			if err != nil {
				log.Warn("fail to rotate data encryption key", zap.Error(err))
			}
			m.muUpdate.Unlock()
			eventAfterTicker()
			// Server shutdown.
		case <-loopCtx.Done():
			log.Info("encryption key manager is closed")
			return
		}
	}
}

func (m *KeyManager) loadKeysFromKV(
	kv *mvccpb.KeyValue,
) (*encryptionpb.KeyDictionary, error) {
	// Sanity check if keys revision is in order.
	// etcd docs indicates watcher event can be out of order:
	// https://etcd.io/docs/v3.4.0/learning/api_guarantees/#isolation-level-and-consistency-of-replicas
	if kv.ModRevision <= m.keysRevision {
		return m.getKeys(), nil
	}
	keys, err := loadKeysFromKV(kv)
	if err != nil {
		return nil, err
	}
	m.keysRevision = kv.ModRevision
	m.keys.Store(keys)
	log.Info("reloaded encryption keys", zap.Int64("revision", kv.ModRevision))
	return keys, nil
}

func (m *KeyManager) loadKeys() (keys *encryptionpb.KeyDictionary, err error) {
	resp, err := etcdutil.EtcdKVGet(m.etcdClient, EncryptionKeysPath)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		return nil, nil
	}
	keys, err = m.loadKeysFromKV(resp.Kvs[0])
	if err != nil {
		return nil, err
	}
	return keys, err
}

func (m *KeyManager) rotateKeyIfNeeded(forceUpdate bool) error {
	if m.leadership == nil || !m.leadership.Check() {
		// We are not leader.
		m.leadership = nil
		return nil
	}
	eventAfterLeaderCheck()
	// Reload encryption keys in case we are not up-to-date.
	keys, err := m.loadKeys()
	if err != nil {
		return err
	}
	// Initialize if empty.
	if keys == nil {
		keys = &encryptionpb.KeyDictionary{
			CurrentKeyId: disableEncryptionKeyID,
		}
	}
	if keys.Keys == nil {
		keys.Keys = make(map[uint64]*encryptionpb.DataKey)
	}
	needUpdate := forceUpdate
	if m.method == encryptionpb.EncryptionMethod_PLAINTEXT {
		if keys.CurrentKeyId == disableEncryptionKeyID {
			// Encryption is not enabled.
			return nil
		}
		keys.CurrentKeyId = disableEncryptionKeyID
		needUpdate = true
	} else {
		needRotate := false
		if keys.CurrentKeyId == disableEncryptionKeyID {
			needRotate = true
		} else {
			currentKey := keys.Keys[keys.CurrentKeyId]
			if currentKey == nil {
				return errs.ErrEncryptionCurrentKeyNotFound.GenWithStack("keyId = %d", keys.CurrentKeyId)
			}
			// Rotate key in case of:
			// * Encryption method is changed.
			// * Currnet key is exposed.
			// * Current key expired.
			if currentKey.Method != m.method || currentKey.WasExposed ||
				time.Unix(int64(currentKey.CreationTime), 0).
					Add(m.dataKeyRotationPeriod).Before(now()) {
				needRotate = true
			}
		}
		if needRotate {
			rotated := false
			for attempt := 0; attempt < keyRotationRetryLimit; attempt += 1 {
				keyID, key, err := encryption.NewDataKey(m.method, uint64(now().Unix()))
				if err != nil {
					return nil
				}
				if keys.Keys[keyID] == nil {
					keys.Keys[keyID] = key
					keys.CurrentKeyId = keyID
					rotated = true
					log.Info("ready to create or rotate data encryption key", zap.Uint64("keyID", keyID))
					break
				}
				// Duplicated key id. retry.
			}
			if !rotated {
				log.Warn("failed to rotate keys. maximum attempts reached")
				return errs.ErrEncryptionRotateDataKey.GenWithStack("maximum attempts reached")
			}
			needUpdate = true
		}
	}
	if !needUpdate {
		return nil
	}
	// Store updated keys in etcd.
	err = saveKeys(m.etcdClient, m.leadership, m.masterKeyMeta, keys)
	if err != nil {
		eventSaveKeysFailure()
		return err
	}
	// Reload keys.
	_, err = m.loadKeys()
	return err
}

func (m *KeyManager) getKeys() *encryptionpb.KeyDictionary {
	keys := m.keys.Load()
	if keys == nil {
		return nil
	}
	return keys.(*encryptionpb.KeyDictionary)
}

// GetCurrentKey get the current encryption key. The key is nil if encryption is not enabled.
func (m *KeyManager) GetCurrentKey() (keyID uint64, key *encryptionpb.DataKey, err error) {
	keys := m.getKeys()
	if keys == nil || keys.CurrentKeyId == disableEncryptionKeyID {
		// Encryption is not enabled.
		return 0, nil, nil
	}
	keyID = keys.CurrentKeyId
	if keys.Keys == nil {
		return 0, nil, errs.ErrEncryptionCurrentKeyNotFound.GenWithStack(
			"empty key list, currentKeyID = %d", keyID)
	}
	key = keys.Keys[keyID]
	if key == nil {
		// Shouldn't happen, unless key dictionary is corrupted.
		return 0, nil, errs.ErrEncryptionCurrentKeyNotFound.GenWithStack("currentKeyID = %d", keyID)
	}
	return keyID, key, nil
}

// GetKey get the encryption key with the specific key id.
func (m *KeyManager) GetKey(keyID uint64) (*encryptionpb.DataKey, error) {
	localGetKey := func(keyId uint64) *encryptionpb.DataKey {
		keys := m.getKeys()
		if keys == nil || keys.Keys == nil {
			return nil
		}
		return keys.Keys[keyId]
	}
	key := localGetKey(keyID)
	if key != nil {
		return key, nil
	}
	// Key not found in memory.
	// The key could be generated by another PD node, which shouldn't happen normally.
	m.muUpdate.Lock()
	defer m.muUpdate.Unlock()
	// Double check, in case keys is updated by watcher or another GetKey call.
	key = localGetKey(keyID)
	if key != nil {
		return key, nil
	}
	// Reload keys from storage.
	keys, err := m.loadKeys()
	if err != nil {
		return nil, err
	}
	if keys == nil {
		key = nil
	} else {
		key = keys.Keys[keyID]
	}
	if key == nil {
		return nil, errs.ErrEncryptionKeyNotFound.GenWithStack("keyId = %d", keyID)
	}
	return key, nil
}

// SetLeadership sets the PD leadership of the current node. PD leader is responsible to update
// encryption keys, e.g. key rotation.
func (m *KeyManager) SetLeadership(leadership *election.Leadership) error {
	m.muUpdate.Lock()
	defer m.muUpdate.Unlock()
	m.leadership = leadership
	return m.rotateKeyIfNeeded(true /*forceUpdate*/)
}
