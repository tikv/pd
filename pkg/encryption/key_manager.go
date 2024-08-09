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
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/tikv/pd/pkg/election"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/etcdutil"
	"github.com/tikv/pd/pkg/utils/syncutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// KeyManager maintains the list to encryption keys. It handles encryption key generation and
// rotation, persisting and loading encryption keys.
type KeyManager interface {
	GetCurrentKey() (keyID uint64, key *encryptionpb.DataKey, err error)
	GetKey(keyID uint64) (key *encryptionpb.DataKey, err error)
}

// Manager maintains the list to encryption keys. It handles encryption key generation and
// rotation, persisting and loading encryption keys.
type Manager struct {
	// Backing storage for key dictionary.
	etcdClient *clientv3.Client
	// Encryption method used to encrypt data
	method encryptionpb.EncryptionMethod
	// Time interval between data key rotation.
	dataKeyRotationPeriod time.Duration
	// Metadata defines the master key to use.
	masterKeyMeta *encryptionpb.MasterKey
	// Helper methods. Tests can mock the helper to inject dependencies.
	helper keyManagerHelper
	// Mutex for updating keys. Used for both of LoadKeys() and rotateKeyIfNeeded().
	mu struct {
		syncutil.Mutex
		// PD leadership of the current PD node. Only the PD leader will rotate data keys,
		// or change current encryption method.
		// Guarded by mu.
		leadership *election.Leadership
		// Revision of keys loaded from etcd. Guarded by mu.
		keysRevision int64
	}
	// List of all encryption keys and current encryption key id,
	// with type *encryptionpb.KeyDictionary. The content is read-only.
	keys atomic.Value
}

// saveKeys saves encryption keys in etcd. Fail if given leadership is not current.
func saveKeys(
	leadership *election.Leadership,
	masterKeyMeta *encryptionpb.MasterKey,
	keys *encryptionpb.KeyDictionary,
	helper keyManagerHelper,
) (err error) {
	// Get master key.
	masterKey, err := helper.newMasterKey(masterKeyMeta, nil)
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
		Content:       ciphertextContent,
		MasterKey:     masterKeyMeta,
		Iv:            iv,
		CiphertextKey: masterKey.CiphertextKey(),
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
		log.Warn("fail to save encryption keys", errs.ZapError(err))
		return errs.ErrEtcdTxnInternal.Wrap(err).GenWithStack("fail to save encryption keys")
	}
	if !resp.Succeeded {
		log.Warn("fail to save encryption keys and leader expired")
		return errs.ErrEncryptionSaveDataKeys.GenWithStack("leader expired")
	}
	// Leave for the watcher to load the updated keys.
	log.Info("saved encryption keys")
	return nil
}

// extractKeysFromKV unpack encrypted keys from etcd KV.
func extractKeysFromKV(
	kv *mvccpb.KeyValue,
	helper keyManagerHelper,
) (*encryptionpb.KeyDictionary, error) {
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
	masterKey, err := helper.newMasterKey(masterKeyConfig, content.CiphertextKey)
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

// NewManager creates a new key manager.
func NewManager(
	etcdClient *clientv3.Client,
	config *Config,
) (*Manager, error) {
	return newKeyManagerImpl(etcdClient, config, defaultKeyManagerHelper())
}

// newKeyManager creates a new key manager, and allow tests to set a mocked keyManagerHelper.
func newKeyManagerImpl(
	etcdClient *clientv3.Client,
	config *Config,
	helper keyManagerHelper,
) (*Manager, error) {
	method, err := config.GetMethod()
	if err != nil {
		return nil, err
	}
	masterKeyMeta, err := config.GetMasterKeyMeta()
	if err != nil {
		return nil, err
	}
	m := &Manager{
		etcdClient:            etcdClient,
		method:                method,
		dataKeyRotationPeriod: config.DataKeyRotationPeriod.Duration,
		masterKeyMeta:         masterKeyMeta,
		helper:                helper,
	}
	// Load encryption keys from storage.
	err = m.loadKeys()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) keysRevision() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.keysRevision
}

// StartBackgroundLoop start the loop to watch encryption keys changes and perform key rotation
// if needed.
func (m *Manager) StartBackgroundLoop(ctx context.Context) {
	// Setup key dictionary watcher
	watcher := clientv3.NewWatcher(m.etcdClient)
	defer watcher.Close()
	// Check data key rotation every min(dataKeyRotationPeriod, keyRotationCheckPeriod).
	checkPeriod := m.dataKeyRotationPeriod
	if keyRotationCheckPeriod < checkPeriod {
		checkPeriod = keyRotationCheckPeriod
	}
	ticker := time.NewTicker(checkPeriod)
	defer ticker.Stop()

	for {
		var (
			resp clientv3.WatchResponse
			ok   bool
		)
		rch := watcher.Watch(ctx, EncryptionKeysPath, clientv3.WithRev(m.keysRevision()))

	keyWatchLoop:
		for {
			select {
			case resp, ok = <-rch:
				if !ok || resp.Err() != nil {
					// If chan is closed or canceled, exit watch loop
					// Ref https://etcd.io/docs/v3.4/learning/api/#watch-streams
					break keyWatchLoop
				}
				for _, event := range resp.Events {
					if event.Type != mvccpb.PUT {
						log.Warn("encryption keys is deleted unexpectedly")
						continue
					}
					_, err := m.loadKeysFromKV(event.Kv)
					if err != nil {
						log.Warn("fail to get encryption keys from watcher result", errs.ZapError(err))
					}
				}
				m.helper.eventAfterReloadByWatcher()
			case <-m.helper.tick(ticker):
				m.checkOnTick()
				m.helper.eventAfterTicker()
			}
		}

		select {
		case <-ctx.Done():
			// Server shutdown.
			return
		default:
		}

		if resp.CompactRevision != 0 {
			// meet compacted error
			log.Warn("revision has been compacted, the watcher will watch again",
				zap.Int64("revision", m.keysRevision()),
				zap.Int64("compact-revision", resp.CompactRevision))
		} else {
			// other error
			log.Error("encryption key watcher canceled, the watcher will watch again",
				errs.ZapError(errs.ErrEncryptionKeysWatcher, resp.Err()))
		}

		if err := m.loadKeys(); err != nil {
			log.Error("encryption key reload failed", errs.ZapError(err))
		}
	}
}

// checkOnTick perform key rotation and key reload on timer tick, if necessary.
func (m *Manager) checkOnTick() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Check data key rotation in case we are the PD leader.
	err := m.rotateKeyIfNeeded(false /*forceUpdate*/)
	if err != nil {
		log.Warn("fail to rotate data encryption key", errs.ZapError(err))
	}
}

// loadKeysFromKVImpl reload keys from etcd result.
// Require mu lock to be held.
func (m *Manager) loadKeysFromKVImpl(
	kv *mvccpb.KeyValue,
) (*encryptionpb.KeyDictionary, error) {
	// Sanity check if keys revision is in order.
	// etcd docs indicates watcher event can be out of order:
	// https://etcd.io/docs/v3.4.0/learning/api_guarantees/#isolation-level-and-consistency-of-replicas
	if kv.ModRevision <= m.mu.keysRevision {
		return m.getKeys(), nil
	}
	keys, err := extractKeysFromKV(kv, m.helper)
	if err != nil {
		return nil, err
	}
	m.mu.keysRevision = kv.ModRevision
	m.keys.Store(keys)
	log.Info("reloaded encryption keys", zap.Int64("revision", kv.ModRevision))
	return keys, nil
}

// loadKeysFromKV reload keys from etcd result.
func (m *Manager) loadKeysFromKV(
	kv *mvccpb.KeyValue,
) (*encryptionpb.KeyDictionary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadKeysFromKVImpl(kv)
}

// loadKeysImpl reload keys from etcd storage.
// Require mu lock to be held.
func (m *Manager) loadKeysImpl() (keys *encryptionpb.KeyDictionary, err error) {
	resp, err := etcdutil.EtcdKVGet(m.etcdClient, EncryptionKeysPath)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		if m.mu.keysRevision > 0 {
			return nil, errs.ErrEncryptionLoadKeys.GenWithStack(
				"encryption keys is deleted unexpectedly")
		}
		return nil, nil
	}
	keys, err = m.loadKeysFromKVImpl(resp.Kvs[0])
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// loadKeys reload keys from etcd storage.
func (m *Manager) loadKeys() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.loadKeysImpl()
	return err
}

// rotateKeyIfNeeded rotate key if one of the following condition is meet.
//   - Encryption method is changed.
//   - Current key is exposed.
//   - Current key expired.
//
// Otherwise re-save all keys to finish master key rotation if forceUpdate = true.
// Require mu lock to be held.
func (m *Manager) rotateKeyIfNeeded(forceUpdate bool) error {
	if m.mu.leadership == nil || !m.mu.leadership.Check() {
		// We are not leader.
		m.mu.leadership = nil
		return nil
	}
	m.helper.eventAfterLeaderCheckSuccess()
	// Reload encryption keys in case we are not up-to-date.
	keys, err := m.loadKeysImpl()
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
			if currentKey.Method != m.method || currentKey.WasExposed ||
				time.Unix(int64(currentKey.CreationTime), 0).
					Add(m.dataKeyRotationPeriod).Before(m.helper.now()) {
				needRotate = true
			}
		}
		if needRotate {
			rotated := false
			for attempt := 0; attempt < keyRotationRetryLimit; attempt += 1 {
				keyID, key, err := NewDataKey(m.method, uint64(m.helper.now().Unix()))
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
	err = saveKeys(m.mu.leadership, m.masterKeyMeta, keys, m.helper)
	if err != nil {
		m.helper.eventSaveKeysFailure()
		log.Error("failed to save keys", errs.ZapError(err))
		return err
	}
	// Reload keys.
	_, err = m.loadKeysImpl()
	return err
}

func (m *Manager) getKeys() *encryptionpb.KeyDictionary {
	keys := m.keys.Load()
	if keys == nil {
		return nil
	}
	return keys.(*encryptionpb.KeyDictionary)
}

// GetCurrentKey get the current encryption key. The key is nil if encryption is not enabled.
func (m *Manager) GetCurrentKey() (keyID uint64, key *encryptionpb.DataKey, err error) {
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

// getKeyLocal gets specific encryption key by key id, from local cache.
func (m *Manager) getKeyLocal(keyID uint64) *encryptionpb.DataKey {
	keys := m.getKeys()
	if keys == nil || keys.Keys == nil {
		return nil
	}
	return keys.Keys[keyID]
}

// GetKey gets specific encryption key by key id.
func (m *Manager) GetKey(keyID uint64) (*encryptionpb.DataKey, error) {
	key := m.getKeyLocal(keyID)
	if key != nil {
		return key, nil
	}
	// Key not found in memory.
	// The key could be generated by another PD node, which shouldn't happen normally.
	m.mu.Lock()
	defer m.mu.Unlock()
	// Double check, in case keys is updated by watcher or another GetKey call.
	key = m.getKeyLocal(keyID)
	if key != nil {
		return key, nil
	}
	// Reload keys from storage.
	keys, err := m.loadKeysImpl()
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
func (m *Manager) SetLeadership(leadership *election.Leadership) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.leadership = leadership
	return m.rotateKeyIfNeeded(true /*forceUpdate*/)
}

// keyManagerHelper provides interfaces for dependencies and event callbacks where tests can mock.
type keyManagerHelper struct {
	now                          func() time.Time
	tick                         func(ticker *time.Ticker) <-chan time.Time
	newMasterKey                 func(*encryptionpb.MasterKey, []byte) (*MasterKey, error)
	eventAfterReloadByWatcher    func()
	eventAfterTicker             func()
	eventAfterLeaderCheckSuccess func()
	eventSaveKeysFailure         func()
}

func defaultKeyManagerHelper() keyManagerHelper {
	return keyManagerHelper{
		now:                          time.Now,
		tick:                         func(ticker *time.Ticker) <-chan time.Time { return ticker.C },
		newMasterKey:                 NewMasterKey,
		eventAfterReloadByWatcher:    func() {},
		eventAfterTicker:             func() {},
		eventAfterLeaderCheckSuccess: func() {},
		eventSaveKeysFailure:         func() {},
	}
}
