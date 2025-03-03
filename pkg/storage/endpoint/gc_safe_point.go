// Copyright 2022 TiKV Project Authors.
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

package endpoint

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/mcs/utils/constant"
	"github.com/tikv/pd/pkg/storage/kv"
	"github.com/tikv/pd/pkg/utils/keypath"
)

// ServiceSafePoint is the safepoint for a specific service
// NOTE: This type is exported by HTTP API. Please pay more attention when modifying it.
// This type is in sync with `client/http/types.go`.
// ServiceSafePoint is also directly used for storing GC barriers in order to make GC barriers in new versions
// can be backward-compatible with service safe points in old versions.
type ServiceSafePoint struct {
	ServiceID string
	ExpiredAt int64
	SafePoint uint64

	// Note than when marshalled into JSON, omitting KeyspaceID stands for the NullKeyspace (0xffffffff),
	// rather than KeyspaceID = 0 which is the ID of the default keyspace.
	// Special marshalling / unmarshalling methods are given for handling this field in a non-default way.
	//
	// The purpose is to make the code (for keyspaced and non-keyspaced/global GC) unified while keeping the
	// data format compatible with the old versions. In old versions, the global GC (or synonymously the GC
	// of the NullKeyspace, represented by KeyspaceID=0xffffffff) saves service safe points without the
	// KeyspaceID field; but for GC API V2 (deprecated), it attaches the KeyspaceID which is possibly zero
	// (representing the default keyspace).
	//
	// Avoid creating and using a new ServiceSafePoint outside this package. When you must do so, assign
	// constant.NullKeyspaceID to the KeyspaceID field as the default, instead of leaving it zero.
	KeyspaceID uint32
}

func (s *ServiceSafePoint) MarshalJSON() ([]byte, error) {
	if s.KeyspaceID == constant.NullKeyspaceID {
		return json.Marshal(struct {
			ServiceID string `json:"service_id"`
			ExpiredAt int64  `json:"expired_at"`
			SafePoint uint64 `json:"safe_point"`
		}{
			ServiceID: s.ServiceID,
			ExpiredAt: s.ExpiredAt,
			SafePoint: s.SafePoint,
		})
	}
	return json.Marshal(struct {
		ServiceID  string `json:"service_id"`
		ExpiredAt  int64  `json:"expired_at"`
		SafePoint  uint64 `json:"safe_point"`
		KeyspaceID uint32 `json:"keyspace_id"`
	}{
		ServiceID:  s.ServiceID,
		ExpiredAt:  s.ExpiredAt,
		SafePoint:  s.SafePoint,
		KeyspaceID: s.KeyspaceID,
	})
}

func (s *ServiceSafePoint) UnmarshalJSON(data []byte) error {
	var repr struct {
		ServiceID  string  `json:"service_id"`
		ExpiredAt  int64   `json:"expired_at"`
		SafePoint  uint64  `json:"safe_point"`
		KeyspaceID *uint32 `json:"keyspace_id"`
	}
	if err := json.Unmarshal(data, &repr); err != nil {
		return errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	s.ServiceID = repr.ServiceID
	s.ExpiredAt = repr.ExpiredAt
	s.SafePoint = repr.SafePoint
	if repr.KeyspaceID != nil {
		s.KeyspaceID = *repr.KeyspaceID
	} else {
		s.KeyspaceID = constant.NullKeyspaceID
	}
	return nil
}

var _ json.Marshaler = &ServiceSafePoint{}
var _ json.Unmarshaler = &ServiceSafePoint{}

type GCBarrier struct {
	BarrierID string
	BarrierTS uint64
	// Nil means never expiring.
	ExpirationTime *time.Time
}

func NewGCBarrier(barrierID string, barrierTS uint64, expirationTime *time.Time) *GCBarrier {
	// Round up the expirationTime.
	if expirationTime != nil {
		rounded := expirationTime.Truncate(time.Second)
		if rounded.Before(*expirationTime) {
			rounded = rounded.Add(time.Second)
		}
		*expirationTime = rounded
	}
	return &GCBarrier{
		BarrierID:      barrierID,
		BarrierTS:      barrierTS,
		ExpirationTime: expirationTime,
	}
}

func gcBarrierFromServiceSafePoint(s *ServiceSafePoint) *GCBarrier {
	if s == nil {
		return nil
	}

	res := &GCBarrier{
		BarrierID:      s.ServiceID,
		BarrierTS:      s.SafePoint,
		ExpirationTime: nil,
	}
	if s.ExpiredAt < math.MaxInt64 && s.ExpiredAt > 0 {
		expirationTime := new(time.Time)
		*expirationTime = time.Unix(s.ExpiredAt, 0)
		res.ExpirationTime = expirationTime
	}
	return res
}

func (b *GCBarrier) toServiceSafePoint(keyspaceID uint32) *ServiceSafePoint {
	res := &ServiceSafePoint{
		ServiceID:  b.BarrierID,
		ExpiredAt:  math.MaxInt64,
		SafePoint:  b.BarrierTS,
		KeyspaceID: keyspaceID,
	}
	if b.ExpirationTime != nil {
		res.ExpiredAt = b.ExpirationTime.Unix()
	}
	return res
}

func (b *GCBarrier) IsExpired(now time.Time) bool {
	return b.ExpirationTime != nil && now.After(*b.ExpirationTime)
}

func (b *GCBarrier) String() string {
	expirationTime := "<nil>"
	if b.ExpirationTime != nil {
		expirationTime = b.ExpirationTime.String()
	}
	return fmt.Sprintf("GCBarrier { BarrierID: %+q, BarrierTS: %d, ExpirationTime: %+q }",
		b.BarrierID, b.BarrierTS, expirationTime)
}

type GCStateStorage interface {
	GetGCStateProvider() GCStateProvider
}

func (se *StorageEndpoint) GetGCStateProvider() GCStateProvider {
	return newGCStateProvider(se)
}

// GCStateProvider is a stateless wrapper over StorageEndpoint that provides methods for reading/writing GC states.
// It can be dangerous to misuse GC related operations. As an explicit wrapper, it hides the GC related methods away
// from the StorageEndpoint type and the Storage interface, making it less likely to be misused unintentionally when
// the Storage or StorageEndpoint is used in other context.
type GCStateProvider struct {
	storage *StorageEndpoint
}

func newGCStateProvider(storage *StorageEndpoint) GCStateProvider {
	return GCStateProvider{storage: storage}
}

type GCStateWriteBatch struct {
	ops []kv.RawTxnOp
}

// LoadGCSafePoint loads current GC safe point from storage.
func (p GCStateProvider) LoadGCSafePoint(keyspaceID uint32) (uint64, error) {
	if keyspaceID == constant.NullKeyspaceID {
		return p.loadGlobalGCSafePoint()
	}
	return p.loadKeyspaceGCSafePoint(keyspaceID)
}

func (p GCStateProvider) loadGlobalGCSafePoint() (uint64, error) {
	value, err := p.storage.Load(keypath.GCSafePointPath())
	if err != nil || value == "" {
		return 0, err
	}
	gcSafePoint, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return gcSafePoint, nil
}

type keyspaceGCSafePoint struct {
	KeyspaceID uint32 `json:"keyspace_id"`
	SafePoint  uint64 `json:"safe_point"`
}

func (p GCStateProvider) loadKeyspaceGCSafePoint(keyspaceID uint32) (uint64, error) {
	key := keypath.KeyspaceGCSafePointPath(keyspaceID)
	value, err := p.storage.Load(key)
	if err != nil {
		return 0, err
	}
	// GC safe point has not been set for the given keyspace
	if value == "" {
		return 0, nil
	}

	gcSafePoint := &keyspaceGCSafePoint{}
	if err = json.Unmarshal([]byte(value), gcSafePoint); err != nil {
		return 0, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByCause()
	}
	return gcSafePoint.SafePoint, nil
}

func (p GCStateProvider) LoadTxnSafePoint(keyspaceID uint32) (uint64, error) {
	key := keypath.TxnSafePointPath()
	if keyspaceID != constant.NullKeyspaceID {
		key = keypath.KeyspaceTxnSafePointPath(keyspaceID)
	}

	value, err := p.storage.Load(key)
	if err != nil || value == "" {
		return 0, err
	}
	txnSafePoint, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
	}
	return txnSafePoint, err
}

func loadJSON[T any](se *StorageEndpoint, key string) (T, error) {
	value, err := se.Load(key)
	if err != nil {
		var empty T
		return empty, err
	}
	if value == "" {
		var empty T
		return empty, nil
	}
	var data T
	if err = json.Unmarshal([]byte(value), &data); err != nil {
		return data, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
	}
	return data, nil
}

func loadJSONByPrefix[T any](se *StorageEndpoint, prefix string, limit int) ([]string, []T, error) {
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
	keys, values, err := se.LoadRange(prefix, prefixEnd, limit)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}

	data := make([]T, 0, len(keys))
	for i := range keys {
		var item T
		if err := json.Unmarshal([]byte(values[i]), &item); err != nil {
			return nil, nil, errs.ErrJSONUnmarshal.Wrap(err).GenWithStackByArgs()
		}
		data = append(data, item)
	}
	return keys, data, nil
}

func (p GCStateProvider) LoadGCBarrier(keyspaceID uint32, barrierID string) (*GCBarrier, error) {
	prefix := keypath.GCBarrierPrefix()
	if keyspaceID != constant.NullKeyspaceID {
		prefix = keypath.KeyspaceGCBarrierPrefix(keyspaceID)
	}
	key := path.Join(prefix, barrierID)
	// GCBarrier is stored in ServiceSafePoint format for compatibility.
	serviceSafePoint, err := loadJSON[*ServiceSafePoint](p.storage, key)
	if err != nil {
		return nil, err
	}
	return gcBarrierFromServiceSafePoint(serviceSafePoint), nil
}

func (p GCStateProvider) LoadAllGCBarriers(keyspaceID uint32) ([]*GCBarrier, error) {
	prefix := keypath.GCBarrierPrefix()
	if keyspaceID != constant.NullKeyspaceID {
		prefix = keypath.KeyspaceGCBarrierPrefix(keyspaceID)
	}
	// TODO: Limit the count for each call.
	_, serviceSafePoints, err := loadJSONByPrefix[*ServiceSafePoint](p.storage, prefix, 0)
	if err != nil {
		return nil, err
	}
	if len(serviceSafePoints) == 0 {
		return nil, nil
	}
	barriers := make([]*GCBarrier, 0, len(serviceSafePoints))
	for _, serviceSafePoint := range serviceSafePoints {
		barriers = append(barriers, gcBarrierFromServiceSafePoint(serviceSafePoint))
	}
	return barriers, nil
}

func (p GCStateProvider) CompatibleLoadTiDBMinStartTS(keyspaceID uint32) (string, uint64, error) {
	prefix := keypath.CompatibleTiDBMinStartTSPrefix()
	if keyspaceID != constant.NullKeyspaceID {
		prefix = keypath.CompatibleKeyspaceTiDBMinStartTSPrefixFormat(keyspaceID)
	}
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)

	// TODO: Limit the count for each call.
	keys, values, err := p.storage.LoadRange(prefix, prefixEnd, 0)
	if err != nil {
		return "", 0, err
	}

	if len(keys) == 0 {
		return "", 0, nil
	}

	var minKey string
	var minMinStartTS uint64

	for i, valueStr := range values {
		minStartTS, err := strconv.ParseUint(valueStr, 10, 64)
		if err != nil {
			return "", 0, errs.ErrStrconvParseUint.Wrap(err).GenWithStackByArgs()
		}
		if len(minKey) == 0 || minStartTS < minMinStartTS {
			minMinStartTS = minStartTS
			minKey = keys[i]
		}
	}

	// Remove prefix from the key and only keep the identifier written by TiDB.
	if len(minKey) < len(prefix) || minKey[:len(prefix)] != prefix {
		// This is expected to be unreachable.
		return "", 0, errors.Errorf("unexpected internal error: loading TiDB min start ts but got mismatching key prefix, expected prefix: %s, got key: %s", prefix, minKey)
	}
	minKey = minKey[len(prefix):]
	return minKey, minMinStartTS, nil
}

// RunInGCStateTransaction runs a transaction for updating GC states or read a batch of GC states.
// The atomicity is guaranteed by a "revision" key. Any non-empty write caused by the transaction will increase the
// revision.
// In the transaction, reads can be performed on the GCStateProvider as usual, while writes should only be performed
// through the GCStateWriteBatch.
func (p GCStateProvider) RunInGCStateTransaction(f func(wb *GCStateWriteBatch) error) error {
	revisionKey := keypath.GCStateRevisionPath()
	currentRevision, err := p.storage.Load(revisionKey)
	if err != nil {
		return errors.AddStack(err)
	}
	condition := kv.RawTxnCondition{
		Key:     revisionKey,
		CmpType: kv.RawTxnCmpNotExists,
	}
	var currentRevisionValue uint64
	if currentRevision != "" {
		condition.CmpType = kv.RawTxnCmpEqual
		condition.Value = currentRevision
		currentRevisionValue, err = strconv.ParseUint(currentRevision, 10, 64)
	}

	if err != nil {
		return errors.AddStack(err)
	}
	nextRevision := fmt.Sprintf("%d", currentRevisionValue+1)

	wb := GCStateWriteBatch{}
	err = f(&wb)
	if err != nil {
		return errors.AddStack(err)
	}

	ops := wb.ops

	// No need to increase the revision if there's no write (so that it acts like an RLock and concurrent reads won't
	// conflict with each other).
	if len(ops) > 0 {
		ops = append(ops, kv.RawTxnOp{
			Key:    revisionKey,
			OpType: kv.RawTxnOpPut,
			Value:  nextRevision,
		})
	}

	txn := p.storage.CreateRawTxn()
	result, err := txn.If(condition).Then(ops...).Commit()
	if err != nil {
		return errors.AddStack(err)
	}
	if !result.Succeeded {
		return errs.ErrEtcdTxnConflict.GenWithStackByArgs()
	}

	if len(ops) != len(result.Responses) {
		return errors.Errorf("unexpected number of results: %d != %d", len(ops), len(result.Responses))
	}
	return nil
}

//
//// LoadMinServiceGCSafePoint returns the minimum safepoint across all services
//func (se *StorageEndpoint) LoadMinServiceGCSafePoint(now time.Time) (*ServiceSafePoint, error) {
//	prefix := keypath.GCSafePointServicePrefixPath()
//	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)
//	keys, values, err := se.LoadRange(prefix, prefixEnd, 0)
//	if err != nil {
//		return nil, err
//	}
//	if len(keys) == 0 {
//		// There's no service safepoint. It may be a new cluster, or upgraded from an older version where all service
//		// safepoints are missing. For the second case, we have no way to recover it. Store an initial value 0 for
//		// gc_worker.
//		return se.initServiceGCSafePointForGCWorker(0)
//	}
//
//	hasGCWorker := false
//	min := &ServiceSafePoint{SafePoint: math.MaxUint64}
//	for i, key := range keys {
//		ssp := &ServiceSafePoint{}
//		if err := json.Unmarshal([]byte(values[i]), ssp); err != nil {
//			return nil, err
//		}
//		if ssp.ServiceID == keypath.GCWorkerServiceSafePointID {
//			hasGCWorker = true
//			// If gc_worker's expire time is incorrectly set, fix it.
//			if ssp.ExpiredAt != math.MaxInt64 {
//				ssp.ExpiredAt = math.MaxInt64
//				err = se.SaveServiceGCSafePoint(ssp)
//				if err != nil {
//					return nil, errors.Trace(err)
//				}
//			}
//		}
//
//		if ssp.ExpiredAt < now.Unix() {
//			if err := se.Remove(key); err != nil {
//				log.Error("failed to remove expired service safepoint", errs.ZapError(err))
//			}
//			continue
//		}
//		if ssp.SafePoint < min.SafePoint {
//			min = ssp
//		}
//	}
//
//	if min.SafePoint == math.MaxUint64 {
//		// There's no valid safepoints and we have no way to recover it. Just set gc_worker to 0.
//		log.Info("there are no valid service safepoints. init gc_worker's service safepoint to 0")
//		return se.initServiceGCSafePointForGCWorker(0)
//	}
//
//	if !hasGCWorker {
//		// If there exists some service safepoints but gc_worker is missing, init it with the min value among all
//		// safepoints (including expired ones)
//		return se.initServiceGCSafePointForGCWorker(min.SafePoint)
//	}
//
//	return min, nil
//}
//
//func (se *StorageEndpoint) initServiceGCSafePointForGCWorker(initialValue uint64) (*ServiceSafePoint, error) {
//	ssp := &ServiceSafePoint{
//		ServiceID: keypath.GCWorkerServiceSafePointID,
//		SafePoint: initialValue,
//		ExpiredAt: math.MaxInt64,
//	}
//	if err := se.SaveServiceGCSafePoint(ssp); err != nil {
//		return nil, err
//	}
//	return ssp, nil
//}

// CompatibleLoadAllServiceGCSafePoints returns all services GC safe points with their etcd key.
func (p GCStateProvider) CompatibleLoadAllServiceGCSafePoints() ([]string, []*ServiceSafePoint, error) {
	prefix := keypath.GCBarrierPrefix()
	keys, ssps, err := loadJSONByPrefix[*ServiceSafePoint](p.storage, prefix, 0)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return []string{}, []*ServiceSafePoint{}, nil
	}

	return keys, ssps, nil
}

//// SaveServiceGCSafePoint saves a GC safepoint for the service
//func (se *StorageEndpoint) SaveServiceGCSafePoint(ssp *ServiceSafePoint) error {
//	if ssp.ServiceID == "" {
//		return errors.New("service id of service safepoint cannot be empty")
//	}
//
//	if ssp.ServiceID == keypath.GCWorkerServiceSafePointID && ssp.ExpiredAt != math.MaxInt64 {
//		return errors.New("TTL of gc_worker's service safe point must be infinity")
//	}
//
//	return se.saveJSON(keypath.GCSafePointServicePath(ssp.ServiceID), ssp)
//}
//
//// RemoveServiceGCSafePoint removes a GC safepoint for the service
//func (se *StorageEndpoint) RemoveServiceGCSafePoint(serviceID string) error {
//	if serviceID == keypath.GCWorkerServiceSafePointID {
//		return errors.New("cannot remove service safe point of gc_worker")
//	}
//	key := keypath.GCSafePointServicePath(serviceID)
//	return se.Remove(key)
//}

func (wb *GCStateWriteBatch) writeJson(key string, data any) error {
	value, err := json.Marshal(data)
	if err != nil {
		return errs.ErrJSONMarshal.Wrap(err).GenWithStackByArgs()
	}
	wb.ops = append(wb.ops, kv.RawTxnOp{
		Key:    key,
		OpType: kv.RawTxnOpPut,
		Value:  string(value),
	})
	return nil
}

func (wb *GCStateWriteBatch) SetGCSafePoint(keyspaceID uint32, gcSafePoint uint64) error {
	if keyspaceID == constant.NullKeyspaceID {
		return wb.setGlobalGCSafePoint(gcSafePoint)
	}
	return wb.setKeyspaceGCSafePoint(keyspaceID, gcSafePoint)
}

func (wb *GCStateWriteBatch) setGlobalGCSafePoint(gcSafePoint uint64) error {
	value := strconv.FormatUint(gcSafePoint, 16)
	wb.ops = append(wb.ops, kv.RawTxnOp{
		Key:    keypath.GCSafePointPath(),
		OpType: kv.RawTxnOpPut,
		Value:  value,
	})
	return nil
}

func (wb *GCStateWriteBatch) setKeyspaceGCSafePoint(keyspaceID uint32, gcSafePoint uint64) error {
	key := keypath.KeyspaceGCSafePointPath(keyspaceID)
	return wb.writeJson(key, keyspaceGCSafePoint{
		KeyspaceID: keyspaceID,
		SafePoint:  gcSafePoint,
	})
}

func (wb *GCStateWriteBatch) SetTxnSafePoint(keyspaceID uint32, txnSafePoint uint64) error {
	key := keypath.TxnSafePointPath()
	if keyspaceID != constant.NullKeyspaceID {
		key = keypath.KeyspaceTxnSafePointPath(keyspaceID)
	}
	value := strconv.FormatUint(txnSafePoint, 10)
	wb.ops = append(wb.ops, kv.RawTxnOp{
		Key:    key,
		OpType: kv.RawTxnOpPut,
		Value:  value,
	})
	return nil
}

func (wb *GCStateWriteBatch) SetGCBarrier(keyspaceID uint32, newGCBarrier *GCBarrier) error {
	prefix := keypath.GCBarrierPrefix()
	if keyspaceID != constant.NullKeyspaceID {
		prefix = keypath.KeyspaceGCBarrierPrefix(keyspaceID)
	}
	key := path.Join(prefix, newGCBarrier.BarrierID)
	return wb.writeJson(key, newGCBarrier.toServiceSafePoint(keyspaceID))
}

func (wb *GCStateWriteBatch) DeleteGCBarrier(keyspaceID uint32, barrierID string) error {
	prefix := keypath.GCBarrierPrefix()
	if keyspaceID != constant.NullKeyspaceID {
		prefix = keypath.KeyspaceGCBarrierPrefix(keyspaceID)
	}
	key := path.Join(prefix, barrierID)
	wb.ops = append(wb.ops, kv.RawTxnOp{
		Key:    key,
		OpType: kv.RawTxnOpDelete,
	})
	return nil
}
