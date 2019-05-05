package server

import (
	"context"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type watchChan chan pdpb.WatchResponse

// type + watch_id = observerid
// type is used to mark the type of component,
// it tell us what kind of component is trying to observer the key in pd
type observerID string
type closed int64

// If many observer is watching the same key, considering performance,
// we'd better create a watcher  and notify all them
// when watcher received a event
type watchKey string

// WatchProxyServer manages all Watcher
type WatchProxyServer struct {
	stopCtx              context.Context
	stopCancel           context.CancelFunc
	proxyClient          *clientv3.Client
	watchers             map[watchKey]Watcher
	closedAllWatcherChan chan closed
}

// Watcher is used to watch a specify key from etcd and notify all observer
type Watcher struct {
	watchChan    watchChan
	watcher      clientv3.Watcher
	watchStreams map[observerID]watchStream
	closedChan   chan closed
}

type watchStream struct {
	stream pdpb.PD_WatchServer
}

// NewWatchProxyServer starts watchProxyServer for watch_service
func NewWatchProxyServer(client *clientv3.Client) *WatchProxyServer {
	ctx, cancel := context.WithCancel(context.Background())
	watchProxy := &WatchProxyServer{
		stopCtx:              ctx,
		stopCancel:           cancel,
		proxyClient:          client,
		watchers:             make(map[watchKey]Watcher),
		closedAllWatcherChan: make(chan closed),
	}
	return watchProxy
}

func (s *Server) runWatchProxy(wps *WatchProxyServer, leaderLease int64) {
	go func() {
		leaderAliveTicker := time.NewTicker(time.Duration(leaderLease) * time.Second)
	DONE:
		for {
			select {
			case <-wps.stopCtx.Done():
				leaderAliveTicker.Stop()
				break DONE
			case <-leaderAliveTicker.C:
				if !s.IsLeader() {
					wps.closedAllWatcherChan <- closed(1)
				}
			case <-wps.closedAllWatcherChan:
				for key := range wps.watchers {
					wps.watchers[key].closedChan <- closed(1)
				}
			}
		}
	}()

}

func (wps *WatchProxyServer) notifyAllObservers(key watchKey) {
	for {
		select {
		// we heard a event from etcd and notify all observer
		case watchResp := <-wps.watchers[key].watchChan:
			for _, ws := range wps.watchers[key].watchStreams {
				ws.stream.Send(&watchResp)
			}
		case <-wps.watchers[key].closedChan:
			return
		case <-wps.stopCtx.Done():
			return
		}
	}
}

func getRespConvert(getRsp clientv3.GetResponse) *pdpb.WatchResponse {
	wsResp := &pdpb.WatchResponse{}
	for i := 0; i < len(getRsp.Kvs); i++ {
		wsResp.Events = append(wsResp.Events, &pdpb.Event{
			Kv: &pdpb.KeyValue{
				Value:          getRsp.Kvs[i].Value,
				Key:            getRsp.Kvs[i].Key,
				Version:        getRsp.Kvs[i].Version,
				CreateRevision: getRsp.Kvs[i].CreateRevision,
				ModRevision:    getRsp.Kvs[i].ModRevision,
				Lease:          getRsp.Kvs[i].Lease,
			},
		})
	}
	return wsResp
}

func watchEventConvert(wrsp clientv3.WatchResponse) *pdpb.WatchResponse {
	wsResp := &pdpb.WatchResponse{}
	wsResp.CompactRevision = wrsp.CompactRevision
	for i := 0; i < len(wrsp.Events); i++ {
		wsResp.Events = append(wsResp.Events, &pdpb.Event{
			Kv: &pdpb.KeyValue{
				Value:          wrsp.Events[i].Kv.Value,
				Key:            wrsp.Events[i].Kv.Key,
				Version:        wrsp.Events[i].Kv.Version,
				CreateRevision: wrsp.Events[i].Kv.CreateRevision,
				ModRevision:    wrsp.Events[i].Kv.ModRevision,
				Lease:          wrsp.Events[i].Kv.Lease,
			},
		})
		if wrsp.Events[i].PrevKv != nil {
			wsResp.Events[i].PrevKv = &pdpb.KeyValue{
				Value:          wrsp.Events[i].PrevKv.Value,
				Key:            wrsp.Events[i].PrevKv.Key,
				Version:        wrsp.Events[i].PrevKv.Version,
				CreateRevision: wrsp.Events[i].PrevKv.CreateRevision,
				ModRevision:    wrsp.Events[i].PrevKv.ModRevision,
				Lease:          wrsp.Events[i].PrevKv.Lease,
			}
		}
		switch wrsp.Events[i].Type {
		case mvccpb.DELETE:
			wsResp.Events[i].Type = pdpb.Event_DELETE
		case mvccpb.PUT:
			wsResp.Events[i].Type = pdpb.Event_PUT
		default:
			break
		}
	}
	return wsResp
}
