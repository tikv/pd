package server

import (
	"context"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"time"
)

type WatchChan chan pdpb.WatchResponse

// type + watch_id = observerid
// type is used to mark the type of component,
// it tell us what kind of component is trying to observer the key in pd
type ObserverID string
type closed int64

// If many observer is watching the same key, considering performance,
// we'd better create a watcher  and notify all them
// when watcher received a event
type watchKey string

type WatchProxyServer struct {
	stopCtx              context.Context
	stopCancel           context.CancelFunc
	ProxyClient          *clientv3.Client
	Watchers             map[watchKey]Watcher
	closedAllWatcherChan chan closed
}

type Watcher struct { // one watchUnit correspond to a key
	watchChan    WatchChan
	watcher      clientv3.Watcher
	watchStreams map[ObserverID]watchStream
	closedChan   chan closed
}

type watchStream struct {
	stream pdpb.PD_WatchServer
}

func NewWatchProxyServer(client *clientv3.Client) *WatchProxyServer {
	ctx, cancel := context.WithCancel(context.Background())
	watchProxy := &WatchProxyServer{
		stopCtx:              ctx,
		stopCancel:           cancel,
		ProxyClient:          client,
		Watchers:             make(map[watchKey]Watcher),
		closedAllWatcherChan: make(chan closed),
	}
	return watchProxy
}

func (s *Server) runWatchProxy() {
	leaderAliveTicker := time.NewTicker(time.Duration(s.cfg.LeaderLease) * time.Second)
	defer leaderAliveTicker.Stop()
	for {
		select {
		case <-s.watchProxyServer.stopCtx.Done():
			return
		case <-leaderAliveTicker.C:
			if !s.IsLeader() {
				s.watchProxyServer.closedAllWatcherChan <- closed(1)
			}
		case <-s.watchProxyServer.closedAllWatcherChan:
			for key, _ := range s.watchProxyServer.Watchers {
				s.watchProxyServer.Watchers[key].closedChan <- closed(1)
			}
		}
	}
}

func (wps *WatchProxyServer) notifyAllObservers(key watchKey) {
	for {
		select {
		// we heard a event from etcd and notify all observer
		case watchResp := <-wps.Watchers[key].watchChan:
			for _, ws := range wps.Watchers[key].watchStreams {
				ws.stream.Send(&watchResp)
			}
		case <-wps.Watchers[key].closedChan:
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
