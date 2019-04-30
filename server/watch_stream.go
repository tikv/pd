package server

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"io"
	"log"
	"strconv"
	"time"
)

type WatchChan chan pdpb.WatchResponse

type ObserverID string

type WatchProxyServer struct {
	stopCtx         context.Context
	stopCancel      context.CancelFunc
	ProxyClient     *clientv3.Client
	Watchers        map[string]Watcher
	stopWatcherChan chan int64
}

type Watcher struct { // one watchUnit correspond to a key
	watchChan    WatchChan
	watcher      clientv3.Watcher
	watchStreams map[ObserverID]watchStream
	closedChan   chan int64
}

type watchStream struct {
	stream pdpb.PD_WatchServer
}

func NewWatchProxyServer(client *clientv3.Client) *WatchProxyServer {
	ctx, cancel := context.WithCancel(context.Background())
	watchProxy := &WatchProxyServer{
		stopCtx:         ctx,
		stopCancel:      cancel,
		ProxyClient:     client,
		Watchers:        make(map[string]Watcher),
		stopWatcherChan: make(chan int64),
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
				s.watchProxyServer.stopWatcherChan <- 1
			}
		case <-s.watchProxyServer.stopWatcherChan:
			for key, _ := range s.watchProxyServer.Watchers {
				s.watchProxyServer.Watchers[key].closedChan <- 1
			}
		}
	}
}

func (s *Server) Watch(stream pdpb.PD_WatchServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//get the info from client
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}

	if err = s.validateRequest(in.GetHeader()); err != nil {
		return err
	}

	if err != nil {
		log.Printf("failed to recv: %v", err)
		return err
	}

	resp, err := get(s.client, string(in.Key))
	if err != nil || resp == nil {
		return err
	}

	if _, ok := s.watchProxyServer.Watchers[string(in.Key)]; !ok {
		s.watchProxyServer.Watchers[string(in.Key)] = Watcher{
			watchChan:    make(chan pdpb.WatchResponse),
			watcher:      clientv3.NewWatcher(s.watchProxyServer.ProxyClient),
			watchStreams: make(map[ObserverID]watchStream),
			closedChan:   make(chan int64),
		}
	}
	observerID := ObserverID("region-" + strconv.FormatInt(in.WatchId, 10))

	//create or update stream
	s.watchProxyServer.Watchers[string(in.Key)].watchStreams[observerID] = watchStream{stream: stream}

	if len(s.watchProxyServer.Watchers[string(in.Key)].watchStreams) == 1 {
		go s.watchRoutine(ctx, string(in.Key), resp.Kvs[len(resp.Kvs)-1].Version)
		go s.notifyObserver(string(in.Key))
	} else {
		s.watchProxyServer.Watchers[string(in.Key)].
			watchStreams[observerID].stream.Send(getRespConvert(*resp))
	}

	select {
	case <-s.watchProxyServer.Watchers[string(in.Key)].closedChan:
		return err
	}

	return nil
}

func (s *Server) watchRoutine(ctx context.Context, key string, revision int64) {
	rch := s.watchProxyServer.Watchers[key].watcher.Watch(ctx, key, clientv3.WithRev(revision))

	for wresp := range rch {
		if !s.IsLeader() {
			s.watchProxyServer.stopWatcherChan <- 1
		} else if wresp.Canceled {
			fmt.Println(key, " is canceld")
			s.watchProxyServer.Watchers[key].closedChan <- 1
		} else {
			s.watchProxyServer.Watchers[key].watchChan <- *watchEventConvert(wresp)
		}
	}
}

func (s *Server) notifyObserver(key string){
	for {
		select {
		case watchResp := <-s.watchProxyServer.Watchers[key].watchChan:
			for _, ws := range s.watchProxyServer.Watchers[key].watchStreams {
				ws.stream.Send(&watchResp)
			}
		case <-s.watchProxyServer.Watchers[key].closedChan:
			return
		case <-s.watchProxyServer.stopCtx.Done():
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
