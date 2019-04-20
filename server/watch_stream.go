package server

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"io"
	"log"
)

type watchStreams struct {
	proxyClient *clientv3.Client
	watcher     map[int64]clientv3.Watcher
}

type watchServer struct {
	stream pdpb.PD_WatchServer
	closed int32
}

func NewWatchStreams(client *clientv3.Client) *watchStreams {
	return &watchStreams{
		proxyClient: client,
		watcher:     make(map[int64]clientv3.Watcher),
	}
}

func (s *Server) Watch(ws pdpb.PD_WatchServer) error {
	//get the info from client
	in, err := ws.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Printf("failed to recv: %v", err)
		return err
	}
	fmt.Println(in.WatchId)

	ctx, cancle := context.WithCancel(s.serverLoopCtx)
	defer cancle()

	resp, err := get(s.client, string(in.Key))
	if err != nil || resp == nil {
		return nil
	}

	s.watchStreams.watcher[in.WatchId] = clientv3.NewWatcher(s.watchStreams.proxyClient)
	rch := s.watchStreams.watcher[in.WatchId].Watch(ctx,
		string(in.Key),
		clientv3.WithRev(resp.Kvs[0].Version))

	for wresp := range rch {
		wsResp := &pdpb.WatchResponse{}
		wsResp.CompactRevision = wresp.CompactRevision
		for i := 0; i < len(wresp.Events); i++ {
			wsResp.Events = append(wsResp.Events, &pdpb.Event{
				Kv: &pdpb.KeyValue{
					Value:          wresp.Events[i].Kv.Value,
					Key:            wresp.Events[i].Kv.Key,
					Version:        wresp.Events[i].Kv.Version,
					CreateRevision: wresp.Events[i].Kv.CreateRevision,
					ModRevision:    wresp.Events[i].Kv.ModRevision,
					Lease:          wresp.Events[i].Kv.Lease,
				},
			})
			if wresp.Events[i].PrevKv != nil {
				fmt.Println("prekv not nil")
				wsResp.Events[i].PrevKv = &pdpb.KeyValue{
					Value:          wresp.Events[i].PrevKv.Value,
					Key:            wresp.Events[i].PrevKv.Key,
					Version:        wresp.Events[i].PrevKv.Version,
					CreateRevision: wresp.Events[i].PrevKv.CreateRevision,
					ModRevision:    wresp.Events[i].PrevKv.ModRevision,
					Lease:          wresp.Events[i].PrevKv.Lease,
				}
			}
			switch wresp.Events[i].Type {
			case mvccpb.DELETE:
				wsResp.Events[i].Type = pdpb.Event_DELETE
			case mvccpb.PUT:
				wsResp.Events[i].Type = pdpb.Event_PUT
			default:
				break
			}
		}
		ws.Send(wsResp)
	}

	return nil
}
