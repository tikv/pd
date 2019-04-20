package server

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/clientv3"
	"io"
	"log"
	"strconv"
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

	resp, err := get(s.client, "/pd/"+strconv.FormatUint(s.clusterID, 10)+"/config")
	if err != nil || resp == nil {
		return nil
	}

	s.watchStreams.watcher[in.WatchId] = clientv3.NewWatcher(s.watchStreams.proxyClient)
	fmt.Println("startRevision")
	rch := s.watchStreams.watcher[in.WatchId].Watch(ctx,
		"/pd/"+strconv.FormatUint(s.clusterID, 10)+"/config",
		clientv3.WithRev(resp.Kvs[0].Version))

	for wresp := range rch {
		wsResp := &pdpb.WatchResponse{}
		wsResp.CompactRevision = wresp.CompactRevision
		fmt.Println("one watch response start")
		for i := 0; i < len(wresp.Events); i++ {
			wsResp.Events = append(wsResp.Events, &pdpb.Event{
				Kv: &pdpb.KeyValue{
					Value: wresp.Events[i].Kv.Value,
					Key:   wresp.Events[i].Kv.Key,
				},
			})
			fmt.Println(wresp.Events[i].Kv.Version)
		}
		fmt.Println("one watch response start")
		ws.Send(wsResp)
	}

	return nil
}
