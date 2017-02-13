// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/pkg/capnslog"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/msgpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/util"
	"github.com/pingcap/pd/pkg/etcdutil"
	"golang.org/x/net/context"
)

const (
	requestTimeout  = etcdutil.DefaultRequestTimeout
	slowRequestTime = etcdutil.DefaultSlowRequestTime
)

// Version information.
var (
	PDBuildTS = "None"
	PDGitHash = "None"
)

// LogPDInfo prints the PD version information.
func LogPDInfo() {
	log.Infof("Welcome to Placement Driver (PD).")
	log.Infof("Version:")
	log.Infof("Git Commit Hash: %s", PDGitHash)
	log.Infof("UTC Build Time:  %s", PDBuildTS)
}

// PrintPDInfo prints the PD version information without log info.
func PrintPDInfo() {
	fmt.Println("Git Commit Hash:", PDGitHash)
	fmt.Println("UTC Build Time: ", PDBuildTS)
}

// A helper function to get value with key from etcd.
// TODO: return the value revision for outer use.
func getValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := kvGet(c, key, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

// Return boolean to indicate whether the key exists or not.
// TODO: return the value revision for outer use.
func getProtoMsg(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, error) {
	value, err := getValue(c, key, opts...)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == nil {
		return false, nil
	}

	if err = proto.Unmarshal(value, msg); err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
}

func initOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), requestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := uint64ToBytes(clusterID)

	// Multiple PDs may try to init the cluster ID at the same time.
	// Only one PD can commit this transaction, then other PDs can get
	// the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.Errorf("txn returns empty response: %v", resp)
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.Errorf("txn returns invalid range response: %v", resp)
	}

	return bytesToUint64(response.Kvs[0].Value)
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Now().Sub(start)
	if cost > slowRequestTime {
		log.Warnf("txn runs too slow, resp: %v, err: %v, cost: %s", resp, err, cost)
	}
	label := "success"
	if err != nil {
		label = "failed"
	}
	txnCounter.WithLabelValues(label).Inc()
	txnDuration.WithLabelValues(label).Observe(cost.Seconds())

	return resp, errors.Trace(err)
}

func sliceClone(strs []string) []string {
	data := make([]string, 0, len(strs))
	for _, str := range strs {
		data = append(data, str)
	}

	return data
}

func rpcConnect(addr string) (net.Conn, error) {
	req, err := http.NewRequest("GET", pdRPCPrefix, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	urls, err := ParseUrls(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, url := range urls {
		var conn net.Conn
		switch url.Scheme {
		// used in tests
		case "unix", "unixs":
			conn, err = net.Dial("unix", url.Host)
		default:
			conn, err = net.Dial("tcp", url.Host)
		}

		if err != nil {
			continue
		}
		err = req.Write(conn)
		if err != nil {
			conn.Close()
			continue
		}
		return conn, nil
	}

	return nil, errors.Errorf("connect to %s failed", addr)
}

func rpcCall(conn net.Conn, reqID uint64, request *pdpb.Request) (*pdpb.Response, error) {
	req := &msgpb.Message{
		MsgType: msgpb.MessageType_PdReq,
		PdReq:   request,
	}
	if err := util.WriteMessage(conn, reqID, req); err != nil {
		return nil, errors.Trace(err)
	}
	resp := &msgpb.Message{}
	respID, err := util.ReadMessage(conn, resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if respID != reqID {
		return nil, errors.Errorf("message id mismatch: reqID %d respID %d", reqID, respID)
	}
	return resp.GetPdResp(), nil
}

// RPCRequest sends a request to addr and waits for the response.
// Export for API test.
func RPCRequest(addr string, reqID uint64, request *pdpb.Request) (*pdpb.Response, error) {
	conn, err := rpcConnect(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rpcCall(conn, reqID, request)
}

type redirectFormatter struct{}

// Format turns capnslog logs to ngaut logs.
// TODO: remove ngaut log caller stack, "util.go:xxx"
func (rf *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	if pkg != "" {
		pkg = fmt.Sprint(pkg, ": ")
	}

	logStr := fmt.Sprint(level.Char(), " | ", pkg, entries)

	switch level {
	case capnslog.CRITICAL:
		log.Fatalf(logStr)
	case capnslog.ERROR:
		log.Errorf(logStr)
	case capnslog.WARNING:
		log.Warningf(logStr)
	case capnslog.NOTICE:
		log.Infof(logStr)
	case capnslog.INFO:
		log.Infof(logStr)
	case capnslog.DEBUG:
		log.Debugf(logStr)
	case capnslog.TRACE:
		log.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (rf *redirectFormatter) Flush() {}

// setLogOutput sets output path for all logs.
func setLogOutput(path string) error {
	// PD log.
	log.SetOutputByName(path)
	log.SetRotateByDay()

	// ETCD log.
	capnslog.SetFormatter(&redirectFormatter{})

	return nil
}

// InitLogger initalizes PD's logger.
func InitLogger(cfg *Config) error {
	log.SetLevelByString(cfg.LogLevel)
	log.SetHighlighting(false)

	// Force redirect etcd log to stderr.
	if len(cfg.LogFile) == 0 {
		capnslog.SetFormatter(capnslog.NewPrettyFormatter(os.Stderr, false))
		return nil
	}

	err := setLogOutput(cfg.LogFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// GetPDMembers return a slice of PDMembers.
func GetPDMembers(etcdClient *clientv3.Client) ([]*pdpb.PDMember, error) {
	listResp, err := etcdutil.ListEtcdMembers(etcdClient)
	if err != nil {
		return nil, errors.Trace(err)
	}

	members := make([]*pdpb.PDMember, 0, len(listResp.Members))
	for _, m := range listResp.Members {
		info := &pdpb.PDMember{
			Name:       &m.Name,
			ClientUrls: m.ClientURLs,
			PeerUrls:   m.PeerURLs,
		}
		members = append(members, info)
	}

	return members, nil
}

func roundUint64(value, min, max uint64) uint64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
