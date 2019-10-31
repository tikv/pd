// Copyright 2019 PingCAP, Inc.
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

package nodeutil

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"

	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/server"
	"github.com/zcalusic/sysinfo"
	"go.etcd.io/etcd/clientv3"
)

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	Port       uint   `json:"listening_port"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// StaticNodeInfo static node info
type StaticNodeInfo struct {
	Name string `json:"pd_name"`
	IP   string `json:"ip"`
	Port int    `json:"port"`

	CPUInfo   *CPUInfo    `json:"cpu_info"`
	MemSize   uint        `json:"mem_size"`
	DiskInfos []*DiskInfo `json:"disk_infos"`

	NetcardInfos []*NetcardInfo `json:"netcard_infos"`
}

// CPUInfo cpu info
type CPUInfo struct {
	CPUVendor    string  `json:"cpu_vendor"`
	CPUModel     string  `json:"cpu_model"`
	CPUFrequency float64 `json:"cpu_frequency"` // MHz
	CPUThread    uint    `json:"cpu_thread"`
}

// DiskInfo disk info
type DiskInfo struct {
	DiskType string `json:"disk_type"`
	DiskSize uint   `json:"disk_size"`
}

// NetcardInfo netcard info
type NetcardInfo struct {
	Name       string `json:"name"`
	Driver     string `json:"driver"`
	MACAddress string `json:"macaddress"`
	Port       string `json:"port"`
	Speed      uint   `json:"speed"` // device max supported speed in Mbps
}

// NetworkLatency network latency
type NetworkLatency struct {
	Source  string `json:"source"`
	Target  string `json:"target"`
	Latency string `json:"latency"`
	Name    string `json:"pd_name"`
}

// StatsInfo pd's real time stat info
type StatsInfo struct {
	IP   string `json:"ip"`
	Name string `json:"pd_name"`

	CPUUsage float64     `json:"cpu_usage"`
	MemUsage float64     `json:"mem_usage"`
	NetUsage []*NetUsage `json:"net_usage"`
}

// NetUsage pd's net usage
type NetUsage struct {
	Name      string `json:"name"`
	BytesSent uint64 `json:"bytesSent"`
	BytesRecv uint64 `json:"bytesRecv"`
}

// GetStaticStatsInfo get static stats info
func GetStaticStatsInfo(svr *server.Server) (interface{}, error) {
	currNodeIP, port, err := getIPPort(svr.GetAddr())
	if err != nil {
		return nil, err
	}
	var si sysinfo.SysInfo
	// TODO: may be we can impl get cpu, disk, netcard sys info.
	si.GetSysInfo()
	cpuInfo := &CPUInfo{
		CPUVendor:    si.CPU.Vendor,
		CPUModel:     si.CPU.Model,
		CPUFrequency: si.CPU.Speed,
		CPUThread:    si.CPU.Threads,
	}
	diskInfos := make([]*DiskInfo, 0, len(si.Storage))
	for _, v := range si.Storage {
		diskInfos = append(diskInfos, &DiskInfo{
			DiskType: v.Driver,
			DiskSize: v.Size,
		})
	}
	netcardInfos := make([]*NetcardInfo, 0, len(si.Network))
	for _, v := range si.Network {
		netcardInfos = append(netcardInfos, &NetcardInfo{
			Name:       v.Name,
			Driver:     v.Driver,
			MACAddress: v.MACAddress,
			Port:       v.Port,
			Speed:      v.Speed,
		})
	}
	return &StaticNodeInfo{
		Name:         svr.Name(),
		IP:           currNodeIP,
		Port:         port,
		CPUInfo:      cpuInfo,
		MemSize:      si.Memory.Size,
		DiskInfos:    diskInfos,
		NetcardInfos: netcardInfos,
	}, nil
}

// GetCurrStatsInfo get current stats info
func GetCurrStatsInfo(svr *server.Server) (interface{}, error) {
	cpuPercent, err := GetCPUUsage()
	if err != nil {
		return nil, err
	}
	memPercent, err := GetMemUsage()
	if err != nil {
		return nil, err
	}
	netInfos, err := GetNetInfo()
	if err != nil {
		return nil, err
	}
	netUsages := make([]*NetUsage, 0, len(netInfos))
	for _, v := range netInfos {
		netUsages = append(netUsages, &NetUsage{
			Name:      v.Name,
			BytesSent: v.BytesSent,
			BytesRecv: v.BytesRecv,
		})
	}
	currNodeIP, _, err := getIPPort(svr.GetAddr())
	if err != nil {
		return nil, err
	}
	return &StatsInfo{
		IP:       currNodeIP,
		Name:     svr.Name(),
		CPUUsage: cpuPercent,
		MemUsage: memPercent,
		NetUsage: netUsages,
	}, nil
}

// GetNetworkLatency get network latency
func GetNetworkLatency(svr *server.Server) (interface{}, error) {
	currNodeIP, _, err := getIPPort(svr.GetAddr())
	if err != nil {
		return "", nil
	}
	allNodeIPs := getAllTargetNodeIPs(svr)
	nls := make([]*NetworkLatency, 0, len(allNodeIPs))
	for _, ip := range allNodeIPs {
		if ip == currNodeIP {
			continue
		}
		pingLatency, err := GetNetLatency(ip)
		if err != nil {
			log.Println("get target latency failed, error:", err)
			continue
		}

		nl := &NetworkLatency{
			Source:  currNodeIP,
			Target:  ip,
			Latency: string(pingLatency),
			Name:    svr.Name(),
		}
		nls = append(nls, nl)
	}
	return nls, nil
}

// getAllTargetNodeIPs get all node ip
func getAllTargetNodeIPs(svr *server.Server) []string {
	existNodeHost := map[string]struct{}{}
	var nodeIPs []string
	nodes := getPDNode(svr, existNodeHost)
	if len(nodes) > 0 {
		nodeIPs = append(nodeIPs, nodes...)
	}
	nodes = getTiKVNode(svr, existNodeHost)
	if len(nodes) > 0 {
		nodeIPs = append(nodeIPs, nodes...)
	}
	nodes = getTiDBNode(svr, existNodeHost)
	if len(nodes) > 0 {
		nodeIPs = append(nodeIPs, nodes...)
	}

	return nodeIPs
}

func getTiKVNode(svr *server.Server, existNodeHost map[string]struct{}) []string {
	nodeIPs := make([]string, 3)
	cluster := svr.GetRaftCluster()
	if cluster != nil {
		stores := cluster.GetMetaStores()
		for _, s := range stores {
			storeID := s.GetId()
			store := cluster.GetStore(storeID)
			if store == nil {
				continue
			}
			storeAddr := store.GetAddress()
			hostIP, _, err := getIPPort(storeAddr)
			if err != nil {
				log.Println("get storeAddr host failed, storeAddr: ", storeAddr)
				continue
			}
			if len(hostIP) > 0 && !checkIsExists(hostIP, existNodeHost) {
				nodeIPs = append(nodeIPs, hostIP)
			}
		}
	} else {
		log.Println("[stats_info] get raft cluster is nil.")
	}
	return nodeIPs
}

func getPDNode(svr *server.Server, existNodeHost map[string]struct{}) []string {
	nodeIPs := make([]string, 3)
	members, err := server.GetMembers(svr.GetClient())
	if err != nil {
		log.Println("get pd members failed, error: ", err)
	}
	for _, v := range members {
		clientUrls := v.GetClientUrls()
		for _, vv := range clientUrls {
			if len(vv) == 0 {
				continue
			}
			hostIP, _, err := getIPPort(vv)
			if err != nil {
				log.Println("get pd host failed, storeAddr: ", vv)
				continue
			}
			if len(hostIP) > 0 && !checkIsExists(hostIP, existNodeHost) {
				nodeIPs = append(nodeIPs, hostIP)
			}
		}
	}
	return nodeIPs
}

func getTiDBNode(svr *server.Server, existNodeHost map[string]struct{}) []string {
	nodeIPs := make([]string, 3)
	resp, err := etcdutil.EtcdKVGet(svr.GetClient(), "/tidb/server/info", clientv3.WithPrefix())
	if err != nil {
		log.Println("get tidb node failed, error:", err)
	}
	if len(resp.Kvs) > 0 {
		for _, v := range resp.Kvs {
			if len(string(v.Value)) == 0 {
				continue
			}
			info := &ServerInfo{}
			err = json.Unmarshal(v.Value, info)
			if err != nil {
				log.Println("unmarshal failed, error:", err)
				continue
			}
			if len(info.IP) > 0 && !checkIsExists(info.IP, existNodeHost) {
				nodeIPs = append(nodeIPs, info.IP)
			}
		}
	}
	return nodeIPs
}

func checkIsExists(hostIP string, existNodeHost map[string]struct{}) bool {
	if _, ok := existNodeHost[hostIP]; !ok {
		existNodeHost[hostIP] = struct{}{}
		return false
	}
	return true
}

func getIPPort(addr string) (string, int, error) {
	if len(addr) == 0 {
		return "", 0, nil
	}
	urlRet, err := url.Parse(addr)
	if err != nil {
		return "", 0, err
	}
	ip := urlRet.Hostname()
	portStr := urlRet.Port()
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return ip, port, nil
}
