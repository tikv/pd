package nodeutil

import (
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"os/user"
	"strconv"
	"strings"

	"github.com/zcalusic/sysinfo"
	// "github.com/zcalusic/sysinfo"
)

// StaticNodeInfo static node info
type StaticNodeInfo struct {
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
	CPUFrequency float64 `json:"cpu_frequency"`
	CPUThread    uint    `json:"cpu_thread"`
}

// DiskInfo disk info
type DiskInfo struct {
	DiskType string `json:"disk_type"`
	DiskSize uint   `json:"disk_size"`
}

// NetcardInfo netcard info
type NetcardInfo struct {
	Name       string `json:"name,omitempty"`
	Driver     string `json:"driver,omitempty"`
	MACAddress string `json:"macaddress,omitempty"`
	Port       string `json:"port,omitempty"`
	Speed      uint   `json:"speed,omitempty"` // device max supported speed in Mbps
}

// NetworkLatency network latency
type NetworkLatency struct {
	Source  string  `json:"source"`
	Target  string  `json:"target"`
	Latency Latency `json:"latency"`
}

// Latency latency
type Latency struct {
	Min float64 `json:"min"`
	Avg float64 `json:"avg"`
	Max float64 `json:"max"`
}

// GetStaticStatsInfo get static stats info
func GetStaticStatsInfo() string {
	err := checkUserPrivilege()
	if err != nil {
		log.Println("user privilege error:", err)
		return ""
	}
	var si sysinfo.SysInfo
	si.GetSysInfo()
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
	sni := &StaticNodeInfo{
		IP:   "",
		Port: 0,
		CPUInfo: &CPUInfo{
			CPUVendor:    si.CPU.Vendor,
			CPUModel:     si.CPU.Model,
			CPUFrequency: float64(si.CPU.Speed) / 1000,
			CPUThread:    si.CPU.Threads,
		},
		MemSize:      si.Memory.Size,
		DiskInfos:    diskInfos,
		NetcardInfos: netcardInfos,
	}
	ret, err := json.Marshal(sni)
	if err != nil {
		return ""
	}
	return string(ret)
}

func checkUserPrivilege() error {
	current, err := user.Current()
	if err != nil {
		return fmt.Errorf("requires superuser privilege, err: ", err)
	}

	if current.Uid != "0" {
		return fmt.Errorf("requires superuser privilege")
	}
	return nil
}

// GetStatsInfo get stats info
func GetStatsInfo() string {
	// TODO: use gopsutil
	return ""
}

// GetNetworkLatency get network latency
func GetNetworkLatency() string {
	currNode := getCurrentNode()
	allNodes := getAllTargetNode()
	nls := make([]*NetworkLatency, 0, len(allNodes))
	for _, v := range allNodes {
		if v == currNode {
			continue
		}
		out, _ := exec.Command("ping", v, "-c 1").Output()
		if strings.Contains(string(out), "Destination Host Unreachable") {
			log.Println("TANGO DOWN")
		} else {
			pingRet := strings.Split(string(out), "min/avg/max/stddev =")
			if len(pingRet) == 2 {
				pr := strings.TrimSpace(pingRet[1])
				mst := strings.Split(pr, "/")
				min, err := strconv.ParseFloat(strings.TrimSpace(mst[0]), 64)
				if err != nil {
					log.Println("error:", err, "mst[0]:", mst[0])
					continue
				}
				avg, err := strconv.ParseFloat(strings.TrimSpace(mst[1]), 64)
				if err != nil {
					log.Println("error:", err, "mst[1]:", mst[1])
					continue
				}
				max, err := strconv.ParseFloat(strings.TrimSpace(mst[2]), 64)
				if err != nil {
					log.Println("error:", err, "mst[2]:", mst[2])
					continue
				}
				nl := &NetworkLatency{
					Source: "localhost",
					Target: v,
					Latency: Latency{
						Min: min,
						Avg: avg,
						Max: max,
					},
				}
				nls = append(nls, nl)
			}
		}
	}

	ret, err := json.Marshal(nls)
	if err != nil {
		return ""
	}
	return string(ret)
}

// getAllTargetNode get all target node
func getAllTargetNode() []string {
	// 	form etcd load all target
	return []string{"192.168.221.18", "192.168.221.19"}
}

// getCurrentNode get current node
func getCurrentNode() string {
	// 	form etcd load all target
	return "192.168.221.18"
}
