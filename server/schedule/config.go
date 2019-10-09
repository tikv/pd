package schedule

type Config interface {
	LoadConfig(path string, maxReplicas int) bool
	GetStoreId(cluster Cluster) map[string][]uint64
	GetInterval() map[string]*TimeInterval
    IfConflict(maxReplicas int) bool
}
