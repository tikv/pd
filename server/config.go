package server

import "time"

const (
	defaultRootPath        = "pd"
	defaultLeaderLease     = 3
	defaultTsoSaveInterval = 2000
	defaultNextRetryDelay  = time.Second
	defaultMaxPeerNumber   = uint32(3)
)

// Config is the pd server configuration.
type Config struct {
	// Server listening address.
	Addr string

	// Server advertise listening address for outer client communication.
	// If not set, using default Addr instead.
	AdvertiseAddr string

	// Etcd endpoints
	EtcdAddrs []string

	// RootPath in Etcd as the prefix for all keys. If not set, use default "pd".
	RootPath string

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd onlys support seoncds TTL, so here is second too.
	LeaderLease int64

	// TsoSaveInterval is the interval time (ms) to save timestamp.
	// When the leader begins to run, it first loads the saved timestamp from etcd, e.g, T1,
	// and the leader must guarantee that the next timestamp must be > T1 + 2 * TsoSaveInterval.
	TsoSaveInterval int64

	// ClusterName is the cluster Name communicating with other services.
	ClusterName string

	// MaxPeerNumber for a region. default is 3.
	MaxPeerNumber uint32

	// Only test can change it.
	nextRetryDelay time.Duration
}

func (c *Config) adjust() {
	if len(c.RootPath) == 0 {
		c.RootPath = defaultRootPath
	}
	// TODO: Maybe we should do more check for root path, only allow letters?

	if c.LeaderLease <= 0 {
		c.LeaderLease = defaultLeaderLease
	}

	if c.TsoSaveInterval <= 0 {
		c.TsoSaveInterval = defaultTsoSaveInterval
	}

	if c.nextRetryDelay == 0 {
		c.nextRetryDelay = defaultNextRetryDelay
	}

	if c.MaxPeerNumber == 0 {
		c.MaxPeerNumber = defaultMaxPeerNumber
	}
}
