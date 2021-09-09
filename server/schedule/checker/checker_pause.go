package checker

import (
	"sync/atomic"
	"time"
)

// CheckerPause sets and stores delay time in checkers.
type CheckerPause struct {
	delayUntil int64
}

func (c *CheckerPause) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&c.delayUntil)
	return time.Now().Unix() < delayUntil
}

func (c *CheckerPause) PauseOrResume(t int64) {
	delayUntil := time.Now().Unix() + t
	atomic.StoreInt64(&c.delayUntil, delayUntil)
}
